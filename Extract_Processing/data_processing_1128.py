import pandas as pd
import os
import re
import json
import xml.etree.ElementTree as ET
from xml.dom import minidom
from datetime import datetime, timedelta
from pathlib import Path
import sys

def add_timezone_to_datetime(datetime_str):
    """KST 시간을 UTC+9 형식으로 변환하는 함수 (KST에서 9시간 빼서 UTC로 변환)"""
    try:
        # 날짜 파싱 (KST 기준)
        dt = datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%S')
        # KST에서 9시간 빼서 UTC로 변환
        utc_dt = dt - timedelta(hours=9)
        # UTC+9 형식으로 표시
        return utc_dt.strftime('%Y-%m-%d %H:%M:%S+09:00')
    except:
        try:
            # 다른 형식 시도
            dt = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
            # KST에서 9시간 빼서 UTC로 변환
            utc_dt = dt - timedelta(hours=9)
            return utc_dt.strftime('%Y-%m-%d %H:%M:%S+09:00')
        except:
            return datetime_str

def escape_xml_text(text):
    """XML 텍스트에서 특수문자 이스케이프"""
    if not text:
        return ""
    text = str(text)
    text = text.replace('&', '&amp;')
    text = text.replace('<', '&lt;')
    text = text.replace('>', '&gt;')
    text = text.replace('"', '&quot;')
    text = text.replace("'", '&apos;')
    return text

def fix_empty_tags(xml_string):
    """빈 태그들을 <tag></tag> 형태로 변환"""
    import re
    # self-closing 태그들을 찾아서 <tag></tag> 형태로 변환
    # 단, <tag/> 형태만 변환하고 <tag /> 형태는 그대로 둠
    xml_string = re.sub(r'<(\w+)([^>]*?)/>', r'<\1\2></\1>', xml_string)
    return xml_string

def remove_empty_lines(xml_string):
    """연속된 빈 줄을 제거하고, 태그 사이의 불필요한 빈 줄 제거"""
    import re
    # 줄을 분리하여 처리
    lines = xml_string.split('\n')
    cleaned_lines = []
    
    for i, line in enumerate(lines):
        # 공백만 있는 줄은 빈 줄로 간주
        stripped = line.strip()
        is_empty = len(stripped) == 0
        
        # 첫 번째 줄은 항상 포함 (XML 선언문)
        if i == 0:
            cleaned_lines.append(line)
            continue
        
        # 이전 줄 확인
        if cleaned_lines:
            prev_line = cleaned_lines[-1].strip()
            prev_is_tag = prev_line.startswith('<?') or (prev_line.startswith('<') and not prev_line.startswith('<!--'))
        else:
            prev_is_tag = False
        
        # 빈 줄인 경우
        if is_empty:
            # 이전 줄도 빈 줄이면 건너뜀 (연속된 빈 줄 제거)
            if cleaned_lines and cleaned_lines[-1].strip() == '':
                continue
            # 이전 줄이 태그면 건너뜀 (태그 다음 빈 줄 제거)
            if prev_is_tag:
                continue
        
        cleaned_lines.append(line)
    
    return '\n'.join(cleaned_lines)

def extract_filename_from_csv(file_name_str):
    """CSV의 file_name 열에서 실제 파일명 추출"""
    try:
        # JSON 문자열 파싱
        data = json.loads(file_name_str)
        return data.get('file_name', '')
    except:
        # JSON이 아닌 경우 그대로 반환
        return file_name_str

def load_json_data(json_file_path):
    """JSON 파일 로드"""
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"JSON 파일 로드 실패 {json_file_path}: {e}")
        return None

def convert_result_json_to_xml(result_json_data, image_id, image_name, source_json_data):
    """26640_result JSON 데이터를 XML 형식으로 변환"""
    
    # XML 루트 요소 생성
    image_elem = ET.Element('image')
    image_elem.set('id', str(image_id - 1))  # 이미지 순서는 0부터 시작
    image_elem.set('name', escape_xml_text(image_name))
    
    # 소스 JSON에서 이미지 크기 가져오기 (convertedImageInfo에서 가져옴)
    width = 0
    height = 0
    images = source_json_data.get('images', [])
    if images and len(images) > 0:
        converted_image_info = images[0].get('convertedImageInfo', {})
        width = converted_image_info.get('width', 0)
        height = converted_image_info.get('height', 0)
    image_elem.set('width', str(width))
    image_elem.set('height', str(height))
    
    # results에서 데이터 처리
    results = result_json_data.get('results', [])
    if not results or len(results) < 2:
        return image_elem
    
    # 첫 번째 행은 헤더, 두 번째 행은 데이터
    data_row = results[1] if len(results) > 1 else []
    
    z_order = 0  # 오브젝트 순서는 0부터 시작
    
    # @name_5OJYEV 값이 있는 경우만 처리
    if len(data_row) > 0 and data_row[0] is not None:
        annotations = data_row[0]
        
        if isinstance(annotations, dict) and 'name_5OJYEV' in annotations:
            annotation_list = annotations['name_5OJYEV']
            
            for annotation in annotation_list:
                points = []
                annotation_type = annotation.get('annotation', '')
                
                # BOX 타입 처리
                if annotation_type == 'BOX':
                    coords = annotation.get('coords', {})
                    if coords:
                        # BBOX 좌표 순서: tl > tr > br > bl
                        tl = coords.get('tl', {})
                        tr = coords.get('tr', {})
                        br = coords.get('br', {})
                        bl = coords.get('bl', {})
                        
                        if tl and tr and br and bl:
                            # 소수점 네자리까지 반올림하여 points 생성 (00 포함)
                            points.append(f"{round(tl.get('x', 0), 4):.4f},{round(tl.get('y', 0), 4):.4f}")
                            points.append(f"{round(tr.get('x', 0), 4):.4f},{round(tr.get('y', 0), 4):.4f}")
                            points.append(f"{round(br.get('x', 0), 4):.4f},{round(br.get('y', 0), 4):.4f}")
                            points.append(f"{round(bl.get('x', 0), 4):.4f},{round(bl.get('y', 0), 4):.4f}")
                    else:
                        # coords가 없는 경우 object 정보 사용
                        obj = annotation.get('object', {})
                        if obj:
                            left = obj.get('left', 0)
                            top = obj.get('top', 0)
                            width = obj.get('width', 0)
                            height = obj.get('height', 0)
                            
                            if width > 0 and height > 0:
                                # BBOX 좌표 순서: tl > tr > br > bl
                                # tl: (left, top)
                                # tr: (left + width, top)
                                # br: (left + width, top + height)
                                # bl: (left, top + height)
                                points.append(f"{round(left, 4):.4f},{round(top, 4):.4f}")
                                points.append(f"{round(left + width, 4):.4f},{round(top, 4):.4f}")
                                points.append(f"{round(left + width, 4):.4f},{round(top + height, 4):.4f}")
                                points.append(f"{round(left, 4):.4f},{round(top + height, 4):.4f}")
                
                # POLYGON 타입 처리
                elif annotation_type == 'POLYGONS':
                    points_data = annotation.get('points', [])
                    if points_data:
                        # JSON에 있는 순서 그대로 사용
                        for point in points_data:
                            x = point.get('x', 0)
                            y = point.get('y', 0)
                            points.append(f"{round(x, 4):.4f},{round(y, 4):.4f}")
                
                # 좌표가 있는 경우에만 처리
                if points:
                    # 텍스트 값 확인 (@ocr 값)
                    text_value = annotation.get('ocr', '')
                    
                    # 텍스트 값에서 모든 공백 제거
                    if isinstance(text_value, str):
                        text_value = text_value.replace(' ', '')
                    
                    # polygon 요소 생성
                    polygon_elem = ET.SubElement(image_elem, 'polygon')
                    
                    # empty인지 확인하여 label 설정
                    if text_value == 'empty':
                        polygon_elem.set('label', 'empty')
                    else:
                        polygon_elem.set('label', 'text')
                    
                    polygon_elem.set('source', 'file')
                    polygon_elem.set('occluded', '0')
                    polygon_elem.set('z_order', str(z_order))
                    polygon_elem.set('points', ';'.join(points))
                    
                    # empty가 아닌 경우에만 attribute 추가
                    if text_value != 'empty':
                        attribute_elem = ET.SubElement(polygon_elem, 'attribute')
                        attribute_elem.set('name', 'text')
                        attribute_elem.text = escape_xml_text(text_value)
                    
                    z_order += 1  # 다음 오브젝트 순서
    
    return image_elem

def process_csv_to_xml(project_id, ticket_number):
    """CSV를 읽어서 JSON과 매칭하여 XML 생성"""
    
    # 경로 설정 (스크립트 위치 기준으로 상대 경로 사용)
    base_dir = Path(__file__).parent.parent
    project_dirs = [d for d in base_dir.iterdir() if d.is_dir() and ticket_number in d.name]
    
    if not project_dirs:
        print(f"프로젝트 폴더를 찾을 수 없습니다: {ticket_number}")
        return False
    
    project_dir = project_dirs[0]
    csv_file = Path.cwd() / f"{ticket_number}_{project_id}.csv"
    template_path = project_dir / "결과데이터_템플릿.xml"
    # 결과 JSON 파일은 현재 디렉토리에서 읽기
    result_json_dir = Path.cwd() / f"{project_id}_result"
    
    # 소스 JSON 디렉토리 찾기 (정수형 숫자_로 시작하는 폴더)
    source_json_dirs = [
        d for d in project_dir.iterdir() 
        if d.is_dir() and len(d.name) > 1 and d.name[0].isdigit() and '_' in d.name
    ]
    if not source_json_dirs:
        print(f"소스 JSON 디렉토리를 찾을 수 없습니다: {project_dir}")
        return False
    source_json_dir = source_json_dirs[0]
    
    if not csv_file.exists():
        print(f"CSV 파일을 찾을 수 없습니다: {csv_file}")
        return False
    
    if not template_path.exists():
        print(f"XML 템플릿 파일을 찾을 수 없습니다: {template_path}")
        return False
    
    # CSV 로드
    df = pd.read_csv(csv_file)
    print(f"CSV 로드 완료: {len(df)} 행")
    
    # 기존 XML 템플릿 로드
    tree = ET.parse(template_path)
    root = tree.getroot()
    
    # 템플릿에서 값들 업데이트
    # CSV에서 가장 최근 날짜 가져오기
    latest_work_date = df['work_edate'].max()
    latest_check_date = df['check_edate'].max()
    created_date = add_timezone_to_datetime(latest_work_date)
    updated_date = add_timezone_to_datetime(latest_check_date)
    
    # 템플릿의 값들 업데이트
    size_elem = root.find('.//size')
    if size_elem is not None:
        size_elem.text = str(len(df))
    
    created_elem = root.find('.//created')
    if created_elem is not None:
        created_elem.text = created_date
    
    updated_elem = root.find('.//updated')
    if updated_elem is not None:
        updated_elem.text = updated_date
    
    # 기존 image 요소들 제거 (템플릿의 예시 요소)
    for image_elem in root.findall('image'):
        root.remove(image_elem)
    
    processed_count = 0
    missing_files = []
    
    # 각 행 처리 (전체 데이터)
    for idx, row in df.iterrows():
        data_idx = row['data_idx']
            
        file_name_str = row['file_name']
        actual_filename = extract_filename_from_csv(file_name_str)
        
        if not actual_filename:
            print(f"행 {idx+1}: 파일명이 없습니다")
            continue
        
        print(f"처리 중: data_idx={data_idx}, filename={actual_filename}")
        
        # result JSON 파일 경로 (data_idx로 매칭)
        result_json_filename = f"{data_idx}_*.json"
        result_json_files = []
        for file in os.listdir(str(result_json_dir)):
            if file.startswith(f"{data_idx}_") and file.endswith('.json'):
                result_json_files.append(file)
        
        if not result_json_files:
            print(f"행 {idx+1}: {project_id}_result JSON 파일을 찾을 수 없습니다 - {data_idx}")
            missing_files.append(f"{data_idx}_result")
            continue
        
        result_json_path = result_json_dir / result_json_files[0]
        print(f"{project_id}_result JSON 경로: {result_json_path}")
        
        # 소스 JSON 파일 경로 
        # 확장자를 .json으로 변경 (대소문자 무관 - .JPG와 .jpg 모두 처리)
        source_json_filename = actual_filename.replace('.JPG', '.json').replace('.jpg', '.json')
        source_json_path = source_json_dir / source_json_filename
        print(f"소스 JSON 경로: {source_json_path}")
        
        if not source_json_path.exists():
            print(f"행 {idx+1}: 소스 JSON 파일을 찾을 수 없습니다 - {source_json_path}")
            missing_files.append(source_json_filename)
            continue
        
        # JSON 데이터 로드
        result_json_data = load_json_data(str(result_json_path))
        source_json_data = load_json_data(str(source_json_path))
        
        if not result_json_data or not source_json_data:
            continue
        
        # XML 변환
        image_elem = convert_result_json_to_xml(result_json_data, idx + 1, actual_filename, source_json_data)
        root.append(image_elem)
        
        processed_count += 1
        print(f"처리 완료: {idx+1}/{len(df)} - {actual_filename}")
    
    # XML 파일 저장
    tree = ET.ElementTree(root)
    
    # 들여쓰기로 포맷팅
    rough_string = ET.tostring(root, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    pretty_xml = reparsed.toprettyxml(indent="\t", encoding='utf-8')
    
    # 빈 태그들을 <tag></tag> 형태로 변환
    pretty_xml_str = pretty_xml.decode('utf-8')
    pretty_xml_str = fix_empty_tags(pretty_xml_str)
    # 불필요한 빈 줄 제거
    pretty_xml_str = remove_empty_lines(pretty_xml_str)
    pretty_xml = pretty_xml_str.encode('utf-8')
    
    # XML 파일 저장 (현재 디렉토리에 저장)
    # 숫자_ 접두사 제거 (예: "2_미주..." -> "미주...", "1_프로젝트명" -> "프로젝트명")
    source_dir_name = re.sub(r'^\d+_', '', source_json_dir.name)
    output_file = Path.cwd() / f"{ticket_number}_{source_dir_name}_{datetime.now().strftime('%Y%m%d')}.xml"
    with open(output_file, 'wb') as f:
        f.write(pretty_xml)
    
    print(f"\n=== 처리 완료 ===")
    print(f"총 행 수: {len(df)}")
    print(f"처리된 행 수: {processed_count}")
    print(f"누락된 파일 수: {len(missing_files)}")
    print(f"출력 파일: {output_file}")
    print(f"Created: {created_date}")
    print(f"Updated: {updated_date}")
    
    if missing_files:
        print(f"\n누락된 파일들:")
        for file in missing_files[:10]:  # 처음 10개만 출력
            print(f"  {file}")
        if len(missing_files) > 10:
            print(f"  ... 외 {len(missing_files) - 10}개")

def main():
    # 명령행 인수 처리
    if len(sys.argv) < 3:
        print("사용법: python data_processing_1031.py <project_id> <ticket_number>")
        print("예시: python data_processing_1031.py 26640 PROJ-15357")
        return 1
    
    project_id = sys.argv[1]
    ticket_number = sys.argv[2]
    
    try:
        process_csv_to_xml(project_id, ticket_number)
    except Exception as e:
        print(f"오류 발생: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())

