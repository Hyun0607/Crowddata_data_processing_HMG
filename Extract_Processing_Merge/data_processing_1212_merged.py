#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
XML 데이터 처리 스크립트
CSV와 JSON을 매칭하여 XML 생성
"""

import pandas as pd
import os
import re
import json
import xml.etree.ElementTree as ET
from xml.dom import minidom
from datetime import datetime, timedelta

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

def convert_result_json_to_xml(result_json_data, image_id, image_name, source_json_data, difficulty):
    """result JSON 데이터를 XML 형식으로 변환"""
    
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
    
    # 난이도에 따라 다른 키값 및 인덱스 사용
    if difficulty == '중':
        coord_key = 'name_RRMP0X'
        text_key = 'ocr'
        data_index = 1  # 중은 results[1]에 데이터
    else:  # 상
        coord_key = 'name_5OJYEV'
        text_key = 'ocr'
        data_index = 1  # 상은 results[1]에 데이터
    
    # results에서 데이터 처리
    results = result_json_data.get('results', [])
    if not results or len(results) <= data_index:
        return image_elem
    
    # 난이도에 따라 다른 인덱스의 데이터 가져오기
    data_row = results[data_index] if len(results) > data_index else []
    
    z_order = 0  # 오브젝트 순서는 0부터 시작
    
    # 좌표 데이터가 있는 경우만 처리
    if len(data_row) > 0 and data_row[0] is not None:
        annotations = data_row[0]
        
        if isinstance(annotations, dict) and coord_key in annotations:
            annotation_list = annotations[coord_key]
            
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
                    # 텍스트 값 확인 (난이도에 따라 다른 키 사용)
                    text_value = annotation.get(text_key, '')
                    
                    # 텍스트 값에서 모든 공백 제거
                    if isinstance(text_value, str):
                        text_value = text_value.replace(' ', '')
                    
                    # polygon 요소 생성
                    polygon_elem = ET.SubElement(image_elem, 'polygon')
                    
                    # OCR 결과가 있으면 text, 없으면 empty
                    # text_value가 'empty' 문자열이거나 빈 문자열/공백인 경우 모두 empty로 처리
                    if text_value and text_value.strip() and text_value != 'empty':
                        polygon_elem.set('label', 'text')
                    else:
                        polygon_elem.set('label', 'empty')
                    
                    polygon_elem.set('source', 'file')
                    polygon_elem.set('occluded', '0')
                    polygon_elem.set('z_order', str(z_order))
                    polygon_elem.set('points', ';'.join(points))
                    
                    # text 라벨인 경우에만 attribute 추가
                    if polygon_elem.get('label') == 'text':
                        attribute_elem = ET.SubElement(polygon_elem, 'attribute')
                        attribute_elem.set('name', 'text')
                        attribute_elem.text = escape_xml_text(text_value)
                    
                    z_order += 1  # 다음 오브젝트 순서
    
    return image_elem

def process_csv_to_xml():
    """CSV를 읽어서 JSON과 매칭하여 XML 생성"""
    
    # CSV 로드
    df = pd.read_csv("/Users/hw.jung/Desktop/Data_engineer/Project_file/1212_PROJ-15684(중상:상)/PROJ-15684_중_상_통합.csv")
    print(f"CSV 로드 완료: {len(df)} 행")
    
    # 기존 XML 템플릿 로드
    template_path = "/Users/hw.jung/Desktop/Data_engineer/Project_file/1212_PROJ-15684(중상:상)/결과데이터_템플릿.xml"
    tree = ET.parse(template_path)
    root = tree.getroot()
    
    # 템플릿에서 값들 업데이트
    # CSV에서 가장 최근 날짜 가져오기
    # 빈 문자열과 NaN 제거 후 최대값 계산
    work_dates = df['work_edate'].replace('', pd.NA).dropna()
    check_dates = df['check_edate'].replace('', pd.NA).dropna()
    
    # created는 work_edate의 최신값 사용
    if len(work_dates) > 0:
        latest_work_date = work_dates.max()
        created_date = add_timezone_to_datetime(str(latest_work_date))
    else:
        created_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S+09:00')
    
    # updated는 check_edate의 최신값 사용
    if len(check_dates) > 0:
        latest_check_date = check_dates.max()
        updated_date = add_timezone_to_datetime(str(latest_check_date))
    else:
        updated_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S+09:00')
    
    # 템플릿의 값들 업데이트
    task_name_elem = root.find('.//task/name')
    if task_name_elem is not None:
        task_name_elem.text = "하와이 한인 잡지 <공동보>, <동지별보>, <태평양주보>"
  
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
    
    # JSON 파일 디렉토리들 (난이도별로 다름)
    result_json_dir_hard = "/Users/hw.jung/Desktop/Data_engineer/Project_file/1212_PROJ-15684(중상:상)/26795_result"  # 중
    result_json_dir_very_hard = "/Users/hw.jung/Desktop/Data_engineer/Project_file/1212_PROJ-15684(중상:상)/26994_result"  # 상
    source_json_dir_hard = "/Users/hw.jung/Desktop/Data_engineer/Project_file/1212_PROJ-15684(중상:상)/9_하와이 잡지 공동보 등 (9_normal)"  # 중
    source_json_dir_very_hard = "/Users/hw.jung/Desktop/Data_engineer/Project_file/1212_PROJ-15684(중상:상)/9_하와이 잡지 공동보 등 (9_hard)"  # 상
    
    processed_count = 0
    missing_files = []
    
    # 각 행 처리 (전체 데이터)
    for idx, row in df.iterrows():
        difficulty = row['난이도']  # 난이도 컬럼 읽기
        data_idx = row['data_idx']
        file_name_str = row['file_name']
        actual_filename = extract_filename_from_csv(file_name_str)
        
        if not actual_filename:
            print(f"행 {idx+1}: 파일명이 없습니다")
            continue
        
        print(f"처리 중: 난이도={difficulty}, data_idx={data_idx}, filename={actual_filename}")
        
        # 난이도에 따라 디렉토리 선택
        if difficulty == '중':
            result_json_dir = result_json_dir_hard
            source_json_dir = source_json_dir_hard
        elif difficulty == '상':
            result_json_dir = result_json_dir_very_hard
            source_json_dir = source_json_dir_very_hard
        else:
            print(f"행 {idx+1}: 알 수 없는 난이도 - {difficulty}")
            continue
        
        # result JSON 파일 경로 (data_idx로 매칭)
        result_json_files = []
        for file in os.listdir(result_json_dir):
            if file.startswith(f"{data_idx}_") and file.endswith('.json'):
                result_json_files.append(file)
        
        if not result_json_files:
            print(f"행 {idx+1}: result JSON 파일을 찾을 수 없습니다 - {data_idx} (난이도: {difficulty})")
            missing_files.append(f"{data_idx}_result_{difficulty}")
            continue
        
        result_json_path = os.path.join(result_json_dir, result_json_files[0])
        print(f"result JSON 경로: {result_json_path}")
        
        # 소스 JSON 파일 경로 (파일명으로 매칭)
        source_json_filename = actual_filename.replace('.jpg', '.json')
        source_json_path = os.path.join(source_json_dir, source_json_filename)
        print(f"소스 JSON 경로: {source_json_path}")
        
        if not os.path.exists(source_json_path):
            print(f"행 {idx+1}: 소스 JSON 파일을 찾을 수 없습니다 - {source_json_path}")
            missing_files.append(source_json_filename)
            continue
        
        # JSON 데이터 로드
        result_json_data = load_json_data(result_json_path)
        source_json_data = load_json_data(source_json_path)
        
        if not result_json_data or not source_json_data:
            continue
        
        # XML 변환 (난이도 정보 전달)
        image_elem = convert_result_json_to_xml(result_json_data, idx + 1, actual_filename, source_json_data, difficulty)
        
        # 디버깅: 크기가 0이면 경고 출력
        width = int(image_elem.get('width', 0))
        height = int(image_elem.get('height', 0))
        if width == 0 or height == 0:
            print(f"경고: 이미지 크기를 찾을 수 없습니다 - {actual_filename} (width={width}, height={height})")
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
    # 연속된 빈 줄 제거
    pretty_xml_str = remove_empty_lines(pretty_xml_str)
    pretty_xml = pretty_xml_str.encode('utf-8')
    
    # XML 파일 저장
    output_file = "/Users/hw.jung/Desktop/Data_engineer/Project_file/1212_PROJ-15684(중상:상)/PROJ-15684_하와이 한인 잡지 공동보, 동지별보, 태평양주보_251212.xml"
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
    try:
        process_csv_to_xml()
    except Exception as e:
        print(f"오류 발생: {e}")

if __name__ == "__main__":
    main()

