"""
데이터 preset 세팅 코드 작성
프로젝트ID:26648 -> 26966   
"""

import pandas as pd
import logging
import json
import sys
from pathlib import Path
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_json_file(json_file_path):
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"JSON 파일 로드 실패: {e}")
        return None


def convert_annotation(annotation):
    """어노테이션 변환: POLYGONS는 points, BOX는 object 사용"""
    ocr_value = annotation.get("ocr", "")
    
    if annotation.get("annotation") == "POLYGONS":
        points = annotation.get("points", [])
        if not points:
            return None
        return {"annotation": "POLYGONS", "points": points, "ocr": ocr_value}
    
    elif annotation.get("annotation") == "BOX":
        obj = annotation.get("object")
        if obj is None:
            coords = annotation.get("coords")
            if coords:
                tl, br = coords.get("tl", {}), coords.get("br", {})
                obj = {
                    "left": tl.get("x", 0),
                    "top": tl.get("y", 0),
                    "width": br.get("x", 0) - tl.get("x", 0),
                    "height": br.get("y", 0) - tl.get("y", 0),
                    "angle": annotation.get("angle", 0)
                }
            else:
                return None
        return {"annotation": "BOX", "object": obj, "ocr": ocr_value}
    
    return None


def load_csv_mapping(csv_file_path):
    """CSV에서 data_idx와 file_name 매핑 로드"""
    try:
        df = pd.read_csv(csv_file_path)
        return dict(zip(df['data_idx'].astype(str), df['file_name']))
    except Exception as e:
        logging.error(f"CSV 파일 로드 실패 ({csv_file_path}): {e}")
        return {}


def get_file_name(json_file_path, csv_mapping=None, json_data=None):
    """파일명 추출: CSV 매핑 > sources > 기본값"""
    if csv_mapping:
        data_id = Path(json_file_path).stem.split('_')[0]
        file_name = csv_mapping.get(data_id)
        if file_name:
            return file_name
    
    if json_data:
        sources = json_data.get("sources", [])
        if len(sources) > 1 and sources[1]:
            return sources[1]
    
    return f"{Path(json_file_path).stem}.jpg"


def extract_annotations(json_data):
    """JSON 데이터에서 어노테이션 추출"""
    results = json_data.get("results", [])
    if len(results) <= 1 or not results[1]:
        return []
    
    result_data = results[1][0]
    if not result_data:
        return []
    
    # name_5OJYEV 키에서 어노테이션 배열 추출
    name_key = "name_5OJYEV"
    if name_key not in result_data:
        logging.warning(f"키 '{name_key}'를 찾을 수 없습니다.")
        return []
    
    annotations = []
    for ann in result_data.get(name_key, []):
        converted = convert_annotation(ann)
        if converted:
            annotations.append(converted)
    
    return annotations


def convert_json_to_jsonl(json_file_path, output_file_path=None, csv_file_path=None, return_data=False):
    """JSON 파일을 JSONL로 변환 (return_data=True면 데이터도 반환)"""
    try:
        data = load_json_file(json_file_path)
        if data is None:
            return None if return_data else False
        
        # 파일명 추출
        csv_mapping = load_csv_mapping(csv_file_path) if csv_file_path else {}
        file_name = get_file_name(json_file_path, csv_mapping, data)
        
        # 어노테이션 추출
        annotations = extract_annotations(data)
        if not annotations:
            logging.warning(f"어노테이션을 찾을 수 없습니다: {json_file_path}")
            return None if return_data else False
        
        # 출력 객체 생성
        output_obj = {
            "file_name": file_name,
            "preset": json.dumps(annotations, ensure_ascii=False)
        }
        
        # return_data가 False일 때만 개별 파일 저장
        if not return_data:
            if output_file_path is None:
                output_file_path = Path(json_file_path).with_suffix('.jsonl')
            else:
                output_file_path = Path(output_file_path)
            
            with open(output_file_path, 'w', encoding='utf-8') as f:
                json.dump(output_obj, f, ensure_ascii=False)
                f.write('\n')
            
            logging.info(f"변환 성공: {json_file_path} -> {output_file_path} ({len(annotations)}개 어노테이션)")
        else:
            logging.info(f"변환 성공: {json_file_path} ({len(annotations)}개 어노테이션)")
        
        return output_obj if return_data else True
        
    except Exception as e:
        logging.error(f"파일 처리 오류 ({json_file_path}): {e}")
        return None if return_data else False


def process_directory_to_jsonl(input_dir, output_jsonl_path=None, csv_file_path=None, output_csv_path=None):
    """디렉토리의 모든 JSON 파일을 하나의 JSONL 파일과 CSV로 변환 (CSV 파일명 순서대로 정렬)"""
    input_path = Path(input_dir)
    
    if not input_path.exists() or not input_path.is_dir():
        logging.error(f"입력 디렉토리가 존재하지 않습니다: {input_dir}")
        return 0, 0
    
    success_count = 0
    failure_count = 0
    jsonl_data_list = []
    
    # CSV 파일에서 file_name 순서 가져오기
    file_name_order = []
    if csv_file_path:
        try:
            df = pd.read_csv(csv_file_path)
            # CSV 파일의 행 순서대로 file_name 저장
            file_name_order = df['file_name'].astype(str).tolist()
        except Exception as e:
            logging.warning(f"CSV 파일 로드 실패 ({csv_file_path}): {e}")
    
    # 모든 JSON 파일 처리 및 데이터 수집
    for file_path in input_path.glob("*.json"):
        try:
            jsonl_data = convert_json_to_jsonl(
                str(file_path),
                None,  # 개별 파일 저장 안 함
                csv_file_path,
                return_data=True
            )
            
            if jsonl_data:
                success_count += 1
                jsonl_data_list.append(jsonl_data)
            else:
                failure_count += 1
        except Exception as e:
            logging.error(f"파일 처리 중 오류 ({file_path}): {e}")
            failure_count += 1
    
    # CSV의 file_name 순서대로 정렬
    if file_name_order:
        def get_sort_key(data):
            """jsonl_data의 file_name을 기준으로 CSV 순서에 맞춰 정렬"""
            file_name = data.get('file_name', '')
            try:
                return file_name_order.index(file_name) if file_name in file_name_order else len(file_name_order)
            except ValueError:
                return len(file_name_order)
        
        jsonl_data_list.sort(key=get_sort_key)
        logging.info(f"CSV 파일명 순서 기준으로 {len(jsonl_data_list)}개 항목 정렬 완료")
    else:
        # CSV가 없으면 file_name 기준으로 정렬
        jsonl_data_list.sort(key=lambda x: x.get('file_name', ''))
        logging.info(f"file_name 기준으로 {len(jsonl_data_list)}개 항목 정렬 완료")
    
    # 하나의 JSONL 파일로 저장
    if jsonl_data_list and output_jsonl_path:
        jsonl_path = Path(output_jsonl_path)
        jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        with open(jsonl_path, 'w', encoding='utf-8') as f:
            for data in jsonl_data_list:
                json.dump(data, f, ensure_ascii=False)
                f.write('\n')
        logging.info(f"JSONL 파일 생성 완료: {jsonl_path} ({len(jsonl_data_list)}개 항목)")
    
    # CSV 파일 생성
    if jsonl_data_list and output_csv_path:
        csv_path = Path(output_csv_path)
        df = pd.DataFrame(jsonl_data_list)
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        logging.info(f"CSV 파일 생성 완료: {csv_path} ({len(df)}개 행)")
    
    return success_count, failure_count


def main():
    """메인 실행 함수 - 여기서 경로 설정"""
    # 기본 설정
    source_project_id = "26648"
    target_project_id = "26966"
    ticket_number = "PROJ-15441"
    
    # 명령행 인수 처리
    if len(sys.argv) > 1:
        source_project_id = sys.argv[1]
    if len(sys.argv) > 2:
        target_project_id = sys.argv[2]
    if len(sys.argv) > 3:
        ticket_number = sys.argv[3]
    
    # 날짜 자동 생성 (오늘 날짜)
    date_str = datetime.now().strftime("%Y%m%d")
    
    # ========== 경로 설정 ==========
    base_dir = Path(__file__).parent
    
    # JSON 파일들이 있는 디렉토리a
    input_dir = str(base_dir / f"{source_project_id}_result")
    
    # 생성할 JSONL 파일 경로 (하나의 JSONL 파일로 통합)
    output_dir = base_dir / f"HMG_{ticket_number}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_jsonl_path = str(output_dir / f"HMG_{ticket_number}_{target_project_id}_preset_data_{date_str}.jsonl")
    
    # CSV 매핑 파일 경로 (파일명 매핑용, data_idx와 file_name 매핑)
    csv_file_path = str(base_dir / f"{ticket_number}_{source_project_id}.csv")
    
    # 생성할 CSV 파일 경로
    output_csv_path = str(output_dir / f"HMG_{ticket_number}_{target_project_id}_preset_data_{date_str}.csv")
    # ==============================
    
    logging.info(f"소스 프로젝트 ID: {source_project_id}")
    logging.info(f"타겟 프로젝트 ID: {target_project_id}")
    logging.info(f"티켓번호: {ticket_number}")
    logging.info(f"입력 디렉토리: {input_dir}")
    logging.info(f"CSV 매핑 파일: {csv_file_path}")
    logging.info(f"출력 JSONL: {output_jsonl_path}")
    logging.info(f"출력 CSV: {output_csv_path}")
    
    # 실행
    success, failure = process_directory_to_jsonl(
        input_dir=input_dir,
        output_jsonl_path=output_jsonl_path,
        csv_file_path=csv_file_path,
        output_csv_path=output_csv_path
    )
    
    print(f"\n처리 완료: 성공 {success}개, 실패 {failure}개")


if __name__ == "__main__":
    main()
