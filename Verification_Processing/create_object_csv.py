#!/usr/bin/env python3
"""
기존 CSV를 오브젝트 단위로 분리하여 새로운 CSV 생성
- 하나의 data_idx에 여러 오브젝트가 있는 경우 각각 별도 행으로 생성
- object_id와 ocr_text 컬럼 추가
"""

import pandas as pd
import os
import sys
import json
import xml.etree.ElementTree as ET
from pathlib import Path

# def extract_filename_from_csv(file_name_str):
#     """
#     CSV의 file_name 열에서 실제 파일명 추출
#     
#     사용 방법:
#     - main() 함수 내 for loop에서:
#       actual_filename = extract_filename_from_csv(row['file_name'])
#     - 함수가 JSON 문자열을 파싱하여 실제 파일명만 추출
#     """
#     try:
#         # 이중 인코딩된 JSON 문자열 파싱
#         if isinstance(file_name_str, str) and file_name_str.startswith('"{') and file_name_str.endswith('}"'):
#             json_str = file_name_str[1:-1].replace('""', '"')
#             data = json.loads(json_str)
#             file_name = data.get('file_name', '')
#             if isinstance(file_name, str) and file_name.startswith('"') and file_name.endswith('"'):
#                 file_name = file_name[1:-1]
#             return file_name
#     except:
#         pass
    
#     try:
#         # 일반 JSON 파싱
#         data = json.loads(file_name_str)
#         return data.get('file_name', '')
#     except:
#         return file_name_str

def load_json_data(json_file_path):
    """JSON 파일 로드"""
    try:
        json_path = Path(json_file_path)
        if not json_path.exists():
            print(f"JSON 파일이 존재하지 않습니다: {json_file_path}")
            return None
        with open(json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"JSON 파일 로드 실패 {json_file_path}: {e}")
        return None

def create_object_level_csv(project_id, ticket_number=None):
    """오브젝트 단위로 CSV 생성"""
    
    # 작업 디렉토리 설정 (스크립트가 있는 디렉토리)
    script_dir = Path(__file__).parent
    
    # 입력 CSV 파일 경로 찾기 (티켓번호가 있으면 사용, 없으면 프로젝트 ID로 찾기)
    if ticket_number:
        input_csv_pattern = f"{ticket_number}_{project_id}.csv"
    else:
        # 프로젝트 ID로 시작하는 CSV 파일 찾기
        csv_files = list(script_dir.glob(f"*_{project_id}.csv"))
        if csv_files:
            input_csv_pattern = csv_files[0].name
        else:
            input_csv_pattern = f"{project_id}.csv"
    
    input_csv_path = script_dir / input_csv_pattern
    
    if not input_csv_path.exists():
        print(f"오류: CSV 파일을 찾을 수 없습니다: {input_csv_path}")
        return None
    
    # 기존 CSV 로드
    df = pd.read_csv(str(input_csv_path))
    print(f"기존 CSV 로드 완료: {len(df)} 행")
    
    # JSON 파일 디렉토리 (상대경로)
    result_json_dir = script_dir / f"{project_id}_result"
    
    # 새로운 데이터를 저장할 리스트
    new_rows = []
    
    processed_count = 0
    missing_files = []
    
    # 각 행 처리
    for idx, row in df.iterrows():
        data_idx = row['data_idx']
        actual_filename = row['file_name']
        
        if not actual_filename:
            print(f"행 {idx+1}: 파일명이 없습니다")
            continue
        
        print(f"처리 중: data_idx={data_idx}, filename={actual_filename}")
        
        # result JSON 파일 경로 찾기
        result_json_files = []
        if result_json_dir.exists():
            for file in os.listdir(str(result_json_dir)):
                if file.startswith(f"{data_idx}_") and file.endswith('.json'):
                    result_json_files.append(file)
        
        if not result_json_files:
            print(f"행 {idx+1}: JSON 파일을 찾을 수 없습니다 - {data_idx}")
            missing_files.append(f"{data_idx}_result")
            continue
        
        result_json_path = result_json_dir / result_json_files[0]
        
        # JSON 데이터 로드
        result_json_data = load_json_data(str(result_json_path))
        
        if not result_json_data:
            continue
        
        # results에서 오브젝트 데이터 추출
        results = result_json_data.get('results', [])
        if not results or len(results) < 2:
            print(f"행 {idx+1}: results 데이터가 없습니다")
            continue
        
        # 첫 번째 행은 헤더, 두 번째 행은 데이터
        data_row = results[1] if len(results) > 1 else []
        
        if len(data_row) > 0 and data_row[0] is not None:
            annotations = data_row[0]
            
            if isinstance(annotations, dict) and 'name_5OJYEV' in annotations:
                annotation_list = annotations['name_5OJYEV']
                
                # 각 오브젝트별로 행 생성
                for obj_idx, annotation in enumerate(annotation_list, start=1):
                    # 텍스트 값 추출 및 공백 제거
                    text_value = annotation.get('ocr', '')
                    if isinstance(text_value, str):
                        text_value = text_value.strip()
                    
                    # 새로운 행 데이터 생성
                    new_row = row.copy()
                    new_row['object_id'] = obj_idx  # 오브젝트 순서 (1부터 시작)
                    new_row['ocr_text'] = text_value  # OCR 텍스트
                    
                    new_rows.append(new_row)
                
                processed_count += 1
                print(f"오브젝트 추출 완료: {data_idx} - {len(annotation_list)}개 오브젝트")
            else:
                print(f"행 {idx+1}: name_5OJYEV 데이터가 없습니다")
        else:
            print(f"행 {idx+1}: annotation 데이터가 없습니다")
    
    # 새로운 DataFrame 생성
    new_df = pd.DataFrame(new_rows)
    
    # 컬럼 이름을 한글로 변경
    new_df = new_df.rename(columns={
        'project_id': '프로젝트ID',
        'data_idx': '데이터ID', 
        'file_name': '이미지 파일명',
        'object_id': '오브젝트ID',
        'ocr_text': 'OCR',
        'prog_state_cd': '작업 상태',
        'problem_yn': '작업불가여부',
        'problem_reason': '작업불가사유',
        'final_object_count': '최종 오브젝트 수',
        'is_modified': '수정 여부',
        'work_object_number': '유효 오브젝트 수',
        'worker_id': 'Worker ID',
        'worker_nickname': '작업자 닉네임',
        'checker_id': 'Checker ID',
        'checker_nickname': '검수자 닉네임',
        'work_edate': '작업 종료일',
        'check_edate': '검수 종료일',
        'modified_dt': '작업 수정 시간',
        'link': 'url'
    })
    
    # 컬럼 순서 재정렬
    column_order = [
        '프로젝트ID', '데이터ID', '이미지 파일명', '오브젝트ID', 'OCR',  # 기본 정보
        '작업 상태', '작업불가여부', '작업불가사유', '최종 오브젝트 수', '수정 여부', '유효 오브젝트 수',  # 작업 관련
        'Worker ID', '작업자 닉네임', 'Checker ID', '검수자 닉네임',  # 사용자 정보
        '작업 종료일', '검수 종료일', '작업 수정 시간',  # 날짜 정보
        'url'  # 링크는 마지막에
    ]
    
    # 결과 CSV 저장 (상대경로)
    if ticket_number:
        output_file = script_dir / f"{ticket_number}_{project_id}_check.csv"
    else:
        output_file = script_dir / f"{project_id}_check.csv"
    new_df.to_csv(str(output_file), index=False, encoding='utf-8-sig', columns=column_order)
    
    print(f"\n=== 처리 완료 ===")
    print(f"기존 행 수: {len(df)}")
    print(f"새로운 행 수: {len(new_df)}")
    print(f"처리된 데이터 수: {processed_count}")
    print(f"누락된 파일 수: {len(missing_files)}")
    print(f"출력 파일: {output_file}")
    
    # 통계 출력
    print(f"\n=== 오브젝트 통계 ===")
    object_counts = new_df.groupby('데이터ID').size()
    print(f"평균 오브젝트 수: {object_counts.mean():.2f}")
    print(f"최대 오브젝트 수: {object_counts.max()}")
    print(f"최소 오브젝트 수: {object_counts.min()}")
    
    # 샘플 출력
    print(f"\n=== 샘플 데이터 ===")
    sample_data = new_df[['데이터ID', '이미지 파일명', '오브젝트ID', 'OCR']].head(10)
    print(sample_data.to_string(index=False))
    
    if missing_files:
        print(f"\n누락된 파일들:")
        for file in missing_files[:10]:
            print(f"  {file}")
        if len(missing_files) > 10:
            print(f"  ... 외 {len(missing_files) - 10}개")
    
    return new_df

def main():
    try:
        # 명령행 인수 처리
        if len(sys.argv) < 2:
            print("사용법: python create_object_csv.py <project_id> [ticket_number]")
            sys.exit(1)
        
        project_id = sys.argv[1]
        ticket_number = sys.argv[2] if len(sys.argv) > 2 else None
        
        create_object_level_csv(project_id, ticket_number)
        print("\n✅ 오브젝트 단위 CSV 생성이 완료되었습니다!")
    except Exception as e:
        print(f"오류 발생: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
