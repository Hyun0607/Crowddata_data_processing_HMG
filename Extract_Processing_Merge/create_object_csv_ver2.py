#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
기존 CSV를 오브젝트 단위로 분리하여 새로운 CSV 생성
- 하나의 data_idx에 여러 오브젝트가 있는 경우 각각 별도 행으로 생성
- object_id와 ocr_text 컬럼 추가
"""

import pandas as pd
import os
import json
import xml.etree.ElementTree as ET
import sys
from pathlib import Path

# ========== 설정 섹션 ==========
# 스크립트 디렉토리 기준 경로
SCRIPT_DIR = Path(__file__).parent

# 처리할 CSV 파일 경로 (스크립트 디렉토리 기준 상대 경로 또는 절대 경로)
CSV_FILE_PATH = SCRIPT_DIR / "PROJ-15684_26994_상.csv"

# 프로젝트 ID (None이면 CSV에서 자동 추출)
PROJECT_ID = None  # 예: "26795" 또는 "26994" 또는 None

# ==============================

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
        with open(json_file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"JSON 파일 로드 실패 {json_file_path}: {e}")
        return None

def extract_filename_from_csv(file_name_str):
    """CSV의 file_name 열에서 실제 파일명 추출"""
    try:
        # JSON 문자열 파싱
        data = json.loads(file_name_str)
        return data.get('file_name', '')
    except:
        # JSON이 아닌 경우 그대로 반환
        return file_name_str

def create_object_level_csv(csv_file_path, project_id=None):
    """오브젝트 단위로 CSV 생성"""
    
    # CSV 파일 경로 처리
    csv_file = Path(csv_file_path)
    if not csv_file.is_absolute():
        csv_file = Path.cwd() / csv_file
    
    if not csv_file.exists():
        print(f"CSV 파일을 찾을 수 없습니다: {csv_file}")
        return None
    
    # CSV에서 project_id 추출 (지정되지 않은 경우)
    if project_id is None:
        df_temp = pd.read_csv(csv_file, nrows=1)
        if 'project_id' in df_temp.columns:
            project_id = df_temp['project_id'].iloc[0]
        else:
            print("project_id를 찾을 수 없습니다. 인자로 지정해주세요.")
            return None
    
    result_json_dir = Path.cwd() / f"{project_id}_result"
    
    # 기존 CSV 로드
    df = pd.read_csv(csv_file)
    print(f"기존 CSV 로드 완료: {len(df)} 행")
    
    # 새로운 데이터를 저장할 리스트
    new_rows = []
    
    processed_count = 0
    missing_files = []
    
    # 각 행 처리
    for idx, row in df.iterrows():
        data_idx = row['data_idx']
        file_name_str = row['file_name']
        actual_filename = extract_filename_from_csv(file_name_str)
        
        if not actual_filename:
            print(f"행 {idx+1}: 파일명이 없습니다")
            continue
        
        print(f"처리 중: data_idx={data_idx}, filename={actual_filename}")
        
        # result JSON 파일 경로 찾기
        result_json_files = []
        for file in os.listdir(str(result_json_dir)):
            if file.startswith(f"{data_idx}_") and file.endswith('.json'):
                result_json_files.append(file)
        
        if not result_json_files:
            print(f"행 {idx+1}: {project_id}_result JSON 파일을 찾을 수 없습니다 - {data_idx}")
            missing_files.append(f"{data_idx}_result")
            continue
        
        result_json_path = result_json_dir / result_json_files[0]
        
        # JSON 데이터 로드
        result_json_data = load_json_data(result_json_path)
        
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
                    new_row['object_id'] = obj_idx  # 오브젝트 순서 (0부터 시작)
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
        'work_object_number': '최종 오브젝트 수',  
        'is_modified': '수정 여부',
        'final_object_count': '유효 오브젝트 수',
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
    
    # 결과 CSV 저장 (입력 파일명 기반으로 생성)
    csv_stem = csv_file.stem  # 확장자 제외한 파일명
    output_file = csv_file.parent / f"{csv_stem}_check.csv"
    new_df.to_csv(output_file, index=False, encoding='utf-8-sig', columns=column_order)
    
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
    # 설정된 경로 사용
    csv_file_path = CSV_FILE_PATH
    project_id = PROJECT_ID
    
    # 명령행 인수가 있으면 우선 사용 (선택사항)
    if len(sys.argv) > 1:
        csv_file_path = sys.argv[1]
    if len(sys.argv) > 2:
        project_id = sys.argv[2]
    
    print(f"CSV 파일 경로: {csv_file_path}")
    if project_id:
        print(f"프로젝트 ID: {project_id}")
    else:
        print("프로젝트 ID: 자동 추출")
    
    try:
        result_df = create_object_level_csv(csv_file_path, project_id)
        if result_df is not None:
            print("\n✅ 오브젝트 단위 CSV 생성이 완료되었습니다!")
        else:
            return 1
    except Exception as e:
        print(f"오류 발생: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
