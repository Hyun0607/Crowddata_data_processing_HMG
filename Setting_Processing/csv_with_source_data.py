#!/usr/bin/env python3
"""
CSV의 src_idx와 GCS 소스 파일을 매핑해서 CSV에 소스 데이터 내용을 추가하는 스크립트
"""

import pandas as pd
import os
import re
import json
import sys
from google.cloud import storage
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

def clean_filename(filename):
    """파일명에서 특수문자를 제거하고 깔끔하게 정리"""
    if not filename:
        return filename
    
    # 특수문자 제거 (한글, 영문, 숫자, 점, 하이픈만 남김)
    import re
    clean_name = re.sub(r'[^\w가-힣.-]', '', filename)
    
    # 연속된 점이나 하이픈 제거
    clean_name = re.sub(r'[.-]+', '.', clean_name)
    
    # 확장자가 없으면 .jpg 추가
    if '.' not in clean_name:
        clean_name += '.jpg'
    
    return clean_name

def download_and_map_source_data(project_id, ticket_number, base_dir=None):
    """CSV의 src_idx와 매칭되는 GCS 소스 파일들을 다운로드하고 CSV에 추가"""
    
    if base_dir is None:
        base_dir = Path(__file__).parent
    
    # CSV 파일 로드
    csv_file = base_dir / "bquxjobRaw.csv"
    df = pd.read_csv(csv_file)
    csv_src_indices = set(df['src_idx'].astype(str).tolist())
    print(f"CSV에서 찾은 src_idx 개수: {len(csv_src_indices)}")
    print(f"src_idx 범위: {df['src_idx'].min()} ~ {df['src_idx'].max()}")
    
    # 크레덴셜 파일 경로 설정
    credential_path = "/Users/hw.jung/Desktop/Data_engineer/GCS/crowdworks-platform-3a4e1c7cbb9f.json"
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
    
    # 환경변수로 인증된 크레덴셜 계정 사용
    client = storage.Client()
    # GCS에서 소스 파일 목록 가져오기
    bucket = client.bucket("cw_platform")
    blobs = list(bucket.list_blobs(prefix=f"1069/{project_id}_source/_source_data/"))
    print(f"GCS에서 찾은 파일 개수: {len(blobs)}")  
    
    # src_idx가 일치하는 파일 찾기
    matching_files = []
    for blob in blobs:
        filename = blob.name.split('/')[-1]
        # 파일명에서 첫 번째 숫자 부분을 src_idx로 추출
        # 예: "1_강마리아.json" -> src_idx: 1
        id_match = re.match(r'^(\d+)_', filename)
        if id_match:
            file_src_idx = id_match.group(1)
            # CSV의 src_idx와 정확히 일치하는지 확인
            if file_src_idx in csv_src_indices:
                matching_files.append((blob, file_src_idx, filename))
                print(f"매칭 성공: {filename} -> src_idx: {file_src_idx}")
    
    print(f"일치하는 파일 개수: {len(matching_files)}")
    
    # 소스 데이터를 저장할 딕셔너리
    source_data_dict = {}
    
    # 일치하는 파일 다운로드 및 데이터 추출
    def download_and_extract_data(blob_info):
        blob, src_idx, filename = blob_info
        try:
            # 파일 내용 다운로드
            content = blob.download_as_text()
            
            # 디버깅: 파일명 확인
            print(f"파일명 확인 {src_idx}: filename = '{filename}'")
            
            # JSON 콘텐츠로 파싱 시도
            try:
                data = json.loads(content)
                # filename 키에서 파일명 추출 (filename, file_name 모두 시도)
                file_name = data.get('filename', data.get('file_name', filename))
                
                # 디버깅: 실제 값 확인
                print(f"디버깅 {src_idx}: data.get('filename') = {file_name}")
                
                # 문자열이 아니면 파일명 그대로 사용
                if not isinstance(file_name, str):
                    print(f"경고: {src_idx}_{filename} - filename이 문자열이 아닙니다.")
                    file_name = filename
                
                # 따옴표가 있다면 제거
                if isinstance(file_name, str) and file_name.startswith('"') and file_name.endswith('"'):
                    file_name = file_name[1:-1]
                
                # 파일명 정리: 특수문자 제거하고 깔끔하게 만들기
                clean_file_name = clean_filename(file_name)
                
                source_data_dict[src_idx] = clean_file_name
                print(f"파일명 추출 완료: {src_idx}_{filename} -> {clean_file_name}")
                return True
            except json.JSONDecodeError:
                # JSON이 아닌 경우 파일명 그대로 사용
                print(f"경고: {src_idx}_{filename} - JSON 파싱 실패")
                source_data_dict[src_idx] = filename
                return True
                
        except Exception as e:
            print(f"데이터 추출 실패 {src_idx}_{filename}: {e}")
            return False
    
    # 병렬 처리
    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(download_and_extract_data, matching_files))
    
    successful_extractions = sum(results)
    print(f"\n데이터 추출 완료: {successful_extractions}/{len(matching_files)} 파일")
    
    # CSV에 소스 데이터 열 추가
    df['file_name'] = df['src_idx'].astype(str).map(lambda x: source_data_dict.get(x, ''))
    
    # 컬럼 순서 정의 (원하는 순서대로 나열)
    column_order = [
        'project_id', 'data_idx', 'src_idx', 'file_name', 
        'prog_state_cd', 'problem_yn', 'work_edate', 'check_edate'
    ]
    
    # 결과 CSV 저장 (지정된 컬럼 순서로)
    output_file = base_dir / f"{ticket_number}_{project_id}.csv"
    df.to_csv(output_file, index=False, encoding='utf-8-sig', columns=column_order)
    print(f"\n결과 CSV 저장 완료: {output_file}")
    
    # 통계 출력
    print(f"\n=== 결과 통계 ===")
    print(f"총 행 수: {len(df)}")
    print(f"소스 데이터가 있는 행 수: {len(df[df['file_name'] != ''])}")
    print(f"소스 데이터가 없는 행 수: {len(df[df['file_name'] == ''])}")
    
    # 매칭되지 않은 src_idx 목록 출력
    matched_indices = set(source_data_dict.keys())
    unmatched_indices = csv_src_indices - matched_indices
    if unmatched_indices:
        print(f"\n매칭되지 않은 src_idx ({len(unmatched_indices)}개):")
        for idx in sorted(unmatched_indices, key=int):
            print(f"  {idx}")
    
    return df

def main():
    # 기본 설정
    project_id = "26648"
    ticket_number = "PROJ-15441"
    
    # 명령행 인수 처리
    if len(sys.argv) > 1:
        project_id = sys.argv[1]
    if len(sys.argv) > 2:
        ticket_number = sys.argv[2]
    
    try:
        print(f"=== GCS에서 새 데이터 다운로드 중 (프로젝트ID: {project_id}, 티켓번호: {ticket_number}) ===")
        result_df = download_and_map_source_data(project_id, ticket_number)
        print("\n작업 완료!")
    except Exception as e:
        print(f"오류 발생: {e}")

if __name__ == "__main__":
    main()
