#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV의 src_idx와 GCS 소스 데이터를 매핑해서 CSV에 파일명을 추가하고,
이미지 정렬 기준에 따라 정렬하는 스크립트
"""

import pandas as pd
import os
import re
import json
import csv
import sys
from pathlib import Path
import logging
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ========== 설정 섹션 ==========
# 스크립트 디렉토리 기준 경로
SCRIPT_DIR = Path(__file__).parent

# 난이도별 설정
DIFFICULTY_CONFIGS = {
    '하': {
        'csv_file': SCRIPT_DIR / "bquxjobRAW_26759_하.csv",
        'gcs_prefix': "1076/26759_source/_source_data/",
        'output_file': "PROJ-15442_26759_하.csv",
        'org_id': "1076",
        'project_id': "26759"
    },
    '중': {
        'csv_file': SCRIPT_DIR / "bquxjob_26795_중.csv",
        'gcs_prefix': "1075/26795_source/_source_data/",
        'output_file': "PROJ-15684_26795_중.csv",
        'org_id': "1075",
        'project_id': "26795"
    },
    '상': {
        'csv_file': SCRIPT_DIR / "bquxjobRaw_filtered_26994_상.csv",
        'gcs_prefix': "1069/26994_source/_source_data/",
        'output_file': "PROJ-15684_26994_상.csv",
        'org_id': "1069",
        'project_id': "26994"
    }
}

# GCS 버킷 이름
BUCKET_NAME = "cw_platform"
# ==============================

def load_image_sort_order(difficulty, org_id, project_id):
    """이미지 정렬 기준 파일을 로드하여 정렬 순서를 반환"""
    try:
        sort_file = SCRIPT_DIR / "1075+1069_26795+26994_하와이 한인 잡지 공동보, 동지별보, 태평양주보_179장(251211).csv"
        
        if not sort_file.exists():
            logger.error(f"정렬 기준 파일을 찾을 수 없습니다: {sort_file}")
            return []
        
        with open(sort_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            lines = list(reader)
        
        # 첫 번째 줄은 헤더이므로 제외하고 조건에 맞는 파일명만 추출
        # CSV 구조: 난이도(0), 기준번호(1), 카테고리명(2), 파일명(3), 조직ID(4), 프로젝트ID(5), 프리셋사용여부(6)
        image_order = []
        for row in lines[1:]:  # 첫 번째 줄(헤더) 제외
            if row and len(row) >= 6:  # 최소 6개 컬럼이 있어야 함
                row_difficulty = row[0].strip()  # 난이도
                filename = row[3].strip()    # 파일명 (4번째 컬럼)
                row_org_id = row[4].strip()      # 조직ID (5번째 컬럼)
                row_project_id = row[5].strip()  # 프로젝트ID (6번째 컬럼)
                
                # 조건: 난이도, 조직ID, 프로젝트ID가 일치하는 경우
                if row_difficulty == difficulty and row_org_id == org_id and row_project_id == project_id:
                    if filename:
                        image_order.append(filename)
                        
        logger.info(f"난이도 {difficulty}: 이미지 정렬 기준 로드 완료: {len(image_order)}개 파일")
        return image_order
        
    except Exception as e:
        logger.error(f"정렬 기준 파일 로드 오류: {e}")
        return []

def clean_filename(filename):
    """파일명에서 특수문자를 제거하고 깔끔하게 정리"""
    if not filename:
        return filename
    
    # 특수문자 제거 (한글, 영문, 숫자, 점, 하이픈만 남김)
    clean_name = re.sub(r'[^\w가-힣.-]', '', filename)
    
    # 연속된 점이나 하이픈 제거
    clean_name = re.sub(r'[.-]+', '.', clean_name)
    
    # 확장자가 없으면 .jpg 추가
    if '.' not in clean_name:
        clean_name += '.jpg'
    
    return clean_name

def extract_filename_from_source_file(src_idx, bucket_name, prefix):
    """GCS 소스 데이터 파일에서 filename을 추출"""
    
    # 크레덴셜 파일 경로 설정
    credential_path = "../crowdworks-platform-3a4e1c7cbb9f.json"
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
    
    try:
        # GCS 클라이언트 초기화
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        
        # 소스 파일명 패턴: {src_idx}_* 형태로 찾기
        blobs = list(bucket.list_blobs(prefix=f"{prefix}{src_idx}_"))

        if not blobs:
            logger.warning(f"GCS에서 소스 파일을 찾을 수 없습니다: {prefix}{src_idx}_*")
            return ""
        
        # 첫 번째 매칭되는 파일 사용
        blob = blobs[0]
        logger.debug(f"GCS 소스 파일 발견: {blob.name}")
        
        # GCS에서 파일 내용 다운로드
        content = blob.download_as_text(encoding='utf-8')
        
        # JSON 파싱 시도
        try:
            data = json.loads(content)
            # 'file_name' 또는 'filename' 키 모두 시도
            filename = data.get('file_name', '') or data.get('filename', '')
            if filename:
                # 파일명 정리: 특수문자 제거하고 깔끔하게 만들기
                clean_file_name = clean_filename(filename)
                logger.debug(f"파일명 추출 완료: {src_idx} -> {clean_file_name}")
                return clean_file_name
        except json.JSONDecodeError:
            # JSON이 아닌 경우 정규식으로 filename 추출 시도
            # 'file_name' 또는 'filename' 패턴 모두 시도
            filename_match = re.search(r'"file_name":"([^"]+)"', content)
            if not filename_match:
                filename_match = re.search(r'"filename":"([^"]+)"', content)
            if filename_match:
                filename = filename_match.group(1)
                clean_file_name = clean_filename(filename)
                logger.debug(f"정규식으로 파일명 추출: {src_idx} -> {clean_file_name}")
                return clean_file_name
        
        logger.warning(f"파일명을 찾을 수 없습니다: {src_idx}")
        return ""
        
    except Exception as e:
        logger.error(f"GCS 소스 파일 처리 오류 ({src_idx}): {e}")
        return ""

def sort_filenames_by_order(filenames, sort_order):
    """파일명들을 정렬 기준에 따라 정렬"""
    if not sort_order:
        return sorted(filenames)
    
    # 정렬 기준에서 각 파일명의 인덱스를 찾아서 정렬
    def get_sort_key(filename):
        try:
            return sort_order.index(filename)
        except ValueError:
            # 정렬 기준에 없는 파일은 맨 뒤로
            return len(sort_order) + hash(filename) % 1000
    
    return sorted(filenames, key=get_sort_key)

def process_csv_with_gcs_sources(difficulty, config):
    """CSV의 src_idx와 GCS 소스 데이터를 매핑하여 파일명을 CSV에 추가"""
    
    csv_file = config['csv_file']
    prefix = config['gcs_prefix']
    output_file = config['output_file']
    org_id = config['org_id']
    project_id = config['project_id']
    
    logger.info(f"\n{'='*60}")
    logger.info(f"난이도: {difficulty}")
    logger.info(f"CSV 파일 로드: {csv_file}")
    logger.info(f"GCS 버킷: {BUCKET_NAME}")
    logger.info(f"GCS prefix: {prefix}")
    logger.info(f"출력 파일: {output_file}")
    logger.info(f"{'='*60}\n")
    
    if not csv_file.exists():
        logger.error(f"CSV 파일을 찾을 수 없습니다: {csv_file}")
        return None
    
    df = pd.read_csv(csv_file)
    logger.info(f"CSV 행 수: {len(df)}")
    
    # src_idx 컬럼이 있는지 확인
    if 'src_idx' not in df.columns:
        logger.error("CSV 파일에 'src_idx' 컬럼이 없습니다.")
        return None
    
    # 이미지 정렬 기준 로드
    logger.info("이미지 정렬 기준 로드 중...")
    sort_order = load_image_sort_order(difficulty, org_id, project_id)
    
    if not sort_order:
        logger.warning("정렬 기준을 로드할 수 없습니다. 기본 정렬을 사용합니다.")
    
    # 고유한 src_idx 목록 가져오기
    unique_src_indices = df['src_idx'].unique().tolist()
    logger.info(f"고유한 src_idx 개수: {len(unique_src_indices)}")
    
    # 각 src_idx에 대해 소스 데이터에서 파일명 추출
    logger.info("소스 데이터에서 파일명 추출 중...")
    source_data_dict = {}
    
    def process_src_idx(src_idx):
        filename = extract_filename_from_source_file(src_idx, BUCKET_NAME, prefix)
        return src_idx, filename
    
    # 병렬 처리로 파일명 추출
    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(process_src_idx, unique_src_indices))
    
    # 결과를 딕셔너리에 저장
    for src_idx, filename in results:
        source_data_dict[src_idx] = filename
        if filename:
            logger.info(f"src_idx {src_idx}: {filename}")
        else:
            logger.warning(f"src_idx {src_idx}: 파일명 없음")
    
    # CSV에 file_name 열 추가
    df['file_name'] = df['src_idx'].map(lambda x: source_data_dict.get(x, ''))
    
    # 정렬 기준 파일의 순서대로 CSV 행들을 정렬
    if sort_order:
        logger.info("정렬 기준에 따라 CSV 행들을 정렬 중...")
        
        # 각 행의 file_name에서 첫 번째 파일명을 추출하여 정렬 키로 사용
        def get_sort_key_for_row(row):
            file_name = row['file_name']
            if not file_name:
                return len(sort_order) + 1000  # file_name이 없는 행은 맨 뒤로
            
            # 첫 번째 파일명 추출
            first_filename = file_name.split(',')[0].strip()
            
            try:
                return sort_order.index(first_filename)
            except ValueError:
                # 정렬 기준에 없는 파일은 맨 뒤로
                return len(sort_order) + hash(first_filename) % 1000
        
        # 정렬 기준에 따라 DataFrame 정렬
        df['sort_key'] = df.apply(get_sort_key_for_row, axis=1)
        df = df.sort_values('sort_key').drop('sort_key', axis=1)
        
        logger.info(f"정렬 완료: {len(df)}개 행")
    
    # 컬럼 순서 정의 (원하는 순서대로 나열)
    original_columns = df.columns.tolist()
    
    # file_name이 이미 있으면 제거하고 src_idx 다음에 삽입
    if 'file_name' in original_columns:
        original_columns.remove('file_name')
    
    # file_name을 src_idx 다음에 삽입
    if 'src_idx' in original_columns:
        src_idx_index = original_columns.index('src_idx')
        column_order = original_columns[:src_idx_index+1] + ['file_name'] + original_columns[src_idx_index+1:]
    else:
        # src_idx가 없으면 맨 앞에 추가
        column_order = ['file_name'] + original_columns
    
    # 결과 CSV 저장
    output_path = SCRIPT_DIR / output_file
    df.to_csv(output_path, index=False, encoding='utf-8-sig', columns=column_order)
    logger.info(f"결과 CSV 저장 완료: {output_path}")
    
    # 통계 출력
    logger.info(f"\n=== 난이도 {difficulty} 결과 통계 ===")
    logger.info(f"총 행 수: {len(df)}")
    logger.info(f"file_name이 있는 행 수: {len(df[df['file_name'] != ''])}")
    logger.info(f"file_name이 없는 행 수: {len(df[df['file_name'] == ''])}")
    
    # 매칭되지 않은 src_idx 목록 출력
    matched_indices = set([k for k, v in source_data_dict.items() if v])
    unmatched_indices = set(unique_src_indices) - matched_indices
    if unmatched_indices:
        logger.warning(f"\n매칭되지 않은 src_idx ({len(unmatched_indices)}개):")
        for idx in sorted(unmatched_indices):
            logger.warning(f"  {idx}")
    
    return df

def main():
    """메인 함수 - 설정된 난이도별로 처리"""
    try:
        logger.info("=== CSV와 GCS 소스 데이터 매핑 시작 ===")
        
        # 명령줄 인자로 특정 난이도 지정 가능 (선택사항)
        target_difficulties = []
        if len(sys.argv) > 1:
            # 난이도 지정: python script.py 하 중
            target_difficulties = sys.argv[1:]
            # 유효한 난이도만 필터링
            target_difficulties = [d for d in target_difficulties if d in DIFFICULTY_CONFIGS]
            if not target_difficulties:
                logger.warning(f"유효하지 않은 난이도입니다. 사용 가능한 난이도: {list(DIFFICULTY_CONFIGS.keys())}")
                logger.info("모든 난이도를 처리합니다.")
                target_difficulties = list(DIFFICULTY_CONFIGS.keys())
        else:
            # 난이도 미지정 시 모든 난이도 처리
            target_difficulties = list(DIFFICULTY_CONFIGS.keys())
        
        logger.info(f"처리할 난이도: {', '.join(target_difficulties)}")
        
        all_success = True
        for difficulty in target_difficulties:
            config = DIFFICULTY_CONFIGS[difficulty]
            result_df = process_csv_with_gcs_sources(difficulty, config)
            
            if result_df is None:
                logger.error(f"난이도 {difficulty} 처리 실패!")
                all_success = False
            else:
                logger.info(f"난이도 {difficulty} 처리 완료!")
        
        logger.info(f"\n{'='*60}")
        logger.info("전체 작업 완료")
        logger.info(f"{'='*60}\n")
        
        return 0 if all_success else 1
            
    except Exception as e:
        logger.error(f"오류 발생: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
