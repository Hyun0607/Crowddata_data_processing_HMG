#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""GCS 경로 기반 다운로드 스크립트 - CSV의 data_idx 기준으로 GCS에서 파일 다운로드"""

import pandas as pd
import subprocess
import sys
from pathlib import Path
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ========== 설정 섹션 ==========
# 스크립트 디렉토리 기준 경로
SCRIPT_DIR = Path(__file__).parent

# CSV 파일 경로 (난이도별)
CSV_CONFIGS = {
    '하': {
        'csv_file': SCRIPT_DIR / "bquxjobRAW_26759_하.csv",
        'gcs_prefix': "gs://cw_platform/1076/26759_result/",
        'output_dir': SCRIPT_DIR / "26759_result"
    },
    '중': {
        'csv_file': SCRIPT_DIR / "bquxjob_26795_중.csv",
        'gcs_prefix': "gs://cw_platform/1075/26795_result/",
        'output_dir': SCRIPT_DIR / "26795_result"   
    },
    '상': {
        'csv_file': SCRIPT_DIR / "bquxjobRaw_filtered_26994_상.csv",
        'gcs_prefix': "gs://cw_platform/1069/26994_result/",
        'output_dir': SCRIPT_DIR / "26994_result"
    }
}

# 병렬 다운로드 워커 수
MAX_WORKERS = 3
# ==============================

def load_data_indices_from_csv(csv_file_path):
    """CSV에서 data_idx 컬럼 읽기"""
    try:
        df = pd.read_csv(csv_file_path)
        if 'data_idx' not in df.columns:
            logger.error("CSV 파일에 'data_idx' 컬럼이 없습니다.")
            return []
        data_indices = sorted(df['data_idx'].unique().tolist())
        logger.info(f"CSV에서 {len(data_indices)}개의 고유한 data_idx를 찾았습니다.")
        return data_indices
    except Exception as e:
        logger.error(f"CSV 파일 읽기 오류: {e}")
        return []

def list_gcs_files(gcs_prefix, data_indices_set):
    """GCS 파일 목록 조회 후 CSV의 data_idx로 필터링"""
    try:
        # GCS 경로가 /로 끝나지 않으면 추가
        if not gcs_prefix.endswith('/'):
            gcs_prefix = gcs_prefix + '/'
        
        result = subprocess.run(["gsutil", "ls", gcs_prefix + "*"], 
                               capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            logger.error(f"GCS 파일 목록 조회 실패: {result.stderr}")
            return []
        
        gcs_files = [f for f in result.stdout.strip().split('\n') if f.strip()]
        filtered = [f for f in gcs_files 
                   if any(f.split('/')[-1].startswith(f"{idx}_") for idx in data_indices_set)]
        
        logger.info(f"GCS에서 {len(gcs_files)}개 파일 중 {len(filtered)}개 파일이 CSV와 매칭되었습니다.")
        return filtered
    except Exception as e:
        logger.error(f"GCS 파일 목록 조회 오류: {e}")
        return []

def download_gcs_file(gcs_file_path, output_dir):
    """개별 GCS 파일 다운로드"""
    try:
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        result = subprocess.run(["gsutil", "cp", gcs_file_path, str(Path(output_dir)) + "/"],
                               capture_output=True, text=True, timeout=300)
        return result.returncode == 0
    except Exception as e:
        logger.error(f"다운로드 오류 ({gcs_file_path}): {e}")
        return False

def download_multiple_data(data_indices, output_dir, gcs_prefix, max_workers=3):
    """CSV의 data_idx로 필터링하여 병렬 다운로드"""
    data_indices_set = set(data_indices)
    filtered_files = list_gcs_files(gcs_prefix, data_indices_set)
    
    if not filtered_files:
        logger.warning("CSV와 매칭되는 GCS 파일이 없습니다.")
        return 0, 0
    
    logger.info(f"총 {len(filtered_files)}개 파일을 다운로드합니다.")
    success_count = failure_count = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(download_gcs_file, f, output_dir): f for f in filtered_files}
        for future in as_completed(futures):
            if future.result():
                success_count += 1
                if success_count % 10 == 0:
                    logger.info(f"진행 상황: {success_count}/{len(filtered_files)} 파일 다운로드 완료")
            else:
                failure_count += 1
    
    return success_count, failure_count

def cleanup_unmatched_files(output_dir, data_indices_set):
    """CSV에 없는 파일 삭제"""
    try:
        local_dir = Path(output_dir)
        if not local_dir.exists():
            return 0
        
        deleted_count = 0
        for file_path in local_dir.glob("*"):
            if file_path.is_file() and '_' in file_path.name:
                try:
                    data_idx = int(file_path.name.split('_')[0])
                    if data_idx not in data_indices_set:
                        file_path.unlink()
                        deleted_count += 1
                except ValueError:
                    pass
        
        if deleted_count > 0:
            logger.info(f"총 {deleted_count}개의 CSV에 없는 파일을 삭제했습니다.")
        return deleted_count
    except Exception as e:
        logger.error(f"파일 정리 오류: {e}")
        return 0

def check_gsutil_available():
    """gsutil 사용 가능 여부 확인"""
    try:
        result = subprocess.run(["gsutil", "version"], capture_output=True, text=True)
        return result.returncode == 0
    except FileNotFoundError:
        return False

def process_difficulty(difficulty, config):
    """난이도별 다운로드 처리"""
    csv_file = config['csv_file']
    gcs_prefix = config['gcs_prefix']
    output_dir = config['output_dir']
    
    logger.info(f"\n{'='*60}")
    logger.info(f"난이도: {difficulty}")
    logger.info(f"CSV: {csv_file}")
    logger.info(f"GCS 경로: {gcs_prefix}")
    logger.info(f"출력 디렉토리: {output_dir}")
    logger.info(f"{'='*60}\n")
    
    if not csv_file.exists():
        logger.error(f"CSV 파일을 찾을 수 없습니다: {csv_file}")
        return False
    
    data_indices = load_data_indices_from_csv(str(csv_file))
    if not data_indices:
        logger.warning(f"난이도 {difficulty}: 다운로드할 data_idx가 없습니다.")
        return False
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"난이도 {difficulty}: GCS 파일 다운로드를 시작합니다...")
    start_time = time.time()
    success_count, failure_count = download_multiple_data(data_indices, str(output_dir), gcs_prefix, MAX_WORKERS)
    
    deleted_count = cleanup_unmatched_files(str(output_dir), set(data_indices))
    elapsed_time = time.time() - start_time
    
    logger.info(f"난이도 {difficulty} 완료: 성공 {success_count}개, 실패 {failure_count}개, 삭제 {deleted_count}개, 소요시간 {elapsed_time:.2f}초")
    return failure_count == 0

def main():
    """메인 함수 - 설정된 경로로 다운로드 실행"""
    if not check_gsutil_available():
        logger.error("gsutil을 사용할 수 없습니다.")
        return 1
    
    # 명령줄 인자로 특정 난이도 지정 가능 (선택사항)
    target_difficulties = []
    if len(sys.argv) > 1:
        # 난이도 지정: python script.py 하 중
        target_difficulties = sys.argv[1:]
        # 유효한 난이도만 필터링
        target_difficulties = [d for d in target_difficulties if d in CSV_CONFIGS]
        if not target_difficulties:
            logger.warning(f"유효하지 않은 난이도입니다. 사용 가능한 난이도: {list(CSV_CONFIGS.keys())}")
            logger.info("모든 난이도를 처리합니다.")
            target_difficulties = list(CSV_CONFIGS.keys())
    else:
        # 난이도 미지정 시 모든 난이도 처리
        target_difficulties = list(CSV_CONFIGS.keys())
    
    logger.info(f"처리할 난이도: {', '.join(target_difficulties)}")
    
    all_success = True
    for difficulty in target_difficulties:
        config = CSV_CONFIGS[difficulty]
        success = process_difficulty(difficulty, config)
        if not success:
            all_success = False
    
    logger.info(f"\n{'='*60}")
    logger.info("전체 다운로드 작업 완료")
    logger.info(f"{'='*60}\n")
    
    return 0 if all_success else 1

if __name__ == "__main__":
    sys.exit(main())
