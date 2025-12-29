#!/usr/bin/env python3
"""GCS 경로 기반 다운로드 스크립트 - CSV의 data_idx 기준으로 GCS에서 파일 다운로드"""

import pandas as pd
import subprocess
import sys
from pathlib import Path
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# 환경 설정 로드
sys.path.append(str(Path(__file__).parent.parent))
from config import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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

def list_gcs_files(project_id, data_indices_set):
    """GCS 파일 목록 조회 후 CSV의 data_idx로 필터링"""
    try:
        gcs_prefix = config.get_gcs_prefix(project_id)
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

def download_multiple_data(data_indices, output_dir, project_id, max_workers=3):
    """CSV의 data_idx로 필터링하여 병렬 다운로드"""
    data_indices_set = set(data_indices)
    filtered_files = list_gcs_files(project_id, data_indices_set)
    
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

def main():
    script_dir = Path(__file__).parent
    
    if len(sys.argv) < 2:
        logger.error("사용법: python gcs_path_downloader.py <project_id> [output_dir] [max_workers]")
        return 1
    
    project_id = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else f"{project_id}_result"
    max_workers = int(sys.argv[3]) if len(sys.argv) > 3 else config.MAX_WORKERS
    csv_file = script_dir / config.FILTERED_CSV_FILE
    
    logger.info(f"CSV: {csv_file}, 출력: {output_dir}, 프로젝트: {project_id}, 워커: {max_workers}")
    
    if not check_gsutil_available():
        logger.error("gsutil을 사용할 수 없습니다.")
        return 1
    
    if not csv_file.exists():
        logger.error(f"CSV 파일을 찾을 수 없습니다: {csv_file}")
        return 1
    
    data_indices = load_data_indices_from_csv(str(csv_file))
    if not data_indices:
        logger.error("다운로드할 data_idx가 없습니다.")
        return 1
    
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    logger.info("GCS 파일 다운로드를 시작합니다...")
    start_time = time.time()
    success_count, failure_count = download_multiple_data(data_indices, output_dir, project_id, max_workers)
    
    deleted_count = cleanup_unmatched_files(output_dir, set(data_indices))
    elapsed_time = time.time() - start_time
    
    logger.info(f"완료: 성공 {success_count}개, 실패 {failure_count}개, 삭제 {deleted_count}개, 소요시간 {elapsed_time:.2f}초")
    return 0 if failure_count == 0 else 1

if __name__ == "__main__":
    sys.exit(main())
