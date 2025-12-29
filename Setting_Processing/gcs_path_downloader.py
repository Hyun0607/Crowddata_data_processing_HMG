#!/usr/bin/env python3
"""
GCS 경로 기반 다운로드 스크립트
CSV 파일의 data_idx를 기준으로 gsutil을 사용해 GCS에서 결과 데이터를 다운로드합니다.
"""

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

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_data_indices_from_csv(csv_file_path):
    """
    CSV 파일에서 data_idx 컬럼을 읽어옵니다.
    
    Args:
        csv_file_path (str): CSV 파일 경로
        
    Returns:
        list: data_idx 리스트
    """
    try:
        df = pd.read_csv(csv_file_path)
        
        if 'data_idx' not in df.columns:
            logger.error("CSV 파일에 'data_idx' 컬럼이 없습니다.")
            return []
        
        # 중복 제거 및 정렬
        data_indices = sorted(df['data_idx'].unique().tolist())
        logger.info(f"CSV에서 {len(data_indices)}개의 고유한 data_idx를 찾았습니다.")
        
        return data_indices
        
    except Exception as e:
        logger.error(f"CSV 파일 읽기 오류: {e}")
        return []

def download_gcs_data(data_idx, output_dir, project_id):
    """
    gsutil을 사용하여 특정 data_idx에 해당하는 파일을 다운로드합니다.
    
    Args:
        data_idx (int): 데이터 인덱스
        output_dir (str): 출력 디렉토리
        project_id (str): 프로젝트 ID
        
    Returns:
        bool: 성공 여부
    """
    try:
        # GCS 경로 구성 (특정 data_idx 파일)
        gcs_path = config.get_gcs_prefix(project_id) + f"{data_idx}_*"
        
        # 출력 디렉토리 생성
        local_dir = Path(output_dir)
        local_dir.mkdir(parents=True, exist_ok=True)
        
        # gsutil 명령어 구성 (특정 패턴의 파일들만 복사)
        cmd = [
            "gsutil", "-m", "cp",  # 멀티스레드로 복사
            gcs_path,  # 특정 data_idx로 시작하는 파일들
            str(local_dir) + "/"
        ]
        
        logger.info(f"파일 다운로드 시작: {data_idx}")
        
        # gsutil 실행
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300  # 5분 타임아웃
        )
        
        if result.returncode == 0:
            # 다운로드된 파일 수 확인
            downloaded_files = list(local_dir.glob(f"{data_idx}_*"))
            downloaded_count = len(downloaded_files)
            
            logger.info(f"성공: {data_idx} - {downloaded_count}개 파일 다운로드")
            return True
        else:
            logger.error(f"다운로드 실패 ({data_idx}): {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error(f"다운로드 타임아웃 ({data_idx})")
        return False
    except Exception as e:
        logger.error(f"다운로드 오류 ({data_idx}): {e}")
        return False

def download_multiple_data(data_indices, output_dir, project_id, max_workers=3):
    """
    여러 data_idx에 대한 파일들을 병렬로 다운로드합니다.
    
    Args:
        data_indices (list): 데이터 인덱스 리스트
        output_dir (str): 출력 디렉토리
        project_id (str): 프로젝트 ID
        max_workers (int): 최대 워커 수
        
    Returns:
        tuple: (성공한 개수, 실패한 개수)
    """
    success_count = 0
    failure_count = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 작업 제출
        future_to_idx = {
            executor.submit(download_gcs_data, idx, output_dir, project_id): idx 
            for idx in data_indices
        }
        
        # 결과 처리
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            try:
                if future.result():
                    success_count += 1
                else:
                    failure_count += 1
            except Exception as e:
                logger.error(f"작업 실행 오류 ({idx}): {e}")
                failure_count += 1
    
    return success_count, failure_count

def check_gsutil_available():
    """
    gsutil이 사용 가능한지 확인합니다.
    
    Returns:
        bool: gsutil 사용 가능 여부
    """
    try:
        result = subprocess.run(
            ["gsutil", "version"],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            logger.info("gsutil이 사용 가능합니다.")
            return True
        else:
            logger.error("gsutil을 찾을 수 없습니다.")
            return False
    except FileNotFoundError:
        logger.error("gsutil이 설치되지 않았습니다.")
        return False

def main():
    """메인 함수"""
    # 기본 설정 (환경변수에서 읽기)
    csv_file = config.RAW_CSV_FILE
    project_id = config.PROJECT_ID or "26648"  # 기본 프로젝트 ID
    max_workers = config.MAX_WORKERS  # 동시 다운로드 수 제한
    
    # 명령행 인수 처리
    if len(sys.argv) > 1:
        project_id = sys.argv[1]
    if len(sys.argv) > 2:
        csv_file = sys.argv[2]
    if len(sys.argv) > 3:
        max_workers = int(sys.argv[3])
    
    # 출력 디렉토리는 프로젝트 ID 기반으로 자동 생성
    output_dir = f"{project_id}_result"
    
    logger.info(f"프로젝트 ID: {project_id}")
    logger.info(f"CSV 파일: {csv_file}")
    logger.info(f"출력 디렉토리: {output_dir}")
    logger.info(f"최대 워커 수: {max_workers}")
    
    # gsutil 사용 가능 여부 확인
    if not check_gsutil_available():
        logger.error("gsutil을 사용할 수 없습니다. Google Cloud SDK를 설치하세요.")
        return 1
    
    # CSV 파일에서 data_idx 읽기
    data_indices = load_data_indices_from_csv(csv_file)
    
    if not data_indices:
        logger.error("다운로드할 data_idx가 없습니다.")
        return 1
    
    # 출력 디렉토리 생성
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # 다운로드 시작
    logger.info("GCS 파일 다운로드를 시작합니다...")
    start_time = time.time()
    
    success_count, failure_count = download_multiple_data(
        data_indices, output_dir, project_id, max_workers
    )
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    # 결과 출력
    logger.info(f"파일 다운로드 완료: 성공 {success_count}개, 실패 {failure_count}개")
    logger.info(f"소요 시간: {elapsed_time:.2f}초")
    
    if failure_count > 0:
        logger.warning(f"{failure_count}개의 파일 다운로드에 실패했습니다.")
        return 
    else:
        logger.info("모든 파일이 성공적으로 다운로드되었습니다.")
        return 0

if __name__ == "__main__":
    sys.exit(main())
