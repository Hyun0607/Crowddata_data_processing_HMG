#!/usr/bin/env python3
"""
환경 설정 관리 모듈
.env 파일에서 환경변수를 로드하고 관리합니다.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# 프로젝트 루트 디렉토리
PROJECT_ROOT = Path(__file__).parent

# .env 파일 로드
load_dotenv(PROJECT_ROOT / '.env')


class Config:
    """환경 설정 클래스"""
    
    # Google Cloud Platform 설정
    GCS_BUCKET = os.getenv('GCS_BUCKET', 'cw_platform')
    GCS_BASE_PATH = os.getenv('GCS_BASE_PATH', '1069')
    BIGQUERY_PROJECT = os.getenv('BIGQUERY_PROJECT', 'crowdworks-platform')
    BIGQUERY_DATASET_DM = os.getenv('BIGQUERY_DATASET_DM', 'crowdworks_dm')
    BIGQUERY_DATASET_DW = os.getenv('BIGQUERY_DATASET_DW', 'crowdworks_dw')
    
    # 파일명 설정
    RAW_CSV_FILE = os.getenv('RAW_CSV_FILE', 'bquxjobRaw.csv')
    FILTERED_CSV_FILE = os.getenv('FILTERED_CSV_FILE', 'bquxjobRaw_filtered.csv')
    
    # 병렬 처리 설정
    MAX_WORKERS = int(os.getenv('MAX_WORKERS', '3'))
    
    # 프로젝트 설정
    PROJECT_ID = os.getenv('PROJECT_ID', '')
    TICKET_NUMBER = os.getenv('TICKET_NUMBER', '')
    
    # 품질검증 설정
    QUALITY_CHECK_LIST_FILE = os.getenv('QUALITY_CHECK_LIST_FILE', '')
    
    @classmethod
    def get_gcs_prefix(cls, project_id):
        """GCS 경로 prefix 반환"""
        return f"gs://{cls.GCS_BUCKET}/{cls.GCS_BASE_PATH}/{project_id}_result/"
    
    @classmethod
    def get_bigquery_table(cls, dataset, table):
        """BigQuery 테이블 전체 경로 반환"""
        return f"{cls.BIGQUERY_PROJECT}.{dataset}.{table}"
    
    @classmethod
    def validate(cls):
        """필수 환경변수 검증"""
        required = {
            'GCS_BUCKET': cls.GCS_BUCKET,
            'BIGQUERY_PROJECT': cls.BIGQUERY_PROJECT,
        }
        
        missing = [key for key, value in required.items() if not value]
        
        if missing:
            raise ValueError(f"필수 환경변수가 설정되지 않았습니다: {', '.join(missing)}")
        
        return True


# 설정 인스턴스 (싱글톤)
config = Config()


if __name__ == "__main__":
    # 설정 테스트
    print("=== 환경 설정 ===")
    print(f"GCS Bucket: {config.GCS_BUCKET}")
    print(f"GCS Base Path: {config.GCS_BASE_PATH}")
    print(f"BigQuery Project: {config.BIGQUERY_PROJECT}")
    print(f"Max Workers: {config.MAX_WORKERS}")
    print(f"\nGCS Prefix (예시 - project_id=26640): {config.get_gcs_prefix('26640')}")
    print(f"BigQuery Table (예시): {config.get_bigquery_table(config.BIGQUERY_DATASET_DW, 'tb_prj_data')}")
    
    try:
        config.validate()
        print("\n✓ 필수 환경변수가 모두 설정되었습니다.")
    except ValueError as e:
        print(f"\n✗ {e}")

