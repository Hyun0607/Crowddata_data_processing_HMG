"""
품질검증용 대상 데이터ID를 제외하고 삭제
PROJ-15399_26946_list_251112에서 품질검증용 대상 데이터ID 확인
bquxjobRaw.csv에서 대상 데이터ID를 제외하고 삭제
"""
import pandas as pd
from pathlib import Path
import os
import sys

# 환경 설정 로드
sys.path.append(str(Path(__file__).parent.parent))
from config import config

def main():
    """메인 함수"""
    # 작업 디렉토리 설정 (스크립트가 있는 디렉토리)
    script_dir = Path(__file__).parent
    os.chdir(script_dir)
    
    # CSV 파일 경로 설정 (환경변수 또는 명령행 인수로 받기)
    quality_check_list_file = config.QUALITY_CHECK_LIST_FILE or sys.argv[1] if len(sys.argv) > 1 else "PROJ-15488_26966_list_251127.csv"
    raw_data_file = config.RAW_CSV_FILE
    
    quality_check_list_path = Path(script_dir) / quality_check_list_file
    raw_data_path = Path(script_dir) / raw_data_file
    
    # 품질검증용 대상 데이터ID 목록 읽기
    print(f"품질검증용 대상 데이터ID 목록 읽기: {quality_check_list_file}")
    quality_check_df = pd.read_csv(quality_check_list_path)
    quality_check_data_ids = set(quality_check_df['dataID'].tolist())
    print(f"품질검증용 대상 데이터ID 개수: {len(quality_check_data_ids)}")
    
    # 원본 데이터 읽기
    print(f"\n원본 데이터 읽기: {raw_data_file}")
    df = pd.read_csv(raw_data_path)
    print(f"원본 데이터 행 수: {len(df)}")
    
    # 품질검증용 대상 데이터ID만 필터링 (제외하고 삭제 = 포함된 것만 남김)
    filtered_df = df[df['data_idx'].isin(quality_check_data_ids)].copy()
    print(f"필터링 후 데이터 행 수: {len(filtered_df)}")
    
    # 결과 저장
    output_file = config.FILTERED_CSV_FILE
    output_path = Path(script_dir) / output_file
    filtered_df.to_csv(output_path, index=False)
    print(f"\n필터링된 데이터 저장 완료: {output_file}")
    
    # 삭제된 행 수 출력
    deleted_count = len(df) - len(filtered_df)
    print(f"삭제된 행 수: {deleted_count}")
    
    # 품질검증용 대상 데이터ID 중 실제로 존재하는 ID 확인
    existing_ids = set(filtered_df['data_idx'].tolist())
    missing_ids = quality_check_data_ids - existing_ids
    if missing_ids:
        print(f"\n경고: 품질검증용 대상 데이터ID 중 {len(missing_ids)}개가 원본 데이터에 없습니다.")
        print(f"누락된 데이터ID: {sorted(missing_ids)}")


if __name__ == "__main__":
    main()
