#!/usr/bin/env python3
"""
파이프라인 실행 스크립트
4개의 Python 스크립트를 순차적으로 실행
순서: gcs_path_downloader.py -> json_formatter.py -> csv_with_source_data.py -> data_setting_1111.py
"""

import subprocess
import sys
import os
import logging
from pathlib import Path
import time

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_script(script_path, script_name, *args):
    """
    Python 스크립트를 실행합니다.
    
    Args:
        script_path (str): 실행할 스크립트 경로
        script_name (str): 스크립트 이름 (로깅용)
        *args: 스크립트에 전달할 추가 인수
    
    Returns:
        bool: 성공 여부
    """
    script_file = Path(script_path)
    
    if not script_file.exists():
        logger.error(f"스크립트 파일을 찾을 수 없습니다: {script_path}")
        return False
    
    logger.info(f"=" * 60)
    logger.info(f"[{script_name}] 실행 시작")
    logger.info(f"=" * 60)
    
    start_time = time.time()
    
    try:
        # Python 스크립트 실행
        cmd = [sys.executable, str(script_file)] + list(args)
        logger.info(f"실행 명령: {' '.join(cmd)}")
        
        result = subprocess.run(
            cmd,
            capture_output=False,  # 실시간 출력을 보기 위해 False로 설정
            text=True,
            check=False  # 에러가 발생해도 예외를 발생시키지 않음
        )
        
        elapsed_time = time.time() - start_time
        
        if result.returncode == 0:
            logger.info(f"[{script_name}] 실행 완료 (소요 시간: {elapsed_time:.2f}초)")
            logger.info("")
            return True
        else:
            logger.error(f"[{script_name}] 실행 실패 (종료 코드: {result.returncode})")
            logger.error(f"소요 시간: {elapsed_time:.2f}초")
            logger.error("")
            return False
            
    except Exception as e:
        elapsed_time = time.time() - start_time
        logger.error(f"[{script_name}] 실행 중 오류 발생: {e}")
        logger.error(f"소요 시간: {elapsed_time:.2f}초")
        logger.error("")
        return False


def get_user_input():
    """사용자로부터 프로젝트 정보 입력받기"""
    print("\n" + "=" * 60)
    print("파이프라인 설정")
    print("=" * 60)
    
    # 명령행 인수로 입력받기
    if len(sys.argv) >= 3:
        source_project_id = sys.argv[1]
        ticket_number = sys.argv[2]
        target_project_id = sys.argv[3] if len(sys.argv) > 3 else source_project_id
        print(f"명령행 인수로 입력받음:")
        print(f"  소스 프로젝트 ID: {source_project_id}")
        print(f"  티켓번호: {ticket_number}")
        print(f"  타겟 프로젝트 ID: {target_project_id}")
        return source_project_id, ticket_number, target_project_id
    
    # 대화형 입력
    print("\n프로젝트 정보를 입력해주세요:")
    source_project_id = input("소스 프로젝트 ID (예: 26648): ").strip()
    if not source_project_id:
        logger.error("소스 프로젝트 ID를 입력해야 합니다.")
        sys.exit(1)
    
    ticket_number = input("티켓번호 (예: PROJ-15441): ").strip()
    if not ticket_number:
        logger.error("티켓번호를 입력해야 합니다.")
        sys.exit(1)
    
    target_project_id = input("타겟 프로젝트 ID (예: 26966, 기본값: 소스 프로젝트 ID와 동일): ").strip()
    if not target_project_id:
        target_project_id = source_project_id
    
    print(f"\n입력된 정보:")
    print(f"  소스 프로젝트 ID: {source_project_id}")
    print(f"  티켓번호: {ticket_number}")
    print(f"  타겟 프로젝트 ID: {target_project_id}")
    
    confirm = input("\n위 정보로 파이프라인을 실행하시겠습니까? (y/n): ").strip().lower()
    if confirm != 'y':
        logger.info("파이프라인 실행이 취소되었습니다.")
        sys.exit(0)
    
    return source_project_id, ticket_number, target_project_id


def main():
    """메인 함수 - 파이프라인 실행"""
    # 작업 디렉토리 설정 (스크립트가 있는 디렉토리)
    script_dir = Path(__file__).parent
    os.chdir(script_dir)
    
    # 사용자 입력 받기
    source_project_id, ticket_number, target_project_id = get_user_input()
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("파이프라인 실행 시작")
    logger.info("=" * 60)
    logger.info(f"소스 프로젝트 ID: {source_project_id}")
    logger.info(f"타겟 프로젝트 ID: {target_project_id}")
    logger.info(f"티켓번호: {ticket_number}")
    logger.info("")
    
    pipeline_start_time = time.time()
    
    # 실행할 스크립트 목록 (순서대로)
    scripts = [
        {
            "path": "gcs_path_downloader.py",
            "name": "1. GCS 경로 다운로드",
            "args": [source_project_id]  # 프로젝트 ID 전달
        },
        {
            "path": "json_formatter.py",
            "name": "2. JSON 포맷터",
            "args": [source_project_id]  # 프로젝트 ID 전달
        },
        {
            "path": "csv_with_source_data.py",
            "name": "3. CSV 소스 데이터 매핑",
            "args": [source_project_id, ticket_number]  # 프로젝트 ID, 티켓번호 전달
        },
        {
            "path": "data_setting_1209.py",
            "name": "4. 데이터 설정",
            "args": [source_project_id, target_project_id, ticket_number]  # 소스 프로젝트 ID, 타겟 프로젝트 ID, 티켓번호 전달
        }
    ]
    
    success_count = 0
    failure_count = 0
    
    # 각 스크립트 순차 실행
    for idx, script_info in enumerate(scripts, 1):
        script_path = script_dir / script_info["path"]
        
        if run_script(script_path, script_info["name"], *script_info["args"]):
            success_count += 1
        else:
            failure_count += 1
            logger.error("=" * 60)
            logger.error(f"파이프라인 중단: [{script_info['name']}] 실행 실패")
            logger.error("=" * 60)
            logger.error("")
            break  # 실패 시 파이프라인 중단
    
    # 전체 결과 출력
    pipeline_elapsed_time = time.time() - pipeline_start_time
    
    logger.info("=" * 60)
    logger.info("파이프라인 실행 완료")
    logger.info("=" * 60)
    logger.info(f"성공: {success_count}개, 실패: {failure_count}개")
    logger.info(f"총 소요 시간: {pipeline_elapsed_time:.2f}초 ({pipeline_elapsed_time/60:.2f}분)")
    logger.info("")
    
    if failure_count > 0:
        logger.error("파이프라인 실행 중 오류가 발생했습니다.")
        return 1
    else:
        logger.info("모든 스크립트가 성공적으로 실행되었습니다!")
        return 0


if __name__ == "__main__":
    sys.exit(main())

