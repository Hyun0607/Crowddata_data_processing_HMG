#!/usr/bin/env python3
"""
JSON 파일 포맷터
26640_result 디렉토리의 모든 파일을 JSON으로 변환하고 들여쓰기를 적용합니다.
"""

import json
import os
import sys
from pathlib import Path
import logging

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def format_json_file(input_file_path, output_file_path=None):
    """
    단일 JSON 파일을 포맷팅합니다.
    
    Args:
        input_file_path (str): 입력 파일 경로
        output_file_path (str, optional): 출력 파일 경로. None이면 원본 파일을 덮어씁니다.
    
    Returns:
        bool: 성공 여부
    """
    try:
        # 파일 읽기
        with open(input_file_path, 'r', encoding='utf-8') as f:
            content = f.read().strip()
        
        # JSON 파싱
        try:
            json_data = json.loads(content)
        except json.JSONDecodeError as e:
            logger.error(f"JSON 파싱 오류 ({input_file_path}): {e}")
            return False
        
        # 들여쓰기 적용하여 JSON 포맷팅
        formatted_json = json.dumps(json_data, ensure_ascii=False, indent=2)
        
        # 출력 파일 경로 설정
        if output_file_path is None:
            output_file_path = input_file_path
        
        # 포맷팅된 JSON 저장
        with open(output_file_path, 'w', encoding='utf-8') as f:
            f.write(formatted_json)
        
        # 파일 확장자를 .json으로 변경
        if not output_file_path.endswith('.json'):
            json_path = output_file_path + '.json'
            os.rename(output_file_path, json_path)
            logger.info(f"성공: {input_file_path} -> {json_path}")
        else:
            logger.info(f"성공: {input_file_path} -> {output_file_path}")
        
        return True
        
    except Exception as e:
        logger.error(f"파일 처리 오류 ({input_file_path}): {e}")
        return False

def process_directory(input_dir, output_dir=None):
    """
    디렉토리의 모든 파일을 처리합니다.
    
    Args:
        input_dir (str): 입력 디렉토리 경로
        output_dir (str, optional): 출력 디렉토리 경로. None이면 원본 디렉토리를 사용합니다.
    
    Returns:
        tuple: (성공한 파일 수, 실패한 파일 수)
    """
    input_path = Path(input_dir)
    
    if not input_path.exists():
        logger.error(f"입력 디렉토리가 존재하지 않습니다: {input_dir}")
        return 0, 0
    
    if not input_path.is_dir():
        logger.error(f"입력 경로가 디렉토리가 아닙니다: {input_dir}")
        return 0, 0
    
    # 출력 디렉토리 설정
    if output_dir is None:
        output_path = input_path
    else:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
    
    success_count = 0
    failure_count = 0
    
    # 모든 파일 처리
    for file_path in input_path.iterdir():
        if file_path.is_file():
            try:
                # 출력 파일 경로 설정
                if output_dir is not None:
                    output_file_path = output_path / file_path.name
                else:
                    output_file_path = None
                
                # JSON 포맷팅
                if format_json_file(str(file_path), str(output_file_path) if output_file_path else None):
                    success_count += 1
                else:
                    failure_count += 1
                    
            except Exception as e:
                logger.error(f"파일 처리 중 오류 ({file_path}): {e}")
                failure_count += 1
    
    return success_count, failure_count

def main():
    """메인 함수"""
    # 작업 디렉토리 설정 (스크립트가 있는 디렉토리)
    script_dir = Path(__file__).parent
    os.chdir(script_dir)
    
    # 명령행 인수 처리
    if len(sys.argv) > 1:
        project_id = sys.argv[1]
        input_dir = str(script_dir / f"{project_id}_result")
    else:
        input_dir = str(script_dir / "result")
    
    output_dir = None
    if len(sys.argv) > 2:
        output_dir = sys.argv[2]
    
    logger.info(f"입력 디렉토리: {input_dir}")
    if output_dir:
        logger.info(f"출력 디렉토리: {output_dir}")
    else:
        logger.info("원본 파일을 덮어씁니다.")
    
    # 디렉토리 처리
    success_count, failure_count = process_directory(input_dir, output_dir)
    
    # 결과 출력
    logger.info(f"처리 완료: 성공 {success_count}개, 실패 {failure_count}개")
    
    if failure_count > 0:
        logger.warning(f"{failure_count}개의 파일 처리에 실패했습니다. 로그를 확인하세요.")
        return 1
    else:
        logger.info("모든 파일이 성공적으로 처리되었습니다.")
        return 0

if __name__ == "__main__":
    sys.exit(main())
