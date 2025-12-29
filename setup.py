#!/usr/bin/env python3
"""
프로젝트 초기 설정 스크립트
.env 파일을 생성하고 필수 환경변수를 설정합니다.
"""

import sys
from pathlib import Path
import shutil

def create_env_file():
    """env.example을 복사하여 .env 파일 생성"""
    project_root = Path(__file__).parent
    example_file = project_root / "env.example"
    env_file = project_root / ".env"
    
    if env_file.exists():
        response = input("⚠️  .env 파일이 이미 존재합니다. 덮어쓰시겠습니까? (y/N): ").strip().lower()
        if response != 'y':
            print("❌ .env 파일 생성을 취소했습니다.")
            return False
    
    if not example_file.exists():
        print(f"❌ env.example 파일을 찾을 수 없습니다: {example_file}")
        return False
    
    try:
        shutil.copy(example_file, env_file)
        print(f"✅ .env 파일이 생성되었습니다: {env_file}")
        return True
    except Exception as e:
        print(f"❌ .env 파일 생성 중 오류 발생: {e}")
        return False

def interactive_setup():
    """대화형 환경 설정"""
    print("\n" + "=" * 60)
    print("프로젝트 환경 설정")
    print("=" * 60)
    print("\n기본 설정값을 사용하려면 Enter를 누르세요.")
    print("커스텀 설정이 필요하면 .env 파일을 직접 수정하세요.\n")
    
    # 환경 변수 입력받기
    settings = {}
    
    settings['GCS_BUCKET'] = input("GCS Bucket 이름 [cw_platform]: ").strip() or "cw_platform"
    settings['GCS_BASE_PATH'] = input("GCS Base Path [1069]: ").strip() or "1069"
    settings['BIGQUERY_PROJECT'] = input("BigQuery 프로젝트 [crowdworks-platform]: ").strip() or "crowdworks-platform"
    settings['MAX_WORKERS'] = input("병렬 처리 워커 수 [3]: ").strip() or "3"
    
    return settings

def update_env_file(settings):
    """환경 변수를 .env 파일에 업데이트"""
    project_root = Path(__file__).parent
    env_file = project_root / ".env"
    
    if not env_file.exists():
        print("❌ .env 파일이 없습니다. 먼저 파일을 생성해주세요.")
        return False
    
    try:
        # 기존 내용 읽기
        with open(env_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # 설정 업데이트
        updated_lines = []
        for line in lines:
            if '=' in line and not line.strip().startswith('#'):
                key = line.split('=')[0].strip()
                if key in settings:
                    updated_lines.append(f"{key}={settings[key]}\n")
                else:
                    updated_lines.append(line)
            else:
                updated_lines.append(line)
        
        # 파일에 쓰기
        with open(env_file, 'w', encoding='utf-8') as f:
            f.writelines(updated_lines)
        
        print("✅ 환경 변수가 업데이트되었습니다.")
        return True
    except Exception as e:
        print(f"❌ 환경 변수 업데이트 중 오류 발생: {e}")
        return False

def check_dependencies():
    """의존성 확인"""
    print("\n" + "=" * 60)
    print("의존성 확인")
    print("=" * 60)
    
    # Python 패키지 확인
    try:
        import pandas
        import dotenv
        print("✅ Python 패키지: 설치됨")
    except ImportError as e:
        print(f"❌ Python 패키지 오류: {e}")
        print("   다음 명령어로 설치하세요: pip install -r requirements.txt")
        return False
    
    # gsutil 확인
    import subprocess
    try:
        result = subprocess.run(["gsutil", "version"], capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ gsutil: 설치됨")
        else:
            print("❌ gsutil을 찾을 수 없습니다.")
            print("   Google Cloud SDK를 설치하세요: https://cloud.google.com/sdk/docs/install")
            return False
    except FileNotFoundError:
        print("❌ gsutil이 설치되지 않았습니다.")
        print("   Google Cloud SDK를 설치하세요: https://cloud.google.com/sdk/docs/install")
        return False
    
    return True

def test_configuration():
    """설정 테스트"""
    print("\n" + "=" * 60)
    print("설정 테스트")
    print("=" * 60)
    
    try:
        from config import config
        config.validate()
        print("✅ 환경 설정이 올바릅니다.")
        print(f"\n현재 설정:")
        print(f"  - GCS Bucket: {config.GCS_BUCKET}")
        print(f"  - GCS Base Path: {config.GCS_BASE_PATH}")
        print(f"  - BigQuery Project: {config.BIGQUERY_PROJECT}")
        print(f"  - Max Workers: {config.MAX_WORKERS}")
        return True
    except Exception as e:
        print(f"❌ 설정 오류: {e}")
        return False

def main():
    """메인 함수"""
    print("\n🚀 따뜻한하루 프로젝트 설정을 시작합니다.\n")
    
    # 1. 의존성 확인
    if not check_dependencies():
        print("\n⚠️  의존성 설치 후 다시 실행해주세요.")
        return 1
    
    # 2. .env 파일 생성
    print("\n" + "=" * 60)
    print("환경 변수 파일 생성")
    print("=" * 60)
    
    env_file = Path(__file__).parent / ".env"
    if env_file.exists():
        print("ℹ️  .env 파일이 이미 존재합니다.")
    else:
        if not create_env_file():
            return 1
    
    # 3. 대화형 설정 (선택사항)
    response = input("\n대화형 설정을 진행하시겠습니까? (y/N): ").strip().lower()
    if response == 'y':
        settings = interactive_setup()
        update_env_file(settings)
    
    # 4. 설정 테스트
    if not test_configuration():
        print("\n⚠️  설정을 확인하고 .env 파일을 수정해주세요.")
        return 1
    
    print("\n" + "=" * 60)
    print("✨ 설정이 완료되었습니다!")
    print("=" * 60)
    print("\n다음 단계:")
    print("  1. Google Cloud 인증: gcloud auth login")
    print("  2. 파이프라인 실행: cd Extract_Processing && python Run_Extracting_Pipeline.py")
    print("  3. 자세한 사용법은 README.md를 참조하세요.")
    print("")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())

