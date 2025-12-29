#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
XML 파일의 오브젝트 수량 체크 스크립트
난이도별 통계 지원

사용법:
    python check_object_count.py [XML 파일 경로] [--csv CSV파일경로]
    또는
    python check_object_count.py  # 현재 디렉토리에서 .xml 파일 자동 검색
"""

import xml.etree.ElementTree as ET
import sys
import os
import argparse
import json
import pandas as pd
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from collections import defaultdict


def load_difficulty_mapping(csv_path: Optional[str] = None) -> Dict[str, str]:
    """
    CSV 파일에서 이미지명과 난이도 매핑을 로드
    
    Args:
        csv_path: CSV 파일 경로 (None이면 자동 검색)
        
    Returns:
        {이미지명: 난이도} 딕셔너리
    """
    difficulty_map = {}
    
    if csv_path:
        csv_file = Path(csv_path)
    else:
        # 현재 디렉토리에서 CSV 파일 자동 검색
        current_dir = Path.cwd()
        # 여러 패턴으로 검색
        csv_files = list(current_dir.glob('*중_상_통합*.csv'))
        if not csv_files:
            csv_files = list(current_dir.glob('PROJ-15684*.csv'))
        if not csv_files:
            csv_files = list(current_dir.glob('*하와이*.csv'))
        if not csv_files:
            csv_files = list(current_dir.glob('*영어 통신문*.csv'))
        if not csv_files:
            csv_files = list(current_dir.glob('PROJ-15442*.csv'))
        
        if not csv_files:
            print("⚠️  CSV 파일을 찾을 수 없습니다. 난이도별 통계는 표시되지 않습니다.")
            return difficulty_map
        
        csv_file = csv_files[0]
        print(f"📋 CSV 파일 자동 검색: {csv_file}")
    
    try:
        df = pd.read_csv(csv_file)
        
        # file_name 컬럼에서 실제 파일명 추출
        for idx, row in df.iterrows():
            # 난이도 컬럼 확인 (난이도 또는 난이도 컬럼)
            if '난이도' not in df.columns:
                continue
            
            difficulty = str(row['난이도']).strip()
            
            # file_name 컬럼 확인
            if 'file_name' in df.columns:
                file_name_str = str(row['file_name'])
            elif '이미지 파일명' in df.columns:
                file_name_str = str(row['이미지 파일명'])
            else:
                continue
            
            # JSON 문자열에서 파일명 추출 시도
            actual_filename = file_name_str
            try:
                file_data = json.loads(file_name_str)
                if isinstance(file_data, dict):
                    actual_filename = file_data.get('file_name', file_name_str)
                else:
                    actual_filename = file_name_str
            except (json.JSONDecodeError, ValueError):
                # JSON이 아닌 경우 그대로 사용
                actual_filename = file_name_str
            
            # 파일명 정리 (공백 제거)
            actual_filename = actual_filename.strip()
            
            # 확장자 제거한 파일명도 매핑
            if actual_filename:
                difficulty_map[actual_filename] = difficulty
                # .jpg 확장자 제거한 버전도 추가
                if actual_filename.endswith('.jpg'):
                    difficulty_map[actual_filename.replace('.jpg', '')] = difficulty
                # 파일명에서 경로 제거 (basename만 사용)
                if '/' in actual_filename:
                    basename = os.path.basename(actual_filename)
                    difficulty_map[basename] = difficulty
                    if basename.endswith('.jpg'):
                        difficulty_map[basename.replace('.jpg', '')] = difficulty
        
        print(f"✅ 난이도 매핑 로드 완료: {len(difficulty_map)}개 이미지")
        
    except Exception as e:
        print(f"⚠️  CSV 파일 로드 오류: {e}")
        print("   난이도별 통계는 표시되지 않습니다.")
    
    return difficulty_map


def parse_xml(xml_path: str, difficulty_map: Optional[Dict[str, str]] = None) -> Tuple[int, int, List[Dict]]:
    """
    XML 파일을 파싱하여 이미지 및 오브젝트 정보를 추출
    
    Args:
        xml_path: XML 파일 경로
        difficulty_map: 이미지명-난이도 매핑 딕셔너리
        
    Returns:
        (이미지 개수, 전체 오브젝트 개수, 이미지별 오브젝트 정보 리스트)
    """
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
    except ET.ParseError as e:
        print(f"❌ XML 파싱 오류: {e}")
        sys.exit(1)
    except FileNotFoundError:
        print(f"❌ 파일을 찾을 수 없습니다: {xml_path}")
        sys.exit(1)
    
    # 이미지 개수
    images = root.findall('.//image')
    image_count = len(images)
    
    # 전체 polygon(오브젝트) 개수
    polygons = root.findall('.//polygon')
    polygon_count = len(polygons)
    
    # 각 이미지별 오브젝트 개수
    image_object_counts = []
    for image in images:
        image_id = image.get('id', 'N/A')
        image_name = image.get('name', 'N/A')
        polygons_in_image = image.findall('.//polygon')
        
        # 난이도 정보 추가
        difficulty = '알 수 없음'
        if difficulty_map:
            # 파일명에서 난이도 찾기 (여러 방법 시도)
            basename = os.path.basename(image_name) if '/' in image_name else image_name
            
            if image_name in difficulty_map:
                difficulty = difficulty_map[image_name]
            elif basename in difficulty_map:
                difficulty = difficulty_map[basename]
            elif image_name.replace('.jpg', '') in difficulty_map:
                difficulty = difficulty_map[image_name.replace('.jpg', '')]
            elif basename.replace('.jpg', '') in difficulty_map:
                difficulty = difficulty_map[basename.replace('.jpg', '')]
        
        image_object_counts.append({
            'id': image_id,
            'name': image_name,
            'count': len(polygons_in_image),
            'difficulty': difficulty
        })
    
    return image_count, polygon_count, image_object_counts


def find_xml_files(directory: str = '.') -> List[str]:
    """
    지정된 디렉토리에서 XML 파일을 찾음
    
    Args:
        directory: 검색할 디렉토리 경로
        
    Returns:
        XML 파일 경로 리스트
    """
    xml_files = []
    for root, dirs, files in os.walk(directory):
        # 특정 디렉토리 제외 (선택사항)
        dirs[:] = [d for d in dirs if d not in ['venv', '__pycache__', '.git']]
        
        for file in files:
            if file.endswith('.xml'):
                xml_files.append(os.path.join(root, file))
    
    return sorted(xml_files)


def print_results(xml_path: str, image_count: int, polygon_count: int, 
                  image_object_counts: List[Dict], show_details: bool = True):
    """
    결과를 포맷팅하여 출력
    
    Args:
        xml_path: XML 파일 경로
        image_count: 이미지 개수
        polygon_count: 전체 오브젝트 개수
        image_object_counts: 이미지별 오브젝트 정보
        show_details: 상세 정보 출력 여부 (사용하지 않음)
    """
    # 난이도별 통계 계산
    difficulty_stats = defaultdict(lambda: {'images': 0, 'objects': 0})
    for img in image_object_counts:
        diff = img.get('difficulty', '알 수 없음')
        difficulty_stats[diff]['images'] += 1
        difficulty_stats[diff]['objects'] += img['count']
    
    print('=' * 60)
    print('난이도별 오브젝트 수량 체크 결과')
    print('=' * 60)
    print(f'\n📄 파일: {os.path.basename(xml_path)}')
    print()
    
    # 난이도 순서대로 출력 (중, 상)
    difficulty_order = ['중', '상']
    for difficulty in difficulty_order:
        if difficulty in difficulty_stats:
            stats = difficulty_stats[difficulty]
            print(f'   [{difficulty}] 이미지: {stats["images"]:,}개, 오브젝트: {stats["objects"]:,}개')
    
    # 알 수 없는 난이도가 있으면 출력
    for diff in sorted(difficulty_stats.keys()):
        if diff not in difficulty_order:
            stats = difficulty_stats[diff]
            print(f'   [{diff}] 이미지: {stats["images"]:,}개, 오브젝트: {stats["objects"]:,}개')
    
    print()
    print(f'   [총계] 이미지: {image_count:,}개, 오브젝트: {polygon_count:,}개')
    print('=' * 60)


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(
        description='XML 파일의 오브젝트 수량을 체크하는 스크립트',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
사용 예시:
  python check_object_count.py path/to/file.xml
  python check_object_count.py --auto
  python check_object_count.py --auto --no-details
        """
    )
    
    parser.add_argument(
        'xml_file',
        nargs='?',
        help='체크할 XML 파일 경로 (지정하지 않으면 --auto 모드로 동작)'
    )
    
    parser.add_argument(
        '--auto',
        action='store_true',
        help='현재 디렉토리에서 XML 파일을 자동으로 찾아서 체크'
    )
    
    parser.add_argument(
        '--no-details',
        action='store_true',
        help='(사용하지 않음) 간단한 통계만 출력합니다'
    )
    
    parser.add_argument(
        '--output',
        '-o',
        help='결과를 CSV 파일로 저장 (선택사항)'
    )
    
    parser.add_argument(
        '--csv',
        help='난이도 정보가 있는 CSV 파일 경로 (지정하지 않으면 자동 검색)'
    )
    
    args = parser.parse_args()
    
    # XML 파일 경로 결정
    xml_files = []
    
    if args.xml_file:
        # 명시적으로 파일 경로가 지정된 경우
        if os.path.exists(args.xml_file):
            xml_files = [args.xml_file]
        else:
            print(f"❌ 파일을 찾을 수 없습니다: {args.xml_file}")
            sys.exit(1)
    elif args.auto or not args.xml_file:
        # 자동 모드 또는 인자가 없는 경우
        xml_files = find_xml_files()
        if not xml_files:
            print("❌ XML 파일을 찾을 수 없습니다.")
            print("   현재 디렉토리에서 .xml 파일을 검색했습니다.")
            sys.exit(1)
        print(f"🔍 {len(xml_files)}개의 XML 파일을 찾았습니다.\n")
    
    # 난이도 매핑 로드
    difficulty_map = load_difficulty_mapping(args.csv)
    
    # 각 XML 파일 처리
    all_results = []
    for xml_file in xml_files:
        image_count, polygon_count, image_object_counts = parse_xml(xml_file, difficulty_map)
        print_results(xml_file, image_count, polygon_count, 
                     image_object_counts, show_details=False)
        
        all_results.append({
            'file': xml_file,
            'image_count': image_count,
            'object_count': polygon_count,
            'details': image_object_counts
        })
        
        if len(xml_files) > 1:
            print('\n')
    
    # CSV 출력 옵션
    if args.output:
        try:
            import csv
            with open(args.output, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                # 헤더 작성
                writer.writerow(['파일명', '이미지 개수', '오브젝트 개수', '중_이미지', '중_오브젝트', '상_이미지', '상_오브젝트', '알수없음_이미지', '알수없음_오브젝트'])
                
                for result in all_results:
                    # 난이도별 통계 계산
                    difficulty_stats = defaultdict(lambda: {'images': 0, 'objects': 0})
                    for img_detail in result['details']:
                        diff = img_detail.get('difficulty', '알 수 없음')
                        difficulty_stats[diff]['images'] += 1
                        difficulty_stats[diff]['objects'] += img_detail['count']
                    
                    writer.writerow([
                        os.path.basename(result['file']),
                        result['image_count'],
                        result['object_count'],
                        difficulty_stats.get('중', {}).get('images', 0),
                        difficulty_stats.get('중', {}).get('objects', 0),
                        difficulty_stats.get('상', {}).get('images', 0),
                        difficulty_stats.get('상', {}).get('objects', 0),
                        difficulty_stats.get('알 수 없음', {}).get('images', 0),
                        difficulty_stats.get('알 수 없음', {}).get('objects', 0)
                    ])
            print(f"\n💾 결과가 {args.output}에 저장되었습니다.")
        except Exception as e:
            print(f"\n⚠️  CSV 저장 중 오류 발생: {e}")


if __name__ == '__main__':
    main()

