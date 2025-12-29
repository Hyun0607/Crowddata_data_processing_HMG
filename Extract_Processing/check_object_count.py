#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
XML 파일의 오브젝트 수량 체크 스크립트
다른 프로젝트에서도 재사용 가능한 통일 스크립트

사용법:
    python check_object_count.py [XML 파일 경로]
    또는
    python check_object_count.py  # 현재 디렉토리에서 .xml 파일 자동 검색
"""

import xml.etree.ElementTree as ET
import sys
import os
import argparse
from pathlib import Path
from typing import List, Dict, Tuple


def parse_xml(xml_path: str) -> Tuple[int, int, List[Dict]]:
    """
    XML 파일을 파싱하여 이미지 및 오브젝트 정보를 추출
    
    Args:
        xml_path: XML 파일 경로
        
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
        image_object_counts.append({
            'id': image_id,
            'name': image_name,
            'count': len(polygons_in_image)
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
        show_details: 상세 정보 출력 여부
    """
    print('=' * 80)
    print('오브젝트 수량 체크 결과')
    print('=' * 80)
    print(f'\n📄 파일: {xml_path}')
    print(f'\n📊 전체 통계:')
    print(f'   - 전체 이미지 개수: {image_count:,}개')
    print(f'   - 전체 오브젝트(polygon) 개수: {polygon_count:,}개')
    
    if image_count > 0:
        avg_objects = polygon_count / image_count
        min_objects = min(img['count'] for img in image_object_counts)
        max_objects = max(img['count'] for img in image_object_counts)
        print(f'   - 이미지당 평균 오브젝트 수: {avg_objects:.2f}개')
        print(f'   - 최소 오브젝트 수: {min_objects}개')
        print(f'   - 최대 오브젝트 수: {max_objects}개')
    
    if show_details and len(image_object_counts) > 0:
        print(f'\n📋 각 이미지별 오브젝트 개수:')
        print('-' * 80)
        for img in image_object_counts:
            print(f'   Image ID: {str(img["id"]):>4} | Name: {img["name"]:25} | 오브젝트 수: {img["count"]:>5}')
        print('-' * 80)
    
    print(f'\n✅ 총계: 이미지 {image_count:,}개, 오브젝트 {polygon_count:,}개')
    print('=' * 80)


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
        help='각 이미지별 상세 정보 출력 생략'
    )
    
    parser.add_argument(
        '--output',
        '-o',
        help='결과를 CSV 파일로 저장 (선택사항)'
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
    
    # 각 XML 파일 처리
    all_results = []
    for xml_file in xml_files:
        image_count, polygon_count, image_object_counts = parse_xml(xml_file)
        print_results(xml_file, image_count, polygon_count, 
                     image_object_counts, show_details=not args.no_details)
        
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
                writer.writerow(['파일명', '이미지 개수', '오브젝트 개수'])
                for result in all_results:
                    writer.writerow([
                        os.path.basename(result['file']),
                        result['image_count'],
                        result['object_count']
                    ])
            print(f"\n💾 결과가 {args.output}에 저장되었습니다.")
        except Exception as e:
            print(f"\n⚠️  CSV 저장 중 오류 발생: {e}")


if __name__ == '__main__':
    main()

