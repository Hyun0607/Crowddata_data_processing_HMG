#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV 파일 병합 스크립트
중과 상 난이도 CSV를 합치고 파일명 기준으로 정렬
"""

import pandas as pd
import os
import re
import json
from pathlib import Path

# 스크립트 디렉토리 기준 경로 설정
SCRIPT_DIR = Path(__file__).parent

# 중 난이도 CSV 읽기
df_middle = pd.read_csv(SCRIPT_DIR / "PROJ-15684_26795_중.csv")
df_middle["난이도"] = "중"

# 난이도 컬럼을 맨 앞으로 이동
cols_middle = ['난이도'] + [col for col in df_middle.columns if col != '난이도']
df_middle = df_middle[cols_middle]

print(f"중 난이도 CSV 로드 완료: {len(df_middle)}개 행")

#상 난이도 CSV 읽기
df_hard = pd.read_csv(SCRIPT_DIR / "PROJ-15684_26994_상.csv")
df_hard["난이도"] = "상"

# 난이도 컬럼을 맨 앞으로 이동
cols_hard = ['난이도'] + [col for col in df_hard.columns if col != '난이도']
df_hard = df_hard[cols_hard]

print(f"상 난이도 CSV 로드 완료: {len(df_hard)}개 행")

# 중과 상을 합치기
df_merged = pd.concat([df_middle, df_hard], ignore_index=True)

print(f"\n합치기 완료: 총 {len(df_merged)}개 행")
print(f"  - 중: {len(df_middle)}개")
print(f"  - 상: {len(df_hard)}개")

# 이미지 파일명 기준으로 정렬
if 'file_name' in df_merged.columns:
    df_merged = df_merged.sort_values('file_name', ignore_index=True)
    print(f"\nfile_name 기준으로 정렬 완료")

# src_idx 컬럼 삭제
if 'src_idx' in df_merged.columns:
    df_merged = df_merged.drop('src_idx', axis=1)
    print(f"\nsrc_idx 컬럼 삭제 완료")

# 합친 CSV 파일로 저장
output_path = SCRIPT_DIR / "PROJ-15684_중_상_통합.csv"
df_merged.to_csv(output_path, index=False, encoding='utf-8-sig')

print(f"\n파일이 저장되었습니다: {output_path}")
print(f"\n컬럼 목록: {list(df_merged.columns)}")
print(f"\n난이도별 행 수:")
print(df_merged['난이도'].value_counts())