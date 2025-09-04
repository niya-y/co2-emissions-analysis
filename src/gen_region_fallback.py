#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
수동 region 매핑 사전 자동 생성/확장 스크립트
- coco/pycountry_convert 없이 ISO3 → continent 매핑 관리
- 실행 흐름:
  1) 입력 CSV에서 ISO3 코드 수집
  2) 기존 region_fallback.json(있으면) + seed_map 병합
  3) 미매핑 코드 목록 CSV(region_unmapped.csv) 생성 (빈 region 컬럼)
  4) 매핑 적용 프리뷰 CSV(region_mapped_preview.csv) 생성
- 다음 실행 때 region_unmapped.csv 를 사람이 채워서 다시 실행하면, 자동 병합되어 사전이 커짐
"""

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

# -----------------------------------------
# 0) 대륙 라벨 표준 (README에도 동일 표기를 권장)
# -----------------------------------------
CONTINENTS = {"Africa", "Asia", "Europe", "North America", "South America", "Oceania", "Antarctica"}

# -----------------------------------------
# 1) 초기 seed_map (자주 쓰는 국가/영토 다수 포함)
#    필요 시 아래 dict를 계속 보강하세요.
# -----------------------------------------
SEED_MAP = {
    # Africa
    "DZA":"Africa","AGO":"Africa","EGY":"Africa","ETH":"Africa","GHA":"Africa","KEN":"Africa","MAR":"Africa","NGA":"Africa","TUN":"Africa","ZAF":"Africa",
    # Asia
    "AFG":"Asia","ARE":"Asia","ARM":"Asia","AZE":"Asia","BGD":"Asia","BHR":"Asia","BRN":"Asia","BTN":"Asia","CHN":"Asia","CYP":"Asia","GEO":"Asia","HKG":"Asia",
    "IDN":"Asia","IND":"Asia","IRN":"Asia","IRQ":"Asia","ISR":"Asia","JPN":"Asia","JOR":"Asia","KAZ":"Asia","KHM":"Asia","KOR":"Asia","KWT":"Asia","LAO":"Asia",
    "LBN":"Asia","LKA":"Asia","MAC":"Asia","MDV":"Asia","MMR":"Asia","MNG":"Asia","MYS":"Asia","NPL":"Asia","OMN":"Asia","PAK":"Asia","PHL":"Asia","QAT":"Asia",
    "SAU":"Asia","SGP":"Asia","SYR":"Asia","THA":"Asia","TJK":"Asia","TKM":"Asia","TUR":"Asia","URY":"South America","UZB":"Asia","VNM":"Asia","YEM":"Asia",
    # Europe
    "ALB":"Europe","AND":"Europe","AUT":"Europe","BEL":"Europe","BGR":"Europe","BIH":"Europe","BLR":"Europe","CHE":"Europe","CZE":"Europe","DEU":"Europe",
    "DNK":"Europe","ESP":"Europe","EST":"Europe","FIN":"Europe","FRA":"Europe","GBR":"Europe","GRC":"Europe","HRV":"Europe","HUN":"Europe","IRL":"Europe",
    "ISL":"Europe","ITA":"Europe","LTU":"Europe","LUX":"Europe","LVA":"Europe","MCO":"Europe","MDA":"Europe","MKD":"Europe","MLT":"Europe","MNE":"Europe",
    "NLD":"Europe","NOR":"Europe","POL":"Europe","PRT":"Europe","ROU":"Europe","RUS":"Europe","SMR":"Europe","SRB":"Europe","SVK":"Europe","SVN":"Europe",
    "SWE":"Europe","UKR":"Europe","VAT":"Europe",
    # North America (카리브/대서양 영토 포함)
    "ABW":"North America","AIA":"North America","ATG":"North America","BHS":"North America","BLZ":"North America","BMU":"North America","BRB":"North America",
    "CAN":"North America","CYM":"North America","CUB":"North America","DMA":"North America","DOM":"North America","GLP":"North America","GRD":"North America",
    "GTM":"North America","HND":"North America","HTI":"North America","JAM":"North America","KNA":"North America","LCA":"North America","MEX":"North America",
    "MSR":"North America","MTQ":"North America","NIC":"North America","PAN":"North America","PRI":"North America","SLV":"North America","TTO":"North America",
    "USA":"North America","VGB":"North America","VIR":"North America",
    # South America
    "ARG":"South America","BOL":"South America","BRA":"South America","CHL":"South America","COL":"South America","ECU":"South America","GUY":"South America",
    "PER":"South America","PRY":"South America","SUR":"South America","URY":"South America","VEN":"South America",
    # Oceania
    "AUS":"Oceania","NZL":"Oceania","FJI":"Oceania","FSM":"Oceania","GUM":"Oceania","KIR":"Oceania","MHL":"Oceania","MNP":"Oceania","NCL":"Oceania",
    "NRU":"Oceania","PLW":"Oceania","PNG":"Oceania","SLB":"Oceania","TON":"Oceania","TUV":"Oceania","VUT":"Oceania","WSM":"Oceania",
    # Antarctica / Special
    "ATA":"Antarctica",
}

# -----------------------------------------
# 2) 유틸
# -----------------------------------------
def load_json(path: Path) -> dict:
    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_json(path: Path, obj: dict):
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)

def normalize_iso3_series(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.upper()

def validate_continent_value(x: str | None) -> bool:
    return isinstance(x, str) and x in CONTINENTS

# -----------------------------------------
# 3) 메인 로직
# -----------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Generate/extend region fallback mapping from dataset ISO3 codes.")
    ap.add_argument("--input", type=str, default="co2_tidy.csv", help="Input CSV path (default: co2_tidy.csv)")
    ap.add_argument("--iso3-col", type=str, default="country", help="Column name containing ISO3 codes (default: country)")
    ap.add_argument("--out-json", type=str, default="region_fallback.json", help="Output JSON mapping file")
    ap.add_argument("--out-unmapped", type=str, default="region_unmapped.csv", help="Output CSV listing unmapped ISO3 codes")
    ap.add_argument("--out-preview", type=str, default="region_mapped_preview.csv", help="Output CSV preview with mapped regions")
    args = ap.parse_args()

    input_path = Path(args.input)
    assert input_path.exists(), f"Input not found: {input_path.resolve()}"

    df = pd.read_csv(input_path, low_memory=False)
    assert args.iso3_col in df.columns, f"Column '{args.iso3_col}' not found in {input_path.name}"

    iso = normalize_iso3_series(df[args.iso3_col])
    unique_iso = iso.dropna().unique().tolist()
    print(f"[INFO] Unique ISO3 codes in data: {len(unique_iso)}")

    # 기존 맵 로드 + SEED 병합
    out_json_path = Path(args.out_json)
    existing = load_json(out_json_path)
    merged = {**SEED_MAP, **existing}  # 기존 우선 반영

    # 매핑 적용
    mapped = pd.Series([merged.get(c) for c in iso], index=df.index)
    missing_mask = mapped.isna()
    missing_codes = iso[missing_mask].dropna().value_counts()
    print(f"[INFO] Mapped: {(~missing_mask).mean():.1%}, Unmapped: {missing_mask.mean():.1%}")

    # 1) 미매핑 목록 CSV 작성 (사람이 채워넣을 템플릿)
    unmapped_df = pd.DataFrame({
        "ISO3": missing_codes.index,
        "count": missing_codes.values,
        "region": [""] * len(missing_codes)  # 비워둠 → 사람이 채움 (Africa/Asia/Europe/...)
    })
    unmapped_df.to_csv(args.out_unmapped, index=False)
    print(f"[WRITE] {args.out_unmapped}  (rows: {len(unmapped_df)})")
    if len(unmapped_df):
        print("       → 'region' 컬럼에 Africa/Asia/Europe/North America/South America/Oceania/Antarctica 중 하나를 채워 넣으세요.")

    # 2) 매핑 프리뷰 저장 (라인 수 과하면 샘플만)
    preview = df[[args.iso3_col]].copy()
    preview["region_mapped"] = mapped
    preview.head(10000).to_csv(args.out_preview, index=False)
    print(f"[WRITE] {args.out_preview}  (first 10k rows)")

    # 3) 사용자가 이전에 채워넣은 unmapped CSV가 있으면 병합해 사전 확장
    unmapped_path = Path(args.out_unmapped)
    if unmapped_path.exists():
        try:
            user_filled = pd.read_csv(unmapped_path)
            if {"ISO3","region"}.issubset(user_filled.columns):
                user_filled["ISO3"] = normalize_iso3_series(user_filled["ISO3"])
                # 유효한 값만 채택
                add_pairs = {row["ISO3"]: row["region"] for _, row in user_filled.iterrows()
                             if isinstance(row["ISO3"], str) and validate_continent_value(row["region"])}
                if add_pairs:
                    merged.update(add_pairs)
                    print(f"[MERGE] Added from {args.out_unmapped}: {len(add_pairs)} pairs")
        except Exception as e:
            print(f"[WARN] Failed to read user unmapped CSV: {e}", file=sys.stderr)

    # 4) 최종 사전 저장
    save_json(out_json_path, merged)
    print(f"[WRITE] {out_json_path}  (size: {len(merged)})")

    print("\n[HOW TO USE]")
    print(f"1) {args.out_unmapped} 파일의 'region'을 채운 뒤 저장")
    print(f"2) 본 스크립트를 다시 실행하면 region_fallback.json에 자동 병합")
    print(f"3) 파이프라인에서 'region' 생성 시, 이 JSON을 사용하세요 (예: apply_region_mapping)")

if __name__ == "__main__":
    main()
