
import re
from pathlib import Path
import numpy as np
import pandas as pd

# INPUT_PATH = Path("/data_raw/Data.csv")
# OUTPUT_PATH = Path("co2_tidy.csv")  # 컬럼 : country, region, sector, year, emissions

CONFIG = {
    "input_unit" : None,
    "aggregate_policy" : "sum",        #중복키 합치는 방식 : sum
    "year_min" : 1800,
    "year_max" : 2100,
    "input_path" : Path("data_raw/Data.csv"),
    "output_path" : Path("data_processed/co2_tidy.csv"),
    "enable_region_mapping" : True
}

#입력 확인
assert CONFIG["input_path"].exists(), f"입력 파일이 없습니다 : {CONFIG['input_path'].resolve()}"

# 1. csv 안전 로드

def read_csv_safely(path : Path) -> pd.DataFrame:
    encodings = ['utf-8', 'utf-8-sig', 'cp949', 'latin-1']
    last_err = None
    for enc in encodings:   
        try :
            df = pd.read_csv(path, low_memory=False, encoding=enc)
            print(f"[로드] encoding='{enc}', shape = {df.shape}")
            #print(df.head())
            return df
        except Exception as e :
            last_err = e
    raise RuntimeError(f"CSV 읽기 실패 (인코딩) : {last_err}")


df_raw = read_csv_safely(CONFIG["input_path"]).copy()
#공백 제거한 원본 컬럼 확보
df_raw.columns = [str(c).strip() for c in df_raw.columns]

# 2. 컬럼 후보 자동 매핑

_lower_map = {c.lower() : c for c in df_raw.columns}

def find_col(candidates):
    for cand in candidates:
        if cand in _lower_map:
            return _lower_map[cand]
        for k, v in _lower_map.items():
            if cand in k:
                return v
    return None

country_col = find_col(['country', 'entitiy', 'nation', 'area', 'state', 'iso_code', 'iso3', 'iso'])
region_col = find_col(['region', 'continent'])
sector_col = find_col(['sector','category','industry','scope'])
year_col = find_col(['year','yr','date'])
emiss_col = find_col(['emissions','emission','co2','co₂','value','ktco2','mtco2','co2_emissions','total','co2e'])

#print(country_col, region_col, sector_col, year_col, emiss_col)

# 3.  연도 컬럼 감지

def detect_wide_year_columns(columns):
    P_ANYYEAR = re.compile(r"(?i)y?[0-9]{4}")
    years = []
    for c in columns:
        s = str(c).strip()
        if P_ANYYEAR.fullmatch(s):
            years.append(c)
    
    def to_year_int(x):
        return int(re.sub(r"(?i)^y", "", str(x)))
    years = sorted(set(years), key=to_year_int)
    return years

wide_year_cols = [] if year_col else detect_wide_year_columns(df_raw.columns)
#print(wide_year_cols)   결과가 [] : 연도는 롱 포맷

# 4. 형 변환 / 단위 변환

def coerce_year(x):
    try : 
        s = str(x).strip()
        s = re.sub(r"(?i)^y", "", s)  # 맨 앞 Y 제거 (예: "Y1990" → "1990")
        y = int(float(s))             # 문자열 → 숫자 → 정수
        if CONFIG['year_min'] <= y <= CONFIG['year_max'] :
            return y
        
    except Exception:
        return np.nan
    return np.nan

def to_float(x):
    try:
        if isinstance(x, str):
            x = x.replace(",", "").strip()      #"1,234" → "1234"
        return float(x)
    except Exception:
        return np.nan
    
def unit_factor_from_config_or_name(emiss_col_name: str) -> tuple[float, str]:
    if CONFIG['input_unit'] == 'kt':
        return 0.001, 'ktCO2 -> MtCO2 (설정)'
    if CONFIG['input_unit'] == 'mt':
        return 1.0, "MtCO2 (설정)"
    if emiss_col_name :
        name = str(emiss_col_name).lower()
        if "kt" in name:
            return 0.001, "ktCO2 → MtCO2 (열이름 추정)"
        if "mtco2" in name or re.search(r"\bmt\b", name):
            return 1.0, "MtCO2 (열이름 추정)"
    return 1.0, "MtCO2 (가정)"

# 5. Tidy 변환 

def build_tidy(df : pd.DataFrame) -> pd.DataFrame:
    # wide 포맷
    if wide_year_cols:
        dims = []
        if country_col : dims.append(country_col)
        if region_col : dims.append(region_col)
        if sector_col : dims.append(sector_col)
        if not dims :
            dims = [df.columns[0]]


        long_df = df.melt(id_vars=dims, var_name="year", value_name="emissions")
        long_df['year'] = long_df['year'].apply(coerce_year)
        long_df['emissions'] = long_df['emissions'].apply(to_float)

        out = pd.DataFrame({
            "country":long_df[dims[0]] if country_col is None else long_df.get(country_col, long_df[dims[0]]),
            "region":long_df.get(region_col, np.nan),
            "sector":long_df.get(sector_col, "Total") if sector_col not in long_df.columns else long_df[sector_col],
            "year":long_df["year"],
            "emissions":long_df["emissions"]
        })

        return out
    
    # Long 포맷
    _year = year_col
    if _year is None:
        for c in df.columns:
            vals = pd.to_numeric(df[c], errors="coerce")
            if vals.notna().mean() > 0.6 and vals.dropna().between(CONFIG['year_max'], CONFIG['year_min']).mean() > 0.8:
                _year = c; break
            
    _emiss = emiss_col
    if _emiss is None:
        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        for c in numeric_cols:
            if c != _year:
                _emiss = c; break
            
    out = pd.DataFrame({
        "country": df.get(country_col, np.nan) if country_col else np.nan,
        "region":  df.get(region_col,  np.nan) if region_col  else np.nan,
        "sector":  df.get(sector_col,  "Total") if sector_col else "Total",
        "year":    df.get(_year, np.nan),
        "emissions": df.get(_emiss, np.nan),
    })
    out['year'] = out['year'].apply(coerce_year)
    out['emissions'] = out['emissions'].apply(to_float)
    return out

tidy = build_tidy(df_raw)

# print(tidy.head(10))
# print(tidy.dtypes)

# 6. 클린업 : 트림 / 결측 / 중복 / 연도 범위 + 결측치 처리

#문자열 정리
for col in ['country','region','sector']:
    if col in tidy.columns:
        tidy[col] = tidy[col].astype(str).str.strip()

#연도 & 배출량 
tidy = tidy.dropna(subset = ['year']).copy()
tidy = tidy[tidy['year'].between(CONFIG['year_min'], CONFIG['year_max'])]
tidy['emissions'] = pd.to_numeric(tidy['emissions'], errors='coerce')

#emissions 결측치 제거
tidy = tidy.dropna(subset=['emissions'])

#sector 결측치 Total로 대체
if 'sector' not in tidy.columns:
    tidy['sector'] = 'Total'
else:
    tidy['sector'] = tidy['sector'].fillna('Total')

#region 결측치 unmapped 두기 (이후 매핑에서 보완)
if 'region' not in tidy.columns:
    tidy['region'] = tidy['region'].fillna('Unmapped')

#중복키 처리
key_cols = ['country', 'region', 'sector', 'year']
if tidy.duplicated(subset=key_cols).any():
    if CONFIG['aggregate_policy'] == 'mean':
        tidy = tidy.groupby(key_cols, as_index=False)['emissions'].mean()
    else : 
        tidy = tidy.groupby(key_cols, as_index=False)['emissions'].sum()



# 7. 단위 정규화 MtCO2

factor, unit_note = unit_factor_from_config_or_name(emiss_col)
tidy['emissions'] = tidy['emissions'] * factor
print(f'[단위]{unit_note} (계수={factor}) ※ 결과는 MtCo2기준')

tidy = tidy[["country","region","sector","year","emissions"]].sort_values(["country","sector","year"])
tidy.to_csv(CONFIG["output_path"], index=False)

# 8. iso_code -> continent(region) 매핑 

import json
from pathlib import Path

def apply_region_mapping(df, iso3_col="country", map_json="region_fallback.json", fill_label="Unmapped"):
    path = Path(map_json)
    assert path.exists(), f"Mapping file not found: {path.resolve()}"
    with path.open("r", encoding="utf-8") as f:
        fmap = json.load(f)
    iso = df[iso3_col].astype(str).str.strip().str.upper()
    region = iso.map(fmap)
    return region.fillna(fill_label)

tidy["region"] = apply_region_mapping(
    tidy, 
    iso3_col="country", 
    map_json="mappings/region_fallback.json"
)


# region 매핑 결과 확인 / 분포
#print(tidy["region"].value_counts(dropna=False))

# 9. 저장 & 요약

tidy = tidy[["country","region","sector","year","emissions"]].sort_values(["country","sector","year"])
tidy.to_csv(CONFIG['output_path'],index=False)

#간단 리포트
dup_rate = tidy.duplicated(subset=['country','region','sector','year']).mean()
print(f"[저장] {CONFIG['output_path'].resolve()}  (행:{len(tidy):,}, 열:{len(tidy.columns)})")
print(f"[중복율] {dup_rate:.2%}")
print("\n[샘플 10행]")
print(tidy.head(10).to_string(index=False))
print("\n[기술통계(수치형)]")
print(tidy.describe())

