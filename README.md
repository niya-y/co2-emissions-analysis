# CO₂ Emissions Analysis Pipeline

이 프로젝트는 국가·지역·부문별 CO₂ 배출 데이터를 전처리하고, 시각화 및 예측 모델을 만드는 데이터 분석 포트폴리오 프로젝트입니다.  
원본 데이터는 [Our World in Data](https://ourworldindata.org/co2-and-greenhouse-gas-emissions)와 유사한 구조를 가정하였으며, GitHub에 재현 가능한 파이프라인을 제공합니다.

---

## 프로젝트 구조

project_root/
├── data_raw/ # 원본 데이터 (수정하지 않음)
│ └── Data.csv
├── data_processed/ # 전처리된 데이터
│ └── co2_tidy.csv
├── mappings/ # ISO3 → Region 매핑 관리
│ ├── region_fallbak.json # 최종 매핑 사전
│ └── region_unmapped.csv # 새로 발견된 unmapped 코드
├── notebooks/ # EDA, 시각화 및 분석 노트북
├── src/ # 파이프라인 코드
│ ├── preprocess.py # 데이터 전처리 메인 파이프라인
│ └── gen_region_fallback.py # 초기 region 매핑 템플릿 생성
└── README.md

---

## 전처리 파이프라인

### 1. 컬럼 후보 자동 매핑
- `country`, `region`, `sector`, `year`, `emissions` 컬럼을 원본 데이터에서 자동 탐지  

### 2. 형 변환 / 단위 정규화
- `year`: 문자열/실수를 정수로 변환 (`Y1990` → `1990`)  
- `emissions`: 문자열 숫자 처리 (`"1,234"` → `1234.0`)  
- 모든 배출량을 **MtCO₂(메가톤 CO₂)** 기준으로 통일  

### 3. Tidy 변환
- Wide 포맷(`1990,1991,...`)을 Long 포맷(`year, emissions`)으로 변환  
- 최종 스키마: country | region | sector | year | emissions


### 4. 클린업
- 문자열 정리, 유효 연도 범위 유지  
- 결측치 처리 (`sector → Total`, `region → Unmapped`)  
- 중복 키 집계(sum/mean)  

### 5. Region 매핑
- ISO3 코드 기반 대륙(region) 매핑  
- `gen_region_fallback.py`로 Unmapped 코드 템플릿 생성  
- 사람이 `region_unmapped.csv`를 채운 뒤 JSON에 반영  

---

## 📊 시각화 예시

아래 그래프는 **전 세계 CO₂ 배출량 추세**를 그린 예시입니다.

![Global Emissions Over Time](data_processed/example_global_emissions.png)

Python 코드 예시:
```python
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("data_processed/co2_tidy.csv")

global_trend = df.groupby("year")["emissions"].sum().reset_index()

plt.figure(figsize=(10,6))
plt.plot(global_trend["year"], global_trend["emissions"], color="tab:red")
plt.title("Global CO₂ Emissions Over Time")
plt.xlabel("Year")
plt.ylabel("Emissions (MtCO₂)")
plt.grid(True)
plt.savefig("data_processed/example_global_emissions.png", dpi=150)
plt.show()
