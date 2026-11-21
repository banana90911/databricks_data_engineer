# 데이터 엔지니어링을 위한 DevOps — 핵심 개념 정리

---

## 목차

1. [소프트웨어 엔지니어링 모범 사례](#1-소프트웨어-엔지니어링-모범-사례)
2. [PySpark 코드 모듈화](#2-pyspark-코드-모듈화)
3. [DevOps와 DataOps](#3-devops와-dataops)
4. [CI/CD](#4-cicd)
5. [테스트 피라미드](#5-테스트-피라미드)
6. [프로젝트 계획과 환경 격리](#6-프로젝트-계획과-환경-격리)
7. [단위 테스트: PySpark 테스트 유틸리티](#7-단위-테스트-pyspark-테스트-유틸리티)
8. [단위 테스트: pytest](#8-단위-테스트-pytest)
9. [통합 테스트](#9-통합-테스트)
10. [Git 버전 관리와 브랜칭](#10-git-버전-관리와-브랜칭)
11. [Databricks Git Folders](#11-databricks-git-folders)
12. [지속적 배포(CD): Databricks 자산 배포 방법](#12-지속적-배포cd-databricks-자산-배포-방법)
13. [종합: 프로젝트 전체 흐름](#13-종합-프로젝트-전체-흐름)
14. [핵심 정리 치트시트](#14-핵심-정리-치트시트)
15. [자가 점검 문제](#15-자가-점검-문제)
16. [참고 링크](#16-참고-링크)

---

## 1. 소프트웨어 엔지니어링 모범 사례

신뢰할 수 있는 데이터 파이프라인을 만들려면 소프트웨어 엔지니어링에서 검증된 모범 사례를 그대로 가져오면 된다. 목표는 효율적이고, 확장 가능하며, 유지 관리하기 쉬운 파이프라인이다.

### 1-1. 여섯 가지 모범 사례

| 사례 | 핵심 내용 |
|---|---|
| **코딩 관행** | 가독성(이해, 탐색, 유지 관리가 쉬운 코드), 명명 규칙(설명적이고 일관된 변수, 함수, 클래스 이름), 모듈식 설계(작고 재사용 가능한 함수로 분리) |
| **코드 문서화** | 유지 관리성 향상, 협업 강화, 지식 이전(팀원이 바뀌어도 설계 의도 보존) |
| **자동화 테스트** | 단위 테스트(한 단위를 격리해 검증), 통합 테스트(여러 구성 요소가 함께 동작하는지 검증) |
| **버전 관리 & 코드 리뷰** | Git으로 변경 추적, 협업, 이력 관리(롤백, 여러 버전 관리, 충돌 방지). 코드 리뷰로 버그 조기 발견과 코딩 표준 준수 |
| **CI/CD** | CI: 공유 저장소에 자주 커밋하고 빌드, 테스트. CD: 테스트를 통과한 코드의 운영 릴리스를 자동화 |
| **격리된 환경** | 운영 코드를 직접 수정하지 않는다. 개발, 스테이징, 운영 환경을 분리해 배포 전에 충분히 테스트 |

- **린팅(linting) 도구**는 잠재적 오류, 불일치, 스타일 위반을 자동으로 분석해 코딩 관행을 지키게 도와준다. Python에서는 `ruff`, `flake8`, `pylint`, 포매터로 `black` 등을 많이 쓴다.
- 문서화는 함수마다 docstring을 달고, 프로젝트 루트에 README를 두는 것부터 시작한다.

```python
def convert_miles_to_km(df, new_column_name, miles_column):
    """마일 단위 컬럼을 킬로미터로 변환해 새 컬럼으로 추가한다.

    Args:
        df: 입력 DataFrame
        new_column_name: 생성할 컬럼 이름
        miles_column: 마일 값이 들어 있는 컬럼 이름
    Returns:
        소수점 둘째 자리로 반올림한 km 컬럼이 추가된 DataFrame
    """
    return df.withColumn(new_column_name, F.round(F.col(miles_column) * 1.60934, 2))
```

### 1-2. Databricks에서 모범 사례를 구현하는 도구

| 도구 | 역할 |
|---|---|
| Databricks Workspaces | 노트북이나 파일(SQL, Python, Scala 등)로 코드 개발, 단위 테스트 실행. 로컬 IDE에서 개발할 수도 있다 |
| Databricks Git Folders | 원격 Git 저장소와 연결해 버전 관리 |
| Unity Catalog | 단일 또는 여러 워크스페이스에서 환경을 안전하게 격리하고 데이터 접근 제어. 리니지, 보안, 모니터링 내장 |
| 배포 도구 (REST API, CLI, SDK, Declarative Automation Bundles) | CI/CD 파이프라인에서 테스트와 배포 자동화 |

---

## 2. PySpark 코드 모듈화

### 2-1. 모듈화되지 않은 코드의 문제

```python
# 데이터 로드
df = spark.read.csv("health.csv", header=True, inferSchema=True)

# 컬럼 생성
df = df.withColumn("NewColumn",
                   when(col("Column") == 0, 'Normal').otherwise('Unknown'))
```

- **모든 코드가 한 덩어리**: 데이터 로드나 컬럼 추가처럼 특정 부분만 수정하거나 테스트하기 어렵다.
- **코드 중복**: 같은 로직이 다른 곳에서 필요하면 복사하게 되고, 로직이 바뀌면 모든 복사본을 고쳐야 한다.
- 노트북에서 탐색하며 셀을 계속 쌓다 보면 흔히 이렇게 된다. 동작은 하지만 CI/CD를 도입하기 어렵다.

### 2-2. 모듈화된 코드

```python
def load_data(file_path):
    return spark.read.csv(file_path, header=True, inferSchema=True)

def add_new_col(df, new, s_col):
    return df.withColumn(new,
                         when(col(s_col) == 0, 'Normal').otherwise('Unknown'))
```

- 함수 이름은 하는 일을 구체적으로 드러내는 것이 좋다. 예를 들어 나이를 범주화한다면 `add_new_col`보다 `add_age_category`가 낫다.

### 2-3. 모듈화의 이점

| 이점 | 설명 |
|---|---|
| 유지 보수 용이성 | 전체 스크립트를 건드리지 않고 특정 함수만 수정 |
| 재사용성 | 한 번 작성한 함수를 여러 프로젝트, 시나리오에서 재사용 |
| 테스트 가능성 | 함수 단위로 단위 테스트를 작성할 수 있음. CI/CD의 전제 조건 |
| 협업 | 팀원이 서로 다른 모듈을 충돌 없이 독립적으로 작업 |

### 2-4. 예제: 메달리온 파이프라인 모듈화

건강 데이터 CSV(컬럼: `ID`, `PII`, `date`, `HighCholest`, `HighBP`, `BMI`, `Age`, `Education`, `income`)를 브론즈, 실버, 골드로 처리하는 코드를 함수로 나눈 예다.

| 함수 | 역할 |
|---|---|
| `get_health_csv_schema()` | CSV 스키마(StructType) 반환 |
| `read_health_data(csv_path, schema)` | CSV를 읽고 메타데이터 컬럼(`_metadata.file_name`, `_metadata.file_modification_time`, `processing_time`) 추가 |
| `high_cholest_map(col_name)` | 콜레스테롤 값을 범주로 매핑하는 Column 표현식 반환 |
| `group_ages_map(col_name)` | 나이를 연령대로 매핑하는 Column 표현식 반환 |
| `save_df_to_delta(df, uc_table, mode)` | DataFrame을 UC 테이블로 저장 |
| `get_cholest_age_agg(catalog, schema, table_name)` | 골드 집계 테이블 생성 |

```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType
from pyspark.sql.functions import when, col, current_timestamp

def get_health_csv_schema():
    return StructType([
        StructField("ID", IntegerType(), True),
        StructField("PII", StringType(), True),
        StructField("date", DateType(), True),
        StructField("HighCholest", IntegerType(), True),
        StructField("HighBP", DoubleType(), True),
        StructField("BMI", DoubleType(), True),
        StructField("Age", DoubleType(), True),
        StructField("Education", DoubleType(), True),
        StructField("income", IntegerType(), True),
    ])

def read_health_data(csv_path, schema):
    return (spark.read.format("csv")
            .option("header", "true")
            .schema(schema)
            .load(csv_path)
            .select("*",
                    "_metadata.file_name",
                    "_metadata.file_modification_time",
                    current_timestamp().alias("processing_time")))

def high_cholest_map(col_name):
    # DataFrame이 아니라 Column 표현식을 반환 → withColumn, select 어디서든 재사용 가능
    return (when(col(col_name) == 0, 'Normal')
            .when(col(col_name) == 1, 'Above Average')
            .when(col(col_name) == 2, 'High')
            .otherwise('Unknown'))

def group_ages_map(col_name):
    return (when(col(col_name) <= 9, "0-9")
            .when((col(col_name) >= 10) & (col(col_name) <= 19), "10-19")
            .when((col(col_name) >= 20) & (col(col_name) <= 29), "20-29")
            .when((col(col_name) >= 30) & (col(col_name) <= 39), "30-39")
            .when((col(col_name) >= 40) & (col(col_name) <= 49), "40-49")
            .when(col(col_name) >= 50, "50+")
            .otherwise('Unknown'))

def save_df_to_delta(dataframe, uc_table, mode):
    dataframe.write.format("delta").mode(mode).saveAsTable(uc_table)
```

함수를 조합해 파이프라인을 구성한다.

```python
# CSV → 브론즈
health_csv_df = read_health_data(f"/Volumes/{catalog}/dev_ops/health", get_health_csv_schema())
save_df_to_delta(health_csv_df, f"{catalog}.dev_ops.health_bronze", mode="overwrite")

# 브론즈 → 실버
silver_df = (spark.table(f"{catalog}.dev_ops.health_bronze")
             .withColumn("HighCholest_Group", high_cholest_map("HighCholest"))
             .withColumn("Age_Group", group_ages_map("Age"))
             .drop("file_name", "file_modification_time", "processing_time"))
save_df_to_delta(silver_df, f"{catalog}.dev_ops.health_silver", mode="overwrite")
```

### 2-5. 함수를 .py 파일로 분리하고 가져오기

모듈화한 함수는 노트북이 아니라 **Python 파일(.py)** 에 두고 import해서 쓴다. 그래야 여러 노트북, 파이프라인, 테스트가 같은 코드를 공유할 수 있다.

프로젝트 폴더 구조 예시는 다음과 같다.

- 프로젝트 루트
  - `src/` : 운영 코드
    - `helpers/project_functions.py` : 재사용 함수
    - `sdp/` : 선언적 파이프라인 소스
  - `tests/`
    - `unit_tests/test_spark_helper_functions.py`
    - `integration_test/`
  - `pytest.ini`
  - `databricks.yml` (번들 설정)

```python
# 노트북과 같은 폴더(또는 하위 폴더)의 모듈은 바로 import 가능
from src_lab.lab_functions import transforms

# 상위 폴더의 모듈을 쓰려면 프로젝트 루트를 sys.path에 추가
import sys, os
root_folder_path = os.path.dirname(os.path.dirname(os.getcwd()))
sys.path.append(root_folder_path)
from src.helpers import project_functions
```

.py 파일을 수정하면서 노트북에서 바로 반영되게 하려면 autoreload를 켠다.

```python
%load_ext autoreload
%autoreload 2
```

### 2-6. 리팩터링 결과 검증

리팩터링 전후 결과가 같은지는 `assertDataFrameEqual`로 확인한다. 다르면 오류가 난다.

```python
from pyspark.testing.utils import assertDataFrameEqual

original_df = spark.read.table(f"{catalog}.dev_ops.nyc_original")
refactored_df = spark.read.table(f"{catalog}.dev_ops.nyc_refactored")
assertDataFrameEqual(original_df, refactored_df)
```

---

## 3. DevOps와 DataOps

### 3-1. DevOps란

DevOps는 **소프트웨어 엔지니어링 모범 사례와 IT 운영을 결합한 문화이자 실천 방법**이다. 개발 팀과 운영 팀의 협업을 촉진하고, 작업을 자동화해 소프트웨어를 더 빠르고 안정적으로, 높은 품질로 제공한다.

주요 이점: 빠른 배포, 향상된 협업, 높은 안정성, 우수한 확장성.

### 3-2. DevOps 라이프사이클 8단계

개발(Dev) 쪽 네 단계와 운영(Ops) 쪽 네 단계가 무한 루프처럼 계속 반복된다.

| 단계 | 내용 |
|---|---|
| 1. Plan (계획) | 목표 정의, 요구 사항 수집, 팀이 무엇을 제공할지 합의 |
| 2. Code (코딩) | 소스 코드 작성, 기능 구현 |
| 3. Build (빌드) | 의존성을 통합해 실행 가능한 아티팩트로 만듦 |
| 4. Test (테스트) | 자동화 테스트로 릴리스 전에 버그 발견. DevOps의 핵심 |
| 5. Release (릴리스) | 운영 준비 상태로 패키징, 통제된 배포 준비 |
| 6. Deploy (배포) | 운영 환경에 반영해 사용자가 쓸 수 있게 함 |
| 7. Operate (운영) | 실행, 리소스 관리, 문제 신속 해결 |
| 8. Monitor (모니터링) | 성능 추적, 피드백 수집, 지속적 개선 |

수정, 업데이트, 새 기능이 필요할 때마다 이 루프를 다시 돈다.

### 3-3. DevOps, DataOps, MLOps

| 구분 | 대상 | 초점 |
|---|---|---|
| **DevOps** | 소프트웨어 개발 + IT 운영 | CI/CD 자동화, 지속적 코드 테스트, 버전 관리, 운영 수준 작업 구축, 오케스트레이션과 자동화, 시스템 성능 모니터링 |
| **DataOps** | 데이터 엔지니어링 | 고품질 데이터 파이프라인 프로세스, 데이터 처리 최적화, 데이터 탐색, 관리, 거버넌스 중앙화, 추적 가능한 리니지와 모니터링, 팀 간 협업, 데이터 품질 모니터링 |
| **MLOps** | 머신 러닝 | 모델 코드를 소프트웨어로, 모델을 데이터로 취급. 모델 라이프사이클 관리, 모델 성능 모니터링 |

- **DataOps = 데이터 엔지니어링을 위한 DevOps.** 자동화, 협업, 지속적 개선이라는 같은 원칙을 데이터 파이프라인에 적용해 **비즈니스 요구 사항을 비즈니스 가치로** 바꾼다.
- Databricks에서는 Unity Catalog가 DataOps의 거버넌스, 리니지, 탐색을 맡는다.

---

## 4. CI/CD

### 4-1. CI/CD 개요

- CI/CD는 **지속적 통합(Continuous Integration)** 과 **지속적 전달/배포(Continuous Delivery/Deployment)** 를 뜻한다. DevOps 실천의 핵심 하위 집합이다.
- 반복 작업을 자동화해 코드 품질, 속도, 안정성을 높이고, 짧고 빈번한 주기로 소프트웨어를 제공한다.
- 코드는 품질 관리 절차를 포함한 자동화된 프로세스를 거쳐 운영에 배포된다. 문제가 커지기 전에 일찍 발견하고 해결할 수 있다.
- 소프트웨어 개발에서는 오래전부터 표준이었고, 데이터 엔지니어링과 데이터 사이언스에서도 중요성이 커지고 있다.

DevOps 라이프사이클과의 대응은 다음과 같다.

| 구분 | 라이프사이클 범위 |
|---|---|
| CI | 계획, 코딩(개발), 빌드, 테스트 (환경 관리 포함) |
| CD | 릴리스, 배포, 운영, 모니터링 |

### 4-2. 지속적 통합(CI)

여러 기여자의 코드 변경을 **중앙 저장소에 정기적으로 병합**하고, 병합할 때마다 **자동화 테스트를 실행**해 코드 품질을 보장하는 방식이다. 테스트에 실패한 코드는 반영되지 않는다.

| 이점 | 설명 |
|---|---|
| 문제 조기 발견 | 버그와 충돌을 초기에 감지 |
| 개발 주기 단축 | 잦은 통합으로 배포 속도 향상 |
| 협업과 코드 품질 향상 | 더 깔끔하고 모듈화된 코드, 팀워크 향상 |
| 자동화된 테스트와 검증 | 통합할 때마다 테스트 실행으로 안정성 확보 |

> 테스트를 언제, 어떤 브랜치에서 실행하고 어떻게 커밋을 반영할지는 조직의 **브랜칭 전략**에 따라 달라진다. 브랜칭 전략과 테스트 전략을 먼저 정하는 것이 중요하다.

### 4-3. 지속적 전달 vs 지속적 배포

둘 다 약어가 CD이지만 운영 배포 단계의 자동화 여부가 다르다.

| 단계 | 개발 | 빌드 | 테스트 | 버전 관리 | 스테이징 배포 | 운영 배포 |
|---|---|---|---|---|---|---|
| 지속적 전달 (Continuous Delivery) | 자동 | 자동 | 자동 | 자동 | 자동 | **수동** |
| 지속적 배포 (Continuous Deployment) | 자동 | 자동 | 자동 | 자동 | 자동 | **자동** |

- **지속적 전달**: 스테이징(사전 운영) 환경까지 자동으로 반영하고, 운영 배포는 사람이 승인해 원하는 시점에 수동으로 한다. 통제된 릴리스가 가능하다.
- **지속적 배포**: 모든 테스트를 통과한 변경이 즉시 운영에 자동 배포된다. 사람의 개입이 없으므로, 배포 여부를 판단할 만큼 **충분히 검증된 테스트**가 전제되어야 한다.

### 4-4. 전체 CI/CD 흐름

개발 → 빌드 → 테스트 → 버전 관리 → 스테이징 배포 → 운영 배포

- 앞의 네 단계가 CI, 뒤의 두 단계가 CD에 해당한다.
- 테스트와 배포를 자동화하면 수동 오류가 줄고, 협업이 개선되며, 고품질 릴리스를 빠르고 일관되게 할 수 있다. 문제가 생기면 이전 버전으로 쉽게 롤백할 수 있다.

---

## 5. 테스트 피라미드

테스트를 범위와 비용에 따라 세 층으로 나눈다. 아래로 갈수록 빠르고 싸며 많이 작성하고, 위로 갈수록 느리고 비싸며 적게 작성한다.

| 층 | 대상 | 속도/비용 | Databricks 예 |
|---|---|---|---|
| **시스템 테스트** (꼭대기) | 전체 애플리케이션을 실제 시나리오로 테스트. 운영과 유사한 환경에서 실행 | 가장 느리고 비쌈 | Job 안의 엔드투엔드 데이터 파이프라인 |
| **통합 테스트** (중간) | 서로 다른 구성 요소, 시스템 간 상호작용 | 중간. 구성 요소가 함께 동작한다는 더 높은 신뢰 제공 | 노트북, 선언적 파이프라인, Job 간 상호작용 |
| **단위 테스트** (기반) | 개별 함수, 메서드를 독립적으로 | 가장 빠르고 쌈. 커버리지가 넓고 자동화 쉬움 | 사용자 정의 PySpark 함수 |

---

## 6. 프로젝트 계획과 환경 격리

### 6-1. 요구 사항부터 정리

프로젝트 계획은 **산출물**에서 출발한다. 예제 프로젝트는 다음과 같다.

| 항목 | 내용 |
|---|---|
| 산출물 | 건강 데이터 시각화 |
| 작업 | 매일 들어오는 증분 CSV 파일을 브론즈로 수집 → 정제된 실버 생성 → 소비자와 공유할 골드 생성 |
| 사용할 Databricks 자산 | 워크스페이스, 노트북, 선언적 파이프라인(SDP), Lakeflow Jobs, Compute |

### 6-2. 환경별 데이터

| 환경 | 데이터 특징 | 목적 |
|---|---|---|
| **개발 (dev)** | 운영 데이터의 소규모 정적 하위 집합. 익명화하거나 합성한 데이터 사용 가능 | 빠른 개발과 테스트, 개인 정보 보호와 운영 데이터 무결성 보장 |
| **스테이징 (stage)** | 운영의 구조와 규모를 최대한 반영. 보통 정적. 민감 정보는 익명화하거나 제거 | 현실적인 테스트와 검증 |
| **운영 (prod)** | 실시간으로 계속 갱신되는 실제 사용자 데이터 | 실제 서비스. 높은 보안, 개인 정보 보호, 규정 준수 기준 필요 |

- 각 환경은 목적에 맞는 데이터를 가져야 하며, 현실성, 보안, 규정 준수 사이의 균형을 맞춘다.
- dev와 stage 데이터는 **정적**이므로 기대 결과(행 수 등)를 미리 알 수 있다. 이것이 통합 테스트를 가능하게 한다.

### 6-3. 환경 격리 방법

운영에 영향을 주기 전에 개발과 스테이징에서 코드를 개발하고 테스트하려면 환경 격리가 필수다. 최소 구성은 "개발 + 스테이징"과 "운영" 두 환경이지만, 조직에 따라 다르다.

| 방법 | 구성 | 특징 |
|---|---|---|
| **워크스페이스 격리** | dev, stg, prd 워크스페이스를 각각 만들고, 스토리지도 환경별로 분리 | 가장 강한 격리. 운영 워크스페이스에서는 개발 작업 자체가 일어나지 않음 |
| **카탈로그 격리 (Unity Catalog)** | 하나의 메타스토어 아래 dev, stg, prd 카탈로그를 두고, 카탈로그별로 스토리지 위치 분리 | 단일 워크스페이스에서도 가능. UC 접근 제어로 사용자는 허용된 데이터, 메타데이터에만 접근 |

- 둘을 조합하는 경우가 많다. 예: 환경별 워크스페이스 + 환경별 카탈로그 + 카탈로그-워크스페이스 바인딩(운영 카탈로그는 운영 워크스페이스에서만 접근).
- 카탈로그 격리는 관리형 스토리지 위치를 카탈로그 수준에서 따로 지정해 스토리지까지 분리할 수 있다.

### 6-4. 예제 프로젝트 아키텍처

| 카탈로그 | 볼륨의 데이터 | 규모 |
|---|---|---|
| `<name>_1_dev` | `dev_health.csv` (PII 마스킹) | 운영의 소규모 하위 집합, 7,500행 |
| `<name>_2_stage` | `stage_health.csv` | 운영의 대규모 하위 집합, 35,000행 |
| `<name>_3_prod` | `2025-01-01_health.csv`, `2025-01-02_health.csv`, ... | 매일 새 파일이 추가됨 |

각 카탈로그에는 `dev_ops` 스키마와 `health` 볼륨이 있다. 워크플로(Lakeflow Job)는 다음 순서로 실행된다.

단위 테스트 → 선언적 파이프라인(ETL + 통합 테스트) → 데이터 시각화

dev 데이터로 개발을 시작해 stage에서 더 넓게 테스트하고, 모든 단계가 통과하면 prod에 배포한다. **같은 코드**가 설정 값만 바꿔 세 환경에서 실행된다.

---

## 7. 단위 테스트: PySpark 테스트 유틸리티

### 7-1. 단위 테스트의 이점

- 특정 함수 하나만 소량의 데이터로 테스트하므로 문제를 쉽게 격리할 수 있다.
- 운영에 배포하기 전에 버그를 발견한다.
- 리팩터링이 쉬워진다. 변경이 기존 기능을 망가뜨리지 않았는지 바로 확인할 수 있다.
- 문제가 생긴 정확한 위치를 알려 주므로 디버깅이 쉬워진다.

### 7-2. `pyspark.testing.utils`

PySpark에 내장된 테스트 도우미 함수다.

| 함수 | 용도 |
|---|---|
| `assertDataFrameEqual(actual, expected, ...)` | 두 DataFrame의 **데이터와 스키마**가 같은지 확인. 다르면 차이를 보여 주며 오류 |
| `assertSchemaEqual(actual, expected)` | 두 **스키마**가 같은지 확인 |

`assertDataFrameEqual`의 주요 선택 인자는 다음과 같다.

| 인자 | 기본값 | 의미 |
|---|---|---|
| `checkRowOrder` | `False` | 행 순서까지 비교할지. 기본은 순서 무시 |
| `rtol`, `atol` | 1e-5, 1e-8 | 부동소수점 비교의 상대/절대 허용 오차 |
| `ignoreNullable` | `True` | 스키마 비교 시 nullable 차이 무시 |
| `ignoreColumnOrder` | `False` | 컬럼 순서 무시 |
| `ignoreColumnName` | `False` | 컬럼 이름 무시하고 위치로 비교 |

> 위 선택 인자들은 PySpark 버전에 따라 추가되었으므로, 사용하는 런타임 버전의 문서를 확인한다.

> 서버리스 compute에서 `assertDataFrameEqual`을 실행하면 "usage logger ... JVM wasn't initialised" 같은 메시지가 출력될 수 있다. 내부적으로 pandas API on Spark를 불러오며 사용량 로거를 붙이려다 생기는 메시지로, 테스트 결과에는 영향이 없다.

### 7-3. 단위 테스트 작성 다섯 단계

테스트할 함수:

```python
from pyspark.sql.functions import col, when

def add_new_col(df, new, s_col):
    return df.withColumn(new, when(col(s_col) == 0, 'Normal').otherwise('Unknown'))
```

원하는 결과: `0 → Normal`, `1 → Unknown`, `-1 → Unknown`, `null → Unknown`

```python
from pyspark.testing.utils import assertDataFrameEqual

def test_add_new_col():                                   # 1. test_ 로 시작하는 이름
    data = [(0,), (1,), (-1,), (None,)]                   # 2. 생각할 수 있는 모든 경우를 담은 샘플 데이터
    df = spark.createDataFrame(data, ["value"])

    actual_df = add_new_col(df, "new_value", "value")     # 3. 함수 실행

    expected_data = [(0, 'Normal'), (1, 'Unknown'),       # 4. 기대 결과
                     (-1, 'Unknown'), (None, 'Unknown')]
    expected_df = spark.createDataFrame(expected_data, ["value", "new_value"])

    assertDataFrameEqual(actual_df, expected_df)          # 5. 동등성 검증 (다르면 오류)
```

- 샘플 데이터에는 정상 값뿐 아니라 **경계값, 음수, null 같은 예외 케이스**를 넣는다.
- 기대 결과의 스키마도 정확해야 한다. 예를 들어 Python 정수로 만든 DataFrame의 컬럼은 `LongType`이 되므로, 기대 DataFrame의 스키마를 명시할 때도 `LongType`으로 맞춰야 한다.

### 7-4. 예제: 스키마 함수와 매핑 함수 테스트

```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType, LongType
from pyspark.testing.utils import assertSchemaEqual, assertDataFrameEqual
from src.helpers import project_functions

def test_get_health_csv_schema_match():
    actual_schema = project_functions.get_health_csv_schema()
    # 함수가 반환해야 하는 스키마. 누군가 함수를 바꾸면 이 테스트가 실패한다
    expected_schema = StructType([
        StructField("ID", IntegerType(), True),
        StructField("PII", StringType(), True),
        StructField("date", DateType(), True),
        StructField("HighCholest", IntegerType(), True),
        StructField("HighBP", DoubleType(), True),
        StructField("BMI", DoubleType(), True),
        StructField("Age", DoubleType(), True),
        StructField("Education", DoubleType(), True),
        StructField("income", IntegerType(), True),
    ])
    assertSchemaEqual(actual_schema, expected_schema)

def test_high_cholest_column_valid_map():
    sample_df = spark.createDataFrame([(0,), (1,), (2,), (3,), (4,), (None,)], ["value"])
    actual_df = sample_df.withColumn("actual", project_functions.high_cholest_map("value"))

    expected_df = spark.createDataFrame(
        [(0, "Normal"), (1, "Above Average"), (2, "High"),
         (3, "Unknown"), (4, "Unknown"), (None, "Unknown")],
        schema=StructType([
            StructField("value", LongType(), True),
            StructField("actual", StringType(), True),
        ]))
    assertDataFrameEqual(actual_df, expected_df)
```

기대 값을 하나라도 틀리게 적으면(예: `(0, "Bad Value")`) `assertDataFrameEqual`이 어느 행이 다른지 보여 주며 실패한다. 테스트가 실패하면 함수와 기대 값 중 어디가 틀렸는지 찾아 고친다.

### 7-5. 예제: 단순 assert와 숫자 변환 테스트

```python
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.testing.utils import assertDataFrameEqual
from src_lab.lab_functions import transforms

def test_uppercase_columns_function():
    df = spark.createDataFrame([(1, 5.0, 1, 1, 1, 1)],
        ["id", "trip_distance", "My_Column", "WithNumbers123", "WithSymbolX@#", "With Space"])
    actual_columns = transforms.uppercase_column_names(df).columns
    expected_columns = ['ID', 'TRIP_DISTANCE', 'MY_COLUMN', 'WITHNUMBERS123', 'WITHSYMBOLX@#', 'WITH SPACE']
    assert actual_columns == expected_columns      # 리스트 비교는 Python assert로 충분

def test_convert_miles_to_km_function():
    schema_in = StructType([StructField("trip_distance_miles", DoubleType(), True)])
    df = spark.createDataFrame([(1.0,), (5.5,), (None,)], schema_in)
    actual_df = transforms.convert_miles_to_km(df, "trip_distance_km", "trip_distance_miles")

    schema_out = StructType([
        StructField("trip_distance_miles", DoubleType(), True),
        StructField("trip_distance_km", DoubleType(), True),
    ])
    expected_df = spark.createDataFrame([(1.0, 1.61), (5.5, 8.85), (None, None)], schema_out)
    assertDataFrameEqual(actual_df, expected_df)
```

> 실제로는 음수, 0, 매우 큰 값 같은 케이스도 테스트해야 한다. 테스트의 철저함은 조직의 기준에 따라 정한다.

---

## 8. 단위 테스트: pytest

### 8-1. pytest의 특징

| 특징 | 설명 |
|---|---|
| 간단한 문법 | `test_`로 시작하는 함수만 정의하면 됨. 클래스 상속 불필요 |
| assert 문 사용 | Python 기본 `assert`를 쓰며, 실패 시 상세한 오류 메시지 제공. PySpark 테스트 유틸리티도 함께 사용 가능 |
| 자동 검색 | 규칙에 맞는 파일과 함수를 자동으로 찾아 실행 |
| 풍부한 플러그인 | 커버리지(`pytest-cov`), 병렬 실행(`pytest-xdist`) 등으로 확장 |

다른 프레임워크로 `unittest`(Python 표준), `chispa`(PySpark DataFrame 비교), `nutter`(노트북 테스트) 등이 있다. 팀과 조직에 맞는 것을 고른다.

### 8-2. 테스트 검색 규칙

- 파일: `test_*.py` 또는 `*_test.py`
- 함수: `test_`로 시작하는 함수
- 예: `test_example.py` 안의 `test_addition()`은 자동으로 감지되어 실행된다.

### 8-3. 테스트를 별도 파일로 분리

노트북에서 테스트를 먼저 만들어 본 뒤, 운영 코드와 분리해 `tests/unit_tests/` 아래 .py 파일로 옮기는 것이 일반적이다. 테스트를 관리, 수정, 실행하기 쉬워지고 운영 코드에 영향을 주지 않는다.

```python
# tests/unit_tests/test_spark_helper_functions.py
import pytest
from pyspark.sql import SparkSession
from pyspark.testing.utils import assertSchemaEqual, assertDataFrameEqual
from src.helpers import project_functions

@pytest.fixture(scope="session")
def spark():
    # 테스트 세션당 한 번 생성되어 모든 테스트 함수가 공유
    spark = SparkSession.builder.getOrCreate()
    yield spark

def test_get_health_csv_schema_match():
    ...

def test_high_cholest_column_valid_map(spark):   # 인자 이름으로 픽스처를 주입받음
    sample_df = spark.createDataFrame([(0,), (None,)], ["value"])
    ...

def test_age_group_column_valid_map(spark):
    ...
```

- **픽스처(fixture)** 는 테스트에 필요한 준비물(여기서는 SparkSession)을 제공하는 함수다. 테스트 함수의 인자 이름으로 주입된다.
- `scope="session"`이면 테스트 세션 전체에서 한 번만 만들어 공유한다. SparkSession처럼 생성 비용이 큰 객체에 적합하다.
- 여러 테스트 파일이 같은 픽스처를 쓰면 `tests/conftest.py`에 두면 된다. pytest가 자동으로 불러온다.

### 8-4. pytest.ini로 모듈 경로 설정

테스트 파일에서 `from src.helpers import ...`가 동작하려면 프로젝트 루트가 Python 모듈 검색 경로에 있어야 한다. `sys.path`를 직접 고치는 대신 `pytest.ini`에 설정한다.

```ini
# 프로젝트 루트의 pytest.ini
[pytest]
pythonpath = .
testpaths = tests
```

### 8-5. Databricks에서 pytest 실행

```python
%pip install pytest==8.3.4
```

```python
import pytest
import sys

sys.dont_write_bytecode = True   # __pycache__/.pyc 생성 방지 (읽기 전용 환경에서 유용)

retcode = pytest.main([
    "../../tests/unit_tests/test_spark_helper_functions.py",   # 실행할 테스트 파일 또는 폴더
    "-v",                                                      # 테스트별 상세 결과 출력
    "-p", "no:cacheprovider",                                  # pytest 캐시 비활성화
])
assert retcode == 0, "pytest 실행 실패. 로그를 확인한다."      # Job 태스크에서 실패를 전파
```

- `pytest.main()`은 종료 코드를 반환한다. 0이면 모두 통과다. **Job의 태스크로 실행할 때는 `assert retcode == 0`을 넣어야** 테스트 실패 시 태스크도 실패하고, 이후 태스크가 실행되지 않는다.
- 테스트 모듈을 수정한 뒤 같은 세션에서 다시 실행할 때는 `importlib.reload()`나 autoreload로 최신 코드를 반영한다.
- 실행 방법은 여러 가지다: 노트북 셀에서 `pytest.main()`, 테스트 실행 전용 노트북을 Job 태스크로 실행, 워크스페이스 파일 편집기의 테스트 실행 기능, 로컬 IDE(예: VS Code의 Databricks 확장)에서 실행.
- CI 시스템(GitHub Actions 등)에 연결하면 코드가 바뀔 때마다 단위 테스트가 자동 실행된다.

---

## 9. 통합 테스트

### 9-1. 데이터 엔지니어링의 통합 테스트

수집, 변환, 저장, 조회 등 파이프라인의 여러 구성 요소가 **실제 환경에서 함께** 잘 동작하는지 확인한다. 데이터 일관성 문제, 형식 불일치, 처리 오류를 점검한다.

자주 쓰는 검증 항목은 다음과 같다.

- 행 수 검증 (dev 7,500행, stage 35,000행처럼 기대 값과 비교)
- 테이블이 성공적으로 생성되었는지
- 컬럼에 올바른 고유 값만 있는지 (예: `HighCholest_Group`이 4개 범주 중 하나)
- 값이 특정 범위 안에 있는지 (예: Age, BMI)
- 기본 키에 중복이 없는지

Databricks에서 통합 테스트를 실행하는 대표적인 방법은 두 가지다.

### 9-2. 방법 1: 선언적 파이프라인(SDP)의 Expectations

- 하나의 SDP 소스 코드가 브론즈, 실버, 골드를 만들고, **파이프라인 설정 변수**(`target`, `raw_data_path`)만 바꿔서 dev, stage, prod에서 똑같이 실행한다.
- dev와 stage 실행 시에는 **테스트용 Materialized View**를 추가로 만들고, 그 MV에 expectation을 걸어 결과를 검증한다. 데이터가 정적이라 기대 결과를 알 수 있기 때문이다.
- prod에서는 테스트 MV를 만들지 않는다.

파이프라인 설정 예:

| 키 | dev | stage | prod |
|---|---|---|---|
| `target` | `development` | `stage` | `production` |
| `raw_data_path` | `/Volumes/<dev 카탈로그>/dev_ops/health` | `/Volumes/<stage 카탈로그>/dev_ops/health` | `/Volumes/<prod 카탈로그>/dev_ops/health` |

파이프라인 소스는 두 종류의 폴더를 함께 포함한다: ETL 소스(`src/sdp/**`)와 통합 테스트 소스(`tests/integration_test/**`).

```python
# src/sdp/ingest_bronze_silver.py : 모듈화한 함수를 재사용하는 ETL
from pyspark import pipelines as dp
from src.helpers import project_functions as pf

raw_data_path = spark.conf.get("raw_data_path")

@dp.table(name="health_bronze")
def health_bronze():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .schema(pf.get_health_csv_schema())
            .load(raw_data_path))

@dp.table(name="health_silver")
def health_silver():
    return (spark.readStream.table("health_bronze")
            .withColumn("HighCholest_Group", pf.high_cholest_map("HighCholest"))
            .withColumn("Age_Group", pf.group_ages_map("Age")))
```

```python
# tests/integration_test/integration_tests_sdp.py : 환경에 따라 테스트 MV 생성
from pyspark import pipelines as dp
from pyspark.sql import functions as F

target = spark.conf.get("target")
expected_rows = {"development": 7500, "stage": 35000}

if target in expected_rows:          # prod에서는 테스트 테이블을 만들지 않음
    n = expected_rows[target]

    @dp.materialized_view(name="test_health_silver_row_count")
    @dp.expect_or_fail("valid_row_count", f"row_count = {n}")
    def test_health_silver_row_count():
        return spark.read.table("health_silver").agg(F.count("*").alias("row_count"))

    @dp.materialized_view(name="test_health_silver_unique_id")
    @dp.expect_or_fail("no_duplicate_id", "total = distinct_ids")
    def test_health_silver_unique_id():
        return spark.read.table("health_silver").agg(
            F.count("*").alias("total"), F.countDistinct("ID").alias("distinct_ids"))

    @dp.materialized_view(name="test_value_ranges")
    @dp.expect("age_in_range", "Age BETWEEN 0 AND 120")    # WARN: 범위 밖 값을 추적만 함
    @dp.expect("bmi_in_range", "BMI BETWEEN 10 AND 80")
    def test_value_ranges():
        return spark.read.table("health_silver").select("ID", "Age", "BMI")
```

- 행 수, 중복처럼 반드시 맞아야 하는 조건은 `expect_or_fail`(위반 시 업데이트 실패)로, 범위 이탈처럼 추적만 할 조건은 `expect`(경고)로 건다.
- 범위 임계값은 비즈니스 규칙이다. dev 실버 테이블의 실제 최솟값, 최댓값을 조회해 보고 정한다.

```sql
SELECT MIN(Age), MAX(Age), MIN(BMI), MAX(BMI) FROM dev_catalog.dev_ops.health_silver;
```

### 9-3. 방법 2: Lakeflow Jobs의 태스크

SDP가 아닌 코드에서 흔히 쓰는 방식이다. 검증 로직을 노트북으로 만들어 Job의 태스크로 넣는다.

Job 구성 예:

| 순서 | 태스크 | 내용 |
|---|---|---|
| 1 | `Unit_Tests` | pytest로 단위 테스트 실행. 실패하면 Job 중단 |
| 2 | `Health_ETL` | 선언적 파이프라인 실행 (expectation 없이 테이블만 생성) |
| 3 | `Integration_Tests` | 검증 노트북 실행 (행 수, 고유 값, 범위, 중복 등) |
| 4 | `Visualization` | 앞의 모든 태스크가 성공하면 최종 시각화 생성 |

구현 단계:

1. 대상 환경과 데이터 위치를 **Job 파라미터**로 받는 설정 노트북을 만든다.
2. 통합 테스트용 노트북이나 파일을 만들어 `tests` 폴더에 정리한다.
3. Job에서 테이블 생성(파이프라인 또는 코드) → 테스트 설정 → 검증 태스크 순으로 구성하고, 모든 태스크가 성공해야 다음으로 넘어가게 한다.

```python
# tests/integration_test/check_silver.py (Job 태스크로 실행)
catalog = dbutils.widgets.get("catalog")
target = dbutils.widgets.get("target")
expected = {"development": 7500, "stage": 35000}[target]

df = spark.table(f"{catalog}.dev_ops.health_silver")
assert df.count() == expected, f"행 수 불일치: {df.count()} != {expected}"
assert df.count() == df.select("ID").distinct().count(), "ID 중복 존재"
valid = {"Normal", "Above Average", "High", "Unknown"}
actual = {r[0] for r in df.select("HighCholest_Group").distinct().collect()}
assert actual <= valid, f"허용되지 않은 값: {actual - valid}"
```

### 9-4. 두 방법 비교

| 항목 | SDP Expectations | Jobs 태스크 |
|---|---|---|
| 위치 | 파이프라인 안의 테스트 MV | 파이프라인 밖의 별도 노트북 태스크 |
| 장점 | 선언적이고 코드가 적음. 결과가 파이프라인 이벤트 로그와 품질 메트릭에 기록됨 | SDP가 아닌 코드에도 적용 가능. 검증 로직을 자유롭게 작성 |
| 단점 | SDP 파이프라인에서만 사용 | 설정과 검증 코드를 더 많이 작성해야 하고, 환경별 동적 처리를 위해 Job 파라미터를 관리해야 함 |

상황에 맞는 방법을 고르면 된다.

---

## 10. Git 버전 관리와 브랜칭

### 10-1. 버전 관리가 없을 때의 문제

1. 시간이 지나며 사일로가 생기고 개발과 팀 운영이 고립된다.
2. 중앙화된 버전 관리가 없으면 중복 코드, 일관성 없는 표준, 코드 리뷰 부재로 품질이 떨어진다.
3. 변경을 추적, 되돌리기, 감사하기 어렵다.
4. 빈번한 업데이트를 관리할 때 위험이 커진다.
5. 브랜칭, 병합, CI/CD 통합이 어려워 개발 규모를 키우기 힘들다.

### 10-2. Git

- 소스 코드 변경을 추적하는 무료 오픈 소스 분산 버전 관리 시스템이다.
- 장점: 협업, 버전 관리, 브랜치와 병합, 분산 구조, 오픈 소스와 광범위한 채택.
- 대표적인 Git 호스팅 서비스: GitHub, GitLab, Azure DevOps, Bitbucket, AWS CodeCommit.
- GitHub 같은 서비스는 저장소 호스팅, 접근 제어, 병합 요청(Pull Request) 도구, CI/CD 도구(GitHub Actions)를 제공한다.

### 10-3. 브랜칭으로 변경을 안전하게 관리

- 브랜칭은 CI/CD를 보완하는 개념으로, **효과적인 CI를 가능하게** 한다.
- 변경을 브랜치에서 버전 관리하고, 메인 브랜치에 병합해 배포하기 전에 품질 관리(코드 리뷰, 자동 테스트)를 거친다.

대표적인 브랜칭 전략:

| 전략 | 구조 | 특징 |
|---|---|---|
| **Gitflow** | `main`(릴리스) + `develop` + `feature/*`, `release/*`, `hotfix/*` | 릴리스 버전(v0.1, v0.2, v1.0)을 명확히 관리. 구조가 복잡 |
| **GitHub Flow** | `main` + 짧게 사는 기능 브랜치 → PR → 병합 | 단순함. 지속적 배포에 적합 |
| **트렁크 기반 개발** | 모두 `main`에 자주(작게) 병합, 기능 플래그 활용 | 통합 지연 최소화. 강력한 자동 테스트 필요 |

### 10-4. 기본 Git 용어

| 용어 | 의미 |
|---|---|
| clone | 원격 저장소를 복제 |
| branch | 독립된 작업 흐름. 기능 개발을 `main`과 분리 |
| commit | 변경 사항을 메시지와 함께 저장소 이력에 기록 |
| push | 로컬 커밋을 원격 저장소에 업로드 |
| pull | 원격의 최신 커밋을 가져와 병합 |
| Pull Request (PR) | 한 브랜치의 변경을 다른 브랜치로 병합하자는 제안. 리뷰어가 검토, 논의 후 병합 |
| merge | 브랜치의 커밋을 다른 브랜치에 합침 |
| conflict | 같은 부분을 서로 다르게 바꿔 자동 병합이 불가능한 상태 |

---

## 11. Databricks Git Folders

### 11-1. Git Folder란

- **원격 Git 저장소에 연결된 워크스페이스 폴더**다. 이전 이름은 **Repos**다.
- 워크스페이스 안에서 clone, checkout, commit, push, pull, 브랜치 생성과 전환, 커밋 전 변경 내용(diff) 시각적 검토를 할 수 있다.
- 노트북뿐 아니라 **임의의 파일**(.py, .yml, .txt, .csv, 이미지 등)을 지원한다.
  - Python/R 파일을 라이브러리처럼 import (상대 import 가능)
  - 환경 설정 파일(.yml 등) 보관
  - 같은 저장소에서 패키지 빌드
  - 소규모 데이터 파일 사용
- 워크스페이스에서 기본으로 활성화되어 있다.

### 11-2. Git 공급자 인증: 개인 액세스 토큰(PAT)

Databricks에서 사용자 설정의 Git 연동 항목에 공급자(GitHub 등)와 사용자 이름, 토큰을 등록한다. GitHub는 두 종류의 PAT가 있다.

**세분화된(fine-grained) PAT (권장)**: 특정 저장소에만 범위를 한정할 수 있다.

| 권한 | 수준 |
|---|---|
| Administration | Read and Write |
| Contents | Read and Write |
| Pull Requests | Read and Write |
| Webhooks | Read and Write |
| Metadata | Read-only (자동 필수) |

**클래식(legacy) PAT**: 범위가 넓다.

| 스코프 | 의미 |
|---|---|
| `repo` | 비공개 저장소 전체 제어 |
| `workflow` | GitHub Actions 워크플로 읽기/쓰기 |
| `admin:repo_hook` | 저장소 훅 관리 |
| `delete_repo` | 저장소 삭제 |

- 토큰에는 **만료일**을 설정하고, 쓰지 않는 토큰은 삭제한다.
- 개인 학습용 GitHub 계정과 업무용 계정은 분리한다.
- GitHub는 PAT 대신 Databricks GitHub App(OAuth)으로 연결할 수도 있다.

### 11-3. 기본 협업 워크플로

1. GitHub에 저장소를 만들고 HTTPS URL(`https://github.com/<user>/<repo>.git`)로 Databricks에 Git Folder를 만든다. 공급자는 URL로 자동 감지된다.
2. `main` 브랜치에서 파일(예: README.md)을 만들고 commit & push한다.
3. 기능 개발용 `dev`(또는 `feature/...`) 브랜치를 만들고 전환한다.
4. 브랜치에서 작업하고 commit & push한다. 이 시점에 `main`에는 아직 반영되지 않는다.
5. GitHub에서 **Pull Request**를 연다. base = `main`, compare = `dev`. 리뷰어, 담당자, 라벨을 지정하고 리뷰를 받는다.
6. 충돌이 없으면 PR을 **병합**한다. 필요하면 기능 브랜치를 삭제한다.
7. Databricks Git Folder에서 `main`을 **pull**해 워크스페이스를 최신 상태로 맞춘다.

> 운영 환경에서는 `main` 브랜치에 **브랜치 보호 규칙**(직접 push 금지, PR 리뷰 필수, CI 테스트 통과 필수)을 거는 것이 일반적이다.

### 11-4. 개발 폴더와 운영 폴더의 분리

- **개발자 폴더**: 각자 자기 사용자 폴더에 Git Folder를 만들고 기능 브랜치에서 작업한다.
- **운영 폴더**: 운영용 Git Folder는 사람이 직접 수정하지 않고, CI/CD가 정해진 브랜치나 태그로 자동 갱신한다(Repos API 활용).
- **Repos API**: Git Folder를 프로그래밍 방식으로 생성하고, 특정 브랜치나 커밋으로 업데이트할 수 있다. 예: `main`에 병합되면 운영 Git Folder를 자동으로 최신 커밋으로 갱신.
- **Job의 Git 소스**: Job 태스크가 워크스페이스 사본 대신 원격 저장소의 특정 **브랜치, 태그, 커밋**을 직접 가져와 실행하게 할 수 있다. 실행할 코드 버전이 명확해진다.

```bash
# CLI로 운영 Git Folder를 main 최신 커밋으로 갱신
databricks repos update /Repos/prod/health_project --branch main
```

---

## 12. 지속적 배포(CD): Databricks 자산 배포 방법

### 12-1. 네 가지 배포 방법

| 방법 | 형태 | 특징 | 적합한 용도 |
|---|---|---|---|
| **REST API** | HTTP 요청 | 모든 기능을 가장 저수준으로 제공. 언어 무관 | 다른 도구가 지원하지 않는 기능, 자체 도구 개발 |
| **Databricks CLI** | `databricks` 명령 | REST API를 감싼 명령줄 도구. 스크립트와 CI에 쓰기 쉬움 | 셸 스크립트, CI 파이프라인 |
| **Databricks SDK** | Python, Java, Go 라이브러리 | 타입이 있는 API 클라이언트. 인증, 재시도, 페이지네이션 처리 | 프로그램 안에서 자동화 (예: 파이프라인 생성 후 상태 대기) |
| **Declarative Automation Bundles** (이전 명칭: Databricks Asset Bundles, DABs) | YAML 설정 + CLI | Job, 파이프라인 등 리소스와 코드를 함께 **선언적으로** 정의하고 환경별로 배포 | **CI/CD의 권장 방식** |

```python
# SDK 예: 파이프라인 업데이트가 끝날 때까지 대기
from databricks.sdk import WorkspaceClient
import time

w = WorkspaceClient()
for _ in range(60):
    latest = (w.pipelines.get(pipeline_id).latest_updates or [None])[0]
    state = latest.state.value if latest and latest.state else None
    if state in ("COMPLETED", "FAILED", "CANCELED"):
        break
    time.sleep(20)
assert state == "COMPLETED"
```

> 파이프라인이나 Job 설정은 UI에서 YAML로 내보낼 수 있다. 이 YAML을 번들 설정의 출발점으로 쓰면 편하다.

### 12-2. Declarative Automation Bundles 기초

번들은 **코드 + 리소스 정의 + 환경별 설정**을 하나의 프로젝트로 묶어 Git으로 버전 관리하고, 같은 정의를 dev, stage, prod에 배포한다.

```yaml
# databricks.yml
bundle:
  name: health_project

variables:
  catalog:
    description: 대상 카탈로그

resources:
  pipelines:
    health_etl:
      name: health_etl_${bundle.target}
      catalog: ${var.catalog}
      schema: dev_ops
      serverless: true
      libraries:
        - glob:
            include: src/sdp/**
        - glob:
            include: tests/integration_test/**
      configuration:
        target: ${bundle.target}
        raw_data_path: /Volumes/${var.catalog}/dev_ops/health

  jobs:
    health_job:
      name: health_job_${bundle.target}
      tasks:
        - task_key: Unit_Tests
          notebook_task:
            notebook_path: ./Run Unit Tests
        - task_key: Health_ETL
          depends_on: [{ task_key: Unit_Tests }]
          pipeline_task:
            pipeline_id: ${resources.pipelines.health_etl.id}
        - task_key: Visualization
          depends_on: [{ task_key: Health_ETL }]
          notebook_task:
            notebook_path: ./src/Final Visualization

targets:
  development:
    mode: development     # 리소스 이름에 사용자 접두사, 스케줄 일시 정지
    default: true
    variables:
      catalog: my_catalog_1_dev
  stage:
    variables:
      catalog: my_catalog_2_stage
  production:
    mode: production      # 운영 배포용 검증 강화
    variables:
      catalog: my_catalog_3_prod
```

```bash
databricks bundle validate -t stage     # 설정 검증
databricks bundle deploy   -t stage     # 리소스와 코드 배포
databricks bundle run      -t stage health_job   # Job 실행
databricks bundle destroy  -t development        # 배포한 리소스 삭제
```

### 12-3. CI/CD 파이프라인 예: GitHub Actions + 번들

GitHub Actions는 저장소에서 일어나는 **이벤트**(push, pull request 등)에 반응해 미리 정의한 작업을 자동 실행한다.

```yaml
# .github/workflows/ci.yml
name: health-project-ci
on:
  pull_request:
    branches: [main]      # main으로 PR이 열리면 stage에서 검증
  push:
    branches: [main]      # main에 병합되면 운영 배포

jobs:
  stage:
    if: github.event_name == 'pull_request'
    runs-on: ubuntu-latest
    env:
      DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
      DATABRICKS_CLIENT_ID: ${{ secrets.SP_CLIENT_ID }}
      DATABRICKS_CLIENT_SECRET: ${{ secrets.SP_CLIENT_SECRET }}
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - run: databricks bundle validate -t stage
      - run: databricks bundle deploy -t stage
      - run: databricks bundle run -t stage health_job   # 단위 + 통합 테스트 포함

  production:
    if: github.event_name == 'push'
    runs-on: ubuntu-latest
    environment: production      # 수동 승인을 걸면 지속적 전달, 없으면 지속적 배포
    env:
      DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
      DATABRICKS_CLIENT_ID: ${{ secrets.SP_CLIENT_ID }}
      DATABRICKS_CLIENT_SECRET: ${{ secrets.SP_CLIENT_SECRET }}
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - run: databricks bundle deploy -t production
```

- CI/CD 도구가 Databricks에 접속할 때는 개인 토큰이 아니라 **서비스 주체(Service Principal)** 의 OAuth 자격 증명을 쓰고, 비밀 값은 CI 도구의 시크릿 저장소에 둔다.
- 운영 배포 전에 수동 승인 단계를 두면 지속적 전달, 두지 않으면 지속적 배포가 된다.

---

## 13. 종합: 프로젝트 전체 흐름

| 단계 | 하는 일 | 관련 개념 |
|---|---|---|
| 1. 계획 | 산출물, 작업, 자산 정의. dev/stage/prod 카탈로그와 데이터 준비 | 프로젝트 계획, 환경 격리 |
| 2. 개발 | dev 데이터로 노트북에서 탐색 후 함수로 모듈화, .py로 분리 | 모듈화 |
| 3. 단위 테스트 | 함수별 테스트를 `tests/unit_tests`에 작성, pytest로 실행 | 테스트 피라미드 기반 |
| 4. 파이프라인 | 모듈화한 함수로 SDP 작성. 설정 변수로 환경 전환 | 공유 파이프라인 코드 |
| 5. 통합 테스트 | dev, stage에서 테스트 MV + expectation 또는 Job 검증 태스크 | 통합 테스트 |
| 6. 버전 관리 | 기능 브랜치에서 commit & push → PR → 리뷰 → `main` 병합 | Git Folders, 브랜칭 |
| 7. 배포 | 번들로 stage 배포 및 검증 후 prod 배포 (CI/CD 자동화) | CD, 번들 |
| 8. 운영과 모니터링 | Job 실행, 알림, 품질 메트릭 모니터링. 개선 사항은 다시 1단계로 | DevOps 루프 |

---

## 14. 핵심 정리 치트시트

### 개념

| 키워드 | 핵심 |
|---|---|
| SWE 모범 사례 | 코딩 관행, 문서화, 자동화 테스트, 버전 관리와 코드 리뷰, CI/CD, 격리된 환경 |
| 모듈화 | 작고 재사용 가능한 함수로 분리. 유지 보수, 재사용, 테스트, 협업 개선 |
| DevOps | 개발 + 운영의 문화와 실천. 8단계: Plan, Code, Build, Test, Release, Deploy, Operate, Monitor |
| DataOps | 데이터 엔지니어링을 위한 DevOps |
| CI | 자주 병합 + 매번 자동 테스트. 실패한 코드는 반영 안 됨 |
| 지속적 전달 | 스테이징까지 자동, 운영은 수동 승인 |
| 지속적 배포 | 테스트 통과 시 운영까지 자동 |
| 테스트 피라미드 | 단위(빠름, 많이) → 통합 → 시스템(느림, 적게) |

### 환경

| 키워드 | 핵심 |
|---|---|
| dev 데이터 | 운영의 소규모 정적 하위 집합, 익명화/합성 |
| stage 데이터 | 운영의 구조와 규모 반영, 정적, 민감 정보 제거 |
| prod 데이터 | 실시간 실제 데이터, 높은 보안 기준 |
| 격리 방법 | 환경별 워크스페이스 또는 환경별 카탈로그(UC), 스토리지도 분리 |

### 테스트

| 키워드 | 핵심 |
|---|---|
| `assertDataFrameEqual` | 데이터 + 스키마 비교, 기본은 행 순서 무시 |
| `assertSchemaEqual` | 스키마만 비교 |
| pytest 규칙 | 파일 `test_*.py` / `*_test.py`, 함수 `test_*` |
| 픽스처 | `@pytest.fixture(scope="session")`으로 SparkSession 공유, `conftest.py`에 공통 픽스처 |
| `pytest.ini` | `pythonpath`로 모듈 경로 설정 |
| `pytest.main([...])` | `-v` 상세 출력, `-p no:cacheprovider` 캐시 끔, 반환값 0 = 통과 |
| SDP 통합 테스트 | 설정 변수로 환경 전환, dev/stage에서만 테스트 MV + expectation |
| Jobs 통합 테스트 | 단위 테스트 → ETL → 검증 노트북 → 시각화 태스크 |

### Git과 배포

| 키워드 | 핵심 |
|---|---|
| Git Folder | 원격 저장소와 연결된 워크스페이스 폴더 (이전 이름 Repos), 임의 파일 지원 |
| PAT | 세분화된 PAT 권장 (Administration, Contents, Pull Requests, Webhooks 읽기/쓰기), 만료일 설정 |
| PR | 브랜치 병합 제안, 리뷰 후 병합 |
| 운영 폴더 | 사람이 수정하지 않고 CI/CD와 Repos API로 갱신, Job은 Git 소스로 브랜치/태그/커밋 지정 가능 |
| 배포 방법 | REST API(저수준), CLI(스크립트), SDK(프로그램), 번들(선언적, CI/CD 권장) |
| 번들 명령 | `bundle validate` / `deploy` / `run` / `destroy`, `-t <target>` |
| CI/CD 인증 | 서비스 주체 OAuth, 비밀 값은 시크릿 저장소 |

---

## 15. 자가 점검 문제

> 답을 가리고 먼저 생각해본 뒤 펼쳐보자.

**Q1.** 소프트웨어 엔지니어링 모범 사례 여섯 가지는?
<details><summary>정답</summary>

코딩 관행(가독성, 명명 규칙, 모듈식 설계), 코드 문서화, 자동화 테스트(단위, 통합), 버전 관리와 코드 리뷰, CI/CD, 격리된 환경.
</details>

**Q2.** 모듈화되지 않은 PySpark 코드의 두 가지 문제와 모듈화의 이점은?
<details><summary>정답</summary>

문제: 모든 코드가 한 덩어리라 특정 부분을 수정하거나 테스트하기 어렵고, 같은 로직이 중복된다. 이점: 유지 보수 용이(함수만 수정), 재사용성, 함수 단위 테스트 가능, 팀원 간 독립 작업.
</details>

**Q3.** `high_cholest_map`처럼 DataFrame이 아니라 Column 표현식을 반환하는 함수의 장점은?
<details><summary>정답</summary>

`withColumn`, `select` 등 어디서든 조합해 쓸 수 있어 재사용성이 높고, 작은 샘플 DataFrame에 적용해 단위 테스트하기 쉽다.
</details>

**Q4.** 노트북에서 상위 폴더의 .py 모듈을 import하려면? .py 파일 수정을 바로 반영하려면?
<details><summary>정답</summary>

프로젝트 루트 경로를 `sys.path.append()`로 추가한 뒤 import한다(같은 폴더의 모듈은 바로 import 가능). 수정 반영은 `%load_ext autoreload`, `%autoreload 2`를 쓴다.
</details>

**Q5.** DevOps 라이프사이클 8단계를 순서대로 말하라.
<details><summary>정답</summary>

Plan → Code → Build → Test → Release → Deploy → Operate → Monitor. 이후 다시 Plan으로 돌아가 반복한다.
</details>

**Q6.** DevOps, DataOps, MLOps의 관계는?
<details><summary>정답</summary>

DataOps는 DevOps 원칙을 데이터 엔지니어링에, MLOps는 머신 러닝에 적용한 것이다. DataOps는 비즈니스 요구 사항을 비즈니스 가치로 바꾸는 고품질 데이터 파이프라인 프로세스(품질, 리니지, 거버넌스, 협업)에 초점을 둔다.
</details>

**Q7.** CI와 CD는 DevOps 라이프사이클의 어느 부분을 담당하는가?
<details><summary>정답</summary>

CI는 계획, 코딩, 빌드, 테스트(환경 관리 포함), CD는 릴리스, 배포, 운영, 모니터링을 담당한다.
</details>

**Q8.** 지속적 통합(CI)의 정의와 네 가지 이점은?
<details><summary>정답</summary>

여러 기여자의 변경을 중앙 저장소에 자주 병합하고 매번 자동 테스트를 실행하는 방식이다(실패한 코드는 반영 안 됨). 이점: 문제 조기 발견, 개발 주기 단축, 협업과 코드 품질 향상, 자동화된 테스트와 검증.
</details>

**Q9.** 지속적 전달과 지속적 배포의 차이는?
<details><summary>정답</summary>

지속적 전달은 스테이징까지 자동으로 반영하고 운영 배포는 수동 승인으로 한다. 지속적 배포는 테스트를 통과한 모든 변경을 운영까지 자동 배포하며, 충분히 검증된 테스트가 전제되어야 한다.
</details>

**Q10.** 테스트 피라미드의 세 층과 Databricks에서의 예는?
<details><summary>정답</summary>

단위 테스트(기반, 빠르고 저렴, 사용자 정의 PySpark 함수), 통합 테스트(구성 요소 간 상호작용, 노트북/SDP/Job 간), 시스템 테스트(꼭대기, 느리고 비쌈, Job 안의 엔드투엔드 파이프라인).
</details>

**Q11.** dev, stage, prod 데이터는 각각 어떤 특징을 가져야 하는가?
<details><summary>정답</summary>

dev: 운영의 소규모 정적 하위 집합, 익명화나 합성 데이터. stage: 운영의 구조와 규모를 반영한 정적 데이터, 민감 정보 제거. prod: 실시간으로 갱신되는 실제 데이터, 높은 보안과 규정 준수 기준.
</details>

**Q12.** dev와 stage 데이터가 정적이어야 하는 이유는?
<details><summary>정답</summary>

기대 결과(행 수, 고유 값 등)를 미리 알 수 있어야 통합 테스트로 파이프라인 결과를 검증할 수 있기 때문이다.
</details>

**Q13.** Databricks에서 환경을 격리하는 두 가지 방법은?
<details><summary>정답</summary>

환경별로 워크스페이스(와 스토리지)를 분리하는 방법, 하나의 메타스토어 아래 환경별 카탈로그를 두고 카탈로그별 스토리지 위치와 UC 접근 제어로 분리하는 방법. 둘을 조합하기도 한다.
</details>

**Q14.** `assertDataFrameEqual`과 `assertSchemaEqual`의 차이는? `assertDataFrameEqual`은 기본적으로 행 순서를 비교하는가?
<details><summary>정답</summary>

`assertDataFrameEqual`은 데이터와 스키마를 모두, `assertSchemaEqual`은 스키마만 비교한다. `assertDataFrameEqual`은 기본적으로 행 순서를 무시하며, `checkRowOrder=True`로 순서까지 비교할 수 있다.
</details>

**Q15.** 단위 테스트를 작성하는 다섯 단계는?
<details><summary>정답</summary>

1) `test_`로 시작하는 테스트 함수 정의, 2) 경계값, null 등을 포함한 샘플 DataFrame 생성, 3) 함수 실행, 4) 기대 결과 DataFrame 생성, 5) 실제와 기대 결과 비교(assert).
</details>

**Q16.** Python 정수 리스트로 만든 DataFrame과 비교할 기대 DataFrame의 스키마를 명시할 때 주의할 점은?
<details><summary>정답</summary>

Python 정수는 `LongType`으로 추론되므로 기대 스키마도 `LongType`으로 맞춰야 한다. `IntegerType`으로 적으면 스키마 불일치로 실패한다.
</details>

**Q17.** pytest가 테스트를 자동으로 찾는 규칙은?
<details><summary>정답</summary>

파일 이름이 `test_*.py` 또는 `*_test.py`이고, 함수 이름이 `test_`로 시작하면 자동으로 검색해 실행한다.
</details>

**Q18.** `@pytest.fixture(scope="session")`으로 SparkSession을 만드는 이유는?
<details><summary>정답</summary>

테스트 세션 전체에서 한 번만 생성해 모든 테스트 함수가 공유하게 하기 위해서다. SparkSession은 생성 비용이 크므로 테스트마다 만들면 느리다. 테스트 함수는 인자 이름(`spark`)으로 주입받는다.
</details>

**Q19.** `pytest.ini`의 `pythonpath` 설정은 무슨 역할을 하는가?
<details><summary>정답</summary>

pytest 실행 시 지정한 디렉터리를 모듈 검색 경로에 추가해, 테스트 파일에서 `sys.path`를 직접 수정하지 않고도 `from src.helpers import ...`처럼 프로젝트 코드를 import할 수 있게 한다.
</details>

**Q20.** `pytest.main([path, "-v", "-p", "no:cacheprovider"])`의 각 옵션 의미와, Job 태스크로 실행할 때 추가해야 할 코드는?
<details><summary>정답</summary>

`-v`는 테스트별 상세 출력, `-p no:cacheprovider`는 pytest 캐시 비활성화다. `pytest.main()`은 종료 코드를 반환하므로 `assert retcode == 0`을 넣어야 테스트 실패 시 태스크가 실패하고 후속 태스크가 멈춘다.
</details>

**Q21.** 선언적 파이프라인으로 통합 테스트를 하는 방식을 설명하라.
<details><summary>정답</summary>

하나의 SDP 코드를 설정 변수(`target`, `raw_data_path`)만 바꿔 dev, stage, prod에서 실행한다. dev와 stage에서만 테스트용 Materialized View를 만들고 expectation으로 행 수, 중복, 값 범위 등을 검증한다. prod에서는 테스트 MV를 만들지 않는다.
</details>

**Q22.** Jobs 태스크로 통합 테스트를 할 때의 태스크 구성과 단점은?
<details><summary>정답</summary>

단위 테스트 → 파이프라인(ETL) → 통합 테스트 노트북 → 시각화 순으로 구성하고, 모든 태스크가 성공해야 다음으로 진행한다. 단점은 설정과 검증 코드를 더 많이 작성해야 하고, 환경별 동적 처리를 위해 Job 파라미터를 관리해야 한다는 점이다.
</details>

**Q23.** 브랜칭이 CI에 주는 효과는?
<details><summary>정답</summary>

변경을 브랜치에서 버전 관리하고, 메인 브랜치에 병합하기 전에 코드 리뷰와 자동 테스트 같은 품질 관리를 거치게 해 효과적인 CI를 가능하게 한다.
</details>

**Q24.** Databricks Git Folder란 무엇이며 어떤 작업을 할 수 있는가?
<details><summary>정답</summary>

원격 Git 저장소에 연결된 워크스페이스 폴더(이전 이름 Repos)다. clone, checkout, commit, push, pull, 브랜치 생성과 전환, 커밋 전 diff 검토를 할 수 있고, 노트북 외에 .py, .yml 등 임의 파일도 지원한다.
</details>

**Q25.** GitHub 연결에 세분화된 PAT가 권장되는 이유와 필요한 권한은?
<details><summary>정답</summary>

특정 저장소로 범위를 한정해 최소 권한을 지킬 수 있기 때문이다. Administration, Contents, Pull Requests, Webhooks를 Read and Write로 주고, Metadata는 자동으로 읽기 전용이 된다.
</details>

**Q26.** 기능 개발부터 워크스페이스 반영까지의 Git 협업 흐름은?
<details><summary>정답</summary>

기능 브랜치 생성 → 작업 후 commit & push → GitHub에서 PR(base main, compare 기능 브랜치) → 리뷰 → 병합 → Databricks Git Folder에서 main pull.
</details>

**Q27.** 운영용 Git Folder와 Job의 코드 버전은 어떻게 관리하는가?
<details><summary>정답</summary>

운영 Git Folder는 사람이 직접 수정하지 않고 CI/CD가 Repos API로 특정 브랜치나 커밋으로 갱신한다. Job은 Git 소스로 원격 저장소의 브랜치, 태그, 커밋을 직접 지정해 실행할 수 있다.
</details>

**Q28.** Databricks 자산을 배포하는 네 가지 방법과 각각의 특징은?
<details><summary>정답</summary>

REST API(모든 기능, 저수준, 언어 무관), CLI(REST API를 감싼 명령줄, 스크립트와 CI에 적합), SDK(Python/Java/Go 라이브러리, 프로그램 안에서 자동화), Declarative Automation Bundles(YAML로 리소스와 코드를 선언적으로 정의하고 환경별 배포, CI/CD 권장).
</details>

**Q29.** 번들에서 `targets`의 역할과 주요 CLI 명령은?
<details><summary>정답</summary>

같은 리소스 정의를 dev, stage, prod 등 환경별 설정(변수, 모드, 워크스페이스)으로 배포하게 한다. 명령은 `databricks bundle validate`, `deploy`, `run`, `destroy`이고 `-t <target>`으로 환경을 고른다.
</details>

**Q30.** CI/CD 도구가 Databricks에 배포할 때 인증은 어떻게 하는 것이 좋은가?
<details><summary>정답</summary>

개인 토큰 대신 서비스 주체의 OAuth 자격 증명(client ID, secret)을 쓰고, 비밀 값은 CI 도구의 시크릿 저장소에 보관한다.
</details>

---

## 16. 참고 링크

- 노트북 소프트웨어 엔지니어링 모범 사례: https://docs.databricks.com/aws/en/notebooks/best-practices
- 워크스페이스 파일과 Python 모듈 (autoreload 포함): https://docs.databricks.com/aws/en/files/workspace-modules
- PySpark 테스트 API: https://spark.apache.org/docs/latest/api/python/reference/pyspark.testing.html
- PySpark 테스트 시작하기: https://spark.apache.org/docs/latest/api/python/getting_started/testing_pyspark.html
- pytest 문서: https://docs.pytest.org/en/stable/
- VS Code Databricks 확장에서 pytest 실행: https://docs.databricks.com/aws/en/dev-tools/vscode-ext/pytest
- 파이프라인 Expectations: https://docs.databricks.com/aws/en/ldp/expectations
- Expectation 권장 사항과 고급 패턴: https://docs.databricks.com/aws/en/ldp/expectation-patterns
- Databricks Git Folders: https://docs.databricks.com/aws/en/repos/
- Git 공급자 토큰 설정: https://docs.databricks.com/aws/en/repos/get-access-tokens-from-git-provider
- Databricks CI/CD 개요: https://docs.databricks.com/aws/en/dev-tools/ci-cd/
- 번들 (Declarative Automation Bundles): https://docs.databricks.com/aws/en/dev-tools/bundles/
- 번들과 GitHub Actions: https://docs.databricks.com/aws/en/dev-tools/bundles/ci-cd-bundles
- Databricks CLI: https://docs.databricks.com/aws/en/dev-tools/cli/
- Databricks SDK for Python: https://docs.databricks.com/aws/en/dev-tools/sdk-python
- Databricks REST API: https://docs.databricks.com/api/workspace/introduction
