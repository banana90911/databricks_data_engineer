# Lakeflow Connect를 활용한 데이터 수집 — 핵심 개념 정리

---

## 목차

1. [Lakeflow와 데이터 수집 개요](#1-lakeflow와-데이터-수집-개요)
2. [수집 방식: 배치, 증분 배치, 스트리밍](#2-수집-방식-배치-증분-배치-스트리밍)
3. [Delta Lake와 메달리온 아키텍처 복습](#3-delta-lake와-메달리온-아키텍처-복습)
4. [클라우드 오브젝트 스토리지 수집 방법 비교](#4-클라우드-오브젝트-스토리지-수집-방법-비교)
5. [파일 직접 조회와 read_files()](#5-파일-직접-조회와-read_files)
6. [CTAS를 이용한 배치 수집](#6-ctas를-이용한-배치-수집)
7. [COPY INTO를 이용한 증분 배치 수집](#7-copy-into를-이용한-증분-배치-수집)
8. [스트리밍 테이블과 Auto Loader](#8-스트리밍-테이블과-auto-loader)
9. [수집 시 메타데이터 컬럼 추가](#9-수집-시-메타데이터-컬럼-추가)
10. [Rescued Data 컬럼 (_rescued_data)](#10-rescued-data-컬럼-_rescued_data)
11. [반정형 데이터(JSON) 수집과 평탄화](#11-반정형-데이터json-수집과-평탄화)
12. [엔터프라이즈 데이터 수집: 관리형 커넥터와 Partner Connect](#12-엔터프라이즈-데이터-수집-관리형-커넥터와-partner-connect)
13. [그 밖의 데이터 통합·공유 기능](#13-그-밖의-데이터-통합공유-기능)
14. [MERGE INTO로 기존 테이블에 수집하기](#14-merge-into로-기존-테이블에-수집하기)
15. [좋은 브론즈 테이블의 조건](#15-좋은-브론즈-테이블의-조건)
16. [명령어 치트시트](#16-명령어-치트시트)
17. [자가 점검 문제](#17-자가-점검-문제)
18. [참고 링크](#18-참고-링크)

---

## 1. Lakeflow와 데이터 수집 개요

### 1-1. Lakeflow 구성

**Lakeflow**는 Databricks의 엔드투엔드 데이터 엔지니어링 솔루션으로, 분석·AI·운영 애플리케이션에 고품질 데이터를 공급한다. 세 가지 구성 요소로 이루어진다.

| 구성 요소 | 역할 |
|---|---|
| **Lakeflow Connect** | 수집(Ingestion). 엔터프라이즈 애플리케이션, 데이터베이스, 클라우드 스토리지, 메시지 버스, 로컬 파일용 커넥터 |
| **Spark Declarative Pipelines (SDP)** | 변환(ETL). SQL/Python으로 배치·스트리밍 파이프라인을 선언적으로 구축 |
| **Lakeflow Jobs** | 오케스트레이션. 여러 작업을 워크플로로 조율·자동화 |

이 세 가지는 다음 기반 위에서 동작한다.
- 처리 엔진: **Apache Spark + Structured Streaming**
- 통합 거버넌스: **Unity Catalog**
- 스토리지 포맷: **Delta Lake / Parquet / Iceberg**

### 1-2. 전통적인 데이터 수집의 문제

- 소스마다(온프레미스 파일, 레거시 시스템, DB, 클라우드 스토리지, SaaS, 실시간 이벤트 스트림) **서로 다른 도구와 커넥터**가 필요했다.
- 서드파티 도구와 사내 개발 솔루션이 뒤섞여 복잡성이 커졌다.
- 결과: **운영 부담 증가, 일관되지 않은 거버넌스, 단편화된 가시성**

### 1-3. Lakeflow Connect: 통합 수집

Lakeflow Connect는 파일, 클라우드 스토리지, 데이터베이스, 엔터프라이즈 애플리케이션, 스트리밍 소스의 데이터를 **하나의 관리형 플랫폼 안에서** 레이크하우스로 가져온다. 오케스트레이션, 관측 가능성(observability), 거버넌스가 모두 플랫폼 내에서 통합 제공된다.

| 주요 이점 | 효과 |
|---|---|
| 관리형·효율적 솔루션 | 비용 절감, 빠른 가치 실현 |
| 모든 실무자를 위한 셀프 서비스 인터페이스 | 데이터 민주화, 혁신 속도 향상 |
| 통합 관측성과 거버넌스 | 안전하고 안정적인 파이프라인과 테이블 |

### 1-4. Lakeflow Connect 커넥터 유형

| 유형 | 대상 소스 | 특징 |
|---|---|---|
| **파일 업로드 (Manual File Upload)** | 로컬 파일 | 로컬 파일을 볼륨에 업로드하거나 바로 테이블로 생성. 빠른 임시 적재용 |
| **표준 커넥터 (Standard Connectors)** | 클라우드 오브젝트 스토리지, Kafka, 기타 소스 | 배치 / 증분 배치 / 스트리밍 수집 지원. CTAS, COPY INTO, Auto Loader가 여기에 해당 |
| **관리형 커넥터 (Managed Connectors)** | SaaS 애플리케이션(Salesforce, Workday 등), 데이터베이스(SQL Server, PostgreSQL 등) | 완전 관리형, 효율적인 증분 읽기·쓰기로 빠르고 확장 가능하며 비용 효율적 |
| (보완) **Partner Connect** | 관리형 커넥터가 없는 소스 | Fivetran, Informatica, Qlik 등 검증된 파트너 솔루션과 연결 |

---

## 2. 수집 방식: 배치, 증분 배치, 스트리밍

| 방식 | 동작 | 대표 기법 | 적합한 상황 |
|---|---|---|---|
| **배치 (Batch)** | 일정에 따라 데이터를 배치 단위로 적재. **실행할 때마다 모든 레코드를 처리** | SQL: `CREATE TABLE AS SELECT` / Python: `spark.read.load()` | 실시간 처리가 필요 없는 대용량 이력 데이터. 구현·관리가 단순 |
| **증분 배치 (Incremental Batch)** | **새 데이터만 수집**, 이미 적재된 레코드는 자동으로 건너뜀 | SQL: `COPY INTO` / Python: `spark.readStream` (Auto Loader + 시간 트리거) / SDP: `CREATE OR REFRESH STREAMING TABLE` | 정기적으로 데이터가 쌓이는 소스. 처리량이 적어 빠르고 자원 효율적 |
| **스트리밍 (Streaming)** | 데이터가 생성되는 즉시 **지속적으로** 적재 → 거의 실시간 조회 가능 | `spark.readStream` (Auto Loader + 연속 트리거) / SDP 연속(continuous) 모드 | Kafka, Kinesis, Pub/Sub, Pulsar 등 스트리밍 소스. 저지연 분석·즉각 대응 |

- **마이크로 배치(micro-batch):** 스트리밍의 한 형태로, 아주 짧고 빈번한 간격(초~분)으로 작은 배치를 처리한다. 지연 시간과 시스템 효율 사이의 균형을 맞춘다.
- 배치와 증분 배치의 핵심 차이: **"매번 전부 처리" vs "새로운 것만 처리"**

---

## 3. Delta Lake와 메달리온 아키텍처 복습

### 3-1. 수집의 목표

클라우드 오브젝트 스토리지 등 **외부 소스의 원시 파일(CSV, JSON, Parquet 등)을 Delta Lake 기반 UC 테이블로 변환**하는 것. 그래야 ACID 트랜잭션, 시간 여행, 스키마 강제 같은 기능을 쓸 수 있다.

### 3-2. UC 테이블의 내부 구조

- UC 테이블의 데이터는 내부적으로 **디렉터리 안의 파일**로 저장된다.
- 데이터 파일은 **Parquet** 형식이고, Delta Lake는 그 옆에 **JSON 기반 트랜잭션 로그(Delta 로그)**를 추가한다.
- Delta 로그가 **트랜잭션과 테이블 버전**을 추적한다.
- Delta Lake는 클라우드 스토리지의 파일을 읽고 쓰기 위한 **오픈 소스 프로토콜**이며 AWS, Azure, GCP 모두에서 동작한다.

### 3-3. UC 테이블 주요 기능

| 기능 | 설명 |
|---|---|
| **ACID 트랜잭션** | 모든 작업에 원자성·일관성·격리성·지속성 보장 → 여러 사용자가 동시에 충돌 없이 읽고 쓰기 |
| **DML** | INSERT, UPDATE, DELETE, MERGE 지원 |
| **시간 여행** | 이전 버전 조회·복원 → 감사, 복구 |
| **스키마 강제와 진화** | 정의된 스키마로 무결성을 지키면서, 필요 시 기존 워크플로를 깨지 않고 구조 변경 |
| 기타 | 배치·스트리밍 통합 처리, 최적화, 확장성, 오픈 소스 |

- 예전 데이터 레이크에서는 데이터를 수정하려면 **파일을 수동으로 다시 만들고 변경을 직접 추적**해야 했다. Delta Lake는 트랜잭션 로그로 이를 단순·효율화한다.

### 3-4. 메달리온 아키텍처 (멀티 홉)

| 레이어 | 내용 | 소비 |
|---|---|---|
| **Bronze** | 다양한 소스의 원시 데이터를 **있는 그대로** 수집. 모든 데이터의 기초 저장소 | 데이터 엔지니어 |
| **Silver** | 정제·변환·보강된 데이터 | 분석, ML |
| **Gold** | 큐레이션·집계된 비즈니스 수준 데이터 | BI, 보고서 |

- 배치든 스트리밍이든 Bronze로 들어온 데이터는 각 레이어를 거치며 **구조와 품질이 점진적으로 향상**된다.
- **Lakeflow Connect가 담당하는 영역은 Bronze 적재**다.

---

## 4. 클라우드 오브젝트 스토리지 수집 방법 비교

클라우드 오브젝트 스토리지에서의 수집은 **표준 커넥터**로 수행하며, 세 가지 주요 방법이 있다.

| 항목 | CTAS (+ `spark.read`) | COPY INTO | Auto Loader |
|---|---|---|---|
| 수집 유형 | 배치 | 증분 배치 | 증분 (배치 또는 스트리밍) |
| 규모 | 소규모 데이터셋 | **수천 개** 파일 | 시간당 **수백만 개 이상** 파일, **수십억 개** 파일 백필 |
| 인터페이스 | SQL (CTAS), Python (`spark.read`) | SQL | Python (`spark.readStream`), SQL 스트리밍 테이블 (SDP / Databricks SQL) |
| 멱등성 | 아니오 | 예 | 예 |
| 스키마 진화 | 읽을 때 수동 지정 또는 자동 추론 | 옵션(`mergeSchema`)으로 지원 | **자동 감지·진화** (새 컬럼 자동 처리) |
| 지연 시간 | 높음 | 보통 (예약 실행) | 구성에 따라 낮음~높음 |
| 사용 난이도 | 간단 | 간단, SQL 기반 | 구현 방식에 따라 중급~고급 |
| 요약 | 일회성·임시 수집. 매번 전체를 읽도록 예약할 수도 있음 | 단순하고 반복 가능한 증분 파일 수집. 예약 작업·파이프라인에 적합 | 높은 자동화·확장성의 준실시간 스트리밍/증분 수집 |

- **멱등성(Idempotency):** 같은 작업을 여러 번 실행해도 한 번 실행한 것과 결과가 같은 성질. 수집에서는 "이미 적재한 파일을 다시 적재하지 않는다"는 의미.
- Databricks는 Databricks SQL로 증분 수집할 때 **COPY INTO보다 스트리밍 테이블(Auto Loader)을 권장**한다. COPY INTO는 공식 문서에서 **레거시**로 분류된다.

---

## 5. 파일 직접 조회와 read_files()

### 5-1. 경로로 파일 직접 조회

테이블을 만들기 전에 파일 내용을 빠르게 확인할 때 사용한다.

```sql
LIST '/Volumes/catalog/schema/volume/dir';                -- 디렉터리의 파일 목록

SELECT * FROM parquet.`/Volumes/catalog/schema/volume/dir`; -- Parquet 직접 조회
SELECT * FROM json.`/Volumes/.../events.json`;             -- JSON 직접 조회
SELECT * FROM csv.`/Volumes/.../sales-csv`;                -- CSV 직접 조회 (옵션 지정 불가)
SELECT * FROM text.`/Volumes/.../file.csv`;                -- 각 줄을 문자열 한 행으로 (원본 그대로 확인)
```

- 경로는 백틱(`` ` ``)으로 감싼다.
- `text.`는 파일의 각 줄을 그대로 보여주므로 **구분자, 헤더 누락, 잘못된 값** 같은 원본 문제를 눈으로 확인할 때 유용하다.
- 디렉터리에 `part-`로 시작하는 파일들이 있다면 여러 개의 Parquet 파일로 나뉘어 저장된 데이터라는 뜻이다.
- 경로 직접 조회는 형식 옵션(구분자, 헤더 등)을 줄 수 없다 → 옵션이 필요하면 `read_files()`를 쓴다.

### 5-2. read_files() 테이블 값 함수

`read_files()`는 지정 위치의 파일을 읽어 **표 형식으로 반환하는 테이블 값 함수(TVF, Table-Valued Function)**다.

```sql
SELECT *
FROM read_files(
  '/Volumes/catalog/schema/volume/sales-csv',   -- 첫 번째 인자: 경로 (파일 또는 디렉터리)
  format    => 'csv',                           -- 파일 형식
  sep       => '|',                             -- 구분자
  header    => true,                            -- 첫 행을 컬럼명으로
  schema    => 'order_id INT, email STRING',    -- 명시적 스키마 (선택)
  rescuedDataColumn => '_rescued_data'          -- 복구 컬럼 이름 (선택)
)
LIMIT 10;
```

- 지원 형식: **JSON, CSV, XML, TEXT, BINARYFILE, PARQUET, AVRO, ORC**
- **형식 자동 감지**, 여러 파일에 걸친 **통합 스키마 추론**
- 형식별로 쓸 수 있는 옵션이 다르다 (CSV 옵션, JSON 옵션, Parquet 옵션 등).
- 명시적 스키마(`schema`)와 일부 컬럼 타입만 지정하는 `schemaHints`도 지원한다.
- 추론된 스키마와 맞지 않는 데이터를 담는 **`_rescued_data` 컬럼이 기본으로 포함**된다 (10장).
- `STREAM read_files(...)` 형태로 쓰면 **Auto Loader 기반 증분 수집**이 된다 (8장).
- 옵션명은 대소문자를 구분하지 않는다 (`rescuedDataColumn` = `rescueddatacolumn`).

### 5-3. 스키마 추론과 명시적 스키마

- 스키마를 주지 않으면 `read_files()`는 발견된 **모든 파일을 읽어** 통합 스키마를 추론하려 한다. `LIMIT`를 써도 대표성 있는 스키마를 위해 필요 이상으로 많은 파일을 읽을 수 있다.
- **Parquet**은 파일 안에 컬럼명·타입 메타데이터가 내장되어 있어 스키마 추론이 정확하고 쉽다.
- **CSV/JSON** 같은 텍스트 기반 형식은 추론 결과가 기대와 다를 수 있다. 예: 숫자 컬럼에 `aaa` 같은 값이 하나만 섞여도 컬럼 전체가 STRING으로 추론된다.

**명시적 스키마의 이점**
1. 반정형 데이터(CSV, JSON)에서 **스키마 추론 불일치 위험 감소**
2. 비용이 큰 추론 단계를 건너뛰어 **파싱·로딩 속도 향상**
3. 대용량 데이터셋에서 **compute 오버헤드 감소**
4. 일관되고 신뢰할 수 있는 파싱 보장

---

## 6. CTAS를 이용한 배치 수집

### 6-1. SQL: CTAS + read_files()

```sql
CREATE TABLE historical_users_bronze AS
SELECT *
FROM read_files(
  '/Volumes/dbacademy_ecommerce/v01/raw/users-historical',
  format => 'parquet'
);
```

- 쿼리 결과로 **테이블 생성과 데이터 적재를 한 번에** 수행한다.
- 기본 테이블 형식은 **Delta**.
- 실행할 때마다 **전체 데이터를 다시 읽는다** (배치).

### 6-2. 테이블 정보 확인

```sql
DESCRIBE TABLE EXTENDED historical_users_bronze;
```

| 항목 | 의미 |
|---|---|
| Type | `MANAGED`(관리형) / `EXTERNAL`(외부) / `STREAMING_TABLE` 등 |
| Location | 데이터가 저장된 클라우드 스토리지 위치 |
| Provider | 테이블 형식 (예: delta) |
| View Text | (스트리밍 테이블·뷰) 테이블을 정의한 쿼리 |

### 6-3. 관리형 테이블 vs 외부 테이블

| | 관리형 (Managed) | 외부 (External) |
|---|---|---|
| 관리 범위 | Databricks가 **데이터와 메타데이터 모두** 관리 | Databricks는 **메타데이터만** 관리 |
| 저장 위치 | Databricks 관리형 스토리지 | 사용자가 지정한 외부 위치 |
| 테이블 삭제 시 | **데이터도 삭제** | **데이터는 남음** |
| 형식 | Delta (UC 관리형) | Delta 외 다양한 형식 지원 |
| 권장 상황 | 새 테이블 생성 시 기본 권장 | 플랫폼 간 데이터 공유, 기존 외부 데이터 활용 |

### 6-4. Python: spark.read + saveAsTable

```python
# 1. 볼륨의 Parquet 파일을 DataFrame으로 읽기
df = (spark.read
        .format("parquet")
        .load("/Volumes/dbacademy_ecommerce/v01/raw/users-historical"))

# 2. UC 테이블로 저장 (이미 있으면 덮어쓰기)
(df.write
   .mode("overwrite")
   .saveAsTable(f"{my_catalog}.data_ingestion.historical_users_bronze_python"))

# 3. 테이블 읽기
spark.table(f"{my_catalog}.data_ingestion.historical_users_bronze_python").display()
```

| `.mode()` 값 | 테이블이 이미 있을 때 |
|---|---|
| `"append"` | 기존 데이터에 추가 |
| `"overwrite"` | 데이터 교체 |
| `"error"` / `"errorifexists"` (기본) | 오류 |
| `"ignore"` | 아무것도 하지 않음 |

CSV를 Python으로 읽을 때는 옵션을 지정한다.

```python
df = (spark.read
        .option("header", True)
        .option("sep", "|")
        .option("rescuedDataColumn", "_rescued_data")   # 복구 컬럼 추가
        .csv("/Volumes/dbacademy_ecommerce/v01/raw/sales-csv"))
```

---

## 7. COPY INTO를 이용한 증분 배치 수집

### 7-1. 기본 구조

```sql
CREATE TABLE new_table;               -- 1. 대상 테이블을 먼저 만든다 (스키마 생략 가능)

COPY INTO new_table                   -- 2. 파일을 기존 테이블로 복사
FROM '/Volumes/catalog/schema/volume/dir'
FILEFORMAT = PARQUET                  -- CSV, JSON, PARQUET, AVRO, ORC, TEXT, BINARYFILE, XML 등
FORMAT_OPTIONS (<options>)            -- 소스 파일을 어떻게 파싱할지
COPY_OPTIONS (<options>);             -- COPY INTO 작업 자체의 동작
```

- 파일 위치의 데이터를 **기존 UC 테이블로** 대량 적재한다.
- **재시도 가능하고 멱등적**이다: 이미 적재한 파일은 건너뛰고 **새 파일만** 가져온다.
- 새 파일이 계속 추가되는 위치를 예약 작업으로 반복 수집할 때 적합하다.

| 절 | 역할 |
|---|---|
| `FROM` | 새 파일이 계속 추가되는 클라우드 스토리지 경로 |
| `FILEFORMAT` | 소스 파일 형식 |
| `FORMAT_OPTIONS()` | 파싱 방법 (형식별로 다름). 예: `'header' = 'true'`, `'inferSchema' = 'true'`, `'sep' = '|'` |
| `COPY_OPTIONS()` | 작업 동작. `'mergeSchema' = 'true'`(스키마 진화), `'force' = 'true'`(멱등성 무시하고 전부 재적재) |

### 7-2. 스키마 불일치 오류와 mergeSchema

대상 테이블의 스키마가 소스 파일과 다르면 오류가 난다.

```sql
-- 소스 Parquet: user_id, user_first_touch_timestamp, email (3개 컬럼)
CREATE TABLE historical_users_bronze_ci (
  user_id STRING,
  user_first_touch_timestamp BIGINT        -- 2개 컬럼만 정의
);

COPY INTO historical_users_bronze_ci
FROM '/Volumes/dbacademy_ecommerce/v01/raw/users-historical'
FILEFORMAT = parquet;
-- 오류: [COPY_INTO_SCHEMA_MISMATCH_WITH_TARGET_TABLE]
```

해결: `COPY_OPTIONS ('mergeSchema' = 'true')`로 들어오는 데이터에 맞춰 스키마를 진화시킨다.

```sql
COPY INTO historical_users_bronze_ci
FROM '/Volumes/dbacademy_ecommerce/v01/raw/users-historical'
FILEFORMAT = parquet
COPY_OPTIONS ('mergeSchema' = 'true');     -- email 컬럼이 자동 추가됨
```

### 7-3. 스키마 없는 빈 테이블 + mergeSchema

스키마를 미리 정의하지 않고 빈 테이블을 만든 뒤, `mergeSchema`로 첫 적재 시 스키마를 정하는 방법도 있다.

```sql
CREATE TABLE historical_users_bronze_ci_no_schema;   -- 컬럼 정의 없는 빈 Delta 테이블

COPY INTO historical_users_bronze_ci_no_schema
FROM '/Volumes/dbacademy_ecommerce/v01/raw/users-historical'
FILEFORMAT = parquet
COPY_OPTIONS ('mergeSchema' = 'true');
```

### 7-4. 멱등성 확인

같은 `COPY INTO`를 다시 실행하면 결과 지표가 모두 0이 된다.

| 결과 컬럼 | 의미 |
|---|---|
| `num_affected_rows` | 영향받은 행 수 |
| `num_inserted_rows` | 삽입된 행 수 |
| `num_skipped_corrupt_files` | 손상되어 건너뛴 파일 수 |

- 새 파일이 추가되면 **그 파일만** 수집한다.
- 데이터 변경이 없으면 테이블에 새 버전도 생기지 않는다.
- 주의: 파일 추적은 **경로 기준**이다. 같은 이름으로 내용만 바꿔 덮어쓴 파일은 다시 처리하지 않는다.

---

## 8. 스트리밍 테이블과 Auto Loader

### 8-1. Auto Loader란

클라우드 스토리지에 도착하는 데이터 파일을 **증분적으로 처리**하는 기능. Spark **Structured Streaming** 기반이다.

- 새 파일을 **자동으로 감지**해 UC 테이블에 적재
- 별도의 복잡한 설정 없이 증분·스트리밍 처리를 단순화
- Python과 SQL(선언적 파이프라인 / 스트리밍 테이블) 모두에서 사용 가능
- **수십억 개 파일**까지 확장
- **스키마 자동 감지와 진화** (새 컬럼 자동 처리)
- 체크포인트로 처리 상태를 저장해 안정적인 수집 보장

Databricks는 클라우드 오브젝트 스토리지 수집의 대부분에 **Auto Loader + 선언적 파이프라인** 조합을 권장한다. 둘은 계속 커지는 데이터셋을 도착하는 대로 증분적·멱등적으로 적재하도록 설계되었다.

### 8-2. 스트리밍 테이블 (Streaming Table)

- **Unity Catalog에 등록된 테이블**로, 스트리밍·증분 처리를 추가로 지원한다.
- Kafka와 클라우드 오브젝트 스토리지 모두에서 증분 적재에 쓸 수 있다.
- SQL에서 Auto Loader를 쓰려면 `FROM` 절에 **`STREAM` 키워드와 `read_files`**를 함께 쓴다.
- Databricks SQL로 데이터를 수집할 때는 **COPY INTO 대신 스트리밍 테이블 사용이 권장**된다.

```sql
CREATE OR REFRESH STREAMING TABLE sql_csv_autoloader
SCHEDULE EVERY 1 WEEK            -- 갱신 일정 (선택). 예: EVERY 1 HOUR
AS
SELECT *
FROM STREAM read_files(
  '/Volumes/catalog/data_ingestion/csv_files_autoloader_source',
  format => 'CSV',
  sep    => '|',
  header => true
);
```

**동작 방식**
- 생성하는 즉시 초기 갱신(데이터 채우기)이 시작된다.
- 각 스트리밍 테이블마다 **전용 서버리스 선언적 파이프라인이 자동 생성·관리**된다. 생성과 갱신에 이 파이프라인을 사용하므로 **SQL Warehouse compute를 소비하지 않는다.**
- 그래서 워크스페이스가 **서버리스 파이프라인을 지원**해야 한다.
- Databricks SQL로 스트리밍 테이블을 만들려면 **SQL Warehouse**에서 실행해야 한다.
- 직접 선언적 파이프라인을 만들어 스트리밍 테이블을 정의할 수도 있다 (증분 처리, 최적화, 모니터링 등 추가 기능).

**갱신 (Refresh)**

```sql
REFRESH STREAMING TABLE sql_csv_autoloader;        -- 새 파일만 증분 수집
REFRESH STREAMING TABLE sql_csv_autoloader FULL;   -- 상태를 초기화하고 전체 재처리
```

- 일정(`SCHEDULE`)을 걸어 두면 자동 갱신되고, `REFRESH`로 수동 갱신할 수도 있다.
- 같은 `CREATE OR REFRESH` 문을 다시 실행해도 **새 파일만** 증분 수집된다.

**확인 방법**

```sql
DESCRIBE TABLE EXTENDED sql_csv_autoloader;
DESCRIBE HISTORY sql_csv_autoloader;
```

| `DESCRIBE TABLE EXTENDED` 항목 | 의미 |
|---|---|
| Type | `STREAMING_TABLE` |
| View Text | 테이블을 정의한 쿼리 |
| Last Refreshed | 마지막 갱신 시각 |
| Last Refresh Type | `INCREMENTAL` (증분) / `FULL` |
| Latest Refresh Status | 성공/실패 |
| Latest Refresh | 자동 생성된 파이프라인 업데이트 링크 |
| Refresh Schedule | 예: `EVERY 1 WEEK` |

| `DESCRIBE HISTORY`의 operation | 의미 |
|---|---|
| `CREATE TABLE` | 테이블 생성 |
| `DLT SETUP` / `SDP SETUP` | 선언적 파이프라인 설정 |
| `STREAMING UPDATE` | 증분 데이터 적재 (갱신마다 하나씩 생성) |

- `operationMetrics`의 `numOutputRows`로 해당 갱신에서 추가된 행 수를 확인할 수 있다. 예: 첫 갱신 3,149행 → 새 파일 추가 후 갱신에서 2,932행만 추가 (총 6,081행).

### 8-3. Python Auto Loader

```python
(spark
  .readStream
    .format("cloudFiles")                                         # Auto Loader 활성화
    .option("cloudFiles.format", "json")                          # 소스 파일 형식
    .option("cloudFiles.schemaLocation", "<checkpoint_path>")     # 스키마 추론·진화 이력 저장 위치
    .load("/Volumes/catalog/schema/files")                        # 소스 경로
  .writeStream
    .option("checkpointLocation", "<checkpoint_path>")            # 처리 상태·진행 상황 저장
    .trigger(processingTime="5 seconds")                          # 5초마다 마이크로 배치
    .toTable("catalog.schema.table")                              # 대상 UC 테이블
)
```

| 요소 | 설명 |
|---|---|
| `.readStream` + `format("cloudFiles")` | Auto Loader 사용 선언 |
| `cloudFiles.format` | 소스 파일 형식 (json, csv, parquet 등) |
| `cloudFiles.schemaLocation` | 추론한 스키마와 스키마 변경 이력을 저장. 스키마 추론·진화에 필수 |
| `.load(path)` | 감시할 소스 경로 |
| `.writeStream` + `checkpointLocation` | 어디까지 처리했는지 저장 → 재시작 시 이어서 처리, 정확히 한 번(exactly-once) 처리 |
| `.trigger(...)` | `processingTime="5 seconds"`: 주기적 실행(스트리밍) / `availableNow=True`: 쌓인 파일만 처리 후 종료(증분 배치) |
| `.toTable(name)` | 결과를 쓸 UC 테이블 |

**자주 쓰는 Auto Loader 옵션**

| 옵션 | 설명 |
|---|---|
| `cloudFiles.inferColumnTypes` | CSV/JSON은 기본적으로 모든 컬럼을 STRING으로 추론한다. `true`면 실제 타입을 추론 |
| `cloudFiles.schemaHints` | 일부 컬럼의 타입을 직접 지정. 예: `"id BIGINT, price DOUBLE"` |
| `cloudFiles.schemaEvolutionMode` | 새 컬럼 발견 시 동작. `addNewColumns`(기본: 스트림이 실패하며 스키마를 갱신, 재시작하면 새 컬럼 반영) / `rescue`(새 컬럼을 `_rescued_data`에 보관) / `failOnNewColumns`(실패) / `none`(무시) |
| `cloudFiles.useNotifications` | `true`면 디렉터리 나열 대신 클라우드 이벤트 알림으로 새 파일 감지 (대규모용) |

---

## 9. 수집 시 메타데이터 컬럼 추가

### 9-1. 왜 필요한가

Bronze 테이블에 행마다 **어느 파일에서 왔는지, 그 파일이 언제 수정되었는지, 언제 수집되었는지**를 함께 저장하면 데이터 출처에 대한 컨텍스트가 보존된다. **감사(audit), 리니지, 디버깅**에 유용하다.

| 원본 컬럼 | | 추가 메타데이터 컬럼 |
|---|---|---|
| users, unix_ts | + | last_mod_time(파일 마지막 수정 시각), source(소스 파일 이름), ingestion_time(수집 시각) |

### 9-2. `_metadata` 숨겨진 컬럼

- **모든 입력 파일 형식에서 사용 가능한 숨겨진 컬럼**이다.
- `SELECT *`에는 포함되지 않는다. 결과에 넣으려면 **읽기 쿼리에서 명시적으로 선택**해야 한다.
- `read_files()`, `spark.read`, Auto Loader, COPY INTO 등 파일 기반 소스에서 쓸 수 있다.

| 필드 | 내용 |
|---|---|
| `_metadata.file_path` | 입력 파일의 전체 경로 |
| `_metadata.file_name` | 입력 파일 이름 |
| `_metadata.file_size` | 파일 크기(바이트) |
| `_metadata.file_modification_time` | 파일의 마지막 수정 타임스탬프 |
| `_metadata.file_block_start` / `file_block_length` | 읽은 블록의 시작 위치와 길이 |

- 주의: 예전의 `input_file_name()` 함수는 Unity Catalog 환경에서 지원되지 않는다. 대신 `_metadata.file_path`를 사용한다.

### 9-3. `current_timestamp()`로 수집 시각 기록

- **쿼리 실행이 시작된 시점**의 타임스탬프(TIMESTAMP 타입)를 반환한다.
- 한 쿼리 안의 모든 행은 같은 값을 가진다.

### 9-4. Unix 타임스탬프 변환

Unix 타임스탬프는 1970-01-01(Unix epoch) 이후 경과 시간이다. 단위(초/밀리초/마이크로초)를 먼저 확인해야 한다.

```sql
-- 마이크로초 단위 → 초로 나눈 뒤 변환 → DATE로 캐스팅
cast(from_unixtime(user_first_touch_timestamp / 1000000) AS DATE) AS first_touch_date
```

| 함수 | 입력 | 반환 |
|---|---|---|
| `from_unixtime(sec)` | 초 단위 | `yyyy-MM-dd HH:mm:ss` 형식 **문자열** |
| `timestamp_seconds(sec)` | 초 단위 | TIMESTAMP |
| `timestamp_millis(ms)` | 밀리초 단위 | TIMESTAMP |
| `timestamp_micros(us)` | 마이크로초 단위 | TIMESTAMP |

### 9-5. 최종 Bronze 테이블 (SQL)

```sql
CREATE TABLE historical_users_bronze AS
SELECT
  *,
  cast(from_unixtime(user_first_touch_timestamp / 1000000) AS DATE) AS first_touch_date,
  _metadata.file_modification_time AS file_modification_time,   -- 소스 파일 마지막 수정 시각
  _metadata.file_name              AS source_file,              -- 소스 파일 이름
  current_timestamp()              AS ingestion_time            -- 수집 시각
FROM read_files(
  '/Volumes/dbacademy_ecommerce/v01/raw/users-historical',
  format => 'parquet'
);

-- 활용 예: 파일별 유입 행 수
SELECT source_file, count(*) AS total
FROM historical_users_bronze
GROUP BY source_file
ORDER BY source_file;
```

### 9-6. 같은 작업 (Python)

```python
from pyspark.sql.functions import col, from_unixtime, current_timestamp
from pyspark.sql.types import DateType

df = spark.read.format("parquet").load("/Volumes/dbacademy_ecommerce/v01/raw/users-historical")

df_with_metadata = (
    df.withColumn("first_touch_date",
                  from_unixtime(col("user_first_touch_timestamp") / 1_000_000).cast(DateType()))
      .withColumn("file_modification_time", col("_metadata.file_modification_time"))
      .withColumn("source_file", col("_metadata.file_name"))
      .withColumn("ingestion_time", current_timestamp())
)

(df_with_metadata.write
   .format("delta")
   .mode("overwrite")
   .saveAsTable(f"{my_catalog}.data_ingestion.historical_users_bronze_python_metadata"))
```

---

## 10. Rescued Data 컬럼 (_rescued_data)

### 10-1. 개념

- `read_files()`, `spark.read`, Auto Loader 등은 원시 데이터가 스키마와 맞지 않을 때 **Rescued Data 컬럼**을 제공한다.
- 스키마와 맞지 않는 값은 **버려지지 않고 JSON 형식 문자열로 `_rescued_data`에 저장**된다.
- 불일치가 없는 행은 `_rescued_data`가 **`null`**이다.
- 저장되는 JSON에는 원래 컬럼명을 키로 한 값과 **`_file_path`**(그 값이 나온 파일 경로)가 들어 있다.
- 효과: **모르는 사이에 데이터가 사라지는 것(silent data loss)을 방지**하고, 수집 후 불일치를 검사·처리할 수 있다.

### 10-2. 예시

원본 파일:

| users | cost |
|---|---|
| Peter | $100 |
| zebi | 300 |

기대 스키마: `users STRING`, `cost BIGINT`

수집 결과:

| users | cost | _rescued_data |
|---|---|---|
| Peter | null | `{"cost": "$100", "_file_path": "<file_path>"}` |
| zebi | 300 | null |

- `$100`은 BIGINT로 변환할 수 없으므로 `cost`는 null, 원래 값은 `_rescued_data`에 보존된다.

### 10-3. 데이터가 복구 컬럼으로 가는 경우

1. **스키마에 없는 컬럼** (추가 필드, 이름 없는 컬럼)
2. **데이터 타입 불일치**
3. **대소문자 불일치** (컬럼명의 대소문자가 스키마와 다름)

### 10-4. CSV 옵션을 제대로 주지 않으면

파이프(`|`) 구분 CSV를 `format => 'csv'`만으로 읽으면, 구분자를 쉼표로 가정해 한 줄 전체가 한 컬럼으로 읽히는 등 **올바르게 파싱되지 않는다.** 구분자와 헤더를 명시해야 한다.

```sql
SELECT *
FROM read_files(
  '/Volumes/dbacademy_ecommerce/v01/raw/sales-csv',
  format => 'csv',
  sep    => '|',      -- 구분자
  header => true      -- 첫 행이 헤더
);
```

### 10-5. 명시적 스키마 + 복구 컬럼

스키마를 직접 지정할 때 복구 컬럼을 만들려면 **`rescuedDataColumn` 옵션을 함께 지정**해야 한다.

```sql
SELECT *
FROM read_files(
  '/Volumes/.../malformed_example_1_data.csv',
  format => 'csv',
  sep    => '|',
  header => true,
  schema => '''
      order_id INT,
      email STRING,
      transactions_timestamp BIGINT''',
  rescuedDataColumn => '_rescued_data'
);
-- transactions_timestamp에 'aaa'가 있던 행: 해당 컬럼은 null, 'aaa'는 _rescued_data로
```

- 스키마 없이 읽었다면 `aaa` 하나 때문에 `transactions_timestamp` 컬럼 전체가 STRING으로 추론되었을 것이다. 스키마를 지정하면 **컬럼 타입은 지키고, 잘못된 값만 따로 보존**할 수 있다.

### 10-6. 헤더 일부가 누락된 CSV

헤더의 첫 번째 컬럼명이 비어 있는 CSV를 `header => true`로 읽으면:
- 이름 없는 컬럼은 일반 컬럼으로 읽히지 않고 `_rescued_data`로 들어간다.
- JSON 안에서 이름 없는 컬럼은 **`_c0`** 키로 표현된다 (`_c1`, `_c2`… 순서대로).

```sql
-- 복구 컬럼에서 값을 꺼내 정식 컬럼으로 만들기
SELECT
  cast(_rescued_data:_c0 AS BIGINT) AS order_id,
  *
FROM read_files(
  '/Volumes/.../malformed_example_2_data.csv',
  format => 'csv', sep => '|', header => true
);
```

- `_rescued_data`는 JSON 문자열이므로 **콜론(`:`) 문법**으로 필드 값을 꺼낼 수 있다 (11장).

### 10-7. 복구 데이터 정제 예시

`price DOUBLE` 스키마로 읽었더니 `$100.00` 같은 값이 `_rescued_data`로 갔을 때, 수집하면서 바로 고친 컬럼을 추가한다.

```sql
CREATE OR REPLACE TABLE items_bronze AS
SELECT
  item_id,
  name,
  price,
  coalesce(price, replace(_rescued_data:price, '$', '')) AS price_fixed,  -- price가 null이면 복구 값에서 $ 제거
  _rescued_data,
  _metadata.file_modification_time AS file_modification_time,
  _metadata.file_name              AS source_file,
  current_timestamp()              AS ingestion_time
FROM read_files(
  '/Volumes/.../malformed_items.csv',
  format => 'csv',
  sep    => ',',
  header => true,
  schema => 'item_id STRING, name STRING, price DOUBLE',
  rescuedDataColumn => '_rescued_data'
);
```

- `coalesce(a, b, ...)`: 처음으로 null이 아닌 값을 반환
- `replace(str, search, replace)`: 문자열 치환
- 원본 `price`와 `_rescued_data`도 그대로 남겨두면 나중에 정제 로직을 검증하거나 다시 처리할 수 있다.

---

## 11. 반정형 데이터(JSON) 수집과 평탄화

### 11-1. JSON 기본 구조

```json
{
  "name": "John Doe",
  "age": 35,
  "address": { "city": "Anytown", "state": "CA" },
  "children": [
    { "name": "Owen", "age": 10 },
    { "name": "Eva", "age": 8 }
  ]
}
```

| 요소 | 설명 |
|---|---|
| **객체 (Object)** | 중괄호 `{ }`로 묶인다. 안에 키-값 쌍을 가진다 |
| **키 (Key)** | 항상 **따옴표로 묶인 문자열** |
| **값 (Value)** | 문자열, 숫자, 불리언, 배열, 객체, null 중 하나 |
| **중첩 객체** | 값이 다시 객체인 경우 (예: `address`) |
| **배열 (Array)** | 대괄호 `[ ]`. 예: `children`은 객체의 배열 |

- 객체는 단순(flat)할 수도, 중첩(nested)될 수도 있다. 구조를 이해해야 파싱·변환 방법을 정할 수 있다.

### 11-2. JSON 문자열 컬럼을 다루는 세 가지 방법

수집 후 테이블 컬럼에 **JSON 형식의 문자열**이 들어 있는 경우가 흔하다 (JSON이 완전히 파싱되지 않았거나, 로그 메시지·메시지 큐 값 안에 JSON이 들어 있는 경우).

| 방법 | 저장 방식 | 장점 | 고려 사항 |
|---|---|---|---|
| **STRING** | JSON을 원시 텍스트로 저장 | 구현이 간단, 어떤 JSON 구조든 제약 없이 저장 | 쿼리·처리 성능이 낮음, 스키마가 없어 무결성 문제 가능, 파싱 코드가 복잡해질 수 있음 |
| **STRUCT** | 정의된 스키마로 파싱한 구조체 | 스키마 강제로 타입·구조 일관성 보장, STRING보다 쿼리 효율이 높음 | JSON 구조가 바뀌면 문제 발생, 유연성 감소 |
| **VARIANT** | 반정형 데이터 전용 바이너리 타입 | 스키마 없이도 유연, STRING·STRUCT보다 성능 좋음 | Delta 테이블에서 지원 활성화 필요, 런타임 버전 요건 (11-9) |

### 11-3. JSON 타입과 Databricks SQL 타입 매핑

| JSON 타입 | Databricks SQL 타입 |
|---|---|
| String | `STRING` |
| Number | `INT` / `BIGINT` / `FLOAT` / `DOUBLE` |
| Boolean | `BOOLEAN` |
| Object | `STRUCT<...>` |
| Array | `ARRAY<...>` |

위 JSON을 STRUCT 스키마로 표현하면:

```
STRUCT<
  name: STRING,
  age: INT,
  address: STRUCT<city: STRING, state: STRING>,
  children: ARRAY<STRUCT<name: STRING, age: INT>>
>
```

- 전체를 감싸는 `STRUCT<>`가 모든 필드의 컨테이너 역할을 한다.
- 중첩 객체 → 중첩 `STRUCT`, 객체 배열 → `ARRAY<STRUCT<...>>`

### 11-4. JSON 파일 수집

```sql
-- 원본 확인
SELECT * FROM text.`/Volumes/dbacademy_ecommerce/v01/raw/events-kafka` LIMIT 5;

-- read_files로 표 형식 수집
CREATE TABLE kafka_events_bronze_raw AS
SELECT *
FROM read_files(
  '/Volumes/dbacademy_ecommerce/v01/raw/events-kafka',
  format => 'json'
);
```

- JSON 파일을 읽는 것은 CSV만큼 간단하다. 최상위 키가 컬럼이 된다.
- 예시 데이터(Kafka 이벤트)의 컬럼: `key`, `offset`, `partition`, `timestamp`, `topic`, `value`

### 11-5. Base64 디코딩

메시지 큐·스트리밍 플랫폼에서 온 데이터는 형식 보존과 손상 방지를 위해 **Base64로 인코딩**된 경우가 많다. Base64는 바이너리 데이터를 읽을 수 있는 ASCII 문자열로 바꾸는 인코딩이다.

```sql
SELECT
  cast(unbase64(key)   AS STRING) AS decoded_key,
  offset, partition, timestamp, topic,
  cast(unbase64(value) AS STRING) AS decoded_value    -- JSON 형식 문자열이 됨
FROM kafka_events_bronze_raw;
```

- `unbase64(str)`: Base64 문자열을 디코딩해 **BINARY**로 반환
- `cast(... AS STRING)`: BINARY를 읽을 수 있는 문자열로 변환
- 참고: Kafka를 Spark에서 직접 읽을 때 `key`, `value`는 BINARY로 오므로 `cast(value AS STRING)`으로 변환한다.

### 11-6. 방법 1 — STRING에서 콜론(`:`) 문법으로 추출

```sql
SELECT
  decoded_value:device,              -- 최상위 필드
  decoded_value:traffic_source,
  decoded_value:geo,                 -- 결과는 또 다른 JSON 문자열
  decoded_value:geo.city,            -- 중첩 필드: 점(.)
  decoded_value:items,               -- 결과는 JSON 배열 문자열
  decoded_value:items[0].item_id     -- 배열 원소: 대괄호 인덱스
FROM kafka_events_bronze_decoded;
```

- 문법: **`<컬럼명>:<추출 경로>`**. 반환값은 **STRING**이다.
- 중첩 필드는 `.` 또는 `['key']`, 배열 원소는 `[인덱스]`로 접근한다.
- 필드명은 대소문자를 구분하지 않는다. 대소문자를 구분하려면 대괄호 표기(`col:['Name']`)를 쓴다.
- 타입이 필요하면 캐스팅한다: `cast(decoded_value:event_timestamp AS TIMESTAMP)` 또는 `decoded_value:event_timestamp::TIMESTAMP`
- 이 방법으로 필요한 필드만 뽑아 **평탄화된 Bronze 테이블**을 만들 수 있다.

### 11-7. 방법 2 — STRUCT로 변환

두 단계로 진행한다.

**1단계: `schema_of_json()`으로 스키마 도출**

```sql
SELECT schema_of_json('{"device":"Linux","geo":{"city":"Houston","state":"TX"},"items":[{"item_id":"M_STAN_K","quantity":1}]}') AS schema;
-- 결과: STRUCT<device: STRING, geo: STRUCT<city: STRING, state: STRING>, items: ARRAY<STRUCT<item_id: STRING, quantity: BIGINT>>>
```

- 샘플 JSON 문자열을 넣으면 추론된 구조를 반환한다. 스키마를 손으로 쓸 필요가 없다.
- 컬럼을 넣을 수도 있다: `SELECT schema_of_json(decoded_value) FROM t LIMIT 1;`

**2단계: `from_json()`으로 파싱**

```sql
CREATE OR REPLACE TABLE kafka_events_bronze_struct AS
SELECT
  * EXCEPT (decoded_value),                 -- decoded_value만 빼고 모든 컬럼
  from_json(
    decoded_value,                          -- JSON 문자열 컬럼
    'STRUCT<device: STRING, ecommerce: STRUCT<purchase_revenue_in_usd: DOUBLE, total_item_quantity: BIGINT, unique_items: BIGINT>, event_name: STRING, event_previous_timestamp: BIGINT, event_timestamp: BIGINT, geo: STRUCT<city: STRING, state: STRING>, items: ARRAY<STRUCT<coupon: STRING, item_id: STRING, item_name: STRING, item_revenue_in_usd: DOUBLE, price_in_usd: DOUBLE, quantity: BIGINT>>, traffic_source: STRING, user_first_touch_timestamp: BIGINT, user_id: STRING>'
  ) AS value
FROM kafka_events_bronze_decoded;
```

- `from_json(jsonStr, schema)`: JSON 문자열을 지정한 스키마의 **STRUCT 컬럼**으로 반환
- 반대 방향은 `to_json(struct)` (STRUCT → JSON 문자열)
- `SELECT * EXCEPT (col1, col2)`: 특정 컬럼만 제외하고 전부 선택

**STRUCT 필드 접근: 점(`.`) 문법**

```sql
SELECT
  value.device       AS device,      -- 필드
  value.geo.city     AS city,        -- 중첩 필드
  value.items        AS items,       -- ARRAY<STRUCT>
  array_size(value.items) AS number_elements_in_array,
  value.*                            -- STRUCT의 모든 필드를 개별 컬럼으로 펼치기
FROM kafka_events_bronze_struct;
```

| 비교 | JSON 문자열 | STRUCT |
|---|---|---|
| 접근 문법 | 콜론 `col:field` | 점 `col.field` |
| 결과 타입 | STRING | 스키마에 정의된 타입 |

### 11-8. 배열 펼치기: explode vs explode_outer

`explode()`는 배열의 **각 원소를 별도 행**으로 만든다 (맵이면 키·값 쌍마다 한 행).

```sql
SELECT
  decoded_key,
  array_size(value.items) AS number_elements_in_array,
  explode(value.items)    AS item_in_array
FROM kafka_events_bronze_struct;
```

| 함수 | 배열이 NULL이거나 비어 있을 때 |
|---|---|
| `explode()` | **행이 생성되지 않는다** (원래 행이 결과에서 사라짐) |
| `explode_outer()` | **NULL 값을 가진 한 행**을 반환 (원래 행 유지) |

- 원소 3개짜리 배열은 3행이 된다. 이후 `item_in_array.item_id`처럼 점 문법으로 각 원소의 필드를 꺼낸다.
- 배열이 없는 이벤트도 결과에 남겨야 한다면 `explode_outer()`를 쓴다.

### 11-9. 방법 3 — VARIANT

**특징**
- **개방성:** 오픈 표준 기반. Apache Spark, Delta Lake, Apache Iceberg, Apache Parquet에서 지원
- **유연성:** 고정 스키마 없이 객체·배열·스칼라 값을 저장
- **성능:** 효율적인 바이너리 인코딩 → JSON 문자열보다 빠른 탐색·쿼리
- **간편한 수집:** JSON, XML, CSV 수집 지원
- **스키마 진화:** 알 수 없거나 자주 바뀌는 스키마를 파이프라인 변경 없이 처리

**고려 사항**
- Databricks에서 정식 출시(GA) 상태
- VARIANT 컬럼이 있는 테이블을 읽고 쓰려면 **Databricks Runtime 15.4 LTS 이상**
- Delta 테이블에서 VARIANT 지원을 활성화하면 **테이블 writer 프로토콜이 업그레이드**되어, 오래된 외부 Delta Lake 클라이언트와 호환되지 않을 수 있다
- Apache Iceberg v3 테이블도 VARIANT를 지원한다

```sql
-- JSON 문자열 → VARIANT
CREATE OR REPLACE TABLE kafka_events_bronze_variant AS
SELECT
  decoded_key, offset, partition, timestamp, topic,
  parse_json(decoded_value) AS json_variant_value
FROM kafka_events_bronze_decoded;

-- 콜론으로 경로 접근, :: 로 캐스팅
SELECT
  json_variant_value:device::STRING AS device,
  json_variant_value:items          AS items       -- 결과는 VARIANT
FROM kafka_events_bronze_variant;
```

```sql
-- 테이블 정의와 삽입 예
CREATE TABLE sales (id INT, customer VARIANT);

INSERT INTO sales VALUES
  (1, PARSE_JSON('{"name": "person1", "revenue": 5}')),
  (2, PARSE_JSON('{"name": "person2", "age": 22}'));   -- 행마다 구조가 달라도 됨

SELECT id, customer:name::STRING AS customer_name FROM sales;
```

- `parse_json(str)`: JSON 문자열을 VARIANT로 변환 (형식이 잘못되면 오류). `try_parse_json()`은 오류 대신 NULL 반환
- VARIANT에서 콜론으로 꺼낸 값은 여전히 **VARIANT**다 → 필요한 타입으로 `::` 캐스팅
- 파일을 읽으면서 바로 VARIANT 한 컬럼으로 수집할 수도 있다: `read_files(path, format => 'json', singleVariantColumn => 'data')`

### 11-10. 같은 SELECT 안에서 별칭 재사용 (Lateral Column Alias)

Databricks SQL은 같은 SELECT 목록에서 **앞서 정의한 별칭을 뒤에서 바로 참조**할 수 있다.

```sql
SELECT
  key,
  timestamp,
  parse_json(decoded_value) AS v,          -- 별칭 정의
  v:user_id::STRING          AS user_id,   -- 같은 SELECT에서 바로 사용
  v:event_type::STRING       AS event_type,
  v:event_timestamp::TIMESTAMP AS event_timestamp,
  v:items                    AS items
FROM kafka_events_raw;
```

### 11-11. 세 가지 평탄화 방식 비교 예시

같은 목표(`key`, `timestamp`, `user_id`, `event_type`, `event_timestamp`, `items` 컬럼 만들기)를 세 방식으로:

```sql
-- (1) STRING + 콜론 문법 (가장 간단)
SELECT
  key, timestamp,
  decoded_value:user_id,
  decoded_value:event_type,
  cast(decoded_value:event_timestamp AS TIMESTAMP) AS event_timestamp,
  from_json(decoded_value:items,
            'ARRAY<STRUCT<item_id: STRING, price_usd: DOUBLE, quantity: BIGINT>>') AS items
FROM kafka_events_raw;

-- (2) VARIANT
SELECT
  key, timestamp,
  parse_json(decoded_value)              AS v,
  v:user_id::STRING                      AS user_id,
  v:event_type::STRING                   AS event_type,
  v:event_timestamp::TIMESTAMP           AS event_timestamp,
  v:items                                AS items
FROM kafka_events_raw;

-- (3) STRUCT
SELECT
  key, timestamp,
  from_json(decoded_value,
    'STRUCT<event_timestamp: STRING, event_type: STRING, items: ARRAY<STRUCT<item_id: STRING, price_usd: DOUBLE, quantity: BIGINT>>, user_id: STRING>') AS s,
  s.user_id,
  s.event_type,
  cast(s.event_timestamp AS TIMESTAMP) AS event_timestamp,
  s.items
FROM kafka_events_raw;
```

---

## 12. 엔터프라이즈 데이터 수집: 관리형 커넥터와 Partner Connect

클라우드 오브젝트 스토리지 외에, **데이터베이스와 SaaS 애플리케이션**에서 데이터를 가져오는 방법이다.

### 12-1. Lakeflow Connect 관리형 커넥터

- Databricks에 **내장된 완전 관리형** 커넥터. Lakehouse로의 빠르고 안정적인 수집을 위해 설계되었다.
- **로우코드(low-code)**: UI(포인트 앤 클릭) 또는 API로 설정. 수동 구성이나 커스텀 통합 코드가 거의 필요 없다.
- 대상 예: Salesforce, Workday, SharePoint, ServiceNow 같은 SaaS / SQL Server, PostgreSQL 같은 데이터베이스
- 효율적인 **증분 읽기·쓰기**로 빠르고 확장 가능하며 비용 효율적이고, 데이터를 최신 상태로 유지한다.
- 결과는 **스트리밍 Unity Catalog 테이블**로 적재된다.
- 커넥터마다 릴리스 상태(Public Preview / GA)가 다르므로 최신 공식 문서를 확인한다.

### 12-2. SaaS 수집 아키텍처

1. **서버리스 선언적 파이프라인**이 Unity Catalog에서 **자격 증명(connection)**을 가져온다.
2. 공개적으로 접근 가능한 소스(API, 개방된 엔드포인트)에 **연결**한다.
3. 데이터를 가져와 **스트리밍 UC 테이블**에 저장한다.

- 이를 위한 파이프라인 유형을 **관리형 수집 파이프라인(managed ingestion pipeline)**이라고 한다. 대부분 사전 정의되어 Databricks가 관리하며, 소스별 복잡성을 알아서 처리한다.
- **데이터 이동은 모두 데이터 플레인(compute plane)에서** 일어난다. **컨트롤 플레인**은 파이프라인 설정, 모니터링(이벤트 로그 읽기), 관리에만 쓰인다.
- SaaS 소스의 부하는 보통 **API 호출 한도**로 제어된다.

### 12-3. 데이터베이스 수집 아키텍처

데이터베이스는 온프레미스나 프라이빗 네트워크에 있는 경우가 많아 SaaS보다 구조가 복잡하다.

1. **수집 게이트웨이(Ingestion Gateway)**가 **클래식 compute**에서 실행되며, Unity Catalog에서 자격 증명을 가져와 데이터베이스에 연결한다.
2. 게이트웨이는 **메타데이터, 스냅샷, 변경 로그(change log)**를 추출해 **Unity Catalog 볼륨에 스테이징**한다. 상태 정보도 여기에 저장된다.
3. **서버리스 선언적 파이프라인**이 스테이징된 데이터를 처리해 **스트리밍 UC 테이블**에 저장한다.

| 구성 요소 | 역할 |
|---|---|
| **수집 게이트웨이** | DB에 연결해 스냅샷·변경 로그를 추출하는 전용 파이프라인 (클래식 compute) |
| **UC 볼륨 (스테이징)** | 중간 저장소. 표준 UC 보안 적용, 기본적으로 파이프라인 실행 사용자만 접근 가능 |
| **수집 파이프라인** | 스테이징 데이터를 스트림으로 처리해 대상 테이블에 기록 (서버리스) |

**게이트웨이를 따로 두는 이유**
- **네트워크:** DB가 방화벽 뒤에 있거나 인터넷에 공개되지 않은 경우가 많다. Private Link 같은 옵션이 없으면 게이트웨이를 고객 네트워크 안에 배치할 수 있다.
- **소스 DB 부하 감소:** DB로의 직접 연결 수를 제한할 수 있다. 게이트웨이 하나가 DB와 통신하고, 여러(N개) 파이프라인이 스테이징 데이터를 사용해 확장한다.

| 비교 | SaaS 커넥터 | 데이터베이스 커넥터 |
|---|---|---|
| 소스 위치 | 공개 API | 프라이빗 네트워크·온프레미스 DB |
| 수집 단계 | 서버리스 파이프라인 1단계 | 게이트웨이(클래식) → UC 볼륨 스테이징 → 서버리스 파이프라인 |
| 부하 제어 | API 호출 한도 | 게이트웨이로 연결 수 제한 |
| 대상 | 스트리밍 UC 테이블 | 스트리밍 UC 테이블 |

### 12-4. Partner Connect

특정 소스에 **관리형 커넥터가 없을 때**의 대안이다.

- 엄선된 Databricks 기술 파트너의 **체험 계정을 생성**하고, Databricks 워크스페이스를 파트너 솔루션에 바로 연결할 수 있다.
- Lakehouse의 데이터로 파트너 솔루션을 테스트·평가한 뒤 적합한 것을 도입한다.
- 수집 파트너 예: Fivetran, Informatica, Qlik, Rivery, Alteryx, Prophecy
- 파트너 커넥터는 전담 엔지니어 팀이 유지 관리하며 폭넓은 소스를 지원한다. 네이티브 커넥터가 있는 소스라도 파트너 선택지는 계속 지원된다.

### 12-5. 파일 업로드 관련

- 로컬 파일은 **테이블 생성/수정** 또는 **볼륨에 파일 업로드** 방식으로 올릴 수 있다.
- 하위 호환을 위해 **DBFS**에 직접 업로드할 수도 있지만, Databricks는 **Unity Catalog(볼륨·테이블)에 업로드할 것을 권장**한다.

---

## 13. 그 밖의 데이터 통합·공유 기능

| 기능 | 설명 | 활용 |
|---|---|---|
| **Lakehouse Federation** | 데이터를 **이동(복사)하지 않고** 외부 데이터 소스를 직접 쿼리 | 임시(ad hoc) 보고, 개념 증명(PoC), 새 ETL 파이프라인·보고서의 탐색 단계, 점진적 마이그레이션 중 워크로드 지원 |
| **Zerobus** | Lakeflow Connect의 API. 이벤트 데이터를 레이크하우스에 **직접 쓰기**. 높은 처리량(약 100 MB/s), 낮은 지연(약 5초 미만) | IoT, 클릭스트림, 텔레메트리. 중간 메시지 버스 같은 홉(hop)을 줄임 |
| **Open Sharing** (Delta Sharing) | 플랫폼·클라우드·리전을 넘어 데이터를 **안전하게 공유**하는 개방형 프로토콜 | 조직 간·외부 파트너와 데이터 공유 |
| **Databricks Marketplace** | Open Sharing 기반의 데이터·분석·AI **개방형 마켓플레이스** | 외부 데이터셋, 노트북, 대시보드, ML 모델, 솔루션 액셀러레이터 획득 |

**Databricks Marketplace 핵심**
- 모든 워크스페이스 사용자가 기본으로 이용할 수 있다.
- 이름·카테고리·제공자로 검색하고, 무료 자산만 필터링할 수 있다. Tables, Files, Notebooks 등 자산 유형이 다양하다.
- 자산을 받으면 **자동으로 Unity Catalog에 추가**되고 즉시 SQL·노트북·대시보드로 쿼리할 수 있다. 무료 자산은 승인 절차가 없다.
- 데이터는 공유 방식으로 제공되므로 **복사 없이** 제공자 쪽 최신 상태가 반영된다.

---

## 14. MERGE INTO로 기존 테이블에 수집하기

### 14-1. 개념

- 소스(테이블, 뷰, DataFrame)의 **업데이트·삽입·삭제를 대상 UC 테이블에 하나의 원자적 작업**으로 적용한다.
- 대상은 **Delta 테이블**이어야 한다.
- 스키마 강제(기본)와 스키마 진화(옵션)를 모두 지원한다.
- 활용: **SCD(Slowly Changing Dimension, 점진적 변경 차원), 증분 로드, CDC(Change Data Capture)**, 데이터 동기화

| 매칭 상태 | 가능한 동작 | 절 |
|---|---|---|
| 소스·대상 모두에 있는 행 | UPDATE 또는 DELETE | `WHEN MATCHED [AND 조건] THEN ...` |
| 소스에만 있는 행 (대상 기준 불일치) | INSERT | `WHEN NOT MATCHED [BY TARGET] [AND 조건] THEN INSERT ...` |
| 대상에만 있는 행 (소스 기준 불일치) | UPDATE 또는 DELETE | `WHEN NOT MATCHED BY SOURCE [AND 조건] THEN ...` |

```sql
MERGE INTO target t
USING source s
ON {merge_condition}
WHEN MATCHED THEN {matched_action}
WHEN NOT MATCHED THEN {not_matched_action}
```

### 14-2. 예제: 상태 컬럼으로 변경 유형 구분

대상 `main_users_target` (모두 status = `current`):

| id | first_name | email | status |
|---|---|---|---|
| 1 | peter | peter@email.com | current |
| 2 | zebi | zebi@email.com | current |
| 3 | ... | ... | current |
| 4 | matt | matt@email.com | current |

소스 `update_users_source`:

| id | first_name | email | status |
|---|---|---|---|
| 1 | peter | peter@email.com | delete |
| 2 | zebi | zebi@other.com | update |
| 5 | samarth | samarth@other.com | new |
| 6 | ... | ... | new |

```sql
MERGE INTO main_users_target target
USING update_users_source source
ON target.id = source.id                                   -- 매칭 키
WHEN MATCHED AND source.status = 'update' THEN             -- 둘 다 있고 update → 수정
  UPDATE SET
    target.email  = source.email,
    target.status = source.status
WHEN MATCHED AND source.status = 'delete' THEN             -- 둘 다 있고 delete → 삭제
  DELETE
WHEN NOT MATCHED THEN                                      -- 대상에 없음 → 삽입
  INSERT (id, first_name, email, sign_up_date, status)
  VALUES (source.id, source.first_name, source.email, source.sign_up_date, source.status);
```

결과 지표: `num_affected_rows` = 4, `num_updated_rows` = 1, `num_deleted_rows` = 1, `num_inserted_rows` = 2

- id 1 삭제, id 2 이메일 수정, id 5·6 추가
- **소스에 없던 행(id 3, 4)은 변경되지 않는다.**
- 이 패턴은 증분 로드, CDC, **SCD Type 1**(이력 없이 최신 값으로 덮어쓰기)에 해당한다.
- MERGE도 히스토리에 새 버전(`MERGE`)으로 기록되므로 `VERSION AS OF`로 병합 전 상태를 볼 수 있다.

**작성 규칙**
- `WHEN MATCHED` 절이 여러 개면 **위에서부터 순서대로** 평가되고, 처음 조건이 맞는 절 하나만 적용된다.
- 조건 없는 `WHEN MATCHED`는 마지막에 둔다.
- `UPDATE SET *`, `INSERT *`: 소스와 대상 컬럼이 같을 때 모든 컬럼을 이름으로 매칭해 갱신·삽입
- 주의: 소스의 여러 행이 대상의 같은 행에 매칭되면 오류가 난다 → 소스를 키 기준으로 먼저 중복 제거해야 한다.

### 14-3. 스키마 강제와 스키마 진화

소스에 대상에 없는 새 컬럼(`country`)이 생겼을 때, 기본 `MERGE INTO`는 **스키마 강제** 때문에 오류가 난다.

```
Cannot resolve country in INSERT clause given columns target.id, target.first_name, target.email, target.sign_up_date, target.status.
```

해결 방법 1: **`MERGE WITH SCHEMA EVOLUTION`** (Databricks Runtime 15.2 이상)

```sql
MERGE WITH SCHEMA EVOLUTION INTO main_users_target target
USING new_users_source source
ON target.id = source.id
WHEN MATCHED AND source.status = 'update' THEN
  UPDATE SET target.email = source.email, target.status = source.status
WHEN MATCHED AND source.status = 'delete' THEN
  DELETE
WHEN NOT MATCHED AND source.status = 'new' THEN
  INSERT (id, first_name, email, sign_up_date, status, country)
  VALUES (source.id, source.first_name, source.email, source.sign_up_date, source.status, source.country);
```

- 대상 테이블에 `country` 컬럼이 추가되고, 기존 행의 `country`는 NULL이 된다.

해결 방법 2: 세션 설정으로 자동 스키마 병합 활성화

```sql
SET spark.databricks.delta.schema.autoMerge.enabled = true;
```

- 이 설정은 현재 SparkSession의 모든 쓰기에 영향을 준다. 특정 문장에만 적용하려면 `MERGE WITH SCHEMA EVOLUTION`이 더 명확하다.

---

## 15. 좋은 브론즈 테이블의 조건

운영 환경의 Bronze 테이블에는 원시 값 외에 다음이 필요하다.

| 요소 | 방법 | 목적 |
|---|---|---|
| 적절한 수집 방법 | 일회성: CTAS / 증분(수천 파일): COPY INTO / 대규모·스트리밍: Auto Loader, 스트리밍 테이블 / 기존 테이블 갱신: MERGE INTO | 데이터 양·최신성·운영 패턴에 맞춤 |
| 소스 컨텍스트 | `_metadata.file_name`, `_metadata.file_modification_time`, `current_timestamp()` | 리니지, 감사, 디버깅 |
| 불일치 데이터 보존 | `_rescued_data` (명시적 스키마 + `rescuedDataColumn`) | 모르는 사이의 데이터 손실 방지 |
| 반정형 데이터 처리 | STRING 콜론 문법 / STRUCT(`schema_of_json` + `from_json`) / VARIANT(`parse_json`) | 분석 가능한 컬럼으로 평탄화 |
| 명시적 스키마 | `schema` 옵션, 스키마 힌트 | 추론 오류 방지, 성능 향상 |

이렇게 만든 Bronze 테이블이 Silver(정제) → Gold(집계)로 이어지며, 전 과정이 Unity Catalog의 거버넌스 아래에서 증분적으로 정제된다.

---

## 16. 명령어 치트시트

```sql
-- ===== 파일 확인 =====
LIST '/Volumes/cat/sch/vol/dir';
SELECT * FROM parquet.`/Volumes/.../dir`;
SELECT * FROM text.`/Volumes/.../file.csv`;
SELECT * FROM read_files('/Volumes/.../dir', format => 'csv', sep => '|', header => true);

-- ===== 배치 (CTAS) =====
CREATE TABLE t AS
SELECT *,
       _metadata.file_name              AS source_file,
       _metadata.file_modification_time AS file_modification_time,
       current_timestamp()              AS ingestion_time
FROM read_files('/Volumes/.../dir',
                format => 'csv', header => true,
                schema => 'id INT, name STRING, price DOUBLE',
                rescuedDataColumn => '_rescued_data');

-- ===== 증분 배치 (COPY INTO) =====
CREATE TABLE t;
COPY INTO t FROM '/Volumes/.../dir'
FILEFORMAT = PARQUET
COPY_OPTIONS ('mergeSchema' = 'true');

-- ===== 스트리밍 테이블 (Auto Loader) =====
CREATE OR REFRESH STREAMING TABLE st
SCHEDULE EVERY 1 HOUR
AS SELECT * FROM STREAM read_files('/Volumes/.../dir', format => 'json');
REFRESH STREAMING TABLE st;

-- ===== JSON =====
SELECT col:field, col:nested.field, col:arr[0].x, col:ts::TIMESTAMP FROM t;  -- 문자열 경로
SELECT schema_of_json('<샘플 JSON>');                                          -- 스키마 도출
SELECT from_json(col, 'STRUCT<a: STRING, b: ARRAY<STRUCT<x: INT>>>') AS s FROM t;
SELECT s.a, s.b, explode(s.b) AS elem FROM t2;                                 -- STRUCT 접근, 배열 펼치기
SELECT explode_outer(s.b) FROM t2;                                             -- NULL 배열도 행 유지
SELECT parse_json(col) AS v FROM t;  SELECT v:a::STRING FROM t3;               -- VARIANT
SELECT cast(unbase64(value) AS STRING) FROM t;                                 -- Base64 디코딩

-- ===== 시간 변환 =====
SELECT cast(from_unixtime(ts_micros / 1000000) AS DATE), timestamp_micros(ts_micros) FROM t;

-- ===== MERGE =====
MERGE [WITH SCHEMA EVOLUTION] INTO target t
USING source s ON t.id = s.id
WHEN MATCHED AND s.op = 'update' THEN UPDATE SET *
WHEN MATCHED AND s.op = 'delete' THEN DELETE
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;
```

```python
# ===== 배치 (Python) =====
df = spark.read.format("parquet").load(path)
df.write.mode("overwrite").saveAsTable("cat.sch.t")

# ===== Auto Loader (Python) =====
(spark.readStream.format("cloudFiles")
   .option("cloudFiles.format", "json")
   .option("cloudFiles.schemaLocation", chk_path)
   .load(src_path)
 .writeStream
   .option("checkpointLocation", chk_path)
   .trigger(availableNow=True)
   .toTable("cat.sch.t"))
```

---

## 17. 자가 점검 문제

> 답을 가리고 먼저 생각해본 뒤 펼쳐보자.

**Q1.** Lakeflow의 세 구성 요소와 각각의 역할은?
<details><summary>정답</summary>

Lakeflow Connect(수집), Spark Declarative Pipelines(선언적 ETL·변환), Lakeflow Jobs(오케스트레이션).
</details>

**Q2.** Lakeflow Connect 커넥터 유형 세 가지와 각각의 대상 소스는?
<details><summary>정답</summary>

파일 업로드(로컬 파일), 표준 커넥터(클라우드 오브젝트 스토리지, Kafka 등 — 배치/증분 배치/스트리밍), 관리형 커넥터(SaaS 애플리케이션, 데이터베이스). 관리형 커넥터가 없으면 Partner Connect를 쓴다.
</details>

**Q3.** 배치 수집과 증분 배치 수집의 차이는? 각각의 대표 기법은?
<details><summary>정답</summary>

배치는 실행할 때마다 모든 레코드를 처리(CTAS, `spark.read`), 증분 배치는 새 레코드만 처리하고 이미 적재한 것은 건너뜀(COPY INTO, Auto Loader + 시간 트리거, 스트리밍 테이블).
</details>

**Q4.** CTAS, COPY INTO, Auto Loader를 규모와 멱등성 기준으로 비교하라.
<details><summary>정답</summary>

CTAS: 소규모, 멱등성 없음(배치). COPY INTO: 수천 개 파일, 멱등. Auto Loader: 시간당 수백만~수십억 개 파일, 멱등, 스키마 자동 진화.
</details>

**Q5.** 대상 테이블에 2개 컬럼만 정의하고 3개 컬럼짜리 Parquet을 COPY INTO 했더니 오류가 났다. 해결 방법은?
<details><summary>정답</summary>

`COPY_OPTIONS ('mergeSchema' = 'true')`를 추가해 스키마 진화를 허용한다. 또는 스키마 없이 `CREATE TABLE t;`로 빈 테이블을 만들고 mergeSchema로 적재한다.
</details>

**Q6.** COPY INTO를 같은 경로에 다시 실행하면 결과는? 이미 적재한 파일을 강제로 다시 적재하려면?
<details><summary>정답</summary>

`num_affected_rows`, `num_inserted_rows` 등이 0이고 새 버전도 생기지 않는다. 강제 재적재는 `COPY_OPTIONS ('force' = 'true')` (멱등성이 사라져 중복 가능).
</details>

**Q7.** Databricks SQL에서 스트리밍 테이블을 만들면 갱신 작업은 어떤 compute로 실행되는가?
<details><summary>정답</summary>

각 스트리밍 테이블마다 자동 생성되는 전용 서버리스 선언적 파이프라인으로 실행된다. SQL Warehouse compute를 소비하지 않는다(생성 문 실행에는 SQL Warehouse가 필요).
</details>

**Q8.** SQL에서 Auto Loader를 쓰려면 어떤 문법을 쓰는가? 새 파일이 들어왔을 때 수동으로 반영하는 명령은?
<details><summary>정답</summary>

`CREATE OR REFRESH STREAMING TABLE ... AS SELECT * FROM STREAM read_files(...)`. 수동 반영은 `REFRESH STREAMING TABLE 이름` (전체 재처리는 `FULL`).
</details>

**Q9.** Python Auto Loader에서 `cloudFiles.schemaLocation`과 `checkpointLocation`은 각각 무엇을 저장하는가?
<details><summary>정답</summary>

schemaLocation: 추론한 스키마와 스키마 변경 이력(스키마 추론·진화용). checkpointLocation: 스트림의 처리 상태·진행 상황(어디까지 처리했는지, 재시작·정확히 한 번 처리용).
</details>

**Q10.** Auto Loader로 CSV를 읽었더니 숫자 컬럼까지 모두 STRING이다. 이유와 해결책은?
<details><summary>정답</summary>

CSV/JSON은 기본적으로 모든 컬럼을 STRING으로 추론한다. `cloudFiles.inferColumnTypes = true`로 타입을 추론하거나 `cloudFiles.schemaHints`로 타입을 지정한다.
</details>

**Q11.** `_metadata` 컬럼은 `SELECT *`로 조회되는가? 주로 쓰는 필드 두 가지는?
<details><summary>정답</summary>

조회되지 않는다. 숨겨진 컬럼이라 명시적으로 선택해야 한다. 주요 필드는 `_metadata.file_name`(소스 파일 이름), `_metadata.file_modification_time`(파일 마지막 수정 시각). 그 외 `file_path`, `file_size` 등.
</details>

**Q12.** 마이크로초 단위 Unix 타임스탬프를 DATE로 바꾸는 식은?
<details><summary>정답</summary>

`cast(from_unixtime(ts / 1000000) AS DATE)`. `from_unixtime`은 초 단위를 받아 문자열을 반환하므로 1,000,000으로 나누고 캐스팅한다. `timestamp_micros(ts)`로 TIMESTAMP를 바로 만들 수도 있다.
</details>

**Q13.** `cost BIGINT` 스키마로 `$100` 값을 읽으면 어떻게 되는가?
<details><summary>정답</summary>

해당 행의 `cost`는 null이 되고, 원래 값은 `_rescued_data`에 `{"cost":"$100","_file_path":"..."}` 형태의 JSON 문자열로 보존된다.
</details>

**Q14.** `read_files()`에 `schema`를 직접 지정할 때 복구 컬럼을 얻으려면? 스키마를 지정하는 장점은?
<details><summary>정답</summary>

`rescuedDataColumn => '_rescued_data'` 옵션을 함께 지정한다. 장점: 추론 불일치 위험 감소, 추론 단계 생략으로 속도·compute 효율 향상, 일관된 파싱.
</details>

**Q15.** 데이터가 `_rescued_data`로 들어가는 세 가지 경우는?
<details><summary>정답</summary>

스키마에 없는 컬럼(이름 없는 컬럼 포함), 데이터 타입 불일치, 컬럼명 대소문자 불일치.
</details>

**Q16.** CSV 헤더의 첫 컬럼명이 비어 있으면 그 값은 어디로 가며, 어떻게 꺼내는가?
<details><summary>정답</summary>

`_rescued_data`에 `_c0` 키로 들어간다. `cast(_rescued_data:_c0 AS BIGINT)`처럼 콜론 문법으로 꺼낸다.
</details>

**Q17.** JSON 문자열 컬럼을 다루는 세 가지 방법(STRING, STRUCT, VARIANT)의 장단점은?
<details><summary>정답</summary>

STRING: 간단·유연하지만 느리고 스키마가 없다. STRUCT: 스키마 강제로 일관성·성능이 좋지만 구조 변화에 취약하고 유연성이 낮다. VARIANT: 스키마 없이 유연하면서 성능도 좋지만 DBR 15.4 LTS 이상이 필요하고, Delta에서 활성화하면 writer 프로토콜이 업그레이드되어 오래된 외부 클라이언트와 호환 문제가 생길 수 있다.
</details>

**Q18.** JSON 문자열 컬럼과 STRUCT 컬럼에서 중첩 필드에 접근하는 문법의 차이는?
<details><summary>정답</summary>

JSON 문자열: 콜론 `col:geo.city` (결과는 STRING). STRUCT: 점 `col.geo.city` (결과는 정의된 타입).
</details>

**Q19.** JSON 문자열을 STRUCT로 바꾸는 두 단계는?
<details><summary>정답</summary>

① `schema_of_json(샘플)`로 스키마 도출 → ② `from_json(컬럼, 스키마)`로 파싱.
</details>

**Q20.** `explode()`와 `explode_outer()`의 차이는?
<details><summary>정답</summary>

둘 다 배열 원소마다 행을 만든다. 배열이 NULL(또는 비어 있음)이면 `explode()`는 행을 만들지 않아 원래 행이 사라지고, `explode_outer()`는 NULL 값을 가진 한 행을 반환해 원래 행을 유지한다.
</details>

**Q21.** Kafka에서 온 `value` 컬럼이 Base64 문자열이다. 읽을 수 있는 JSON 문자열로 바꾸는 식은?
<details><summary>정답</summary>

`cast(unbase64(value) AS STRING)`. `unbase64`는 BINARY를 반환하므로 STRING으로 캐스팅한다.
</details>

**Q22.** 관리형 데이터베이스 커넥터에서 수집 게이트웨이를 따로 두는 이유 두 가지는?
<details><summary>정답</summary>

① 네트워크: 방화벽 뒤·비공개 DB에 연결하기 위해 고객 네트워크 안에 배치 가능. ② 소스 DB 부하 감소: 게이트웨이 하나만 DB에 연결하고 여러 파이프라인은 UC 볼륨의 스테이징 데이터를 사용.
</details>

**Q23.** SaaS 커넥터와 DB 커넥터의 아키텍처 차이는?
<details><summary>정답</summary>

SaaS: 서버리스 파이프라인이 UC에서 자격 증명을 받아 공개 API에 직접 연결해 스트리밍 테이블에 기록. DB: 클래식 compute의 수집 게이트웨이가 스냅샷·변경 로그를 UC 볼륨에 스테이징하고, 서버리스 파이프라인이 이를 처리해 스트리밍 테이블에 기록.
</details>

**Q24.** 데이터를 복사하지 않고 외부 DB를 직접 쿼리하는 기능은? 적합한 상황은?
<details><summary>정답</summary>

Lakehouse Federation. 임시 보고, PoC, 새 파이프라인의 탐색 단계, 점진적 마이그레이션 중 워크로드 지원.
</details>

**Q25.** MERGE INTO의 세 가지 매칭 상태와 각각 가능한 동작은?
<details><summary>정답</summary>

WHEN MATCHED(양쪽에 있음): UPDATE/DELETE. WHEN NOT MATCHED [BY TARGET](소스에만 있음): INSERT. WHEN NOT MATCHED BY SOURCE(대상에만 있음): UPDATE/DELETE.
</details>

**Q26.** 소스에 새 컬럼이 생겨 MERGE의 INSERT가 실패했다. 해결 방법 두 가지는?
<details><summary>정답</summary>

`MERGE WITH SCHEMA EVOLUTION INTO ...` (DBR 15.2+), 또는 `spark.databricks.delta.schema.autoMerge.enabled = true` 세션 설정.
</details>

**Q27.** MERGE에서 소스의 두 행이 대상의 같은 행에 매칭되면?
<details><summary>정답</summary>

오류가 난다. 소스를 매칭 키 기준으로 먼저 중복 제거해야 한다.
</details>

---

## 18. 참고 링크

- Lakeflow Connect: https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/
- read_files 함수: https://docs.databricks.com/aws/en/sql/language-manual/functions/read_files
- COPY INTO: https://docs.databricks.com/aws/en/ingestion/copy-into/
- Auto Loader: https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader
- 스트리밍 테이블: https://docs.databricks.com/aws/en/dlt/dbsql/streaming
- CREATE STREAMING TABLE: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-streaming-table
- 파일 메타데이터 컬럼: https://docs.databricks.com/aws/en/ingestion/file-metadata-column
- Rescued data 컬럼: https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema#what-is-the-rescued-data-column
- JSON 문자열 쿼리: https://docs.databricks.com/aws/en/semi-structured/json
- VARIANT 데이터 쿼리: https://docs.databricks.com/aws/en/semi-structured/variant
- MERGE (Upsert): https://docs.databricks.com/aws/en/delta/merge
- MERGE 스키마 진화: https://docs.databricks.com/aws/en/delta/update-schema
- Lakehouse Federation: https://docs.databricks.com/aws/en/query-federation/
- Delta Sharing (Open Sharing): https://docs.databricks.com/aws/en/delta-sharing/
- Databricks Marketplace: https://docs.databricks.com/aws/en/marketplace/
- Partner Connect: https://docs.databricks.com/aws/en/partner-connect/
