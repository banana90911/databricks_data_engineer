# Lakeflow Spark Declarative Pipelines 고급 기법 — 핵심 개념 정리

---

## 목차

1. [Flow: 파이프라인의 기본 실행 단위](#1-flow-파이프라인의-기본-실행-단위)
2. [Default Flow, Explicit Flow, Multi-Flow 패턴](#2-default-flow-explicit-flow-multi-flow-패턴)
3. [Multi-Flow 구현 예제: 여러 자회사 주문 통합](#3-multi-flow-구현-예제-여러-자회사-주문-통합)
4. [데이터 품질 Expectations 복습과 모니터링](#4-데이터-품질-expectations-복습과-모니터링)
5. [Liquid 클러스터링](#5-liquid-클러스터링)
6. [실버, 골드 레이어와 태그](#6-실버-골드-레이어와-태그)
7. [Multiplex 패턴과 VARIANT](#7-multiplex-패턴과-variant)
8. [Sink: 파이프라인 밖으로 쓰기](#8-sink-파이프라인-밖으로-쓰기)
9. [Delta UniForm으로 Iceberg 읽기](#9-delta-uniform으로-iceberg-읽기)
10. [CDC와 SCD 복습](#10-cdc와-scd-복습)
11. [AUTO CDC INTO](#11-auto-cdc-into)
12. [SCD Type 2 파이프라인 구현 예제](#12-scd-type-2-파이프라인-구현-예제)
13. [고급 데이터 품질 Expectations](#13-고급-데이터-품질-expectations)
14. [스키마 진화에 강한 브론즈 설계](#14-스키마-진화에-강한-브론즈-설계)
15. [격리(Quarantine) 패턴](#15-격리quarantine-패턴)
16. [스트림-정적 조인과 종합 예제](#16-스트림-정적-조인과-종합-예제)
17. [핵심 정리 치트시트](#17-핵심-정리-치트시트)
18. [자가 점검 문제](#18-자가-점검-문제)
19. [참고 링크](#19-참고-링크)

---

## 1. Flow: 파이프라인의 기본 실행 단위

### 1-1. Flow = 쿼리 + 대상

**Flow**는 Spark Declarative Pipelines(SDP)의 기본 작업 단위다. 각 flow는 독립적으로 데이터를 읽고, 처리하고, 쓴다.

| 구성 | 의미 |
|---|---|
| **쿼리** | 무엇을 읽고 어떻게 변환할지 (SQL, DataFrame 로직: 필터, 조인, 집계) |
| **대상** | 결과를 어디에 저장할지 (증분 수집은 Streaming Table, 계산/집계 결과는 Materialized View, 또는 Sink) |

### 1-2. 처리 모드

| 모드 | 동작 | 사용 시점 |
|---|---|---|
| **증분 (Incremental)** | flow별 **체크포인트**를 읽고 마지막 실행 이후의 **새 레코드만** 처리 → 대상에 쓰기 → 체크포인트 갱신 | Kafka, Auto Loader, 추가 전용 소스에서 지속적으로 수집 |
| **전체 갱신 (Full refresh)** | 체크포인트 삭제 → 소스 전체를 처음부터 읽고 → 대상 **덮어쓰기** | 변환 로직 변경, 데이터 품질 수정, 완전한 초기화가 필요할 때 |

### 1-3. 체크포인트 규칙

- flow가 시작되면 체크포인트를 읽는다. 있으면 마지막 위치부터 재개하고, 없으면 처음부터 시작한다.
- **Flow 이름이 곧 체크포인트다.** flow 이름은 스토리지의 체크포인트 위치에 매핑된다. 이름을 바꾸면 새 체크포인트가 생기고 기존 것은 버려져 **처음부터 재처리**된다. flow 이름은 안정적으로 유지해야 한다.
- **Flow는 독립적으로 진행하고 독립적으로 실패한다.** 느리거나 실패한 flow가 같은 파이프라인의 다른 flow에 영향을 주지 않는다.

---

## 2. Default Flow, Explicit Flow, Multi-Flow 패턴

### 2-1. Default Flow vs Explicit Flow

| 구분 | Default Flow (암묵적) | Explicit Flow (명시적) |
|---|---|---|
| 생성 | 테이블 정의 시 자동 생성 | 테이블과 별도로 `CREATE FLOW`로 정의 |
| 이름 | 대상 테이블 이름 | 직접 지정 |
| 용도 | 일반적인 단일 소스 테이블 | 기존 테이블에 새 소스로 쓰기, 여러 소스를 하나로 통합 |

```sql
-- Default Flow: 테이블과 flow가 한 번에
CREATE OR REFRESH STREAMING TABLE target_table
AS SELECT * FROM STREAM source_table;

-- Explicit Flow: 테이블을 먼저 정의하고, flow를 따로 연결
CREATE OR REFRESH STREAMING TABLE target_table;

CREATE FLOW my_flow
AS INSERT INTO target_table BY NAME
SELECT * FROM STREAM source_table;
```

`BY NAME`은 컬럼을 위치가 아니라 **이름으로** 맞춰 삽입한다. 소스마다 컬럼 순서가 달라도 안전하다.

### 2-2. Multi-Flow 패턴

명시적 flow를 여러 개 정의해 **같은 대상 테이블**에 쓴다. 여러 소스를 하나의 통합 테이블로 모을 때 권장되는 방식이다.

- 소스 A(CSV) → flow A
- 소스 B(JSON) → flow B
- 소스 C(CSV) → flow C
- → 모두 하나의 Streaming Table에 `INSERT INTO ... BY NAME`

각 flow가 독립 체크포인트를 가지므로, 나중에 새 소스를 추가해도 **그 flow만 백필**하면 된다.

### 2-3. Multi-Flow vs UNION

| UNION (하나의 테이블 정의 안에서 소스 결합) | Multi-Flow |
|---|---|
| 모든 소스가 **하나의 체크포인트** 공유 | flow마다 **독립 체크포인트** |
| 새 소스 추가 시 **전체 갱신** 필요 | 전체 갱신 없이 새 소스 추가 |
| 한 소스의 오류가 다른 소스를 막을 수 있음 | flow가 격리되어 오류가 번지지 않음 |
| 소스별 리니지 추적이 어려움 | 소스별 리니지와 모니터링이 명확 |
| 오류 처리가 복잡 | 소스별 독립 오류 처리와 복구 |
| 소스가 늘수록 확장성 제한 | 확장성과 유지 보수성 향상 |

### 2-4. Python으로 Multi-Flow

```python
from pyspark import pipelines as dp

# 대상 Streaming Table을 먼저 생성
dp.create_streaming_table(name="orders_bronze", comment="모든 소스의 주문")

@dp.append_flow(target="orders_bronze", name="web_orders_flow")
def web_orders_flow():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .load(spark.conf.get("web_orders_source")))

@dp.append_flow(target="orders_bronze", name="app_orders_flow")
def app_orders_flow():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .load(spark.conf.get("app_orders_source")))
```

> 과거 데이터를 한 번만 채워 넣는 일회성 백필은 `INSERT INTO ONCE`(SQL) 또는 `@dp.append_flow(once=True)`(Python) 형태의 flow로 처리할 수 있다. 구문은 버전에 따라 다를 수 있으므로 문서를 확인한다.

---

## 3. Multi-Flow 구현 예제: 여러 자회사 주문 통합

### 3-1. 문제: 소스마다 다른 추론 타입

세 자회사가 매일 주문 파일을 서로 다른 볼륨에 떨군다. 두 곳은 CSV, 한 곳은 JSON이다. 형식마다 스키마를 따로 추론하므로 같은 컬럼의 타입이 다르다.

| 컬럼 | CSV 추론 타입 | JSON 추론 타입 |
|---|---|---|
| `order_timestamp` | timestamp | string |
| `qty` | int | bigint |
| `discount_pct` | int | bigint |
| `order_date` | date | string |

이대로 하나의 테이블에 합치면 **스키마 불일치**로 수집이 실패한다.

**해결**: 브론즈에서는 **모든 비즈니스 컬럼을 STRING으로** 수집하고, 실버에서 올바른 타입을 적용한다.

### 3-2. 브론즈 대상 테이블

```sql
CREATE OR REFRESH STREAMING TABLE multi_flow_1_bronze.orders_bronze
(
  subsidiary_id   STRING,
  order_id        STRING,
  order_timestamp STRING,
  customer_id     STRING,
  region          STRING,
  sku             STRING,
  category        STRING,
  qty             STRING,
  unit_price      STRING,
  discount_pct    STRING,
  total_amount    STRING,
  order_date      STRING,
  source_file     STRING,     -- _metadata에서 추출한 파일 이름
  file_mod_time   TIMESTAMP   -- _metadata에서 추출한 파일 수정 시각
)
COMMENT "모든 자회사의 주문을 multi-flow로 수집한 브론즈 테이블"
TBLPROPERTIES ('pipelines.reset.allowed' = false);  -- 전체 갱신 방지
```

> **`pipelines.reset.allowed = false`가 중요한 이유**: 소스 스토리지가 일정 기간 후 파일을 자동 삭제하는 경우, 전체 갱신을 하면 체크포인트와 테이블이 지워진 뒤 **이미 사라진 파일은 다시 수집되지 않아** 데이터를 잃는다. 브론즈 테이블에는 이 속성으로 실수에 의한 전체 갱신을 막는다.

### 3-3. 소스별 Flow

볼륨 경로는 파이프라인 **설정(configuration) 파라미터**로 넘기고 SQL에서 `${키}`로 참조한다.

| 설정 키 | 값 |
|---|---|
| `bright_home_orders_source` | `/Volumes/<catalog>/multi_flow_1_bronze/bright_home_orders` |
| `lumina_sports_orders_source` | `/Volumes/<catalog>/multi_flow_1_bronze/lumina_sports_orders` |
| `northstar_outfitters_orders_source` | `/Volumes/<catalog>/multi_flow_1_bronze/northstar_outfitters_orders` |

```sql
CREATE FLOW bright_home_orders_flow
AS INSERT INTO multi_flow_1_bronze.orders_bronze BY NAME
SELECT
  CAST(subsidiary_id   AS STRING) AS subsidiary_id,
  CAST(order_id        AS STRING) AS order_id,
  CAST(order_timestamp AS STRING) AS order_timestamp,
  -- ... 모든 비즈니스 컬럼을 STRING으로 ...
  CAST(order_date      AS STRING) AS order_date,
  _metadata.file_name              AS source_file,
  _metadata.file_modification_time AS file_mod_time
FROM STREAM read_files('${bright_home_orders_source}', format => 'csv', header => true);

CREATE FLOW lumina_sports_orders_flow
AS INSERT INTO multi_flow_1_bronze.orders_bronze BY NAME
SELECT /* 같은 컬럼 목록 */ ...
FROM STREAM read_files('${lumina_sports_orders_source}', format => 'csv', header => true);

CREATE FLOW northstar_outfitters_orders_flow
AS INSERT INTO multi_flow_1_bronze.orders_bronze BY NAME
SELECT /* 같은 컬럼 목록 */ ...
FROM STREAM read_files('${northstar_outfitters_orders_source}', format => 'json');
```

- 세 flow의 차이는 **flow 이름, 설정 파라미터, 파일 형식**뿐이다.
- `FROM STREAM read_files(...)`는 Auto Loader로 증분 수집한다.
- 메타데이터 컬럼(`source_file`, `file_mod_time`)은 리니지와 디버깅에 유용하다.

### 3-4. 실행 결과 흐름

| 실행 | 브론즈 | 실버 | 골드 MV |
|---|---|---|---|
| 1회차 (각 볼륨에 파일 1개: 157 + 110 + 182) | 449행 수집 | 449행 처리 | 최초 생성은 Full recompute |
| 2회차 (각 볼륨에 다음 날 파일 추가: 191 + 170 + 141) | 새 502행만 수집 | 새 502행만 처리 | **Incremental** 갱신 |
| 누적 | 951행 | 951행 | |

```sql
-- 파일별 수집 행 수 확인
SELECT source_file, count(*) AS total_rows
FROM multi_flow_1_bronze.orders_bronze
GROUP BY source_file ORDER BY source_file;
```

---

## 4. 데이터 품질 Expectations 복습과 모니터링

### 4-1. 위반 시 동작

| 동작 | 구문 | 결과 |
|---|---|---|
| **WARN** (기본) | `ON VIOLATION` 절 없음 | 행 유지, 위반 수를 메트릭에 기록 |
| **DROP ROW** | `ON VIOLATION DROP ROW` | 행 제거, 제거 수를 메트릭에 기록 |
| **FAIL UPDATE** | `ON VIOLATION FAIL UPDATE` | 위반 행이 있으면 그 flow 업데이트 실패. Triggered 파이프라인에서는 다른 flow는 계속 실행 |

```sql
CREATE OR REFRESH STREAMING TABLE orders_silver
(
  CONSTRAINT valid_qty      EXPECT (qty >= 0)                    ON VIOLATION DROP ROW,
  CONSTRAINT valid_amount   EXPECT (total_amount >= 0)           ON VIOLATION DROP ROW,
  CONSTRAINT not_null_ts    EXPECT (order_timestamp IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_email    EXPECT (customer_email RLIKE '^[^@]+@[^@]+\\.[^@]+$'),  -- WARN
  CONSTRAINT reasonable_qty EXPECT (qty <= 1000)                                     -- WARN
)
AS SELECT ...;
```

> **Expectations는 flow에 정의할 수 없다.** Multi-Flow에서는 품질 규칙을 **대상 테이블**에 정의하며, 그 테이블에 쓰는 모든 flow에 중앙에서 적용된다. `CREATE FLOW`나 `@dp.append_flow` 안에는 지정할 수 없다.

Python에서는 데코레이터로 지정한다.

| 데코레이터 | 동작 |
|---|---|
| `@dp.expect(name, cond)` / `@dp.expect_all({...})` | WARN |
| `@dp.expect_or_drop(name, cond)` / `@dp.expect_all_or_drop({...})` | DROP ROW |
| `@dp.expect_or_fail(name, cond)` / `@dp.expect_all_or_fail({...})` | FAIL UPDATE |

### 4-2. 모니터링

Expectations는 자동으로 메트릭을 만든다. 파이프라인 UI에서 한눈에 보거나, 이벤트 로그를 쿼리해 분석한다.

| 메트릭 | 용도 |
|---|---|
| 처리된 레코드 | 업데이트마다 읽고 쓴 행 수 (flow별 처리량 기준) |
| 제약 조건별 실패 레코드 | 어떤 규칙이 몇 건 실패했는지 |
| 위반 비율 | 일시적 이상과 구조적 품질 문제 구분 |
| 시간에 따른 추세 | 점진적 드리프트, 업스트림 스키마 변경으로 인한 회귀 파악 |

```sql
SELECT timestamp, origin.flow_name, details:flow_progress.metrics.num_output_rows,
       details:flow_progress.data_quality.expectations
FROM event_log('<pipeline_id>')
WHERE event_type = 'flow_progress'
  AND details:flow_progress.data_quality.expectations IS NOT NULL
ORDER BY timestamp DESC;
```

> 주의: 한 행이 여러 제약을 동시에 위반하면 그래프의 요약 숫자와 제약별 숫자가 다르게 보일 수 있다. 예를 들어 DROP으로 이미 제거된 행은 뒤의 WARN 제약 집계에 포함되지 않을 수 있으므로, 정확한 수치는 제약별 메트릭에서 확인한다.

---

## 5. Liquid 클러스터링

### 5-1. 데이터 레이아웃 기법의 발전

| 기법 | 특징 | 한계 |
|---|---|---|
| Hive 파티셔닝 | 고정된 디렉터리 구조 | 키를 바꾸려면 전체 재작성. 저카디널리티 컬럼에만 적합, 작은 파일 문제 |
| Z-Ordering | 파일 안에서 다차원 정렬 | 수동 `OPTIMIZE` 필요, 실행할 때마다 전체 재작성 |
| **Liquid 클러스터링** | 클러스터링 키 기반 파일 구성 | 증분 방식, 언제든 키 변경 가능 |

Liquid 클러스터링의 세 가지 특성은 다음과 같다.

- **증분**: 새 데이터나 아직 클러스터링되지 않은 파일만 정리한다. 이미 정리된 파일은 다시 쓰지 않아 스트리밍, 쓰기 집약 워크로드에 효율적이다.
- **유연성**: 테이블 전체를 다시 쓰지 않고 언제든 클러스터링 키를 바꿀 수 있다.
- **자동 조정**: `CLUSTER BY AUTO`면 Databricks가 실제 쿼리 패턴을 보고 키를 고른다.

작동 방식: 클러스터링 키(예: 날짜, 고객 ID) 범위별로 파일을 묶는다. 볼륨이 작은 키 값들은 함께 묶어 작은 파일 문제를 줄이고, 볼륨이 큰 값은 그대로 둔다. 쿼리는 필터와 맞지 않는 파일을 건너뛴다(데이터 스키핑). 내부적으로 힐베르트 곡선을 써서 파일 경계를 촘촘하게 만든다.

### 5-2. SDP에서 사용하기

스트리밍 테이블이나 Materialized View 정의에 `CLUSTER BY`를 붙이면 된다. 별도의 `OPTIMIZE` 명령이 필요 없다.

```sql
-- 자동 선택
CREATE OR REFRESH STREAMING TABLE orders_silver
CLUSTER BY AUTO
AS SELECT * FROM STREAM orders_bronze;

-- 키 직접 지정
CREATE OR REFRESH STREAMING TABLE orders_silver
CLUSTER BY (region, order_date)
AS SELECT * FROM STREAM orders_bronze;
```

```python
@dp.table(cluster_by=["region", "order_date"])   # 또는 cluster_by_auto=True
def orders_silver():
    return spark.readStream.table("orders_bronze")
```

### 5-3. AUTO vs 직접 지정

| `CLUSTER BY AUTO` | `CLUSTER BY (columns)` |
|---|---|
| 새 테이블, 쿼리 패턴을 아직 모를 때 | 자주 필터링하는 컬럼을 정확히 알 때 |
| 쿼리 패턴 변화에 자동 적응 | 완전한 제어, 예측 가능한 레이아웃 |
| 수동 튜닝 불필요 | 안정적인 워크로드, 규정 준수나 성능이 중요한 테이블 |

- AUTO는 처음엔 키가 정해지지 않은 상태다. Databricks가 먼저 쿼리를 관찰한 뒤 키를 고른다. `DESCRIBE TABLE EXTENDED`의 테이블 속성에서 `clusterByAuto=true`로 활성화 여부를 확인한다.
- AUTO는 **비용-편익 분석**을 한다. 데이터 스키핑으로 얻는 이득이 파일 재구성 비용보다 클 때만 키를 바꾼다.
- **하이브리드**: 명시적 컬럼을 초기 힌트로 주면서 AUTO를 함께 켜 장기 변화에 대응할 수도 있다.
- AUTO 키 선택은 **Predictive Optimization**에 의존하므로 UC 관리 테이블에 Predictive Optimization이 켜져 있어야 한다.

---

## 6. 실버, 골드 레이어와 태그

### 6-1. 실버: 고정 스키마 + 안전한 타입 변환 + 품질 + 클러스터링

```sql
CREATE OR REFRESH STREAMING TABLE multi_flow_2_silver.orders_silver
(
  -- 고정 스키마 (원치 않는 스키마 진화 방지)
  subsidiary_id   STRING,
  order_id        STRING,
  order_timestamp TIMESTAMP,
  order_date      DATE,
  qty             INT,
  unit_price      DOUBLE,
  discount_pct    DOUBLE,
  total_amount    DOUBLE,
  -- 품질 규칙
  CONSTRAINT qty_valid          EXPECT (qty >= 0)                    ON VIOLATION DROP ROW,
  CONSTRAINT total_amount_valid EXPECT (total_amount >= 0)           ON VIOLATION DROP ROW,
  CONSTRAINT timestamp_not_null EXPECT (order_timestamp IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT '여러 flow로 수집한 브론즈를 정제, 표준화'
CLUSTER BY AUTO
AS SELECT
  subsidiary_id,
  order_id,
  TRY_CAST(order_timestamp AS TIMESTAMP) AS order_timestamp,
  TRY_CAST(order_date      AS DATE)      AS order_date,
  TRY_CAST(qty             AS INT)       AS qty,
  TRY_CAST(unit_price      AS DOUBLE)    AS unit_price,
  TRY_CAST(discount_pct    AS DOUBLE)    AS discount_pct,
  TRY_CAST(total_amount    AS DOUBLE)    AS total_amount
FROM STREAM multi_flow_1_bronze.orders_bronze;
```

- `TRY_CAST`는 변환에 실패하면 오류 대신 **NULL**을 반환한다. 파이프라인을 멈추지 않고, 이후 expectation이 NULL을 걸러 낸다.

### 6-2. 골드: Materialized View

```sql
CREATE OR REFRESH MATERIALIZED VIEW multi_flow_3_gold.mv_daily_subsidiary_scorecard
AS SELECT
  order_date,
  subsidiary_id,
  COUNT(DISTINCT order_id)    AS order_count,
  ROUND(SUM(total_amount), 2) AS total_revenue,
  SUM(qty)                    AS total_units
FROM multi_flow_2_silver.orders_silver
WHERE order_date IS NOT NULL
GROUP BY order_date, subsidiary_id;
```

- MV는 첫 생성 때 **Full recompute**, 이후 가능하면 **Incremental**로 갱신된다(서버리스 파이프라인). 어느 쪽으로 갱신됐는지는 파이프라인 UI 테이블 목록의 Incrementalization 정보로 확인한다.
- 윈도우 함수보다 `GROUP BY` 집계가 증분 갱신에 더 유리하다.

### 6-3. Unity Catalog 태그

```sql
ALTER TABLE multi_flow_1_bronze.orders_bronze SET TAGS ('department' = 'Sales', 'quality' = 'bronze');
ALTER TABLE multi_flow_2_silver.orders_silver SET TAGS ('department' = 'Sales', 'quality' = 'silver');
ALTER TABLE multi_flow_3_gold.mv_daily_subsidiary_scorecard SET TAGS ('department' = 'Sales', 'quality' = 'gold');

-- 태그 조회
SELECT * FROM information_schema.table_tags;
```

- 사용자 정의 태그 외에 Databricks **시스템 태그**(예: 인증된 객체를 나타내는 certified 태그)로 신뢰할 수 있는 운영 객체를 표시할 수 있다.
- 태그를 추가하려면 해당 권한(`APPLY TAG` 등)이 필요하다.

---

## 7. Multiplex 패턴과 VARIANT

### 7-1. 문제: 하나의 스트림, 다양한 스키마

여러 비즈니스 시스템이 **하나의 전송 수단**(Kafka 토픽 하나, 스토리지 경로 하나, 메시지 큐 하나)을 공유하고, 각 메시지에 어느 도메인인지 나타내는 **유형 필드**가 있는 경우가 많다.

| Multiplex 없이 | Multiplex 사용 |
|---|---|
| N개 이벤트 유형 → N개 파이프라인 | N개 이벤트 유형 → 수집 파이프라인 1개 |
| N개 체크포인트, N번 소스 스캔 | 체크포인트 1개, 소스 스캔 1번을 모든 도메인이 공유 |
| 소스 변경을 N곳에 반영 | 소스 변경을 한 곳에 반영 |

### 7-2. 한 번 수집하고 유형별로 팬아웃

1. **단일 수집**: 모든 이벤트를 하나의 브론즈 테이블로 읽는다.
2. **유형 기반 필터링**: 유형 필드로 도메인을 나눈다.
3. **팬아웃**: 도메인별 테이블을 만든다(역다중화, demultiplexing).

이점: 단일 체크포인트 관리, 공유 소스 스캔, 중앙화된 오류 처리, 단순한 모니터링.

### 7-3. VARIANT 데이터 타입

도메인마다 페이로드 구조가 다르므로 브론즈에서는 **VARIANT**로 받는다.

- 반정형 JSON을 하나의 컬럼에 담는다(여러 이벤트 형태를 한 컬럼에).
- 필요할 때 경로로 필드를 꺼낸다.
- 문자열 JSON을 매번 파싱하는 것보다 빠르다.
- 클래식 compute에서는 Databricks Runtime 15.4 LTS 이상이 필요하다.

```sql
SELECT
  CAST(value AS STRING)                             AS value_str,
  parse_json(CAST(value AS STRING))                 AS event_data,       -- 문자열 → VARIANT
  event_data:event_id::STRING                       AS event_id,         -- 경로 추출 + 캐스팅
  CAST(event_data:timestamp AS TIMESTAMP)           AS event_ts
FROM read_files('/Volumes/.../business_events');
```

| 문법 | 의미 |
|---|---|
| `parse_json(str)` | JSON 문자열을 VARIANT로 변환 (잘못된 JSON이면 오류. `try_parse_json`은 NULL 반환) |
| `v:field` | VARIANT에서 필드 추출 (결과도 VARIANT) |
| `v:a.b[0]` | 중첩 필드, 배열 원소 접근 |
| `v:field::TYPE` | 추출 후 캐스팅 (`CAST(v:field AS TYPE)`과 같음) |

### 7-4. 구현 예제

소스: Kafka에서 받은 Parquet 파일. `key`, `value`가 BINARY이고 `topic` 컬럼이 도메인(마케팅, 물류, 매장 운영)을 나타낸다.

```sql
-- 1) 단일 브론즈: 모든 이벤트를 VARIANT로
CREATE OR REFRESH STREAMING TABLE multiplex_1_bronze.bronze_events
TBLPROPERTIES ('pipelines.reset.allowed' = false)
AS SELECT
  CAST(key AS STRING)               AS event_id,
  PARSE_JSON(CAST(value AS STRING)) AS event_data_variant,
  CAST(topic AS STRING)             AS event_group,
  CAST(partition AS STRING)         AS partition,
  CAST(offset AS STRING)            AS offset,
  _metadata.file_name               AS source_file,
  _metadata.file_modification_time  AS file_mod_time
FROM STREAM read_files('${business_events_source}');

-- 2) 팬아웃: 도메인별 중간 테이블 (필드 추출 + 타입 지정)
CREATE OR REFRESH STREAMING TABLE multiplex_1_bronze.marketing_intermediate
AS SELECT
  event_id,
  event_data_variant:event_id::STRING    AS extracted_event_id,
  event_data_variant:timestamp::TIMESTAMP AS timestamp,
  event_data_variant:campaign_id::STRING AS campaign_id,
  event_data_variant:channel::STRING     AS channel,
  event_data_variant:impressions::LONG   AS impressions,
  event_data_variant:clicks::LONG        AS clicks,
  event_data_variant:conversions::LONG   AS conversions,
  event_data_variant:spend_usd::DOUBLE   AS spend_usd
FROM STREAM multiplex_1_bronze.bronze_events
WHERE event_group = 'business_events_marketing';

-- logistics_intermediate, store_ops_intermediate도 같은 방식 (WHERE 조건과 추출 필드만 다름)

-- 3) 실버: 정제와 파생 컬럼
CREATE OR REFRESH STREAMING TABLE multiplex_2_silver.marketing_silver
AS SELECT
  COALESCE(extracted_event_id, event_id) AS event_id,   -- 신뢰할 수 있는 ID 확보
  timestamp, campaign_id, channel, impressions, clicks, conversions, spend_usd,
  CASE WHEN impressions > 0 THEN clicks / impressions ELSE 0 END AS click_through_rate,
  CASE WHEN clicks > 0 THEN spend_usd / clicks ELSE 0 END        AS cost_per_click
FROM STREAM multiplex_1_bronze.marketing_intermediate;

-- 4) 골드: 캠페인 요약 (0으로 나누기 방지에 NULLIF)
CREATE OR REFRESH MATERIALIZED VIEW multiplex_3_gold.marketing_campaign_summary
AS SELECT
  campaign_id, channel,
  SUM(impressions) AS total_impressions,
  SUM(clicks)      AS total_clicks,
  ROUND(SUM(clicks) * 100.0 / NULLIF(SUM(impressions), 0), 2) AS ctr_percentage,
  ROUND(SUM(spend_usd) / NULLIF(SUM(conversions), 0), 2)      AS cost_per_conversion
FROM multiplex_2_silver.marketing_silver
GROUP BY campaign_id, channel;
```

1회차에 372행이 브론즈로 들어와 물류 194, 마케팅 105, 매장 운영 73행으로 팬아웃되고, 2회차 파일(300행)은 새 행만 각 도메인으로 증분 처리된다.

---

## 8. Sink: 파이프라인 밖으로 쓰기

### 8-1. Sink란

SDP의 표준 데이터셋(Streaming Table, Materialized View)은 **파이프라인이 소유하고 관리**한다. **Sink**는 의도적으로 이를 벗어나, 스트리밍 데이터를 **파이프라인 관리 범위 밖의 대상**에 쓴다.

| 관리형 데이터셋 (기본) | Sink |
|---|---|
| 파이프라인이 수명 주기 관리 | 파이프라인 밖의 외부 대상 |
| 전체 파이프라인 리니지 | 외부 Delta 테이블, Kafka, Event Hubs, 사용자 정의 대상 |
| Expectations, AUTO CDC 지원 | **Expectations 없음, 추가(append) 전용** |
| 테이블 속성 일부 제한 | 대상 테이블 속성을 자유롭게 설정 (예: Iceberg 호환) |

### 8-2. Sink 유형

| 유형 | 용도 |
|---|---|
| **Delta 테이블 Sink** | UC 관리 테이블 또는 외부 Delta 테이블에 쓰기 (테이블 이름 또는 경로) |
| **Apache Kafka Sink** | Kafka 토픽에 다시 쓰기. 저지연 운영 시스템 연동, 리버스 ETL |
| **Azure Event Hubs Sink** | Kafka 인터페이스로 Event Hubs에 쓰기. 실시간 이벤트 (사기 탐지, 추천) |
| **Python 사용자 정의 Sink** | PySpark 사용자 정의 데이터 소스로 임의의 저장소에 쓰기 |

### 8-3. 구현

```python
from pyspark import pipelines as dp

my_catalog = spark.conf.get("my_catalog")

# 1) Sink 등록 (대상 정의)
dp.create_sink(
    name="delta_sink_logistics",
    format="delta",
    options={"tableName": f"{my_catalog}.multiplex_3_gold.logistics_delta_sink"}
)

# 2) append flow로 Sink에 쓰기
@dp.append_flow(name="delta_sink_logistics_flow", target="delta_sink_logistics")
def delta_sink_logistics_flow():
    return spark.readStream.table("multiplex_2_silver.logistics_silver")
```

- **Python API만 지원**한다. SQL로는 Sink를 만들 수 없다.
- Sink에는 append flow(`@dp.append_flow`)로만 쓸 수 있다.
- 체크포인트는 append flow가 자동 관리한다. 실행마다 새 레코드만 추가되고 덮어쓰기는 없다.
- 경로로 쓰려면 `options={"path": "..."}`를 쓴다. Kafka Sink는 `format="kafka"`와 `kafka.bootstrap.servers`, `topic` 옵션을 준다.

---

## 9. Delta UniForm으로 Iceberg 읽기

### 9-1. 개념

Delta UniForm(Universal Format)은 **하나의 Parquet 파일 세트** 위에 Delta 메타데이터와 **Iceberg 메타데이터**를 함께 유지한다. 데이터를 복사하지 않고 Snowflake, Trino, Athena, 오픈 소스 Spark 같은 Iceberg 클라이언트가 같은 테이블을 읽을 수 있다.

| 기존 방식 | Delta UniForm |
|---|---|
| 플랫폼별로 데이터 복사 (Iceberg 사본, Parquet 사본 등) | 하나의 Parquet 파일 세트 |
| 사본 간 동기화 문제 | Delta + Iceberg 두 메타데이터 레이어, 자동 동기화 |
| 스토리지 비용 증가 | 중복 없음 |

- Iceberg 메타데이터는 Delta 커밋 후 **비동기**로 생성되어 쓰기 성능에 거의 영향이 없다.
- Unity Catalog가 **Iceberg REST 카탈로그** 역할을 하므로 외부 클라이언트가 거버넌스가 적용된 상태로 접근한다.

### 9-2. Iceberg 읽기 활성화 (Iceberg v2 호환)

```sql
-- 1) Deletion Vectors 비활성화 (Iceberg v2는 Delta의 소프트 삭제 마커를 표현하지 못함)
ALTER TABLE gold.logistics_delta_sink
SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false');

-- 2) 컬럼 매핑, Iceberg 호환, UniForm 활성화
ALTER TABLE gold.logistics_delta_sink
SET TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',     -- Delta/Iceberg 간 컬럼 식별자 일관성
  'delta.enableIcebergCompatV2'          = 'true',     -- Iceberg v2 호환 쓰기 프로토콜
  'delta.universalFormat.enabledFormats' = 'iceberg'   -- 커밋마다 Iceberg 메타데이터 생성
);

-- 확인: "Delta UniForm Iceberg" 섹션 (metadata location, converted delta version 등)
DESCRIBE TABLE EXTENDED gold.logistics_delta_sink;
SHOW TBLPROPERTIES gold.logistics_delta_sink;
```

새 테이블이라면 생성 시점에 속성을 함께 지정할 수 있다.

### 9-3. 제한 사항

| 제한 | 내용 |
|---|---|
| Iceberg 클라이언트는 읽기 전용 | 모든 쓰기는 Delta로 해야 함 |
| Deletion Vectors 비활성화 필요 | Iceberg v2 기준. **Iceberg v3**(Databricks Runtime 18 LTS 이상)에서는 필요 없음 |
| 파이프라인 관리 테이블은 직접 불가 | SDP의 Streaming Table, MV에는 UniForm(Iceberg v2) 속성을 직접 설정할 수 없음 → **Delta Sink로 일반 Delta 테이블에 쓴 뒤 UniForm 활성화** |
| 컬럼 매핑은 영구적 | `columnMapping.mode = 'name'`은 한 번 켜면 끌 수 없음 |
| 런타임 요구 | UniForm 테이블에 쓰는 Databricks 클라이언트는 DBR 14.3 LTS 이상 |
| 이름으로 접근 | Iceberg 메타데이터 생성은 테이블을 경로가 아닌 **이름**으로 접근할 때만 트리거 |

> 최신 런타임(17.3 이상)에서는 파이프라인 Streaming Table과 MV를 외부 Iceberg v3 클라이언트에 직접 노출하는 방법도 있다. Iceberg v2 호환이 필요하면 Delta Sink 패턴을 쓴다.

### 9-4. 세 개념의 연결

| 단계 | 문제 | 해결 | 결과 |
|---|---|---|---|
| Multiplex | 하나의 소스에 여러 이벤트 유형 | 단일 수집 + 유형별 팬아웃 | 도메인별 실버 테이블 |
| Delta Sink | 파이프라인 관리 테이블은 Iceberg 설정 불가 | `dp.create_sink()` + `@dp.append_flow` | 파이프라인 밖의 일반 Delta 테이블 |
| Delta UniForm | 외부 도구가 Iceberg로 읽어야 함 | 일반 Delta 테이블에 Iceberg 메타데이터 자동 생성 | 모든 플랫폼에서 읽을 수 있는 테이블 |

---

## 10. CDC와 SCD 복습

### 10-1. CDC(Change Data Capture)

데이터베이스, 레이크하우스, 웨어하우스 같은 소스의 **INSERT, UPDATE, DELETE** 변경을 추적, 캡처해서 대상 테이블에 적용하는 기법이다. 대상이 소스의 최신 상태(또는 이력)를 반영하게 한다. SDP에서 변경 사항의 소스는 스트리밍 소스여야 한다.

### 10-2. SCD Type 1 vs Type 2

예: 대상 customers(Peter: 1 Blue Rd., Samarth: 22 Front St)에 다음 변경이 들어온다.

| CustomerID | Operation | 내용 | ProcessDate |
|---|---|---|---|
| 1 | UPDATE | Peter → 99 Sunny St. | 5/15 |
| 1 | UPDATE | Peter → 123 Main St. | 5/20 |
| 2 | DELETE | Samarth 계정 삭제 | 5/20 |
| 3 | NEW | Kostas 신규 가입 | 5/20 |

**SCD Type 1 결과** (덮어쓰기, 이력 없음):

| CustomerID | Name | Address | ProcessDate |
|---|---|---|---|
| 1 | Peter | 123 Main St. | 5/20 |
| 3 | Kostas | 100 Athens Dr. | 5/20 |

- 같은 키의 여러 변경 중 **시퀀스상 가장 최신 값만** 적용된다.
- DELETE는 행을 실제로 제거한다.

**SCD Type 2 결과** (이력 보존):

| CustomerID | Name | Address | __START_AT | __END_AT |
|---|---|---|---|---|
| 1 | Peter | 123 Main St. | 5/20 | NULL |
| 1 | Peter | 1 Blue Rd. | 5/1 | 5/20 |
| 2 | Samarth | 22 Front St | 5/1 | 5/20 |
| 3 | Kostas | 100 Athens Dr. | 5/20 | NULL |

- `__START_AT`: 이 버전이 유효해진 시점, `__END_AT`: 비활성화된 시점. **`__END_AT IS NULL`이면 현재 행**이다.
- 변경 시 이전 버전을 닫고 새 버전을 추가한다.
- DELETE는 **소프트 삭제**: 행은 남고 `__END_AT`만 채워진다. 새 행은 생기지 않는다.

| 선택 기준 | SCD Type 1 | SCD Type 2 |
|---|---|---|
| 필요한 것 | 현재 상태만 | 이력 분석, 특정 시점 보고 |
| 저장 효율 | 높음 | 낮음 (버전 누적) |
| 감사 추적 | 없음 | 있음 |

---

## 11. AUTO CDC INTO

### 11-1. 왜 AUTO CDC인가

직접 작성한 `MERGE INTO` CDC 로직은 취약하다. INSERT, UPDATE, DELETE, 중복 이벤트, 재처리, **순서가 뒤바뀌어 늦게 도착한 변경**, 매칭 조건을 모두 직접 처리해야 하고, 규칙이 바뀔수록 유지 보수가 어려워진다. `AUTO CDC INTO`(이전 이름 `APPLY CHANGES INTO`)는 이를 **선언적으로** 처리한다.

### 11-2. 구문

```sql
-- 1) 대상 Streaming Table을 먼저 만들고
CREATE OR REFRESH STREAMING TABLE customers;

-- 2) AUTO CDC flow로 변경 적용
CREATE FLOW scd_type_1_flow AS
AUTO CDC INTO customers
FROM STREAM updates
KEYS (CustomerID)
APPLY AS DELETE WHEN operation = "DELETE"
SEQUENCE BY ProcessDate
COLUMNS * EXCEPT (operation)
STORED AS SCD TYPE 1;
```

| 절 | 의미 |
|---|---|
| `AUTO CDC INTO <대상>` | 변경을 적용할 대상 테이블 |
| `FROM STREAM <소스>` | CDC 이벤트가 담긴 스트리밍 소스 |
| `KEYS (...)` | 소스와 대상 행을 매칭할 키. **복합 키 가능** (예: `KEYS (CustomerID, OrderID)`) |
| `APPLY AS DELETE WHEN <조건>` | 어떤 이벤트를 upsert가 아닌 DELETE로 처리할지 |
| `APPLY AS TRUNCATE WHEN <조건>` | 테이블 전체 비우기 이벤트 조건 (SCD Type 1에서만) |
| `SEQUENCE BY <컬럼>` | 이벤트의 논리적 순서. 늦게 도착한 변경도 올바른 순서로 적용 |
| `COLUMNS * EXCEPT (...)` / `COLUMNS (...)` | 대상에 포함할 컬럼 |
| `STORED AS SCD TYPE 1 / 2` | 저장 방식. **기본은 Type 1** |
| `TRACK HISTORY ON (...)` | SCD2에서 특정 컬럼이 바뀔 때만 새 버전 생성 (`* EXCEPT (...)`도 가능) |

- **INSERT와 UPDATE는 KEYS로 암묵적으로 처리**된다(키가 있으면 업데이트, 없으면 삽입). 별도 코드가 필요 없다.
- DELETE 이벤트의 비키 컬럼은 NULL이어도 된다. **키 컬럼은 반드시 값이 있어야** 삭제할 행을 찾을 수 있다.
- 데이터베이스 **스냅샷**을 비교해 변경을 추출하려면 `AUTO CDC FROM SNAPSHOT`(Python `dp.create_auto_cdc_from_snapshot_flow`)을 쓴다.

Python 형태:

```python
from pyspark import pipelines as dp
from pyspark.sql.functions import expr, col

dp.create_streaming_table("customers_silver")

dp.create_auto_cdc_flow(
    target="customers_silver",
    source="customers_bronze_clean",
    keys=["customer_id"],
    sequence_by=col("timestamp_datetime"),
    apply_as_deletes=expr("operation = 'DELETE'"),
    except_column_list=["operation", "timestamp", "_rescued_data"],
    stored_as_scd_type=2,
)
```

---

## 12. SCD Type 2 파이프라인 구현 예제

### 12-1. 전체 구조

JSON 고객 CDC 파일 → **브론즈 raw** (Auto Loader) → **브론즈 clean** (품질 규칙) → **실버 SCD2** (`AUTO CDC INTO`) → **골드 MV** (현재 고객 / 삭제된 고객)

소스 데이터 특징:

- `operation` 컬럼: `NEW`, `UPDATE`, `DELETE`
- `timestamp` 컬럼: 이벤트 순서를 나타내는 UNIX 타임스탬프
- **DELETE 이벤트는 키(`customer_id`) 외 모든 컬럼이 NULL**

### 12-2. 브론즈 raw

```sql
CREATE OR REFRESH STREAMING TABLE sdp_cdc_1_bronze.customers_bronze_raw
COMMENT "Raw data from customers CDC feed"
AS SELECT
  *,
  current_timestamp()  AS processing_time,
  _metadata.file_name  AS source_file
FROM STREAM read_files("${source}", format => "json");
```

### 12-3. 브론즈 clean: CDC 피드용 조건부 품질 규칙

DELETE 이벤트는 비키 컬럼이 NULL이므로, 규칙에 `OR operation = "DELETE"`를 붙여 **DELETE는 검사에서 제외**한다.

| 제약 | 규칙 | 동작 | 목적 |
|---|---|---|---|
| `valid_id` | `customer_id IS NOT NULL` | FAIL UPDATE | 키가 없으면 수동 조치가 필요하므로 업데이트 실패 |
| `valid_operation` | `operation IS NOT NULL` | DROP ROW | 변경 유형이 없는 행 제거 |
| `valid_name` | `name IS NOT NULL OR operation = "DELETE"` | WARN | DELETE가 아닌데 이름이 없으면 표시 |
| `valid_address` | 주소 4개 필드 모두 NOT NULL, 또는 DELETE | WARN | INSERT/UPDATE의 주소 완전성 |
| `valid_email` | 이메일 정규식 일치, 또는 DELETE | DROP ROW | 잘못된 형식 제거 |

```sql
CREATE OR REFRESH STREAMING TABLE sdp_cdc_1_bronze.customers_bronze_clean
(
  CONSTRAINT valid_id        EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_operation EXPECT (operation IS NOT NULL)   ON VIOLATION DROP ROW,
  CONSTRAINT valid_name      EXPECT (name IS NOT NULL OR operation = "DELETE"),
  CONSTRAINT valid_address   EXPECT (
    (address IS NOT NULL AND city IS NOT NULL AND state IS NOT NULL AND zip_code IS NOT NULL)
    OR operation = "DELETE"),
  CONSTRAINT valid_email     EXPECT (
    rlike(email, '^([a-zA-Z0-9_\\-\\.]+)@([a-zA-Z0-9_\\-\\.]+)\\.([a-zA-Z]{2,5})$')
    OR operation = "DELETE") ON VIOLATION DROP ROW
)
COMMENT "Clean raw bronze data and apply quality constraints"
AS SELECT
  *,
  CAST(from_unixtime(timestamp) AS TIMESTAMP) AS timestamp_datetime   -- UNIX 시간 → TIMESTAMP
FROM STREAM sdp_cdc_1_bronze.customers_bronze_raw;
```

### 12-4. 실버: AUTO CDC로 SCD Type 2

```sql
CREATE OR REFRESH STREAMING TABLE sdp_cdc_2_silver.customers_silver_scd2
COMMENT 'SCD Type 2 Historical Customer Data';

CREATE FLOW customers_scd_type_2_flow AS
AUTO CDC INTO sdp_cdc_2_silver.customers_silver_scd2
FROM STREAM sdp_cdc_1_bronze.customers_bronze_clean
KEYS (customer_id)
APPLY AS DELETE WHEN operation = "DELETE"
SEQUENCE BY timestamp_datetime
COLUMNS * EXCEPT (timestamp, _rescued_data, operation)
STORED AS SCD TYPE 2;
```

`__START_AT`, `__END_AT`은 `SEQUENCE BY` 컬럼 값을 기준으로 자동 관리된다.

### 12-5. 골드: 현재 고객과 삭제된 고객

```sql
-- 현재(활성) 고객
CREATE OR REFRESH MATERIALIZED VIEW sdp_cdc_3_gold.current_customers
COMMENT "Current updated list of active customers"
AS SELECT * EXCEPT (processing_time), current_timestamp() AS updated_at
FROM sdp_cdc_2_silver.customers_silver_scd2
WHERE __END_AT IS NULL;

-- 삭제된 고객: 고객별 최신 버전의 __END_AT이 NULL이 아닌 경우
CREATE OR REFRESH MATERIALIZED VIEW sdp_cdc_3_gold.removed_customers
AS SELECT
  customer_id,
  MAX_BY(name, __START_AT)       AS name,
  MAX_BY(address, __START_AT)    AS address,
  MAX_BY(__START_AT, __START_AT) AS __START_AT,
  MAX_BY(__END_AT, __START_AT)   AS __END_AT
FROM sdp_cdc_2_silver.customers_silver_scd2
GROUP BY customer_id
HAVING MAX_BY(__END_AT, __START_AT) IS NOT NULL;
```

- `MAX_BY(값, 기준)`: 기준 컬럼이 최대인 행의 값을 반환한다. 여기서는 고객별 **가장 최근 버전**의 값을 고른다.
- 업데이트된 고객은 이전 버전의 `__END_AT`이 채워져 있지만 최신 버전은 NULL이므로 제외된다. 최신 버전까지 닫힌 고객만 삭제된 고객이다.
- 윈도우 함수 대신 `GROUP BY` 집계를 써서 MV가 증분 갱신되기 쉽다.

> 개발 중 파이프라인 코드를 바꿔 기존 데이터를 다시 만들어야 할 때는 **전체 갱신(full refresh)** 으로 실행한다. 일반 실행은 이미 처리한 데이터를 다시 처리하지 않으므로 Streaming Table에 0행이 처리된 것으로 보인다.

### 12-6. 실행 결과 해석

**1회차** (00.json: 939명 모두 신규)

| 테이블 | 결과 |
|---|---|
| 실버 SCD2 | 939행 upsert, 모두 `__END_AT = NULL` |
| current_customers | 939 |
| removed_customers | 0 |

**2회차** (01.json: 23행 = UPDATE 12, DELETE 1, NEW 10)

| 항목 | 계산 |
|---|---|
| 실버 upsert 수 | UPDATE 12건 × 2 (이전 버전 닫기 + 새 버전 추가) = 24, DELETE 1 (`__END_AT` 설정), NEW 10 → **35** |
| 실버 총 행 수 | 939 + 12(새 버전) + 10(신규) = **961** |
| `__END_AT IS NOT NULL` 행 | 12(닫힌 이전 버전) + 1(삭제) = **13** |
| current_customers | 939 + 10 - 1 = **948** |
| removed_customers | **1** |

---

## 13. 고급 데이터 품질 Expectations

### 13-1. 기본 NOT NULL 검사의 한계

값이 있다고 해서 올바른 것은 아니다.

| 문제 | 예 | NOT NULL로 잡히는가 |
|---|---|---|
| 숫자 이상 | 음수 수량 | 아니오 |
| 시간 불일치 | 이벤트 날짜가 1970년 (시스템 기본값) | 아니오 |
| 범위 위반 | 할인율 120% | 아니오 |
| 선택 필드 규칙 | NULL일 수 있지만 값이 있으면 0 이상이어야 함 | 아니오 |
| 스키마 진화 | 스트림 중간에 새 컬럼이 추가되어 기존 규칙이 깨짐 | 아니오 |
| 데이터 손실 | 잘못된 레코드가 영구 삭제되고 감사 추적이 없음 | 아니오 |

예: `discount_rate = 120`은 모든 NOT NULL 검사를 통과하지만 불가능한 값이며 하위 매출 계산을 조용히 망친다. `discount_rate BETWEEN 0 AND 100` 같은 **범위 제약**만이 잡을 수 있다.

### 13-2. NULL을 허용하는 제약 작성

SQL에서 `NULL >= 0`은 TRUE가 아니라 NULL이므로, 단순 범위 검사는 **모든 NULL을 위반으로** 처리한다.

```sql
-- 단순 방식: NULL 레코드가 모두 위반으로 집계됨
CONSTRAINT valid_discount EXPECT (discount_rate >= 0 AND discount_rate <= 100)

-- NULL 허용 방식: 값이 있을 때만 검증
CONSTRAINT valid_discount EXPECT (
  CASE WHEN discount_rate IS NOT NULL
       THEN discount_rate >= 0 AND discount_rate <= 100
       ELSE TRUE
  END
)

-- 같은 의미의 짧은 형태
CONSTRAINT valid_discount EXPECT (discount_rate IS NULL OR discount_rate BETWEEN 0 AND 100)
```

> **경험칙**: 값이 없을 수 있는 컬럼(선택 필드, 파이프라인 시작 후 추가된 컬럼)에 대한 제약은 항상 NULL 허용 형태로 감싼다. 그렇지 않으면 스키마 진화 이전의 과거 레코드가 모두 위반으로 잡혀 UI가 오탐으로 넘친다.

또한 **CONSTRAINT 하나에는 논리 규칙 하나만** 둔다. 그래야 UI에서 제약별 메트릭을 정확히 볼 수 있다.

### 13-3. 테이블 수준 검증 패턴

행 단위 검사로는 잡을 수 없는 문제를, 검증용 Materialized View에 expectation을 걸어 잡는다.

```sql
-- 1) 행 수 검증: 조인, 집계, 팬아웃 후 레코드가 사라지지 않았는지
CREATE OR REFRESH MATERIALIZED VIEW count_verification (
  CONSTRAINT no_rows_dropped EXPECT (a_count = b_count) ON VIOLATION FAIL UPDATE
)
AS SELECT * FROM
  (SELECT COUNT(*) AS a_count FROM table_a),
  (SELECT COUNT(*) AS b_count FROM table_b);

-- 2) 누락 레코드 감지: 검증 사본에는 있는데 보고서 테이블에는 없는 키
CREATE OR REFRESH MATERIALIZED VIEW report_compare_tests (
  CONSTRAINT no_missing_records EXPECT (r_key IS NOT NULL) ON VIOLATION FAIL UPDATE
)
AS SELECT v.*, r.key AS r_key
FROM validation_copy v
LEFT OUTER JOIN report r ON v.key = r.key;

-- 3) 기본 키 고유성: 키별로 정확히 한 행인지
CREATE OR REFRESH MATERIALIZED VIEW report_pk_tests (
  CONSTRAINT unique_pk EXPECT (num_entries = 1) ON VIOLATION FAIL UPDATE
)
AS SELECT pk, COUNT(*) AS num_entries
FROM report
GROUP BY pk;
```

> 이런 검증 MV는 테스트 전용 테이블을 만들어 결과를 확인하는 방식이다. 운영 테이블을 추가로 만들므로 필요한 범위만 두고, 개발/스테이징 환경에서만 생성하는 방식도 흔하다.

---

## 14. 스키마 진화에 강한 브론즈 설계

### 14-1. 모든 것을 STRING으로 수집

가장 견고한 브론즈는 **타입 불일치로 레코드를 거부하지 않는다.** 모든 필드를 STRING으로 받고, 타입 적용은 실버에서 `TRY_CAST`로 한다.

| 레이어 | 전략 |
|---|---|
| 브론즈 | 모든 비즈니스 필드를 STRING으로. 어떤 값이 와도 수용 |
| 실버 | `TRY_CAST`로 타입 변환 (실패하면 NULL) → NULL 허용 제약이 나머지를 처리 |

### 14-2. 스키마 진화 도구

| 도구 | 역할 |
|---|---|
| **`schemaHints`** | 아직 파일에 없는, 앞으로 올 컬럼을 미리 선언. 새 컬럼이 나타나면 자동으로 채워지고, 이전 레코드는 NULL |
| **`_rescued_data`** | 선언된 스키마에 맞지 않는 데이터(예상치 못한 컬럼, 타입 불일치)를 JSON으로 보관하는 최후의 방어선. 데이터가 조용히 버려지지 않음 |
| **`schemaEvolutionMode`** | 새 컬럼이 나타날 때의 동작. `addNewColumns`(기본, 스트림이 멈춘 뒤 새 스키마로 재시작), `rescue`(스키마 변경 없이 `_rescued_data`로), `failOnNewColumns`, `none` |

```sql
CREATE OR REFRESH STREAMING TABLE dq_1_bronze.sales_bronze_raw
AS SELECT
  CAST(order_id      AS STRING) AS order_id,
  CAST(customer_id   AS STRING) AS customer_id,
  CAST(discount_pct  AS STRING) AS discount_pct,
  CAST(order_date    AS STRING) AS order_date,
  -- 아직 첫 파일에는 없는 컬럼 (스키마 힌트로 미리 선언)
  CAST(order_status  AS STRING) AS order_status,
  CAST(shipping_cost AS STRING) AS shipping_cost,
  CAST(_rescued_data AS STRING) AS _rescued_data,
  _metadata.file_name              AS source_file,
  _metadata.file_modification_time AS file_mod_time
FROM STREAM read_files(
  '${source}',
  format => 'csv',
  schemaHints => 'order_status STRING, shipping_cost STRING'
);
```

```sql
-- 구조 밖으로 들어온 필드 조사
SELECT order_id, _rescued_data:unexpected_field AS unexpected_field
FROM dq_1_bronze.sales_bronze_raw
WHERE _rescued_data IS NOT NULL;
```

> **중요한 상호작용**: 스키마 진화로 컬럼이 추가되면, 그 이전에 수집된 레코드는 해당 컬럼이 NULL이다. 그 컬럼에 대한 제약은 반드시 NULL 허용 형태(`CASE WHEN ... IS NOT NULL ... ELSE TRUE END`)여야 한다.

결과적으로 파이프라인 코드를 바꾸지 않고도 소스 컬럼이 16개에서 18개로 늘어나는 변화를 흡수하고, 새 컬럼에 대한 품질 규칙도 컬럼이 나타나는 순간부터 자동 적용된다.

---

## 15. 격리(Quarantine) 패턴

### 15-1. 개념

DROP ROW는 잘못된 레코드를 **영구 삭제**해 복구할 수 없다. 격리 패턴은 모든 레코드를 품질 평가 후 두 경로로 나눈다.

- 모든 기대치를 통과 → `is_quarantined = FALSE` → **정상 레코드** (분석용)
- 하나라도 실패 → `is_quarantined = TRUE`, `quarantine_reason` = 실패한 규칙 목록 → **격리 레코드** (조사, 재처리용)

**핵심 보장**: 레코드는 절대 사라지지 않는다. `전체 입력 = 정상 + 격리`.

### 15-2. 역논리(Inverse Logic)

`is_quarantined`는 **모든 규칙을 AND로 묶어 NOT** 한 값이다. 규칙 하나라도 실패하면 TRUE가 된다.

```sql
NOT (rule1 AND rule2 AND rule3) AS is_quarantined
```

### 15-3. 구현

```sql
-- 1단계: 품질 추적 테이블 (모든 제약은 WARN, is_quarantined로 파티셔닝)
CREATE OR REFRESH STREAMING TABLE dq_2_silver.sales_silver_dq
(
  order_id      STRING,
  customer_id   STRING,
  sku           STRING,
  discount_pct  DOUBLE,
  order_date    DATE,
  shipping_cost DOUBLE,
  source_file   STRING,
  is_quarantined    BOOLEAN,
  quarantine_reason STRING,
  CONSTRAINT check_customer_id    EXPECT (customer_id IS NOT NULL),
  CONSTRAINT check_sku            EXPECT (sku IS NOT NULL),
  CONSTRAINT valid_discount_range EXPECT (discount_pct IS NULL OR discount_pct BETWEEN 0 AND 100),
  CONSTRAINT valid_date_range     EXPECT (order_date IS NULL OR
                                          order_date BETWEEN DATE_SUB(DATE '2026-01-01', 1460) AND DATE '2026-01-01'),
  CONSTRAINT valid_shipping_cost  EXPECT (shipping_cost IS NULL OR (shipping_cost > 0 AND shipping_cost < 100))
)
COMMENT "격리 패턴 품질 추적 테이블"
PARTITIONED BY (is_quarantined);

-- 2단계: 역논리로 플래그와 사유를 계산해 채우는 flow
CREATE FLOW apply_inverse_logic_flow
AS INSERT INTO dq_2_silver.sales_silver_dq BY NAME
SELECT
  order_id, customer_id, sku, discount_pct, order_date, shipping_cost, source_file,
  NOT (
    customer_id IS NOT NULL AND
    sku IS NOT NULL AND
    (discount_pct IS NULL OR discount_pct BETWEEN 0 AND 100) AND
    (order_date IS NULL OR order_date BETWEEN DATE_SUB(DATE '2026-01-01', 1460) AND DATE '2026-01-01') AND
    (shipping_cost IS NULL OR (shipping_cost > 0 AND shipping_cost < 100))
  ) AS is_quarantined,
  CONCAT_WS('; ',
    CASE WHEN customer_id IS NULL THEN 'Missing customer_id' END,
    CASE WHEN sku IS NULL THEN 'Missing sku' END,
    CASE WHEN discount_pct NOT BETWEEN 0 AND 100 THEN 'Invalid discount_pct' END,
    CASE WHEN order_date < DATE_SUB(DATE '2026-01-01', 1460) OR order_date > DATE '2026-01-01'
         THEN 'Invalid order_date' END,
    CASE WHEN shipping_cost <= 0 OR shipping_cost >= 100 THEN 'Invalid shipping_cost' END
  ) AS quarantine_reason
FROM STREAM dq_1_bronze.sales_bronze_clean;

-- 3단계: 정상 경로와 격리 경로 분리
CREATE OR REFRESH STREAMING TABLE dq_2_silver.sales_silver_valid
AS SELECT * EXCEPT (is_quarantined, quarantine_reason)
FROM STREAM dq_2_silver.sales_silver_dq
WHERE is_quarantined = FALSE;

CREATE OR REFRESH STREAMING TABLE dq_2_silver.sales_silver_quarantined
AS SELECT *
FROM STREAM dq_2_silver.sales_silver_dq
WHERE is_quarantined = TRUE;
```

- `CONCAT_WS`는 NULL 인자를 건너뛰므로, 실패한 규칙의 메시지만 `;`로 이어진다. 모두 통과하면 빈 문자열이다.
- 분석은 정상 테이블만 읽고, 수정 작업은 격리 테이블을 대상으로 한다. 규칙을 고친 뒤 격리 레코드를 다시 처리할 수 있다.
- 날짜 기준을 고정값(`DATE '2026-01-01'`)으로 둔 것은 정적 예제 데이터 때문이다. 실무에서는 `current_date()` 기준으로 쓴다.

> **품질 추적 테이블에는 반드시 WARN만 쓴다.** DROP ROW나 FAIL UPDATE를 쓰면 격리 플래그가 계산되기도 전에 잘못된 레코드가 제거되거나 파이프라인이 멈춰 무손실 보장이 깨진다. WARN을 쓰는 유일한 이유는 UI에 **제약별 위반 메트릭**을 보여 주기 위해서이고, 실제 라우팅은 역논리 컬럼이 한다.

> **`is_quarantined`로 파티셔닝하는 이유**: 정상과 격리 레코드가 물리적으로 분리되어, `WHERE is_quarantined = FALSE/TRUE`로 읽는 하위 테이블이 **파티션 프루닝**으로 해당 파티션만 읽는다. 값이 두 개뿐인 저카디널리티 컬럼이라 파티셔닝에 적합하다.

### 15-4. 실행 결과 예

| 테이블 | 1회차 (정상 157건) | 2회차 (+60건, 새 컬럼 2개, 의도적 오류) | 누적 |
|---|---|---|---|
| 브론즈 raw | 157 | +60 | 217 |
| 품질 추적 테이블 | 157 | +60 | 217 |
| 정상 | 157 | +51 | 208 |
| 격리 | 0 | +9 | 9 |

무손실 검증: 217 = 208 + 9. 품질 점수는 sales_1 100%, sales_2 85%(51/60), 전체 95.85%(208/217)다.

```sql
SELECT
  (SELECT COUNT(*) FROM dq_1_bronze.sales_bronze_raw)        AS total_ingested,
  (SELECT COUNT(*) FROM dq_2_silver.sales_silver_valid)       AS total_valid,
  (SELECT COUNT(*) FROM dq_2_silver.sales_silver_quarantined) AS total_quarantined;
```

### 15-5. DROP ROW vs 격리 패턴

| 항목 | DROP ROW | 격리 패턴 |
|---|---|---|
| 잘못된 레코드 | 영구 삭제 | 격리 테이블에 보존 |
| 감사 추적 | 없음 | 쿼리 가능 |
| 데이터 복구 | 불가능 | 규칙 수정 후 격리에서 재처리 |
| UI 위반 메트릭 | 표시 | WARN으로 제약별 표시 |
| 읽기 성능 | 정제 테이블 전체 스캔 | `is_quarantined` 파티션 프루닝 |
| 복잡도 | 낮음 (테이블 하나) | 보통 (추적 테이블 + 분리 테이블 2개) |
| 적합한 경우 | 규칙이 확립된 비중요 스트림 | 규정 준수, 감사, 수정이 필요한 운영 파이프라인 |

---

## 16. 스트림-정적 조인과 종합 예제

### 16-1. 스트림-정적 조인

스트리밍 테이블(사실 데이터)을 정적 참조 테이블(차원 데이터)과 조인해 보강한다.

- **스트리밍 쪽**: 새로 들어온 행만 처리된다.
- **정적 쪽**: 마이크로배치(업데이트)마다 **그 시점의 최신 스냅샷**을 읽는다.
- 주의: 정적 쪽이 나중에 바뀌어도 **이미 처리된 스트리밍 행은 다시 계산되지 않는다.** 과거 행까지 최신 참조 정보로 반영하려면 MV로 조인하거나 전체 갱신이 필요하다.
- `LEFT JOIN`을 쓰면 참조 테이블에 없는 키의 행도 버려지지 않는다.

### 16-2. 종합 예제: 웹 + 앱 주문 파이프라인

두 채널(웹 CSV, 앱 JSON)의 주문을 하나의 파이프라인으로 처리해 매출 보고서를 만든다.

| 컬럼 | 웹 CSV | 앱 JSON | 브론즈 처리 |
|---|---|---|---|
| `order_id`, `customer_id`, `sku` 등 공통 컬럼 | 있음 | 있음 | 두 flow 모두 |
| `discount_code` | 일부 파일에 없을 수 있음 | 일부 파일에 없을 수 있음 | `schemaHints`로 선언 |
| `browser`, `session_id` | 있음 | 없음 | 웹 전용, 앱 행은 NULL |
| `app_version`, `device_model` | 없음 | 있음 | 앱 전용, 웹 행은 NULL |

**브론즈: Multi-Flow**

```sql
CREATE OR REFRESH STREAMING TABLE lab_1_bronze.combined_orders_raw
(
  order_id STRING, order_date STRING, customer_id STRING, sku STRING, order_type STRING,
  quantity STRING, unit_price STRING, discount_code STRING, discount_amount STRING,
  total_amount STRING, region STRING, ship_date STRING,
  browser STRING, session_id STRING,          -- 웹 전용
  app_version STRING, device_model STRING,    -- 앱 전용
  source_file STRING, file_mod_time TIMESTAMP, ingestion_time TIMESTAMP
)
COMMENT "웹과 앱 주문을 multi-flow로 통합한 브론즈"
TBLPROPERTIES ('pipelines.reset.allowed' = false);

CREATE FLOW app_orders_flow
AS INSERT INTO lab_1_bronze.combined_orders_raw BY NAME
SELECT
  CAST(order_id AS STRING) AS order_id,
  -- ... 공통 컬럼 STRING 캐스팅 ...
  CAST(app_version  AS STRING) AS app_version,
  CAST(device_model AS STRING) AS device_model,
  _metadata.file_name              AS source_file,
  _metadata.file_modification_time AS file_mod_time,
  current_timestamp()              AS ingestion_time
FROM STREAM read_files('${app_orders_source}', format => 'json', schemaHints => 'discount_code STRING');

CREATE FLOW web_orders_flow
AS INSERT INTO lab_1_bronze.combined_orders_raw BY NAME
SELECT
  CAST(order_id AS STRING) AS order_id,
  -- ... 공통 컬럼 STRING 캐스팅 ...
  CAST(browser    AS STRING) AS browser,
  CAST(session_id AS STRING) AS session_id,
  _metadata.file_name              AS source_file,
  _metadata.file_modification_time AS file_mod_time,
  current_timestamp()              AS ingestion_time
FROM STREAM read_files('${web_orders_source}', format => 'csv', header => true,
                       schemaHints => 'discount_code STRING');
```

`BY NAME` 삽입이므로 한쪽 flow에 없는 컬럼(웹 flow의 `app_version` 등)은 자동으로 NULL이 된다.

**실버: 품질 규칙 + 타입 변환 + 클러스터링**

| 제약 | 규칙 | 동작 | 이유 |
|---|---|---|---|
| `valid_order_id` | `order_id IS NOT NULL` | FAIL UPDATE | 기본 키. NULL이면 하위 조인이 모두 깨짐 |
| `valid_sku` | `sku IS NOT NULL` | DROP ROW | 제품 참조가 없으면 분석 불가 |
| `positive_quantity` | `quantity >= 1` | DROP ROW | 0 이하 수량은 유효한 판매가 아님 |
| `positive_unit_price` | `unit_price > 0` | WARN | 파이프라인을 막지 않고 가격 이상만 표시 |
| `valid_total_amount` | `total_amount >= 0` | WARN | 음수 합계를 검토용으로 표시 |

```sql
CREATE OR REFRESH STREAMING TABLE lab_2_silver.orders_clean
(
  CONSTRAINT valid_order_id      EXPECT (order_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_sku           EXPECT (sku IS NOT NULL)      ON VIOLATION DROP ROW,
  CONSTRAINT positive_quantity   EXPECT (quantity >= 1)        ON VIOLATION DROP ROW,
  CONSTRAINT positive_unit_price EXPECT (unit_price > 0),
  CONSTRAINT valid_total_amount  EXPECT (total_amount >= 0)
)
COMMENT "타입 변환, 품질 검증, Liquid 클러스터링이 적용된 실버 주문"
CLUSTER BY AUTO
AS SELECT
  order_id,
  TRY_CAST(order_date      AS TIMESTAMP) AS order_date,
  customer_id, sku, order_type, discount_code, region,
  TRY_CAST(quantity        AS INT)       AS quantity,
  TRY_CAST(unit_price      AS DOUBLE)    AS unit_price,
  TRY_CAST(discount_amount AS DOUBLE)    AS discount_amount,
  TRY_CAST(total_amount    AS DOUBLE)    AS total_amount,
  TRY_CAST(ship_date       AS DATE)      AS ship_date,
  source_file
FROM STREAM lab_1_bronze.combined_orders_raw;

-- 정적 참조: 제품 카탈로그를 MV로
CREATE OR REFRESH MATERIALIZED VIEW lab_2_silver.product_catalog_ref
COMMENT "SKU별 브랜드, 카테고리, 정가"
AS SELECT
  sku, brand,
  category                   AS catalog_category,
  CAST(list_price AS DOUBLE) AS list_price,
  CAST(is_active AS BOOLEAN) AS is_active
FROM read_files('${product_catalog_source}', format => 'csv', header => true);

-- 스트림-정적 조인으로 보강
CREATE OR REFRESH STREAMING TABLE lab_2_silver.orders_enriched
CLUSTER BY AUTO
AS SELECT
  o.order_id, DATE(o.order_date) AS order_date, o.customer_id, o.sku, o.order_type,
  o.quantity, o.unit_price, o.discount_code, o.discount_amount, o.total_amount, o.region,
  p.brand, p.catalog_category, p.list_price,
  ROUND(o.unit_price - p.list_price, 2) AS price_vs_catalog,
  p.is_active AS sku_is_active
FROM STREAM lab_2_silver.orders_clean AS o
LEFT JOIN lab_2_silver.product_catalog_ref AS p
  ON o.sku = p.sku;
```

**골드: 매출 MV**

```sql
CREATE OR REFRESH MATERIALIZED VIEW lab_3_gold.revenue_by_category
COMMENT "일별, 카테고리별, 채널별 매출"
AS SELECT
  order_date,
  catalog_category AS category,
  brand,
  order_type       AS channel,
  region,
  COUNT(DISTINCT order_id)                              AS total_orders,
  SUM(quantity)                                         AS units_sold,
  ROUND(SUM(total_amount), 2)                           AS gross_revenue,
  ROUND(SUM(discount_amount), 2)                        AS total_discounts,
  ROUND(SUM(total_amount) - SUM(discount_amount), 2)    AS net_revenue,
  ROUND(AVG(total_amount), 2)                           AS avg_order_value,
  COUNT(CASE WHEN discount_code IS NOT NULL THEN 1 END) AS discounted_orders
FROM lab_2_silver.orders_enriched
WHERE order_date IS NOT NULL AND catalog_category IS NOT NULL
GROUP BY order_date, catalog_category, brand, order_type, region;
```

**품질 영향 확인**: 브론즈와 실버의 차이가 DROP된 레코드다.

```sql
SELECT source_file, order_id, sku, quantity FROM lab_1_bronze.combined_orders_raw
EXCEPT
SELECT source_file, order_id, sku, quantity FROM lab_2_silver.orders_clean;

SELECT
  (SELECT COUNT(*) FROM lab_1_bronze.combined_orders_raw) AS bronze_rows,
  (SELECT COUNT(*) FROM lab_2_silver.orders_clean)        AS silver_rows;
```

예: 1회차 200행 중 8행(수량 위반)이 DROP되어 192행, 2회차 200행 중 9행(수량 8, SKU 누락 1)이 DROP되어 191행이 실버로 간다.

---

## 17. 핵심 정리 치트시트

### Flow와 Multi-Flow

| 키워드 | 핵심 |
|---|---|
| Flow | 쿼리 + 대상. 독립 체크포인트, 독립 실패 |
| Flow 이름 | = 체크포인트 위치. 바꾸면 처음부터 재처리 |
| 증분 / 전체 갱신 | 새 레코드만 / 체크포인트 삭제 후 전체 재처리 |
| Default / Explicit flow | 테이블 정의 시 자동 생성 / `CREATE FLOW ... AS INSERT INTO ... BY NAME` |
| Multi-Flow | 여러 flow → 하나의 테이블. 새 소스는 그 flow만 백필 |
| UNION 대비 | 체크포인트 공유, 새 소스 시 전체 갱신 필요 → Multi-Flow 권장 |
| Python | `dp.create_streaming_table()` + `@dp.append_flow(target=...)` |
| 브론즈 보호 | `pipelines.reset.allowed = false` |
| 파라미터 | 파이프라인 설정에 키-값, SQL에서 `${키}`, Python에서 `spark.conf.get()` |

### 품질과 레이아웃

| 키워드 | 핵심 |
|---|---|
| WARN / DROP ROW / FAIL UPDATE | 유지+기록 / 제거+기록 / flow 실패 |
| Expectations 위치 | 대상 테이블에만. flow에는 정의 불가 |
| 모니터링 | 파이프라인 UI, `event_log()`의 `flow_progress` |
| Liquid 클러스터링 | 증분, 키 변경 자유, `CLUSTER BY AUTO` 또는 `CLUSTER BY (cols)`, OPTIMIZE 불필요 |
| AUTO | 쿼리 관찰 후 비용-편익 판단으로 키 선택, `clusterByAuto=true`, Predictive Optimization 필요 |
| TRY_CAST | 변환 실패 시 NULL |

### Multiplex, Sink, UniForm

| 키워드 | 핵심 |
|---|---|
| Multiplex | 한 번 수집(체크포인트 1개) → 유형 필드로 팬아웃 |
| VARIANT | `parse_json()`, `v:field::TYPE` |
| Sink | 파이프라인 밖 대상 (Delta, Kafka, Event Hubs, 사용자 정의). Python 전용, append 전용, expectations 없음 |
| Sink 구현 | `dp.create_sink(name, format, options)` + `@dp.append_flow(target=sink)` |
| UniForm 속성 | `enableDeletionVectors=false`, `columnMapping.mode=name`, `enableIcebergCompatV2=true`, `universalFormat.enabledFormats=iceberg` |
| UniForm 제약 | Iceberg 쪽은 읽기 전용, 파이프라인 관리 테이블에 직접 불가 → Delta Sink 경유, 컬럼 매핑 영구 |

### CDC와 고급 품질

| 키워드 | 핵심 |
|---|---|
| SCD1 / SCD2 | 덮어쓰기, DELETE는 실제 삭제 / 이력 보존, DELETE는 `__END_AT` 설정 |
| `__END_AT IS NULL` | 현재 행 |
| AUTO CDC 절 | `KEYS`, `APPLY AS DELETE WHEN`, `SEQUENCE BY`, `COLUMNS * EXCEPT`, `STORED AS SCD TYPE 1/2` (기본 1), `TRACK HISTORY ON` |
| CDC 품질 규칙 | DELETE는 비키 NULL → `... OR operation = 'DELETE'` |
| 삭제 고객 조회 | `GROUP BY key HAVING MAX_BY(__END_AT, __START_AT) IS NOT NULL` |
| NULL 허용 제약 | `CASE WHEN col IS NOT NULL THEN ... ELSE TRUE END` 또는 `col IS NULL OR ...` |
| 테이블 수준 검증 | 행 수 비교, LEFT JOIN 누락 감지, GROUP BY 키 고유성 MV + FAIL UPDATE |
| 스키마 진화 | 브론즈 STRING, `schemaHints`, `_rescued_data`, `schemaEvolutionMode` |
| 격리 패턴 | 추적 테이블은 WARN만, `NOT(모든 규칙)` = `is_quarantined`, `CONCAT_WS` 사유, `PARTITIONED BY (is_quarantined)`, 입력 = 정상 + 격리 |
| 스트림-정적 조인 | 정적 쪽은 업데이트마다 최신 스냅샷, 과거 행은 재계산 안 됨, LEFT JOIN으로 행 보존 |

---

## 18. 자가 점검 문제

> 답을 가리고 먼저 생각해본 뒤 펼쳐보자.

**Q1.** SDP에서 flow란 무엇이며, 두 가지 처리 모드는?
<details><summary>정답</summary>

쿼리(무엇을 읽고 어떻게 변환할지)와 대상(어디에 쓸지)으로 이루어진 기본 실행 단위다. 증분 모드는 체크포인트 이후의 새 레코드만 처리하고, 전체 갱신 모드는 체크포인트를 지우고 소스 전체를 처음부터 다시 처리해 대상을 덮어쓴다.
</details>

**Q2.** flow 이름을 바꾸면 어떤 일이 생기는가?
<details><summary>정답</summary>

flow 이름이 체크포인트 위치에 매핑되므로 새 체크포인트가 생기고 기존 체크포인트는 버려진다. flow가 처음 실행되는 것처럼 데이터를 처음부터 재처리한다.
</details>

**Q3.** Default flow와 Explicit flow의 차이는?
<details><summary>정답</summary>

Default flow는 Streaming Table이나 MV를 정의할 때 자동으로 생기며 테이블 이름을 쓴다. Explicit flow는 테이블을 먼저 정의한 뒤 `CREATE FLOW ... AS INSERT INTO ... BY NAME`으로 따로 만들어 연결하며, 기존 테이블에 새 소스로 쓰거나 여러 소스를 통합할 때 쓴다.
</details>

**Q4.** 여러 소스를 하나의 테이블로 합칠 때 UNION보다 Multi-Flow가 나은 이유는?
<details><summary>정답</summary>

UNION은 모든 소스가 체크포인트 하나를 공유해 새 소스를 추가하면 전체 갱신이 필요하고, 한 소스 오류가 다른 소스를 막는다. Multi-Flow는 flow마다 독립 체크포인트를 가져 새 소스는 그 flow만 백필하면 되고, 오류가 격리되며 소스별 리니지와 모니터링이 명확하다.
</details>

**Q5.** Multi-Flow로 쓰는 테이블의 품질 규칙은 어디에 정의하는가?
<details><summary>정답</summary>

대상 테이블에 정의한다. Expectations는 `CREATE FLOW`나 `@dp.append_flow` 안에 지정할 수 없으며, 테이블에 쓰는 모든 flow에 테이블의 규칙이 중앙에서 적용된다.
</details>

**Q6.** CSV와 JSON 소스를 한 브론즈 테이블에 모을 때 모든 컬럼을 STRING으로 받는 이유는?
<details><summary>정답</summary>

형식마다 스키마를 따로 추론해 같은 컬럼의 타입이 달라지므로(예: timestamp vs string, int vs bigint) 합치면 스키마 불일치로 수집이 실패한다. 브론즈에서 STRING으로 통일하고 실버에서 TRY_CAST로 올바른 타입을 적용한다.
</details>

**Q7.** `pipelines.reset.allowed = false`는 언제 특히 중요한가?
<details><summary>정답</summary>

소스 스토리지가 일정 기간 후 파일을 자동 삭제하는 경우다. 전체 갱신으로 체크포인트와 테이블이 지워지면 이미 사라진 파일은 다시 수집할 수 없어 데이터를 잃으므로, 브론즈 테이블의 전체 갱신을 막는다.
</details>

**Q8.** WARN, DROP ROW, FAIL UPDATE의 차이는?
<details><summary>정답</summary>

WARN(기본, ON VIOLATION 생략)은 행을 유지하고 위반을 메트릭에 기록한다. DROP ROW는 위반 행을 제거하고 기록한다. FAIL UPDATE는 위반 행이 있으면 해당 flow 업데이트를 실패시킨다.
</details>

**Q9.** Liquid 클러스터링이 Hive 파티셔닝, Z-Order보다 나은 점 세 가지는?
<details><summary>정답</summary>

증분(새 파일만 재구성), 유연성(테이블 재작성 없이 키 변경), 자동 조정(`CLUSTER BY AUTO`로 쿼리 패턴 기반 키 선택).
</details>

**Q10.** `CLUSTER BY AUTO`와 `CLUSTER BY (columns)`는 각각 언제 쓰는가? AUTO의 키는 언제 정해지는가?
<details><summary>정답</summary>

AUTO는 새 테이블이거나 쿼리 패턴을 모를 때, 직접 지정은 자주 필터링하는 컬럼을 확실히 알 때 쓴다. AUTO는 처음엔 키가 없고, 쿼리를 관찰한 뒤 데이터 스키핑 이득이 재구성 비용보다 클 때 키를 고른다(Predictive Optimization 필요).
</details>

**Q11.** Multiplex 패턴이 해결하는 문제와 처리 단계는?
<details><summary>정답</summary>

하나의 전송 수단(Kafka 토픽, 스토리지 경로)에 여러 도메인의 이벤트가 섞여 오는 문제다. 모든 이벤트를 하나의 브론즈에 한 번 수집(체크포인트 1개, 소스 스캔 1번)하고, 유형 필드로 필터링해 도메인별 테이블로 팬아웃한다.
</details>

**Q12.** Multiplex 브론즈에서 VARIANT를 쓰는 이유와 필드 추출 문법은?
<details><summary>정답</summary>

도메인마다 페이로드 구조가 달라 하나의 고정 스키마로 담기 어렵기 때문이다. `parse_json()`으로 VARIANT를 만들고 `event_data:field::TYPE`처럼 경로로 추출해 캐스팅한다.
</details>

**Q13.** Sink란 무엇이며 관리형 Streaming Table과 어떻게 다른가?
<details><summary>정답</summary>

파이프라인 관리 범위 밖의 대상(외부 Delta 테이블, Kafka, Event Hubs, 사용자 정의)에 스트리밍 데이터를 쓰는 메커니즘이다. 관리형 테이블과 달리 Expectations와 AUTO CDC를 지원하지 않고 append 전용이지만, 대상 테이블 속성을 자유롭게 설정할 수 있다.
</details>

**Q14.** Delta Sink를 만드는 방법과 제약은?
<details><summary>정답</summary>

`dp.create_sink(name, format="delta", options={"tableName": ...})`로 등록하고 `@dp.append_flow(target=<sink 이름>)`로 데이터를 쓴다. Python API만 지원하며 SQL로는 만들 수 없고, append flow로만 쓸 수 있다.
</details>

**Q15.** Delta UniForm으로 Iceberg 읽기를 켜기 위한 테이블 속성은?
<details><summary>정답</summary>

`delta.enableDeletionVectors = false`, `delta.columnMapping.mode = 'name'`, `delta.enableIcebergCompatV2 = true`, `delta.universalFormat.enabledFormats = 'iceberg'`.
</details>

**Q16.** SDP Streaming Table을 Iceberg v2 클라이언트에 노출하려면 왜 Delta Sink를 거쳐야 하는가?
<details><summary>정답</summary>

파이프라인이 관리하는 Streaming Table과 MV에는 UniForm(Iceberg v2) 속성을 직접 설정할 수 없기 때문이다. Delta Sink로 일반 Delta 테이블에 쓴 뒤 그 테이블에 UniForm을 활성화한다.
</details>

**Q17.** UniForm의 제한 사항 세 가지는?
<details><summary>정답</summary>

Iceberg 클라이언트는 읽기만 가능하고 쓰기는 Delta로 해야 한다. Iceberg v2 기준 Deletion Vectors를 꺼야 한다(Iceberg v3에서는 불필요). 컬럼 매핑은 한 번 켜면 끌 수 없다. 이 밖에 테이블을 이름으로 접근해야 메타데이터가 생성되고, 쓰기 클라이언트는 DBR 14.3 LTS 이상이어야 한다.
</details>

**Q18.** SCD Type 1과 Type 2에서 DELETE 이벤트는 각각 어떻게 처리되는가?
<details><summary>정답</summary>

Type 1에서는 행이 실제로 삭제된다. Type 2에서는 행이 남고 `__END_AT`에 삭제 시점이 기록되는 소프트 삭제로 처리되며, 새 행은 생기지 않는다.
</details>

**Q19.** AUTO CDC INTO의 주요 절과 각각의 역할은?
<details><summary>정답</summary>

`KEYS`(매칭 키, 복합 가능), `APPLY AS DELETE WHEN`(삭제로 처리할 조건), `SEQUENCE BY`(이벤트 순서), `COLUMNS * EXCEPT`(포함 컬럼), `STORED AS SCD TYPE 1/2`(저장 방식, 기본 1). INSERT와 UPDATE는 키로 암묵적으로 처리된다.
</details>

**Q20.** `SEQUENCE BY`가 필요한 이유는?
<details><summary>정답</summary>

같은 키에 여러 변경이 올 때, 그리고 변경이 도착 순서와 다르게 늦게 들어올 때 논리적 순서대로 적용하기 위해서다. Type 1에서는 시퀀스상 최신 값만 남고, Type 2에서는 `__START_AT`, `__END_AT`이 이 값으로 정해진다.
</details>

**Q21.** CDC 피드의 품질 규칙에 `OR operation = "DELETE"`를 붙이는 이유는?
<details><summary>정답</summary>

DELETE 이벤트는 키 외 모든 컬럼이 NULL이라, 그대로 두면 이름, 주소, 이메일 규칙을 모두 위반해 잘못 경고되거나 DROP된다. DELETE는 검사에서 제외해야 삭제 이벤트가 실버까지 전달된다.
</details>

**Q22.** SCD2 테이블에서 현재 고객과 삭제된 고객을 각각 어떻게 조회하는가?
<details><summary>정답</summary>

현재 고객은 `WHERE __END_AT IS NULL`. 삭제된 고객은 키별 최신 버전의 `__END_AT`이 NULL이 아닌 경우이므로 `GROUP BY customer_id HAVING MAX_BY(__END_AT, __START_AT) IS NOT NULL`로 찾는다(업데이트된 고객은 최신 버전이 열려 있어 제외됨).
</details>

**Q23.** 939명이 있는 SCD2 테이블에 UPDATE 12, DELETE 1, NEW 10이 들어오면 upsert 수, 총 행 수, 현재 고객 수는?
<details><summary>정답</summary>

upsert는 24(업데이트마다 이전 버전 닫기 + 새 버전 추가) + 1 + 10 = 35. 총 행 수는 939 + 12 + 10 = 961. 현재 고객 수는 939 + 10 - 1 = 948.
</details>

**Q24.** 범위 제약을 `discount >= 0 AND discount <= 100`으로 쓰면 어떤 문제가 생기며, 어떻게 고치는가?
<details><summary>정답</summary>

SQL에서 NULL 비교는 TRUE가 아니므로 NULL 값이 모두 위반으로 집계된다. 특히 스키마 진화로 추가된 컬럼은 과거 레코드가 모두 NULL이라 오탐이 넘친다. `CASE WHEN discount IS NOT NULL THEN ... ELSE TRUE END` 또는 `discount IS NULL OR ...` 형태로 값이 있을 때만 검증한다.
</details>

**Q25.** 행 단위 검사로 잡을 수 없는 문제를 잡는 테이블 수준 검증 패턴 세 가지는?
<details><summary>정답</summary>

두 테이블의 행 수 비교(조인/팬아웃 후 누락 확인), LEFT OUTER JOIN으로 검증 사본에는 있는데 보고서에 없는 레코드 감지, 키로 GROUP BY해서 키별 행 수가 1인지 확인하는 기본 키 고유성 검사. 검증용 MV에 FAIL UPDATE expectation을 건다.
</details>

**Q26.** `schemaHints`와 `_rescued_data`의 역할 차이는?
<details><summary>정답</summary>

`schemaHints`는 앞으로 올 것으로 예상되는 컬럼과 타입을 미리 선언해, 새 컬럼이 나타나면 자동으로 채우고 이전 레코드는 NULL로 둔다. `_rescued_data`는 선언된 스키마에 맞지 않는 예상치 못한 컬럼이나 타입 불일치 데이터를 JSON으로 보관해 데이터가 조용히 버려지지 않게 하는 최후의 방어선이다.
</details>

**Q27.** 격리 패턴의 역논리란 무엇이며, 데이터 무손실을 어떻게 보장하는가?
<details><summary>정답</summary>

`is_quarantined = NOT(모든 규칙을 AND로 묶은 식)`이다. 규칙 하나라도 실패하면 TRUE가 된다. 모든 레코드를 추적 테이블에 쓰고 플래그로 정상/격리 테이블에 나누므로 `전체 입력 = 정상 + 격리`가 성립하고 어떤 레코드도 사라지지 않는다.
</details>

**Q28.** 격리 패턴의 품질 추적 테이블에 DROP ROW나 FAIL UPDATE를 쓰면 안 되는 이유는? 그렇다면 WARN 제약은 왜 두는가?
<details><summary>정답</summary>

DROP ROW는 플래그가 계산되기 전에 잘못된 레코드를 제거하고, FAIL UPDATE는 파이프라인을 멈춰 무손실 보장이 깨진다. WARN 제약은 실제 라우팅에는 관여하지 않고, 파이프라인 UI에 제약별 위반 메트릭을 보여 주기 위해 둔다.
</details>

**Q29.** 품질 추적 테이블을 `is_quarantined`로 파티셔닝하는 이유는?
<details><summary>정답</summary>

정상과 격리 레코드를 물리적으로 분리해, `is_quarantined = FALSE/TRUE`로 읽는 하위 테이블이 파티션 프루닝으로 해당 파티션만 읽게 하기 위해서다. 값이 두 개뿐인 저카디널리티 컬럼이라 파티셔닝에 적합하다.
</details>

**Q30.** DROP ROW와 격리 패턴 중 무엇을 고를지 판단하는 기준은?
<details><summary>정답</summary>

규칙이 잘 확립된 비중요 스트림이면 단순한 DROP ROW로 충분하다. 규정 준수, 감사 추적, 잘못된 레코드의 수정과 재처리가 필요한 운영 파이프라인이면 격리 패턴이 권장된다.
</details>

**Q31.** 스트림-정적 조인에서 정적 테이블이 나중에 바뀌면 이미 처리된 스트리밍 행은 어떻게 되는가?
<details><summary>정답</summary>

다시 계산되지 않는다. 정적 쪽은 각 업데이트 시점의 최신 스냅샷으로 새 스트리밍 행에만 조인된다. 과거 행까지 최신 참조 정보로 반영하려면 MV로 조인하거나 전체 갱신이 필요하다.
</details>

**Q32.** Multi-Flow에서 웹 flow에는 없고 앱 flow에만 있는 컬럼은 웹 행에서 어떤 값이 되는가?
<details><summary>정답</summary>

`INSERT INTO ... BY NAME`은 이름으로 컬럼을 맞추므로, 웹 flow가 제공하지 않는 앱 전용 컬럼은 웹 행에서 NULL이 된다.
</details>

---

## 19. 참고 링크

- Flow: https://docs.databricks.com/aws/en/ldp/flows
- 여러 flow로 하나의 테이블에 쓰기: https://docs.databricks.com/aws/en/ldp/flow-examples
- 파이프라인 파라미터: https://docs.databricks.com/aws/en/ldp/parameters
- 전체 갱신 여부 판단: https://docs.databricks.com/aws/en/ldp/updates
- 파이프라인 Expectations: https://docs.databricks.com/aws/en/ldp/expectations
- Expectation 권장 사항과 고급 패턴: https://docs.databricks.com/aws/en/ldp/expectation-patterns
- 파이프라인 이벤트 로그: https://docs.databricks.com/aws/en/ldp/monitor-event-logs
- Liquid 클러스터링: https://docs.databricks.com/aws/en/delta/clustering
- Materialized View 증분 갱신: https://docs.databricks.com/aws/en/optimizations/incremental-refresh
- VARIANT 데이터 타입: https://docs.databricks.com/aws/en/sql/language-manual/data-types/variant-type
- 파이프라인 Sink: https://docs.databricks.com/aws/en/ldp/sinks
- Delta UniForm (Iceberg 읽기): https://docs.databricks.com/aws/en/delta/uniform
- Deletion Vectors: https://docs.databricks.com/aws/en/delta/deletion-vectors
- CDC란: https://docs.databricks.com/aws/en/ldp/what-is-change-data-capture
- AUTO CDC API: https://docs.databricks.com/aws/en/ldp/cdc
- AUTO CDC INTO SQL 참조: https://docs.databricks.com/aws/en/ldp/developer/ldp-sql-ref-apply-changes-into
- max_by 함수: https://docs.databricks.com/aws/en/sql/language-manual/functions/max_by
- Auto Loader 스키마 힌트와 진화: https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema
- Unity Catalog 태그: https://docs.databricks.com/aws/en/database-objects/tags
