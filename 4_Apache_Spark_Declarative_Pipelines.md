# Apache Spark Declarative Pipelines로 데이터 파이프라인 구축하기 — 핵심 개념 정리

---

## 목차

1. [Spark Declarative Pipelines(SDP) 개요](#1-spark-declarative-pipelinessdp-개요)
2. [전통적인 파이프라인의 한계와 SDP가 해결하는 것](#2-전통적인-파이프라인의-한계와-sdp가-해결하는-것)
3. [증분 처리의 흐름](#3-증분-처리의-흐름)
4. [데이터셋 유형: Streaming Table, Materialized View, View](#4-데이터셋-유형-streaming-table-materialized-view-view)
5. [SQL로 메달리온 파이프라인 작성하기](#5-sql로-메달리온-파이프라인-작성하기)
6. [파이프라인 그래프와 자동 의존성 해결](#6-파이프라인-그래프와-자동-의존성-해결)
7. [파이프라인 개발 환경](#7-파이프라인-개발-환경)
8. [파이프라인 설정](#8-파이프라인-설정)
9. [파이프라인 실행: Dry run, Refresh, Full refresh](#9-파이프라인-실행-dry-run-refresh-full-refresh)
10. [데이터 품질: Expectations](#10-데이터-품질-expectations)
11. [기존 SQL ETL을 SDP로 바꾸는 방법](#11-기존-sql-etl을-sdp로-바꾸는-방법)
12. [스트리밍 조인](#12-스트리밍-조인)
13. [운영 배포: 주석, 테이블 속성, 모드, 스케줄, 알림](#13-운영-배포-주석-테이블-속성-모드-스케줄-알림)
14. [이벤트 로그로 모니터링](#14-이벤트-로그로-모니터링)
15. [변경 데이터 캡처(CDC)와 SCD](#15-변경-데이터-캡처cdc와-scd)
16. [AUTO CDC INTO](#16-auto-cdc-into)
17. [Python으로 작성하는 SDP](#17-python으로-작성하는-sdp)
18. [DLT에서 SDP로: 구문 변경 정리](#18-dlt에서-sdp로-구문-변경-정리)
19. [명령어 치트시트](#19-명령어-치트시트)
20. [자가 점검 문제](#20-자가-점검-문제)
21. [참고 링크](#21-참고-링크)

---

## 1. Spark Declarative Pipelines(SDP) 개요

### 1-1. 정의

**Apache Spark Declarative Pipelines(SDP)**는 SQL 또는 Python으로 **배치·스트리밍 데이터 파이프라인**을 구축·관리·자동화하는 **로우코드 선언형 프레임워크**다.

- 이전 이름: **Delta Live Tables(DLT)**, Lakeflow Declarative Pipelines
- SDP는 Apache Spark 오픈 소스 프로젝트에 포함되어 있다. Databricks의 Lakeflow SDP는 이를 확장해 성능 최적화된 Databricks 런타임에서 실행한다.
- Lakeflow의 구성 요소 중 **변환(ETL)**을 담당한다. (수집: Lakeflow Connect, 오케스트레이션: Lakeflow Jobs)

### 1-2. 핵심 아이디어: "어떻게"가 아니라 "무엇"을

데이터 엔지니어가 정말 집중하고 싶은 것은 **변환 로직**이다. 그러나 실제로는 다음까지 직접 처리해야 했다.

| 부담 영역 | 예 |
|---|---|
| 운영 작업 | 백필, 의존성 관리, 일별 파티션 계산, 체크포인트, 재시도 |
| 인프라 | 버전 관리, CI/CD, 배포 인프라 |
| 거버넌스 | 데이터 품질 검사, 거버넌스, 데이터 탐색 |
| 다양한 소비처 | BI, 데이터 사이언스, ML, 생성형 AI, 스트리밍 |

SDP를 쓰면 **테이블을 선언하고 변환 로직만 작성**한다. 실행 순서, 증분 처리, 체크포인트, 재시도, 인프라는 프레임워크가 처리한다.

### 1-3. 신뢰할 수 있는 파이프라인 구축이 어려웠던 이유와 SDP의 답

| 어려움 | 결과 | SDP의 해결 |
|---|---|---|
| 노동 집약적 개발 | 느리고 오류가 잦은 파이프라인, 인사이트 제공 지연 | **간소화된 작성**: SQL/Python으로 정의, 오케스트레이션 로직 불필요, 보일러플레이트 제로(오류 처리·의존성 관리 내장) |
| 운영 복잡성 | 높은 운영 비용, 다운타임, 리소스 낭비 | **지능형 최적화**: 자동 확장, 장애 시 자가 복구, 낮은 운영 부담 |
| 배치와 스트리밍의 분리 | 지연 시간·비용·SLA 변화에 대응하기 어려움 | **배치·스트리밍 통합**: 단일 플랫폼, 워크로드 인식 최적화, **같은 코드로 배치와 스트리밍 모두 처리** |

### 1-4. SDP의 세 가지 강점

| 강점 | 내용 |
|---|---|
| **효율적인 수집** | Databricks의 Spark가 지원하는 모든 소스에서 로드. 배치·스트리밍·CDC 패턴 지원. 데이터 엔지니어, Python 개발자, 데이터 과학자, SQL 분석가 모두 사용 가능 |
| **지능형 변환** | 몇 줄의 코드로 파이프라인의 실행 계획을 SDP가 결정. 비용·성능에 맞춰 자동 최적화 |
| **자동화된 운영** | 모범 사례를 기본 내장. 의존성 관리, 확장, 복구, 데이터 품질 규칙 적용을 자동화. 인프라 관리 불필요 |

---

## 2. 전통적인 파이프라인의 한계와 SDP가 해결하는 것

### 2-1. 전통적인 배치 ETL

```sql
-- 브론즈: 실행할 때마다 디렉터리의 모든 파일을 읽는다
CREATE OR REPLACE TABLE orders_bronze AS
SELECT *, current_timestamp() AS processing_time, _metadata.file_name AS source_file
FROM read_files('/Volumes/cat/sch/source/orders', format => 'json');

-- 실버: 실행할 때마다 브론즈 전체를 읽는다
CREATE OR REPLACE TABLE orders_silver AS
SELECT order_id, timestamp(order_timestamp) AS order_timestamp, customer_id, notifications
FROM orders_bronze;

-- 골드: 일반 뷰는 조회할 때마다 다시 계산된다
CREATE OR REPLACE VIEW orders_by_date_vw AS
SELECT date(order_timestamp) AS order_date, count(*) AS total_daily_orders
FROM orders_silver
GROUP BY date(order_timestamp);
```

| 한계 | 내용 | 영역 |
|---|---|---|
| 브론즈: 매번 전체 재읽기 | `spark.read`(정적 전체 읽기) 방식. 이미 처리한 파일까지 매번 스캔 → **비용이 새 데이터가 아니라 전체 파일 수에 비례** | 효율 |
| 실버: 매번 전체 재처리 | 마지막 실행 이후 추가된 행이 아니라 브론즈 전체를 다시 변환 | 효율 |
| 뷰는 호출할 때마다 재실행 | 일반 뷰는 저장된 SQL일 뿐 결과를 저장하지 않음 → 대시보드가 조회할 때마다 전체 스캔 | 성능 |
| 데이터 품질에 별도 코드 필요 | WHERE, CASE, 커스텀 검증을 파이프라인 밖에서 따로 관리. 잘못된 데이터를 버릴지, 멈출지, 기록할지 표준 패턴이 없음 | 품질 |
| 실행 모니터링 어려움 | 작업 성공/실패는 알지만 몇 행이 기록됐는지, 중복이 있었는지 등 데이터 수준 가시성이 없음 | 관측성 |
| 문제 탐색·수정용 UI 없음 | 리니지 뷰, 단계별 미리보기 없음. 문제 하나를 고치려 전체를 다시 실행 | 관측성 |

### 2-2. "Structured Streaming을 쓰면 되지 않나?"

`spark.readStream`으로 바꾸면 증분 처리는 되지만, 그 대가로 많은 배관(plumbing) 코드가 필요하다.

```python
checkpoint_base = "/Volumes/.../checkpoints"

# 브론즈: readStream + writeStream + trigger + checkpoint
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{checkpoint_base}/bronze_schema")
    .load(source_path)
  .writeStream
    .option("checkpointLocation", f"{checkpoint_base}/bronze")
    .trigger(availableNow=True)
    .toTable("bronze_orders"))

# 실버: upsert를 하려면 foreachBatch + MERGE가 필요
def upsert_to_silver(batch_df, batch_id):
    batch_df.createOrReplaceTempView("updates")
    batch_df.sparkSession.sql("""
        MERGE INTO silver_orders AS target
        USING updates AS source
        ON target.order_id = source.order_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

(spark.readStream.table("bronze_orders")
  .writeStream
    .foreachBatch(upsert_to_silver)
    .option("checkpointLocation", f"{checkpoint_base}/silver")
    .trigger(availableNow=True)
    .start())
```

| 복잡성 | 내용 |
|---|---|
| 체크포인트 관리 | 스트림마다 고유한 영구 체크포인트 위치 필요. 삭제·손상되면 전체 재처리 또는 실패. 레이어마다 경로를 따로 관리 |
| foreachBatch + MERGE | 기본 출력 모드(append, complete, update)는 MERGE/upsert를 직접 지원하지 않음 → 함수 정의, 임시 뷰, MERGE, foreachBatch 연결이 필요 |
| 트리거·스키마 구성 | `availableNow`(쌓인 것만 처리 후 종료), `processingTime`(주기), `continuous`(저지연) 중 선택. Auto Loader는 `schemaLocation` 필요. `.start()`/`.toTable()`/`.awaitTermination()`을 잘못 다루면 조용한 실패나 끝나지 않는 스트림 |
| 여전히 없는 것 | 데이터 품질 규칙, 파이프라인 UI, 리니지, 테이블별 행 수·품질 메트릭, 불량 데이터 격리 패턴 |

> **정리:** Structured Streaming은 "매번 전부 다시 읽는" 문제는 해결하지만, 데이터 품질·관측성·운영 단순성은 해결하지 못한다. SDP는 **테이블을 선언하고 변환 로직만 쓰면** 증분 처리, 체크포인트, 데이터 품질, 파이프라인 UI를 모두 자동으로 관리한다. foreachBatch도, 체크포인트 경로도, 트리거 구성도 직접 다룰 필요가 없다.

---

## 3. 증분 처리의 흐름

예시 파이프라인: **원시 파일 → 브론즈(Streaming Table) → 실버(Streaming Table) → 골드(Materialized View)**

| 실행 | 브론즈 ST | 실버 ST | 골드 MV |
|---|---|---|---|
| **1회차** (File 1 존재) | File 1의 데이터를 읽어 **추가(append)** | 브론즈의 새 데이터를 변환해 **추가** | 실버 전체를 집계. 서버리스의 내장 최적화 적용 |
| **2회차** (File 2 추가) | 체크포인트로 File 1은 건너뛰고 **File 2만** 추가 | 브론즈에 **새로 들어온 행만** 변환해 추가 | 실버 전체 기준으로 결과 갱신. 가능하면 **증분**, 아니면 **전체 재계산** |

- 스트리밍 테이블은 Auto Loader의 **체크포인트**로 이미 처리한 데이터를 추적해 재처리 오버헤드를 없앤다.
- 같은 파이프라인을 **배치(예약 실행)**로도, **스트리밍(연속 실행)**으로도 돌릴 수 있다.
- 예: 첫 실행에서 174행 → 새 파일(25행) 추가 후 재실행하면 브론즈·실버에 **25행만** 추가된다. 배치 방식이었다면 199행을 다시 읽었을 것이다.

---

## 4. 데이터셋 유형: Streaming Table, Materialized View, View

### 4-1. 한눈에 비교

| 유형 | 구문 | 저장 | 처리 방식 | 주 용도 |
|---|---|---|---|---|
| **Streaming Table (ST)** | `CREATE OR REFRESH STREAMING TABLE` | O | **새 데이터만 증분 처리** (append-only 소스) | 브론즈 수집, 실버 정제 |
| **Materialized View (MV)** | `CREATE OR REFRESH MATERIALIZED VIEW` | O | 현재 데이터 기준으로 정확한 결과를 **재계산** (가능하면 증분 갱신) | 집계, 조인, 골드 레이어, 느린 쿼리 사전 계산 |
| **View** | `CREATE VIEW` | X | 조회할 때마다 계산 | Unity Catalog에 등록되는 가상 테이블 |
| **Temporary View** | `CREATE TEMPORARY VIEW` | X | 파이프라인 실행 중에만 존재 | 파이프라인 내부의 중간 로직 (외부 비노출) |

### 4-2. Streaming Table

1. **증분 처리 모드**: 배치 또는 스트리밍으로 증분 처리. 파일은 **정확히 한 번(exactly-once)** 처리된다.
2. **효율적인 업데이트**: 갱신할 때마다 소스에 새로 추가된 데이터만 스트리밍 테이블에 추가된다.
3. **구문**: `CREATE OR REFRESH STREAMING TABLE`
4. **스트리밍 읽기**: 체크포인트 기반 증분 읽기를 하려면 반드시 `FROM STREAM`을 쓴다. 파일이면 `FROM STREAM read_files(...)`, 테이블이면 `FROM STREAM 테이블명`.
5. **Auto Loader 통합**: `STREAM read_files`는 Auto Loader를 사용해 새 파일을 자동 추적한다.
6. **중복 읽기 없음**: 같은 파일 이름은 한 번만 읽힌다.

- 스트리밍 테이블의 히스토리에는 `STREAMING UPDATE` 작업이 기록되며, `operationParameters`에 `"outputMode": "Append"`와 실행마다 증가하는 `epochId`가, `operationMetrics`에 `numOutputRows` 등이 남는다.

### 4-3. Materialized View

1. **동적 재계산**: 갱신될 때마다 업스트림 변경을 반영하도록 쿼리 결과를 다시 계산한다.
2. **파이프라인이 관리**: 파이프라인이 자동으로 생성·갱신한다.
3. **구문**: `CREATE OR REFRESH MATERIALIZED VIEW`
4. **어디에나 배치 가능**: 골드뿐 아니라 파이프라인 어느 위치에서도 쓸 수 있다.
5. **증분 갱신**: 가능하면 전체를 다시 만들지 않고 증분 갱신한다 (**서버리스 compute에서 지원**).
6. **비용 기반 최적화**: 내부의 비용 기반 옵티마이저가 **증분 갱신할지 전체 재계산할지** 결정한다.

- MV의 `FROM` 절에서는 소스 테이블을 `STREAM` **키워드 없이** 참조한다. MV는 변경 추적과 갱신을 스스로 관리하기 때문이다.
- `GROUP BY` 같은 집계는 스트리밍 테이블로 증분 계산하기에 적합하지 않으므로 MV를 쓴다.

### 4-4. View와 Temporary View

| | Temporary View | View |
|---|---|---|
| 존재 범위 | **파이프라인 실행 중에만** | Unity Catalog에 등록되어 **실행 후에도 유지** |
| UC 등록 | X | O |
| 용도 | 사용자에게 노출하지 않는 중간 로직 | 다른 사용자가 조회할 수 있는 논리 계층 |

**뷰의 제한 사항**

- 파이프라인이 **Unity Catalog 파이프라인**이어야 한다.
- 뷰는 **스트리밍 쿼리를 가질 수 없고**, 파이프라인의 **스트리밍 소스로 쓸 수 없다**.
- 두 뷰 모두 `OR REFRESH` 없이 표준 `CREATE` 구문을 쓴다.

```sql
-- 파이프라인 내부 전용
CREATE TEMPORARY VIEW orders_active AS
SELECT * FROM sdp_2_silver.orders_silver WHERE notifications = 'Y';

-- Unity Catalog에 등록
CREATE VIEW sdp_3_gold.orders_active_view AS
SELECT * FROM sdp_2_silver.orders_silver WHERE notifications = 'Y';
```

---

## 5. SQL로 메달리온 파이프라인 작성하기

```sql
---------------------------------------------------------------------
-- 브론즈: 원시 JSON 파일을 증분 수집하는 스트리밍 테이블
---------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE sdp_1_bronze.orders_bronze
AS
SELECT
  *,
  current_timestamp() AS processing_time,   -- 수집 시각 (감사)
  _metadata.file_name AS source_file        -- 소스 파일 이름 (리니지)
FROM STREAM read_files(                      -- Auto Loader 기반 증분 수집
  "${source}/orders",                        -- 파이프라인 configuration 값 참조
  format => 'JSON'
);

---------------------------------------------------------------------
-- 실버: 브론즈에서 새 행만 읽어 정제하는 스트리밍 테이블
---------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE sdp_2_silver.orders_silver
AS
SELECT
  order_id,
  timestamp(order_timestamp) AS order_timestamp,   -- 타입 변환
  customer_id,
  notifications
FROM STREAM sdp_1_bronze.orders_bronze;            -- 테이블에서 증분 읽기

---------------------------------------------------------------------
-- 골드: 실버 전체를 집계하는 구체화된 뷰 (STREAM 없음)
---------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW sdp_3_gold.gold_orders_by_date
AS
SELECT
  date(order_timestamp) AS order_date,
  count(*) AS total_daily_orders
FROM sdp_2_silver.orders_silver
GROUP BY date(order_timestamp);
```

- 테이블 이름에 카탈로그를 생략하면 파이프라인의 **기본 카탈로그**, 스키마까지 생략하면 **기본 스키마**에 만들어진다.
- 파이프라인은 기본 카탈로그·스키마뿐 아니라 **여러 카탈로그와 스키마에 동시에 게시**할 수 있다 (예: 브론즈·실버·골드를 각각 다른 스키마에).

---

## 6. 파이프라인 그래프와 자동 의존성 해결

- SDP는 각 데이터셋 정의의 `FROM` 절을 분석해 **의존성을 자동으로 파싱**한다.
- 코드 순서와 상관없이, 여러 파일에 나뉘어 있어도 의존성이 **파이프라인 그래프(DAG)**로 자동 연결된다.
- 예: `orders_bronze` → `orders_silver`(브론즈를 STREAM으로 읽음) → `gold_orders_by_date`(실버를 읽음)
- 효과: 실행 순서를 수동으로 관리할 필요가 없어 오류가 줄고, 데이터 흐름을 그래프로 바로 확인·모니터링할 수 있다.
- 그래프의 각 노드에서 처리된 행 수, 소요 시간, 데이터 품질 결과를 확인할 수 있다.

---

## 7. 파이프라인 개발 환경

### 7-1. 멀티 파일 편집기 (Lakeflow Pipelines Editor)

하나의 큰 노트북 대신, 파이프라인을 **여러 파일의 집합**으로 구성한다.

- 파이프라인 코드는 `.sql` 또는 `.py` 파일(권장) 으로 작성하며, 노트북도 사용할 수 있다.
- 파일을 흐름(orders, status, customers 등)별 폴더로 나누면 관리가 쉽다.

| 기능 | 역할 |
|---|---|
| 파이프라인 자산 브라우저 | 파이프라인 파일 탐색 |
| 멀티 파일 코드 편집기 | 단계별 파이프라인 개발 |
| 파이프라인 전용 툴바 | 실행, Dry run, 설정, 스케줄 등 자주 쓰는 작업 |
| 인터랙티브 DAG | 파이프라인 의존성 시각화 |
| 데이터 미리보기 | 중간 결과 확인 |
| 실행 인사이트·디버깅·Dry run | 실행 모니터링, 통합 디버깅, 전체 실행 없이 유효성 검사 |

- 실행 결과 패널에서 데이터셋별 **Data, Columns, Table metrics, Performance**를 확인할 수 있다.
- 파이프라인은 ETL Pipeline으로 생성하며, 생성 후에는 작업 및 파이프라인 목록에서 실행 이력과 실행별 그래프·메트릭을 다시 볼 수 있다.

### 7-2. 중요한 동작 차이

- SDP 소스 코드는 노트북 셀처럼 **대화형으로 한 셀씩 실행되지 않는다.** 파이프라인으로 등록되어 **파이프라인 엔진이 전체를 실행**한다.

---

## 8. 파이프라인 설정

### 8-1. 설정 항목 전체

| 항목 | 내용 |
|---|---|
| **Pipeline settings** | 파이프라인 ID, 이름, 모드, 생성자, 소유자, **Run as**(실행 주체) |
| **Code assets** | 루트 폴더, 소스 코드 경로 |
| **Default location for data assets** | 기본 카탈로그, 기본 스키마 |
| **Compute** | 서버리스 / 클래식 |
| **Configuration** | 코드에서 참조할 키-값 파라미터 |
| **Environment** | 환경 의존성(pip 요구 사항 형식, 한 줄에 패키지 하나) |
| **Usage / Budget** | 서버리스 예산 정책, 태그(청구 기록에 남음) |
| **Tags** | 파이프라인 분류·관리 |
| **Notifications** | 실행 이벤트 알림 |
| **Advanced settings** | Channel(런타임 버전), Event logs(이벤트 로그 게시) |

### 8-2. Compute

| | 서버리스 (권장) | 클래식 (고정 크기) |
|---|---|---|
| 관리 | 인프라 관리·프로비저닝 불필요 | 클러스터 생성 권한 필요. 관리자가 **클러스터 정책**으로 사용 범위 제어 |
| 비용·성능 | 비용 최적화와 고성능을 함께 | 향상된 자동 확장(Enhanced Autoscaling)이 새 파이프라인에 기본 활성화 |
| MV 증분 갱신 | **지원** (비용 기반 옵티마이저) | — |
| 기타 | 시간에 민감한 경우 서버리스 성능 최적화 옵션 | — |

### 8-3. Code assets

- **루트 폴더**: 파이프라인 프로젝트 파일이 모이는 폴더. 자동 설정되며 **Git 폴더**로 만들어 버전 관리·협업할 수 있다.
- **소스 코드**: 루트 폴더 안에서 파이프라인에 포함할 하위 폴더·파일을 지정 (Python 스크립트, SQL 파일, 노트북 등). 지정한 코드만 파이프라인으로 실행된다.

### 8-4. Configuration (파라미터)

파이프라인 수준의 **키-값 맵**으로 코드를 파라미터화한다.

| 이점 | 내용 |
|---|---|
| 가독성·유지보수 | 핵심 값을 한 곳에서 관리 |
| 재사용 | 여러 파일에 값을 하드코딩하지 않음 |
| 유연성 | 코드 수정 없이 설정만 바꿔 환경 전환 (dev/prod 경로 등) |

```sql
-- Configuration: source = /Volumes/my_catalog/sdp_1_bronze/source
CREATE OR REFRESH STREAMING TABLE bronze
AS SELECT *
FROM STREAM read_files("${source}/orders", format => 'JSON');
```

- SQL에서는 `${키}` 형식으로 참조하며, 실행 시 값이 치환된다.
- Python에서는 `spark.conf.get("source")`로 읽는다.

### 8-5. Advanced settings

| 항목 | 내용 |
|---|---|
| **Channel** | **Current**: 최신 안정 런타임 (운영 권장) / **Preview**: 더 새로운 런타임, 향후 기능 테스트용 |
| **Event logs** | 이벤트 로그를 지정한 카탈로그·스키마의 **UC 테이블로 게시** (14장) |

---

## 9. 파이프라인 실행: Dry run, Refresh, Full refresh

### 9-1. Dry run

- 테이블을 실제로 만들거나 갱신하지 않고 **소스 코드의 문제만 검사**한다.
- 잘못된 테이블·컬럼 이름, 구문 오류를 빠르게 찾는다. 결과로 파이프라인 그래프와 각 객체가 기록될 카탈로그·스키마·유형이 표시된다.

### 9-2. 실행 유형

| 실행 유형 | Materialized View | Streaming Table |
|---|---|---|
| **Run pipeline** (갱신) | 정의된 쿼리의 현재 결과를 반영. 비용을 검토해 더 효율적이면 **증분 갱신** | 스트리밍 테이블과 플로우에 정의된 로직으로 **새 레코드만** 처리 |
| **Run pipeline with full table refresh** (전체 갱신) | 정의된 쿼리의 현재 결과를 반영 | **데이터를 지우고, 체크포인트(상태)를 초기화**하고, 소스의 **모든 레코드를 재처리** |

- 새 파일이 없는데 다시 실행하면 스트리밍 테이블에는 **새 행이 추가되지 않는다** (체크포인트가 이미 처리했음을 알고 있음).
- 주의: 전체 갱신은 스트리밍 테이블의 데이터와 체크포인트를 삭제한다. 원본 파일이 이미 사라졌다면 데이터를 복구할 수 없다.
- 특정 테이블의 전체 갱신을 막으려면 테이블 속성 `pipelines.reset.allowed = false`를 설정한다 (브론즈처럼 원본 보존이 중요한 테이블).

### 9-3. 개발 모드와 운영 모드

| 모드 | 동작 |
|---|---|
| **Development** | 실행 후에도 compute를 재사용해 반복 개발이 빠름. 실패 시 자동 재시도하지 않아 오류를 바로 확인 |
| **Production** | 실패 시 재시도, 필요하면 compute를 재시작해 복구 |

- 에디터에서 직접 실행하는 것은 개발용이고, 스케줄된 작업으로 실행되는 것은 운영용 실행이다.

---

## 10. 데이터 품질: Expectations

### 10-1. 개념과 구문

**Expectations**는 ETL 중에 **행 단위로 데이터를 검증**하는 데이터 품질 규칙이다. 테이블 정의 안에 `CONSTRAINT`로 선언한다.

```sql
CONSTRAINT constraint_name
EXPECT (column_condition)
[ON VIOLATION action]
```

### 10-2. 세 가지 위반 동작

| 동작 | 구문 | 위반 행 | 파이프라인 | 적합한 상황 |
|---|---|---|---|---|
| **WARN** (기본) | `ON VIOLATION` 절 **생략** | 테이블에 **포함** | 계속 진행 | 중단할 필요 없는 품질 모니터링 |
| **DROP** | `ON VIOLATION DROP ROW` | 테이블에서 **제외** | 계속 진행 | 잘못된 데이터는 빼되 처리는 계속해야 할 때 |
| **FAIL** | `ON VIOLATION FAIL UPDATE` | — | 해당 **플로우(flow)가 실패**. 그 배치의 어떤 레코드도 기록되지 않음. 수동 개입 필요 | 기본 키처럼 절대 틀리면 안 되는 필드 |

- 주의: `ON VIOLATION WARN`이라는 키워드는 **없다.** 경고 동작은 `ON VIOLATION` 절을 생략하는 것이다.
- **FAIL은 해당 플로우만 실패**시키며, 파이프라인의 다른 독립 플로우에는 영향을 주지 않는다.
- 세 동작 모두 통과/실패 건수가 메트릭과 이벤트 로그에 기록된다.

### 10-3. 여러 제약 조건 함께 쓰기

```sql
CREATE OR REFRESH STREAMING TABLE sdp_2_silver.orders_silver
(
  CONSTRAINT valid_notifications EXPECT (notifications IN ('Y','N')),                       -- WARN
  CONSTRAINT valid_date EXPECT (order_timestamp > "2021-01-01") ON VIOLATION DROP ROW,      -- DROP
  CONSTRAINT valid_id   EXPECT (customer_id IS NOT NULL)        ON VIOLATION FAIL UPDATE    -- FAIL
)
AS
SELECT
  order_id,
  timestamp(order_timestamp) AS order_timestamp,
  customer_id,
  notifications
FROM STREAM sdp_1_bronze.orders_bronze;
```

- 여러 제약 조건은 **각각 독립적으로 평가**된다.
- 행 처리 흐름: 행 입력 → 모든 제약 조건 평가 → 모두 통과하면 기록 / 실패하면 해당 제약의 동작(WARN·DROP·FAIL) 적용

### 10-4. 고급 제약 조건 패턴 (CDC 피드 검증)

삭제 레코드는 키 외의 값이 모두 null인 CDC 피드를 검증하는 예.

```sql
CREATE OR REFRESH STREAMING TABLE sdp_1_bronze.customers_bronze_clean
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
COMMENT "Clean raw bronze timestamp column and add data quality constraints"
AS
SELECT
  *,
  CAST(from_unixtime(timestamp) AS timestamp) AS timestamp_datetime
FROM STREAM sdp_1_bronze.customers_bronze_raw;
```

| 제약 | 동작 | 의도 |
|---|---|---|
| `valid_id` | FAIL | null 키가 하나라도 오면 배치 전체 실패. 기본 키처럼 필수인 필드 |
| `valid_operation` | DROP | operation이 null이면 CDC 로직을 적용할 수 없으므로 해당 행만 제거 |
| `valid_name` | WARN | `OR operation = "DELETE"`로 삭제 레코드의 null은 허용 |
| `valid_address` | WARN | 여러 조건을 AND로 묶어 한 제약에서 검사, DELETE는 OR로 우회 |
| `valid_email` | DROP | `rlike()` 정규식으로 형식 검증 |

- 제약 조건 안에서 **SQL 내장 함수**(rlike 등)와 **복합 조건**을 쓸 수 있다.

### 10-5. 결과 확인

- 테이블별 **Table metrics**: Output records, Expectations(met/unmet 수), Dropped(드롭된 행), Warnings(경고 행)
- 제약 조건별 상세: 동작, 실패율, 실패 행 수
- 테이블 메트릭의 경고 수는 중복 행을 제거해 세고, 제약별 상세는 한 행이 여러 제약을 어기면 제약마다 센다. 그래서 두 숫자가 다를 수 있다.
- UI 메트릭은 **실행(업데이트) 단위**다. 여러 실행에 걸친 품질 추이는 **이벤트 로그**를 쿼리한다 (14장).

### 10-6. 알아둘 점

- 2025년 2분기 기준 문서에서는 **Expectations가 있는 Materialized View는 실행 때마다 항상 전체 재계산**된다고 안내했다 (버전에 따라 바뀔 수 있음).
- 위반 행을 따로 보관하고 싶다면 반대 조건으로 별도 테이블을 만들어 격리(quarantine)한다.

---

## 11. 기존 SQL ETL을 SDP로 바꾸는 방법

변환 로직은 그대로 두고 `CREATE` **구문과** `FROM` **절만** 바꾸면 된다.

| 레이어 | 기존 | SDP |
|---|---|---|
| 브론즈 | `CREATE OR REPLACE TABLE ... FROM read_files(...)` | `CREATE OR REFRESH STREAMING TABLE ... FROM STREAM read_files(...)` |
| 실버 | `CREATE OR REPLACE TABLE ... FROM bronze` | `CREATE OR REFRESH STREAMING TABLE (제약 조건) ... FROM STREAM bronze` |
| 골드 | `CREATE OR REPLACE VIEW ... GROUP BY` | `CREATE OR REFRESH MATERIALIZED VIEW ... GROUP BY` (STREAM 없음) |

```sql
-- 브론즈: CSV, 명시적 스키마
CREATE OR REFRESH STREAMING TABLE sdp_lab_1_bronze.employees_bronze
AS
SELECT *, current_timestamp() AS ingestion_time, _metadata.file_name AS raw_file_name
FROM STREAM read_files(
  '${source}',
  format => 'CSV',
  schema => 'EmployeeID STRING, FirstName STRING, Country STRING, Department STRING,
             Salary DOUBLE, HireDate DATE, Operation STRING, ProcessDate DATE',
  header => 'true',
  inferSchema => 'false'
);

-- 실버: 정제 + 품질 규칙 (테스트용 null ID 행 제거)
CREATE OR REFRESH STREAMING TABLE sdp_lab_2_silver.employees_silver
(
  CONSTRAINT check_country EXPECT (Country IN ('US','GR')),
  CONSTRAINT check_salary  EXPECT (Salary > 0),
  CONSTRAINT check_null_id EXPECT (EmployeeID IS NOT NULL) ON VIOLATION DROP ROW
)
AS
SELECT
  EmployeeID, FirstName, upper(Country) AS Country, Department, Salary, HireDate,
  date_format(HireDate, 'MMMM') AS HireMonthName,
  year(HireDate) AS HireYear,
  Operation
FROM STREAM sdp_lab_1_bronze.employees_bronze;

-- 골드: 집계 MV
CREATE OR REFRESH MATERIALIZED VIEW sdp_lab_3_gold.employees_by_country_gold
AS
SELECT Country, count(*) AS TotalEmployees, sum(Salary) AS TotalSalary
FROM sdp_lab_2_silver.employees_silver
GROUP BY Country;
```

- 주의: 스트리밍 테이블은 **추가(append)만** 한다. 소스에 같은 직원의 update·delete 레코드가 와도 실버에는 행이 계속 쌓인다. 최신 상태만 유지하려면 CDC(AUTO CDC INTO)를 써야 한다 (15~16장).

---

## 12. 스트리밍 조인

### 12-1. 세 가지 조인 유형

| 조인 유형 | 입력 | 출력 | 처리되는 데이터 | 용도 |
|---|---|---|---|---|
| **Stream-snapshot (stream-static)** | 스트리밍 + 정적 | 스트리밍 테이블 | **새 행만**, 정적 테이블 전체와 조인 | 국가 코드·제품 ID 같은 잘 안 바뀌는 참조 데이터로 보강 |
| **MV 조인** | 스트리밍 + 스트리밍 | 구체화된 뷰 | 실행마다 **양쪽의 모든 행** (가능하면 증분 갱신) | 두 소스가 모두 자주 바뀌고, 항상 동기화된 최신 결과가 필요할 때 |
| **Stream-stream** | 스트리밍 + 스트리밍 | 스트리밍 테이블 | 각 스트림의 **새 행만** (윈도우 기반) | 시간적으로 가까운 이벤트 상관 분석 (클릭스트림 + 광고 노출). 워터마크·윈도우 필요 (고급) |

### 12-2. Stream-snapshot 조인

- 스트리밍 테이블에 새 행이 들어오면, **새 행만** 정적 테이블의 **모든 데이터**와 조인되어 결과 스트리밍 테이블에 추가된다.
- 정적 테이블을 재처리하지 않으므로 효율적이다.
- 주의: 이미 처리된 행은 다시 조인하지 않는다. 정적 테이블이 나중에 바뀌어도 **과거 결과는 갱신되지 않는다.**

```sql
CREATE OR REFRESH STREAMING TABLE silver.transactions_enriched AS
SELECT t.*, c.country_name
FROM STREAM bronze.transactions AS t          -- 스트리밍 쪽: 새 행만
JOIN reference.country_lookup AS c            -- 정적 쪽: 전체 스냅샷
  ON t.country_code = c.country_code;
```

### 12-3. Materialized View로 두 스트리밍 테이블 조인

- MV에서 두 스트리밍 테이블을 `STREAM` 없이 참조하면 **두 테이블의 모든 데이터**를 조인한다.

```sql
-- 주문과 주문 상태(한 주문에 여러 상태 행)를 조인
CREATE OR REFRESH MATERIALIZED VIEW sdp_3_gold.full_order_info_gold
COMMENT "Joining the orders and order status silver tables to view all orders with each individual status per order"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
  orders.order_id,
  orders.order_timestamp,
  status.order_status,
  status.order_status_timestamp
FROM sdp_2_silver.status_silver AS status
INNER JOIN sdp_2_silver.orders_silver AS orders
  ON orders.order_id = status.order_id;

-- 조인 결과를 상태별로 나눈 골드 MV
CREATE OR REFRESH MATERIALIZED VIEW sdp_3_gold.cancelled_orders_gold
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
  order_id, order_timestamp, order_status, order_status_timestamp,
  datediff(DATE(order_status_timestamp), DATE(order_timestamp)) AS days_to_cancel
FROM sdp_3_gold.full_order_info_gold
WHERE order_status = 'canceled';
```

- `datediff(end, start)`: 두 날짜 사이의 일수

### 12-4. Stream-stream 조인

- 각 실행에서 두 스트림의 **새 데이터끼리만** 조인한다. 과거 데이터는 고려하지 않는다.
- 상태(state)를 무한히 쌓지 않으려면 **워터마크와 시간 범위 조건**이 필요하다 (상태 저장 처리, 고급 주제).

---

## 13. 운영 배포: 주석, 테이블 속성, 모드, 스케줄, 알림

### 13-1. COMMENT와 TBLPROPERTIES

```sql
CREATE OR REFRESH STREAMING TABLE sdp_1_bronze.status_bronze
COMMENT "Ingest raw JSON order status files from cloud storage"
TBLPROPERTIES (
  "quality" = "bronze",                 -- 메달리온 레이어 표시 (사용자 정의 속성)
  "pipelines.reset.allowed" = false     -- 전체 갱신 시 데이터·체크포인트 삭제 방지
)
AS
SELECT *, current_timestamp() AS processing_time, _metadata.file_name AS source_file
FROM STREAM read_files("${source}/status", format => "JSON");

CREATE OR REFRESH STREAMING TABLE sdp_2_silver.status_silver
(
  CONSTRAINT valid_timestamp    EXPECT (status_timestamp IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_order_status EXPECT (order_status IN ('on the way','canceled','return canceled',
      'reported shipping error','delivered','return processed','return picked up',
      'placed','preparing','return requested'))
)
COMMENT "Order with each status and times"
TBLPROPERTIES ("quality" = "silver")
AS
SELECT
  order_id,
  order_status,
  timestamp(status_timestamp) AS order_status_timestamp
FROM STREAM sdp_1_bronze.status_bronze;
```

- `COMMENT`: Unity Catalog에서 테이블 목적을 설명하는 메타데이터
- `TBLPROPERTIES`: 테이블 수준 구성. 파이프라인 전용 속성(`pipelines.*`)과 사용자 정의 속성이 있다.

### 13-2. 운영 배포 체크리스트

| 항목 | 내용 |
|---|---|
| Run as | 운영 파이프라인은 개인 계정 대신 **서비스 주체(Service Principal)**로 실행 |
| 파이프라인 모드 | Triggered 또는 Continuous |
| 스케줄 | 일정한 간격 또는 연속 실행 |
| 알림 | 시작·성공·실패 |
| 이벤트 로그 | UC 테이블로 게시해 모니터링 |
| 채널 | Current (안정 런타임) |
| 문서화 | COMMENT, TBLPROPERTIES |
| 보호 | 원본 보존이 중요한 테이블에 `pipelines.reset.allowed = false` |

### 13-3. Triggered vs Continuous 모드

| | Triggered | Continuous |
|---|---|---|
| 동작 | 실행 시작 시점에 사용 가능한 데이터로 테이블을 갱신하고 **끝나면 중지** | 계속 실행되며 새 데이터를 지속 처리. 소스가 바뀔 때만 업데이트 |
| 실행 방법 | 수동 또는 스케줄(15분마다, 매시간, 매일 등) | 항상 실행 |
| 적합 | 배치 워크로드, 비연속 예약 작업 (비용 효율적) | 데이터 신선도가 중요한 스트리밍 (준실시간) |

### 13-4. 스케줄과 알림

- 파이프라인 스케줄은 내부적으로 **Lakeflow Jobs 작업**으로 만들어진다 (작업 안의 파이프라인 태스크).
- 고급 스케줄에서 요일·시각·시간대를 지정하고, **시작 / 성공 / 실패** 이벤트에 대한 이메일 알림을 원하는 조합으로 설정한다.
- 여러 파이프라인과 다른 태스크를 함께 오케스트레이션하려면 Lakeflow Jobs의 파이프라인 태스크를 쓴다.

---

## 14. 이벤트 로그로 모니터링

### 14-1. 이벤트 로그가 담는 것

| 범주 | 내용 |
|---|---|
| **감사 로그** | 누가, 무엇을, 언제 했는지 (파이프라인·데이터셋에 대한 작업 이력) |
| **데이터 품질 검사** | Expectations의 통과/실패 건수, 위반 상세 |
| **파이프라인 진행 상태** | 실행 상태, 처리된 행 수, 완료된 단계 |
| **데이터 리니지** | 소스 → 브론즈 → 실버 → 골드로의 흐름 |

### 14-2. 이벤트 로그의 위치

| 방식 | 내용 |
|---|---|
| **기본** | 파이프라인 기본 카탈로그·스키마에 **숨겨진 UC 테이블**로 기록. 이름 형식: `catalog.schema.event_log_{pipeline_id}` (ID의 `-`는 `_`로 바뀜). 기본적으로 파이프라인 소유자만 조회 가능 |
| **UC에 게시 (권장)** | 고급 설정의 Event logs에서 **카탈로그, 스키마, 테이블 이름**을 지정해 일반 UC 테이블로 게시 → 다른 테이블처럼 쿼리·대시보드 구축 |

- 테이블 값 함수로도 조회할 수 있다: `SELECT * FROM event_log(TABLE(catalog.schema.my_table))`, `SELECT * FROM event_log("<pipeline-id>")`

### 14-3. 이벤트 로그 스키마

| 컬럼 | 의미 |
|---|---|
| `id` | 이벤트 레코드 고유 ID |
| `sequence` | 이벤트 식별·순서 메타데이터 (JSON) |
| `origin` | 이벤트 출처 (클라우드, 리전, user_id, pipeline_id, pipeline_type 등) |
| `timestamp` | 기록 시각 |
| `message` | 사람이 읽는 설명 |
| `level` | INFO, WARN, ERROR, METRICS |
| `maturity_level` | 이벤트 스키마 안정성: STABLE, NULL(오래된 레코드, 안정), EVOLVING, DEPRECATED |
| `error` | 오류 상세 |
| `details` | 이벤트 상세 (JSON). **분석에 가장 많이 쓰는 컬럼** |
| `event_type` | 이벤트 유형 (예: `flow_progress`, `user_action`, `update_progress`) |

### 14-4. 쿼리 예

```sql
-- JSON 문자열 컬럼(details)을 콜론(:) 문법으로 파싱
SELECT id, event_type, details, details:flow_progress, details:user_action
FROM my_catalog.sdp_1_bronze.event_log_demo;

-- 실행 전체에 걸친 제약 조건별 통과/실패 건수
CREATE OR REPLACE TEMPORARY VIEW dq_source_vw AS
SELECT explode(
         from_json(details:flow_progress:data_quality:expectations,
                   "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>")
       ) AS row_expectations
FROM my_catalog.sdp_1_bronze.event_log_demo
WHERE event_type = 'flow_progress';

SELECT
  row_expectations.dataset AS dataset,
  row_expectations.name    AS expectation,
  SUM(row_expectations.passed_records) AS passing_records,
  SUM(row_expectations.failed_records) AS failing_records
FROM dq_source_vw
GROUP BY row_expectations.dataset, row_expectations.name
ORDER BY dataset;
```

- 데이터 품질 메트릭은 `event_type = 'flow_progress'` 이벤트의 `details:flow_progress:data_quality:expectations`에 들어 있다.
- 전체 갱신을 한 적이 있다면 이전 실행과 전체 갱신 이후의 메트릭이 섞여 있으므로 구분 로직이 필요하다.

---

## 15. 변경 데이터 캡처(CDC)와 SCD

### 15-1. CDC란

**변경 데이터 캡처(Change Data Capture)**: 소스(데이터베이스, 레이크하우스, DW)의 **삽입·업데이트·삭제를 추적·캡처**해 대상 테이블에 적용함으로써, 대상을 소스의 최신 상태와 동기화하는 기법.

- CDC 피드의 일반적인 형태: 키, 변경 유형(INSERT/UPDATE/DELETE), 순서를 정하는 컬럼(타임스탬프·시퀀스), 변경된 값
- 삭제 레코드는 보통 키와 operation, 순서 컬럼 외의 값이 null이다.

### 15-2. SCD (Slowly Changing Dimension, 점진적 변경 차원)

대상 테이블에서 **이력 변경을 어떻게 저장할지** 정의하는 방식.

| | SCD Type 1 | SCD Type 2 |
|---|---|---|
| 업데이트 | 키 기준으로 기존 값을 **덮어씀** | 기존 행은 비활성으로 남기고 **새 행 추가** |
| 삭제 | 행 **제거** | 행을 남기고 **비활성 표시** (`__END_AT`에 날짜) |
| 삽입 | 새 행 추가 | 새 행 추가 (`__START_AT` 기록) |
| 이력 | 없음 (현재 상태만) | **모든 버전 보존** |
| 용도 | 최신의 정확한 값만 필요할 때 | 시간에 따른 속성 변화 추적이 필요할 때 (주소·상태 변경 이력) |

### 15-3. 예시

대상 고객 테이블: (1, Peter), (2, Samarth)
소스 변경: Peter 주소 변경 2회(5/15, 5/20), Samarth 삭제, Kostas(3) 신규

**SCD Type 1 결과** (키: customer_id, 순서: ProcessDate)

| customer_id | name | address |
|---|---|---|
| 1 | Peter | 5/20 기준 최신 주소 |
| 3 | Kostas | 신규 주소 |

**SCD Type 2 결과**

| customer_id | name | address | `__START_AT` | `__END_AT` |
|---|---|---|---|---|
| 1 | Peter | 이전 주소 | 최초 | 변경 시점 (비활성) |
| 1 | Peter | 최신 주소 | 변경 시점 | null (활성) |
| 2 | Samarth | 주소 | 최초 | 삭제 시점 (비활성) |
| 3 | Kostas | 신규 주소 | 가입 시점 | null (활성) |

- `__END_AT`이 **null인 행이 현재 활성 행**이다.

---

## 16. AUTO CDC INTO

### 16-1. 개념

- SDP에 내장된 **선언적 CDC 처리** 구문. 이전 이름: `APPLY CHANGES INTO` (같은 구문, 계속 지원되지만 AUTO CDC 권장)
- 복잡한 `MERGE INTO` + foreachBatch 로직을 짧은 선언으로 대체한다.

**보장 사항과 특징**

- CDC 데이터를 **증분(스트리밍)**으로 수집
- 하나 또는 여러 컬럼을 **기본 키**로 지정
- 기본적으로 각 행을 **삽입 또는 업데이트(upsert)**로 간주, **삭제는 선택적으로** 적용
- 사용자가 지정한 **시퀀싱 컬럼**으로 **늦게 도착하거나 순서가 뒤바뀐 레코드**를 올바른 순서로 처리
- `EXCEPT`로 대상에 넣지 않을 컬럼 지정
- 기본은 **SCD Type 1**, SCD Type 2도 지원

### 16-2. 구문

```sql
-- 1. 대상 스트리밍 테이블 선언
CREATE OR REFRESH STREAMING TABLE sdp_2_silver.scd_type_1_customers_silver;

-- 2. CDC 플로우 정의
CREATE FLOW scd_type_1_flow AS
AUTO CDC INTO sdp_2_silver.scd_type_1_customers_silver    -- 대상
FROM STREAM sdp_1_bronze.customers_bronze_clean           -- CDC 피드 (스트리밍 소스)
KEYS (customer_id)                                        -- 기본 키
APPLY AS DELETE WHEN operation = "DELETE"                 -- 이 조건이면 삭제로 처리
SEQUENCE BY timestamp_datetime                            -- 변경 순서 결정 컬럼
COLUMNS * EXCEPT (operation, timestamp, _rescued_data, source_file, processing_time)  -- 제외할 컬럼
STORED AS SCD TYPE 1;                                     -- SCD 유형
```

| 절 | 역할 |
|---|---|
| `AUTO CDC INTO` | 변경을 적용할 대상 스트리밍 테이블 |
| `FROM STREAM` | CDC 레코드를 담은 스트리밍 소스 |
| `KEYS (...)` | 행을 식별하는 기본 키 (복수 컬럼 가능) |
| `APPLY AS DELETE WHEN` | 삭제로 처리할 조건 |
| `APPLY AS TRUNCATE WHEN` | 테이블 전체를 비울 조건 (SCD Type 1에서만) |
| `SEQUENCE BY` | 같은 키의 변경 순서를 정하는 컬럼 (타임스탬프, 시퀀스 번호). 늦게 온 레코드 정렬에 사용 |
| `COLUMNS` | 대상에 포함할 컬럼. `* EXCEPT (...)`로 제외 |
| `STORED AS SCD TYPE 1 / 2` | 이력 저장 방식 (기본 1) |
| `TRACK HISTORY ON` | (SCD Type 2) 이력을 남길 컬럼 지정. 나머지 컬럼 변경은 새 버전을 만들지 않음 |

- SCD Type 2로 저장하면 대상에 `__START_AT`, `__END_AT` 컬럼이 자동으로 추가된다.

### 16-3. 동작 예 (SCD Type 1)

| 실행 | 소스 | 대상 결과 |
|---|---|---|
| 1회차 | 939명 신규 | **Upserted 939** → 939행 |
| 2회차 | 23행: UPDATE 12, DELETE 1, NEW 10 | **Upserted 22** (업데이트 12 + 삽입 10), **Deleted 1** → 939 + 10 − 1 = **948행** |

- 업데이트된 고객은 원래 행이 **직접 수정**되어 이전 주소가 남지 않는다.
- 삭제된 고객은 테이블에서 **사라진다**.
- 고객 플로우 전체: 원시 JSON → `customers_bronze_raw`(ST, `pipelines.reset.allowed = false`) → `customers_bronze_clean`(ST + 제약 조건) → `AUTO CDC INTO` → `scd_type_1_customers_silver` → (필요하면) 현재 고객 골드 MV

### 16-4. 스냅샷 기반 CDC

- 소스가 변경 피드가 아니라 **주기적인 전체 스냅샷**만 제공한다면 `AUTO CDC FROM SNAPSHOT` 계열 API로 스냅샷 간 차이를 계산해 적용할 수 있다 (Python API).

---

## 17. Python으로 작성하는 SDP

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# 파이프라인 configuration 값 읽기
source = spark.conf.get("source")

# 브론즈 스트리밍 테이블
@dp.table(name="sdp_1_bronze.orders_bronze", comment="Raw orders")
def orders_bronze():
    return (spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", "json")
              .load(f"{source}/orders")
              .select("*",
                      F.current_timestamp().alias("processing_time"),
                      F.col("_metadata.file_name").alias("source_file")))

# 실버 스트리밍 테이블 + Expectations
@dp.table(name="sdp_2_silver.orders_silver")
@dp.expect("valid_notifications", "notifications IN ('Y','N')")         # WARN
@dp.expect_or_drop("valid_date", "order_timestamp > '2021-01-01'")      # DROP
@dp.expect_or_fail("valid_id", "customer_id IS NOT NULL")               # FAIL
def orders_silver():
    return (spark.readStream.table("sdp_1_bronze.orders_bronze")
              .select("order_id",
                      F.col("order_timestamp").cast("timestamp"),
                      "customer_id", "notifications"))

# 골드 구체화된 뷰
@dp.materialized_view(name="sdp_3_gold.gold_orders_by_date")
def gold_orders_by_date():
    return (spark.read.table("sdp_2_silver.orders_silver")
              .groupBy(F.to_date("order_timestamp").alias("order_date"))
              .count())
```

| SQL | Python |
|---|---|
| `CREATE OR REFRESH STREAMING TABLE` | `@dp.table` + `spark.readStream` |
| `CREATE OR REFRESH MATERIALIZED VIEW` | `@dp.materialized_view` + `spark.read` |
| `CREATE TEMPORARY VIEW` | `@dp.temporary_view` |
| `FROM STREAM t` | `spark.readStream.table("t")` |
| `EXPECT (...)` | `@dp.expect(...)` |
| `ON VIOLATION DROP ROW` | `@dp.expect_or_drop(...)` |
| `ON VIOLATION FAIL UPDATE` | `@dp.expect_or_fail(...)` |
| `${key}` | `spark.conf.get("key")` |

- 이전 DLT 시절에는 `import dlt`, `@dlt.table` 형태였다.
- 반환 DataFrame이 스트리밍(`readStream`)이면 스트리밍 테이블, 배치(`read`)면 구체화된 뷰가 된다.

---

## 18. DLT에서 SDP로: 구문 변경 정리

| 이전 (DLT, 계속 지원) | 현재 (SDP) |
|---|---|
| `CREATE OR REFRESH STREAMING LIVE TABLE` | `CREATE OR REFRESH STREAMING TABLE` |
| `CREATE OR REFRESH LIVE TABLE` | `CREATE OR REFRESH MATERIALIZED VIEW` |
| `CREATE LIVE VIEW` / `CREATE TEMPORARY LIVE VIEW` | `CREATE VIEW` / `CREATE TEMPORARY VIEW` |
| `FROM STREAM(LIVE.table)`, `LIVE.table` | `FROM STREAM table`, `table` |
| `APPLY CHANGES INTO` | `AUTO CDC INTO` |
| `import dlt`, `@dlt.table` | `from pyspark import pipelines as dp`, `@dp.table` |

- 의미와 기능은 같다. 이전 구문도 하위 호환으로 동작하지만 새 파이프라인은 새 구문을 쓴다.

---

## 19. 명령어 치트시트

```sql
-- ===== 데이터셋 정의 =====
CREATE OR REFRESH STREAMING TABLE b AS
SELECT *, current_timestamp() AS processing_time, _metadata.file_name AS source_file
FROM STREAM read_files("${source}/orders", format => 'JSON');

CREATE OR REFRESH STREAMING TABLE s (
  CONSTRAINT c1 EXPECT (x IN ('Y','N')),                          -- WARN
  CONSTRAINT c2 EXPECT (ts > '2021-01-01') ON VIOLATION DROP ROW, -- DROP
  CONSTRAINT c3 EXPECT (id IS NOT NULL) ON VIOLATION FAIL UPDATE  -- FAIL
)
COMMENT "설명"
TBLPROPERTIES ("quality" = "silver", "pipelines.reset.allowed" = false)
AS SELECT ... FROM STREAM b;

CREATE OR REFRESH MATERIALIZED VIEW g AS
SELECT k, count(*) FROM s GROUP BY k;                             -- STREAM 없음

CREATE TEMPORARY VIEW tv AS SELECT ...;                           -- 파이프라인 내부 전용
CREATE VIEW v AS SELECT ...;                                      -- UC 등록, 스트리밍 소스 불가

-- ===== 조인 =====
-- Stream-snapshot: 새 행만 정적 테이블과 조인 → ST
CREATE OR REFRESH STREAMING TABLE j AS
SELECT * FROM STREAM s JOIN lookup USING (key);
-- 두 ST 전체 조인 → MV
CREATE OR REFRESH MATERIALIZED VIEW j2 AS
SELECT * FROM s1 JOIN s2 USING (key);

-- ===== CDC =====
CREATE OR REFRESH STREAMING TABLE target;
CREATE FLOW cdc_flow AS
AUTO CDC INTO target
FROM STREAM source
KEYS (id)
APPLY AS DELETE WHEN operation = "DELETE"
SEQUENCE BY event_ts
COLUMNS * EXCEPT (operation)
STORED AS SCD TYPE 1;          -- 또는 SCD TYPE 2 (+ TRACK HISTORY ON ...)

-- ===== 모니터링 =====
SELECT * FROM my_catalog.my_schema.my_event_log WHERE event_type = 'flow_progress';
SELECT details:flow_progress:data_quality:expectations FROM my_event_log;
DESCRIBE HISTORY b;            -- STREAMING UPDATE, outputMode Append, numOutputRows
```

---

## 20. 자가 점검 문제

> 답을 가리고 먼저 생각해본 뒤 펼쳐보자.

**Q1.** SDP는 무엇이며, Lakeflow의 세 구성 요소 중 어느 역할을 하는가? 이전 이름은?

정답

SQL/Python으로 배치·스트리밍 파이프라인을 선언적으로 구축하는 프레임워크로, Lakeflow에서 변환(ETL)을 담당한다. 이전 이름은 Delta Live Tables(DLT).



**Q2.** 전통적인 배치 ETL(CREATE OR REPLACE TABLE + 일반 VIEW)의 한계 세 가지는?

정답

매 실행 시 모든 파일·행을 다시 읽고 처리한다(비용이 전체 데이터량에 비례). 일반 뷰는 조회할 때마다 재계산된다. 데이터 품질 검증과 모니터링(행 수, 리니지, UI)을 별도로 만들어야 한다.



**Q3.** Structured Streaming으로 직접 구현할 때 필요한 배관 코드는? SDP는 이를 어떻게 해결하는가?

정답

스트림마다 체크포인트 위치 관리, upsert를 위한 foreachBatch + MERGE, 트리거와 Auto Loader schemaLocation 구성, 스트림 수명 관리. 품질 규칙과 UI도 없다. SDP는 테이블 선언과 변환 로직만 쓰면 증분 처리, 체크포인트, 품질, UI를 자동 관리한다.



**Q4.** Streaming Table과 Materialized View의 차이는? 각각 어떤 레이어에 주로 쓰는가?

정답

ST는 append-only 소스에서 새 데이터만 증분 처리하며 브론즈·실버에 쓴다. MV는 현재 데이터 기준으로 결과를 재계산(서버리스에서 가능하면 증분 갱신)하며 집계·조인·골드에 쓴다.



**Q5.** 스트리밍 테이블에서 파일과 다른 테이블을 증분으로 읽는 구문은? MV에서는?

정답

파일: `FROM STREAM read_files(...)`, 테이블: `FROM STREAM 테이블명`. MV에서는 STREAM 없이 `FROM 테이블명`으로 참조한다.



**Q6.** MV를 증분 갱신할지 전체 재계산할지는 누가 결정하며, 어떤 compute에서 가능한가?

정답

비용 기반 옵티마이저가 결정하며, 증분 갱신은 서버리스 compute에서 지원된다.



**Q7.** Temporary View와 View의 차이와, 뷰의 제한 사항은?

정답

Temporary View는 파이프라인 실행 중에만 존재하고 UC에 등록되지 않는다. View는 UC에 등록되어 유지된다. 뷰는 UC 파이프라인에서만 쓸 수 있고, 스트리밍 쿼리를 가질 수 없으며 스트리밍 소스로 쓸 수 없다.



**Q8.** 파일 순서와 상관없이 파이프라인이 올바른 순서로 실행되는 이유는?

정답

SDP가 각 데이터셋의 FROM 절을 분석해 의존성을 자동으로 파싱하고 파이프라인 그래프(DAG)를 만들기 때문이다.



**Q9.** 파이프라인 configuration에 `source` 키를 설정했다. SQL과 Python에서 각각 어떻게 참조하는가? 쓰는 이유는?

정답

SQL은 `${source}`, Python은 `spark.conf.get("source")`. 값을 한 곳에서 관리하고 하드코딩을 없애며, 코드 수정 없이 환경을 바꿀 수 있다.



**Q10.** 서버리스와 클래식 파이프라인 compute의 차이는?

정답

서버리스는 인프라 관리가 필요 없고 MV 증분 갱신과 비용 기반 최적화를 지원한다(권장). 클래식은 고정 크기 클러스터로 생성 권한과 클러스터 정책이 필요하며, 향상된 자동 확장이 기본 활성화된다.



**Q11.** Dry run은 무엇을 하는가?

정답

테이블을 실제로 만들거나 갱신하지 않고 소스 코드의 오류(잘못된 테이블·컬럼 이름, 구문)를 검사하고 파이프라인 그래프를 보여준다.



**Q12.** 일반 실행과 전체 갱신(full refresh)은 스트리밍 테이블에 대해 어떻게 다른가? 전체 갱신을 막는 방법은?

정답

일반 실행은 새 레코드만 처리한다. 전체 갱신은 데이터를 지우고 체크포인트를 초기화해 소스의 모든 레코드를 재처리한다. 테이블 속성 `pipelines.reset.allowed = false`로 막는다.



**Q13.** 새 파일이 없는 상태에서 파이프라인을 다시 실행하면 스트리밍 테이블은 어떻게 되는가?

정답

체크포인트가 이미 처리한 파일을 알고 있으므로 새 행이 추가되지 않는다.



**Q14.** Expectations의 세 가지 동작과 구문, 각 결과는?

정답

WARN(기본, ON VIOLATION 생략): 행 유지·위반 기록. DROP(`ON VIOLATION DROP ROW`): 행 제외·기록. FAIL(`ON VIOLATION FAIL UPDATE`): 해당 플로우 실패, 그 배치는 기록되지 않음, 수동 개입 필요. `ON VIOLATION WARN` 키워드는 없다.



**Q15.** FAIL 제약이 위반되면 파이프라인의 다른 플로우도 멈추는가?

정답

아니다. 해당 플로우만 실패하고 독립된 다른 플로우에는 영향을 주지 않는다.



**Q16.** 삭제 레코드는 키 외의 값이 null인 CDC 피드에서, name이 null인 행을 경고하되 삭제 레코드는 통과시키려면?

정답

`CONSTRAINT valid_name EXPECT (name IS NOT NULL OR operation = "DELETE")`처럼 OR 조건으로 삭제 레코드를 허용한다.



**Q17.** 여러 실행에 걸친 데이터 품질 추이를 보려면 어디를 쿼리하는가?

정답

이벤트 로그. `event_type = 'flow_progress'`인 이벤트의 `details:flow_progress:data_quality:expectations`를 파싱한다.



**Q18.** 기존 `CREATE OR REPLACE TABLE`/`VIEW` 기반 ETL을 SDP로 바꿀 때 바꿔야 하는 두 가지는?

정답

CREATE 구문(ST 또는 MV로)과 FROM 절(스트리밍 테이블이면 STREAM 추가). 변환 로직은 그대로 둔다.



**Q19.** Stream-snapshot 조인, MV 조인, Stream-stream 조인을 비교하라.

정답

Stream-snapshot: 스트리밍 + 정적, 새 행만 정적 테이블 전체와 조인해 ST에 추가(참조 데이터 보강). MV 조인: 두 ST의 모든 행을 실행마다 조인(가능하면 증분 갱신). Stream-stream: 두 스트림의 새 행끼리 조인, 워터마크·윈도우가 필요한 고급 패턴.



**Q20.** Stream-snapshot 조인에서 정적 테이블이 나중에 바뀌면 이미 만들어진 결과는?

정답

갱신되지 않는다. 새로 들어오는 행만 조인 시점의 정적 테이블과 조인된다.



**Q21.** Triggered 모드와 Continuous 모드의 차이는?

정답

Triggered는 시작 시점의 데이터로 갱신하고 끝나면 중지한다(수동·스케줄, 배치, 비용 효율). Continuous는 계속 실행되며 새 데이터를 준실시간으로 처리한다(스트리밍, 신선도 중요).



**Q22.** 이벤트 로그의 기본 위치와, 권장 설정은?

정답

기본은 파이프라인 기본 카탈로그·스키마의 숨겨진 테이블 `event_log_{pipeline_id}`. 권장은 고급 설정에서 카탈로그·스키마·이름을 지정해 UC 테이블로 게시하는 것.



**Q23.** 이벤트 로그가 담는 네 가지 범주와, 분석에 가장 많이 쓰는 컬럼은?

정답

감사 로그, 데이터 품질 검사, 파이프라인 진행 상태, 데이터 리니지. 가장 많이 쓰는 컬럼은 JSON인 `details`.



**Q24.** CDC란 무엇이며, SCD Type 1과 Type 2의 차이는?

정답

소스의 삽입·업데이트·삭제를 추적·캡처해 대상에 적용하는 기법. Type 1은 덮어쓰기로 현재 상태만 유지하고 삭제는 행 제거. Type 2는 `__START_AT`, `__END_AT`으로 모든 버전을 보존하고 삭제는 비활성 표시.



**Q25.** SCD Type 2 테이블에서 현재 활성 행을 찾는 조건은?

정답

`__END_AT IS NULL`.



**Q26.** AUTO CDC INTO의 `KEYS`, `APPLY AS DELETE WHEN`, `SEQUENCE BY`, `COLUMNS * EXCEPT`, `STORED AS`의 역할은?

정답

KEYS: 기본 키. APPLY AS DELETE WHEN: 삭제로 처리할 조건. SEQUENCE BY: 변경 순서를 정하는 컬럼(늦게 온 레코드 정렬). COLUMNS * EXCEPT: 대상에서 제외할 컬럼. STORED AS: SCD Type 1 또는 2(기본 1).



**Q27.** 939행이 있는 SCD Type 1 대상에 UPDATE 12, DELETE 1, NEW 10이 들어오면 결과 행 수와 메트릭은?

정답

948행 (939 + 10 − 1). Upserted 22(업데이트 12 + 삽입 10), Deleted 1.



**Q28.** AUTO CDC INTO의 이전 이름과, MERGE INTO 대비 장점은?

정답

APPLY CHANGES INTO. foreachBatch + MERGE 같은 복잡한 코드 없이 증분 CDC 처리, 키·삭제 조건·순서 처리(늦게 온 레코드)·컬럼 제외·SCD 유형을 선언만으로 처리한다.



**Q29.** Python SDP에서 Expectations 세 동작에 해당하는 데코레이터는?

정답

`@dp.expect`(WARN), `@dp.expect_or_drop`(DROP), `@dp.expect_or_fail`(FAIL).



**Q30.** DLT의 `CREATE OR REFRESH LIVE TABLE`은 SDP에서 무엇이 되었는가?

정답

`CREATE OR REFRESH MATERIALIZED VIEW`.



---

## 21. 참고 링크

- Spark Declarative Pipelines: https://docs.databricks.com/aws/en/ldp/
- 모범 사례: https://docs.databricks.com/aws/en/ldp/best-practices
- 멀티 파일 편집기: https://docs.databricks.com/aws/en/ldp/multi-file-editor
- 파이프라인 파라미터: https://docs.databricks.com/aws/en/ldp/parameters
- 파이프라인 업데이트와 갱신 시맨틱: https://docs.databricks.com/aws/en/ldp/updates
- Triggered vs Continuous: https://docs.databricks.com/aws/en/ldp/pipeline-mode
- 서버리스 파이프라인: https://docs.databricks.com/aws/en/ldp/serverless
- 구체화된 뷰 증분 갱신: https://docs.databricks.com/aws/en/optimizations/incremental-refresh
- Expectations: https://docs.databricks.com/aws/en/ldp/expectations
- 파이프라인 테이블 속성: https://docs.databricks.com/aws/en/ldp/properties
- 이벤트 로그 모니터링: https://docs.databricks.com/aws/en/ldp/monitor-event-logs
- 이벤트 로그 스키마: https://docs.databricks.com/aws/en/ldp/monitor-event-log-schema
- AUTO CDC (CDC 처리): https://docs.databricks.com/aws/en/ldp/cdc
- 플로우: https://docs.databricks.com/aws/en/ldp/flows
- 싱크: https://docs.databricks.com/aws/en/ldp/sinks
- DLT 명칭 변경 안내: https://docs.databricks.com/aws/en/ldp/where-is-dlt
- Apache Spark Declarative Pipelines 프로그래밍 가이드: https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html
