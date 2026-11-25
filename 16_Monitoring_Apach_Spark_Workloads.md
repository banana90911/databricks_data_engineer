# Apache Spark 워크로드 모니터링과 최적화 — 핵심 개념 정리

---

## 목차

1. [Databricks 플랫폼과 레이크하우스](#1-databricks-플랫폼과-레이크하우스)
2. [런타임과 컴퓨트](#2-런타임과-컴퓨트)
3. [Apache Spark 4의 주요 변화](#3-apache-spark-4의-주요-변화)
4. [실행 구조와 Spark UI](#4-실행-구조와-spark-ui)
5. [오픈 테이블 형식과 Delta Lake](#5-오픈-테이블-형식과-delta-lake)
6. [트랜잭션 로그와 삭제 벡터](#6-트랜잭션-로그와-삭제-벡터)
7. [Delta 테이블 읽기, 쓰기, DML](#7-delta-테이블-읽기-쓰기-dml)
8. [테이블 검사: DESCRIBE DETAIL과 DESCRIBE HISTORY](#8-테이블-검사-describe-detail과-describe-history)
9. [시간 여행과 RESTORE](#9-시간-여행과-restore)
10. [MERGE로 업서트](#10-merge로-업서트)
11. [테이블 유지 관리: OPTIMIZE와 VACUUM](#11-테이블-유지-관리-optimize와-vacuum)
12. [데이터 레이아웃: 파티셔닝에서 Liquid 클러스터링으로](#12-데이터-레이아웃-파티셔닝에서-liquid-클러스터링으로)
13. [Apache Iceberg와 UniForm](#13-apache-iceberg와-uniform)
14. [Spark 성능의 구성 요소](#14-spark-성능의-구성-요소)
15. [파티셔닝: repartition과 coalesce](#15-파티셔닝-repartition과-coalesce)
16. [셔플](#16-셔플)
17. [Adaptive Query Execution](#17-adaptive-query-execution)
18. [조인 전략](#18-조인-전략)
19. [DataFrame 캐싱](#19-dataframe-캐싱)
20. [Catalyst와 실행 계획 읽기](#20-catalyst와-실행-계획-읽기)
21. [모니터링 툴킷과 진단 절차](#21-모니터링-툴킷과-진단-절차)
22. [종합 예제: 거래 분석 워크로드 튜닝](#22-종합-예제-거래-분석-워크로드-튜닝)
23. [모범 사례](#23-모범-사례)
24. [핵심 정리 치트시트](#24-핵심-정리-치트시트)
25. [자가 점검 문제](#25-자가-점검-문제)
26. [참고 링크](#26-참고-링크)

---

## 1. Databricks 플랫폼과 레이크하우스

### 1-1. Databricks의 배경

- 2013년 Apache Spark의 원래 창시자들이 설립한 통합 데이터 및 AI 플랫폼이다.
- 오픈 소스 기반(Apache Spark, Delta Lake, MLflow) 위에 구축되어 클라우드와 엔진 사이에서 데이터와 코드의 이식성을 보장한다.
- 주요 클라우드에서 같은 경험을 제공한다.
- Photon, 예측 최적화(Predictive Optimization) 같은 독자 기능으로 오픈 소스 Spark보다 성능을 높인다.

### 1-2. 레이크하우스

데이터 웨어하우스의 구조와 BI 성능, 데이터 레이크의 확장성과 모든 데이터 유형 지원을 하나로 합친 아키텍처다.

| 특징 | 내용 |
|---|---|
| 구조 + 확장성 | 웨어하우스 수준의 구조를 레이크 규모로 |
| 스토리지와 컴퓨트 분리 | 각각 독립적으로 확장 |
| 모든 데이터 유형 | 정형, 반정형, 비정형 |
| 배치와 스트리밍 통합 | 하나의 엔진과 테이블 |
| 개방형 형식의 ACID | Delta Lake 등 오픈 테이블 형식 |
| 내장 거버넌스 | Unity Catalog |

### 1-3. 메달리온 아키텍처

소스 시스템 → 브론즈 → 실버 → 골드 → BI, ML, 데이터 제품

| 계층 | 역할 | 특징 |
|---|---|---|
| 브론즈 | 이력을 포함한 소스 데이터의 동일 복사본 | 원시, 추가 위주, 스키마 보존. 레이크하우스에서 데이터 레이크에 해당 |
| 실버 | 정제, 표준화된 데이터 | 검증, 중복 제거, 집계 전 상태 |
| 골드 | 비즈니스 수준의 특성과 집계 | BI, ML, 데이터 제품에 바로 사용 |

### 1-4. 워크스페이스와 Unity Catalog

**워크스페이스**는 조직, 부서, 팀, 프로젝트를 위한 격리된 협업 환경이다. 컴퓨트(클러스터, SQL 웨어하우스), 노트북과 디렉터리, Lakeflow Jobs와 파이프라인, Unity Catalog 객체 접근, 접근 제어를 하나로 묶는다.

**Unity Catalog**는 여러 워크스페이스에 걸친 통합 거버넌스 계층이다.

| 기능 | 내용 |
|---|---|
| 단일 제어 지점 | 모든 데이터와 AI 자산 |
| 세분화된 접근 제어 | 테이블, 컬럼, 행 수준 |
| 리니지 | 자동 추적 |
| 감사와 규정 준수 | 중앙화 |
| 탐색과 공유 | 검색, Delta Sharing |
| 3단계 네임스페이스 | `catalog.schema.table` |

> 주의: 성능 최적화는 Unity Catalog의 역할이 아니다. 성능은 엔진(Catalyst, AQE, Photon)과 테이블 유지 관리 기능이 담당한다.

---

## 2. 런타임과 컴퓨트

**Databricks Runtime(DBR)** 은 Spark 핵심과 주변 구성 요소를 미리 패키징하고 테스트한 조합이다. 운영을 단순화하고 Databricks 성능 최적화를 포함한다. 각 릴리스는 특정 Spark 버전을 고정하며, **DBR 17.3 LTS에는 Apache Spark 4.0**이 들어 있다.

| 런타임 | 용도 |
|---|---|
| Standard | Databricks 최적화가 적용된 Apache Spark |
| Machine Learning | ML 라이브러리 사전 구성 |
| Photon 사용 | SQL과 DataFrame 성능을 위한 네이티브 벡터화 엔진 |

| 컴퓨트 | 적합한 용도 |
|---|---|
| 다목적(all-purpose) 클러스터 | 대화형 개발과 분석 |
| Job 클러스터 | 자동화된 운영 워크로드 |
| SQL 웨어하우스, 서버리스 | 클러스터 관리 불필요, 빠른 시작 |

> 주의: 서버리스 컴퓨트는 클러스터와 함께 **Spark UI를 노출하지 않는다.** Job, Stage, Task, 실행 계획 지표를 직접 들여다보며 튜닝하려면 클래식 컴퓨트 클러스터를 쓴다. 서버리스에서는 쿼리 프로필(Query Profile)로 실행 정보를 본다.

---

## 3. Apache Spark 4의 주요 변화

| 기능 | 의미 |
|---|---|
| **ANSI SQL 모드 기본 활성화** | 잘못된 형 변환과 산술 오버플로가 조용히 NULL을 반환하지 않고 **오류**를 낸다. 관대한 처리는 `try_cast` 등 `try_` 함수 |
| `VARIANT` 타입 | 반정형 JSON 유사 데이터를 빠르고 유연하게 저장 |
| SQL 스크립팅과 세션 변수 | 절차적 SQL 블록, `DECLARE`, `EXECUTE IMMEDIATE` |
| Spark Connect | 애플리케이션을 드라이버에서 분리하는 경량 클라이언트-서버 프로토콜 |
| 구조화된 로깅 | 드라이버와 Executor 로그를 JSON으로 출력해 Spark로 직접 쿼리 가능 |
| AQE | 기본 활성화 유지, 런타임 재최적화 계속 개선 |

구조화된 로깅은 모니터링 기능이다. 로그가 JSON이므로 DataFrame으로 읽어 오류와 경고를 같은 엔진으로 분석할 수 있다.

```python
# 로그 디렉터리 경로는 클러스터 로그 전달 설정에 따라 다르다
logs_df = spark.read.json("/Volumes/<catalog>/<schema>/<volume>/cluster-logs/*/driver/")
logs_df.filter("level IN ('ERROR', 'WARN')").groupBy("level", "logger").count().display()
```

### 3-1. 환경 확인

튜닝은 항상 같은 세 가지 질문으로 시작한다. **코어가 몇 개인가, 셔플 파티션 설정은 무엇인가, AQE가 켜져 있는가.**

```python
print(f"Apache Spark version: {spark.version}")
print(f"Default parallelism (cores): {sc.defaultParallelism}")
print(f"ANSI mode:          {spark.conf.get('spark.sql.ansi.enabled')}")
print(f"AQE:                {spark.conf.get('spark.sql.adaptive.enabled')}")
print(f"Shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")
```

> 주의: `sc`(SparkContext)는 클래식 컴퓨트의 전용(dedicated) 접근 모드에서 쓸 수 있다. 표준(공유) 접근 모드나 서버리스처럼 Spark Connect 기반 환경에서는 SparkContext와 RDD API를 쓸 수 없다.

---

## 4. 실행 구조와 Spark UI

### 4-1. Job, Stage, Task

| 단위 | 생기는 시점 | 개수 |
|---|---|---|
| Job | 액션(`display()`, `count()`, `write`) 하나마다 | 액션당 하나 이상 |
| Stage | Job을 **셔플 경계**에서 나눈 구간 | 셔플 수 + 1 |
| Task | Stage 안에서 파티션 하나를 처리 | Stage의 파티션 수 |

첫 Stage는 읽기와 좁은 변환을 수행하고, 셔플 뒤의 Stage는 셔플 파티션마다 Task 하나를 실행한다.

### 4-2. SQL과 DataFrame API는 같은 계획

```sql
SELECT product, SUM(quantity) AS units_sold, SUM(totalPrice) AS revenue
FROM samples.bakehouse.sales_transactions
GROUP BY product
ORDER BY revenue DESC;
```

```python
from pyspark.sql.functions import sum, desc

revenue_df = (spark.read.table("samples.bakehouse.sales_transactions")
    .groupBy("product")
    .agg(sum("quantity").alias("units_sold"), sum("totalPrice").alias("revenue"))
    .orderBy(desc("revenue")))
```

두 표현은 같은 물리 계획으로 컴파일되어 같은 종류의 Job을 만든다. 어떤 API로 작성하든 **같은 엔진을 모니터링하고 최적화한다.**

### 4-3. Spark UI 구성

노트북 셀 결과 아래의 Spark Jobs 표시에서 Job별로 열거나, 클러스터 상세 화면의 Spark UI에서 연다.

| 화면 | 보여 주는 것 |
|---|---|
| Jobs | 모든 액션, 소속 Stage, 소요 시간 |
| Stages | Task 단위 세부: Shuffle Read/Write, Spill, 스큐(Task 시간 분포) |
| Storage | 캐시된 DataFrame, 메모리 점유 |
| SQL / DataFrame | 실행된 쿼리 계획과 연산자별 런타임 지표(행 수, 시간) |
| Executors | Executor별 리소스 사용량, GC 시간, 실패 Task |

**SQL / DataFrame 화면이 가장 유용한 진단 지점**이다. 테이블 스캔, 조인, 집계가 그래프로 나타나고 각 단계를 지난 행 수가 표시된다.

---

## 5. 오픈 테이블 형식과 Delta Lake

### 5-1. 왜 테이블 형식이 필요한가

전통적인 데이터 레이크의 문제:

- 트랜잭션 보장이 없어 실패한 작업이 불완전한 데이터를 남긴다.
- 스키마 강제가 없어 잘못된 레코드가 다운스트림을 망친다.
- 작은 파일이 많으면 성능이 떨어진다.
- 데이터 품질 제어가 없다.
- 대규모 메타데이터 관리가 어렵다.

| 세대 | 예 | 특징 |
|---|---|---|
| 파일 형식 | JSON, CSV | 행 지향, 스키마 없음 |
| 컬럼 형식 | Parquet, ORC | 압축, 컬럼 기반이지만 트랜잭션 없음 |
| 오픈 테이블 형식 | Delta Lake, Apache Iceberg | ACID, 버전 관리, 스키마 진화 |

### 5-2. Delta Lake

| 무엇인가 | 주요 기능 | 효과 |
|---|---|---|
| 오픈 소스 스토리지 프레임워크 | 대규모 ACID 트랜잭션 | 데이터 불일치 제거 |
| Parquet 기반 | 스키마 강제와 진화 | 동시 읽기, 쓰기 |
| Spark 네이티브 통합 | 시간 여행 | 데이터 품질 강제 |
| Databricks의 기본 형식 | 배치와 스트리밍 통합 | 파이프라인 단순화 |
| | 확장 가능한 메타데이터 | 레이크 관리 부담 감소 |

Databricks에서 `CREATE TABLE`로 만든 테이블은 형식을 지정하지 않아도 Delta 테이블이다. `USING DELTA`나 `format("delta")`가 필요 없다.

---

## 6. 트랜잭션 로그와 삭제 벡터

모든 작업은 테이블 디렉터리의 **`_delta_log`** 를 거친다. 트랜잭션 로그는 테이블에 일어난 모든 변경의 순서 있는 기록이다(`00000.json`, `00001.json`, ... 형태의 JSON 커밋과 주기적인 체크포인트 Parquet).

| 작업 | 동작 |
|---|---|
| 쓰기 (`INSERT`, `UPDATE`, `DELETE`, `MERGE`) | 현재 버전과의 차이를 계산해 데이터 파일을 추가/제거 → 필요하면 새 Parquet 파일 작성 → `_delta_log`에 새 버전 커밋, 이것이 현재 버전이 된다 |
| 읽기 (`SELECT`) | `_delta_log`에서 요청 버전(기본은 최신)의 파일 목록(매니페스트)을 구함 → 그 파일들에서 데이터 반환 |

Parquet 파일은 데이터만 보관하고, **ACID, 버전 관리, 시간 여행은 트랜잭션 로그가 가능하게 한다.** 커밋은 원자적이어서 읽는 쪽은 커밋 전 상태나 후 상태만 보고, 중간 상태는 보지 않는다.

### 삭제 벡터

최신 런타임에서 `UPDATE`, `DELETE`, `MERGE`는 파일 전체를 다시 쓰지 않을 수 있다. **삭제 벡터(deletion vector)** 가 파일 안의 개별 행을 삭제된 것으로 표시해 행 수준 DML 비용을 크게 줄인다. 표시된 변경은 이후 `OPTIMIZE`(또는 `REORG TABLE ... APPLY (PURGE)`)가 깨끗한 파일로 다시 쓴다.

---

## 7. Delta 테이블 읽기, 쓰기, DML

| 작업 | Spark SQL | DataFrame API |
|---|---|---|
| 읽기 | `SELECT * FROM t` | `spark.read.table("t")` |
| 생성 | `CREATE TABLE t AS SELECT ...` | `df.write.saveAsTable("t")` |
| 추가 | `INSERT INTO t ...` | `df.write.mode("append").saveAsTable("t")` |
| 덮어쓰기 | `INSERT OVERWRITE t ...` | `df.write.mode("overwrite").saveAsTable("t")` |

일반 Parquet 레이크에는 없는 작업:

| 작업 | 설명 |
|---|---|
| `UPDATE`, `DELETE` | 조건 기반 행 수준 수정 |
| `MERGE` | 업데이트, 삽입, 삭제를 하나의 원자적 문장으로 |
| 스키마 진화 | 컬럼 추가, 변경 (`CREATE OR REPLACE TABLE` 포함) |
| 시간 여행 | `VERSION AS OF`, `TIMESTAMP AS OF` |
| `RESTORE` | 이전 버전으로 롤백 |

```sql
-- 버전 0: 작업용 복사본
CREATE OR REPLACE TABLE transactions_delta AS
SELECT * FROM samples.bakehouse.sales_transactions;

-- 버전 1: 테스트 행 삽입
INSERT INTO transactions_delta VALUES
  (9999991, 2000001, 3000001, current_timestamp(), 'Outback Oatmeal',   10, 3, 30, 'visa',       1234567890123456),
  (9999992, 2000002, 3000002, current_timestamp(), 'Golden Gate Ginger', 5, 3, 15, 'mastercard', 2345678901234567);

-- 버전 2: 수정
UPDATE transactions_delta SET quantity = 12, totalPrice = 36
WHERE transactionID = 9999991;

-- 버전 3: 삭제
DELETE FROM transactions_delta WHERE transactionID IN (9999991, 9999992);
```

**커밋된 작업 하나는 영향받은 행 수와 상관없이 정확히 한 버전을 만든다.**

---

## 8. 테이블 검사: DESCRIBE DETAIL과 DESCRIBE HISTORY

테이블 문제를 진단할 때 가장 먼저 보는 두 명령이다.

| 명령 | 보여 주는 것 | 주요 컬럼 |
|---|---|---|
| `DESCRIBE DETAIL t` | 테이블의 현재 물리 상태 | `numFiles`, `sizeInBytes`, `location`, `clusteringColumns`, `partitionColumns`, `properties` |
| `DESCRIBE HISTORY t` | 트랜잭션 로그의 버전 이력 | `version`, `timestamp`, `operation`, `operationParameters`, `operationMetrics`, `userName` |

```sql
-- 이력을 쿼리처럼 다룰 수 있다
SELECT version, timestamp, operation, operationMetrics
FROM (DESCRIBE HISTORY transactions_delta)
ORDER BY version;
```

```python
spark.sql("DESCRIBE DETAIL transactions_bronze").select("numFiles", "sizeInBytes").display()
```

- `operationMetrics`는 작업별로 영향받은 행과 파일 수(예: `numUpdatedRows`, `numTargetRowsInserted`, `numAddedFiles`)를 보여 줘 파이프라인 동작 감사에 쓴다.
- `numFiles`가 크기에 비해 많으면 작은 파일 문제를 의심한다.

---

## 9. 시간 여행과 RESTORE

```sql
-- 이전 버전과 현재 비교
SELECT
  (SELECT COUNT(*) FROM transactions_delta VERSION AS OF 1) AS rows_at_version_1,
  (SELECT COUNT(*) FROM transactions_delta)                 AS rows_current;

-- 타임스탬프로 조회
SELECT * FROM transactions_delta TIMESTAMP AS OF '2026-06-01T00:00:00';

-- 테이블 전체를 롤백
RESTORE TABLE transactions_delta TO VERSION AS OF 1;
```

```python
spark.read.option("versionAsOf", 1).table("transactions_delta")
spark.read.option("timestampAsOf", "2026-06-01").table("transactions_delta")
```

| | 시간 여행 | `RESTORE` |
|---|---|---|
| 목적 | 과거 상태를 **조회** | 테이블을 과거 상태로 **되돌림** |
| 로그 | 변화 없음 | **새 버전으로 커밋** |

`RESTORE`는 이력을 지우거나 덮어쓰지 않는다. 롤백한 작업을 포함한 전체 이력이 남아 감사 가능성이 유지된다. 버전 0, 1(UPDATE), 2(DELETE)가 있는 테이블을 버전 1로 복원하면 버전 3이 RESTORE 커밋으로 추가된다.

---

## 10. MERGE로 업서트

`MERGE`는 조인 조건으로 소스 행을 타깃과 맞춘 뒤 업데이트, 삽입, (선택적으로) 삭제를 **하나의 원자적 커밋**으로 적용한다. Spark SQL에는 별도의 `UPSERT` 문이 없다. CDC 피드 적용, SCD, 중복 제거 같은 증분 파이프라인의 핵심이다.

```sql
CREATE OR REPLACE TEMPORARY VIEW customer_updates AS
SELECT * FROM VALUES
  (2000262, 'Jennifer', 'Forbes', 'belldonna@example.com', '633-427-4977',
   '12 New Harbour Esplanade', 'Docklands', 'Victoria', 'Australia', 'Oceania', 3008, 'female'),
  (2999999, 'Marcus', 'Webb', 'marcus.webb@example.com', '555-014-2210',
   '88 Collins Street', 'Melbourne', 'Victoria', 'Australia', 'Oceania', 3000, 'male')
AS t(customerID, first_name, last_name, email_address, phone_number,
     address, city, state, country, continent, postal_zip_code, gender);

MERGE INTO customers_delta AS target
USING customer_updates AS source
ON target.customerID = source.customerID
WHEN MATCHED THEN UPDATE SET *          -- 기존 고객: 주소 변경
WHEN NOT MATCHED THEN INSERT *;         -- 신규 고객: 삽입
```

| 절 | 대상 |
|---|---|
| `WHEN MATCHED [AND 조건] THEN UPDATE SET ...` / `DELETE` | ON 조건이 일치한 행 |
| `WHEN NOT MATCHED [BY TARGET] THEN INSERT ...` | 소스에만 있는 행 |
| `WHEN NOT MATCHED BY SOURCE THEN UPDATE` / `DELETE` | 타깃에만 있는 행 |

- 업데이트와 삽입은 **두 버전이 아니라 한 버전**으로 커밋된다. 읽는 쪽은 둘 중 하나만 반영된 상태를 보지 않는다.
- 결과 지표(`num_updated_rows`, `num_inserted_rows` 등)로 기대한 행 수가 반영됐는지 확인한다.

> 주의: 소스에 같은 키의 행이 여러 개 있어 타깃 한 행에 여러 소스 행이 일치하면 `MERGE`는 오류를 낸다. 소스를 먼저 키 기준으로 중복 제거한다.

---

## 11. 테이블 유지 관리: OPTIMIZE와 VACUUM

잦은 작은 쓰기는 테이블을 수많은 작은 파일로 흩어 놓는다. 파일을 열고 메타데이터를 처리하는 오버헤드 때문에 이후 모든 쿼리가 느려진다. **엔진 튜닝만으로는 단편화된 테이블을 고칠 수 없다.**

| 명령 | 목적 |
|---|---|
| `OPTIMIZE t` | 작은 파일을 더 적고 큰 파일로 압축. 결과에 `filesAdded`, `filesRemoved` 보고 |
| `VACUUM t` | 로그가 더 이상 참조하지 않고 보존 기간이 지난 데이터 파일을 **영구 삭제** |

```python
spark.sql("DESCRIBE DETAIL transactions_bronze").select("numFiles", "sizeInBytes").display()   # 예: 48개
spark.sql("OPTIMIZE transactions_bronze")
spark.sql("DESCRIBE DETAIL transactions_bronze").select("numFiles", "sizeInBytes").display()   # 예: 1개
```

```sql
-- 삭제될 파일을 미리 보기만 한다
VACUUM transactions_bronze RETAIN 168 HOURS DRY RUN;
```

- `UPDATE`, `DELETE`, `RESTORE`, `OPTIMIZE`는 시간 여행을 위해 옛 파일을 남겨 둔다. `VACUUM`이 이것을 정리한다.
- 기본 보존 기간은 **7일(168시간)** 이며, 더 짧게 지정하면 안전 검사가 막는다.

> 주의: `VACUUM`은 시간 여행을 영구히 끊는다. 보존 기간보다 오래된 버전이 참조하는 파일이 삭제되므로 그 버전은 더 이상 조회하거나 복원할 수 없다. 보존 기간을 줄이기 전에 신중히 판단한다. 실행 중인 장기 쿼리나 스트림이 옛 파일을 읽고 있을 수도 있다.

### 자동화

| 기능 | 내용 |
|---|---|
| 최적화된 쓰기 (optimized writes) | 쓰기 시 파일 크기를 맞춤. 새 테이블에서 기본 활성 |
| 자동 압축 (auto compaction) | 쓰기 후 작은 파일을 자동 압축 |
| **예측 최적화** | Unity Catalog 관리형 테이블에 대해 사용 패턴을 보고 `OPTIMIZE`, `VACUUM`, 통계 수집을 자동 실행 |

운영 테이블에서는 이 기능들이 대부분을 처리하지만, 느린 테이블을 진단하려면 동작 원리를 알아야 한다.

---

## 12. 데이터 레이아웃: 파티셔닝에서 Liquid 클러스터링으로

파일이 어떻게 정리되어 있느냐에 따라 쿼리가 건너뛸 수 있는 데이터 양(데이터 스키핑)이 결정된다. 권장 방식은 크게 바뀌었고, 예전 Spark 자료의 기법 상당수가 이제 레거시다.

| 전략 | 상태 | 비고 |
|---|---|---|
| Hive 스타일 파티셔닝 (`PARTITIONED BY`) | 레거시 | 고정 디렉터리 레이아웃. 과도한 파티셔닝과 작은 파일 문제 |
| `OPTIMIZE ... ZORDER BY` | 레거시 | 다중 컬럼 클러스터링 가능하나 수동 실행, 전체 재작성 |
| **Liquid 클러스터링** (`CLUSTER BY`) | 권장 | 증분 클러스터링, 테이블 재작성 없이 키 변경 |
| **`CLUSTER BY AUTO`** | 권장 | 쿼리 패턴을 보고 Databricks가 키를 자동 선택하고 조정 (예측 최적화와 함께) |

```sql
-- 생성 시
CREATE TABLE transactions (...) CLUSTER BY (dateTime, franchiseID);

-- 기존 테이블에 적용 또는 키 변경 후 OPTIMIZE로 클러스터링 수행
ALTER TABLE transactions_bronze CLUSTER BY (product);
OPTIMIZE transactions_bronze;

-- 자동 선택
ALTER TABLE transactions_bronze CLUSTER BY AUTO;
```

```python
spark.sql("DESCRIBE DETAIL transactions_bronze").select("clusteringColumns", "numFiles").display()
```

- 클러스터링 키는 자주 필터하거나 조인하는 컬럼으로 정한다. 키 값이 없는 파일은 쿼리가 건너뛴다.
- 새 Unity Catalog 테이블은 Liquid 클러스터링(또는 `CLUSTER BY AUTO`)으로 시작한다. 물리적 파티셔닝은 카디널리티가 낮은 자연스러운 경계를 가진 매우 큰 테이블에만 고려한다.
- Liquid 클러스터링은 Hive 파티셔닝, ZORDER와 함께 쓸 수 없다.

---

## 13. Apache Iceberg와 UniForm

Apache Iceberg는 Netflix에서 시작된 또 다른 주요 오픈 테이블 형식이다. ACID와 스키마 진화, 숨겨진 파티셔닝과 파티션 진화, 브랜치와 태그를 지원한다.

Databricks에서 두 형식은 경쟁이 아니라 상호 운용 관계다.

- Unity Catalog는 **관리형 Iceberg 테이블**을 지원하고, 외부 Iceberg 엔진이 읽고 쓸 수 있다.
- **UniForm**을 켠 Delta 테이블은 Iceberg 메타데이터도 함께 생성해 Iceberg 클라이언트가 같은 위치에서 바로 읽는다.
- Spark 4는 Delta와 같은 DataFrame, SQL API로 Iceberg를 읽고 쓴다.

---

## 14. Spark 성능의 구성 요소

Spark 애플리케이션의 시간은 **컴퓨트, 데이터 이동, I/O, 조정** 네 영역에서 쓰인다. 최적화는 어느 영역이 워크로드를 지배하는지 찾는 것에서 시작한다.

| 영향 요소 | 예 |
|---|---|
| 리소스 활용 | CPU, 메모리, 네트워크, 디스크 I/O |
| 데이터 특성 | 크기, 형식, 분포 |
| 설정 | Spark 구성, 클러스터 크기와 유형 |

| 흔한 병목 | 증상 |
|---|---|
| 데이터 스큐, 부적절한 파티셔닝 | 한 Stage에서 소수 Task만 오래 걸림 |
| 과도한 셔플 | Shuffle Read/Write가 입력보다 큼 |
| 메모리 부족, GC | Spill 발생, Executors 화면의 GC 시간 증가, OOM |
| 작은 파일 | 스캔 Task가 매우 많고 짧음 |

---

## 15. 파티셔닝: repartition과 coalesce

### 15-1. 파티션의 출처

파티션은 병렬 처리의 단위다. **모든 Stage는 파티션당 Task 하나를 실행한다.** 파티셔닝은 Executor당 메모리, 조인과 집계 효율, 네트워크 트래픽을 좌우한다.

| 종류 | 결정 요인 |
|---|---|
| 초기 파티션 | 읽는 테이블의 파일과 블록 (`spark.sql.files.maxPartitionBytes`, 기본 128MB) |
| 셔플 파티션 | 넓은 변환(`groupBy`, `join` 등)이나 명시적 `repartition` / `coalesce` |

기본 파티셔너는 해시 파티셔너 `hash(key) % numPartitions`다.

좁은 변환(`filter` 등)은 **파티션 수를 바꾸지 않는다.** 각 출력 파티션이 입력 파티션 하나에만 의존하기 때문이다. 하지만 필터 결과는 파티션마다 남는 행 수가 달라 **필터 후에 스큐가 생길 수 있다.**

```python
from pyspark.sql.functions import col, spark_partition_id

def count_partitions(df):
    # 데이터가 있는 파티션 수를 센다
    return df.select(spark_partition_id().alias("partition_id")).distinct().count()

transactions_df = spark.read.table("samples.bakehouse.sales_transactions")
print(count_partitions(transactions_df))                                # 초기 파티션
print(count_partitions(transactions_df.filter(col("totalPrice") >= 100)))  # 필터 후에도 대체로 같음
```

> 주의: `spark_partition_id()`로 세는 방식은 **행이 있는 파티션**만 센다. 비어 있는 파티션은 빠진다. 클래식 전용 접근 모드에서는 `df.rdd.getNumPartitions()`로 계획상 파티션 수를 바로 볼 수 있다(Spark Connect 환경에서는 RDD API를 쓸 수 없다).

### 15-2. repartition vs coalesce

| 메서드 | 동작 | 계획에 나타나는 연산자 |
|---|---|---|
| `repartition(n)` | 전체 셔플로 정확히 n개의 **균등한** 파티션 | `Exchange RoundRobinPartitioning(n)` |
| `repartition(n, col)` | 키 기준으로 같은 값을 같은 파티션에 모음 | `Exchange hashpartitioning(col, n)` |
| `coalesce(n)` | 셔플 없이 기존 파티션을 합쳐 수를 줄임. 불균형을 그대로 받아들임 | `Coalesce n` (Exchange 없음) |

```python
card_df = transactions_df.filter(col("totalPrice") >= 50).repartition(8)  # 8개로 균등 재분배
compact_df = card_df.coalesce(2)                                          # 셔플 없이 2개로 축소
card_df.explain()
compact_df.explain()
```

- 파티션 수를 싸게 **줄일** 때는 `coalesce`, 균등 분배나 특정 키 레이아웃이 필요할 때는 `repartition`을 쓴다.
- `repartition`은 늘리기와 줄이기 모두 가능하지만 항상 셔플한다. `coalesce`는 늘리지 못한다.

> 주의: `coalesce`는 셔플 경계가 없어서 앞 단계까지 같은 Stage로 묶인다. `coalesce(1)`을 무거운 계산 바로 뒤에 붙이면 그 계산 전체가 Task 하나로 실행될 수 있다. 이럴 때는 셔플 비용을 치르더라도 `repartition`이 빠를 수 있다.

### 15-3. 크기 지침

| 지침 | 내용 |
|---|---|
| 셔플 파티션 크기 | 대략 100~200MB 수준이 병렬성과 Task 오버헤드의 균형점 |
| 파티션 수 ≥ 코어 수 | 그렇지 않으면 코어가 논다 |
| Task가 매우 짧음 (약 50ms 미만) | 파티션이 너무 많다는 신호 |
| Task가 매우 길거나 Spill 발생 | 파티션이 너무 크다는 신호 |

AQE가 켜진 Spark 4에서는 실제 데이터 크기에 맞춰 셔플 파티션이 자동으로 병합되므로 `spark.sql.shuffle.partitions`를 수동으로 맞추는 일은 대부분 필요 없다. AQE 병합의 목표 크기는 `spark.sql.adaptive.advisoryPartitionSizeInBytes`(기본 64MB)로 조정한다.

---

## 16. 셔플

셔플은 Spark에서 가장 비싼 작업이다. 데이터를 직렬화해 디스크에 쓰고, 네트워크로 보낸 뒤, 다음 Stage에서 다시 읽는다.

- 넓은 변환(`groupBy`, `join`, `distinct`, `orderBy`)과 `repartition`이 셔플을 일으킨다.
- 모든 셔플은 **Stage 경계**다. 앞 Stage 전체가 끝나야 다음 Stage가 시작한다.
- **Spill**(메모리에서 디스크로 넘침)은 중요한 경고 신호다.

| Stages 화면 지표 | 의미 |
|---|---|
| Shuffle Write | 이 Stage가 다음 Stage를 위해 쓴 데이터 양 |
| Shuffle Read | 이 Stage가 앞 Stage에서 읽어 온 데이터 양 |
| Spill (Memory) | 메모리에서 넘친 데이터의 메모리상 크기 |
| Spill (Disk) | 디스크에 쓴 크기 (직렬화, 압축 후) |

이 네 지표로 대부분의 느린 Stage를 진단할 수 있다.

셔플 영향을 줄이는 방법:

- 일찍, 자주 필터해 이후 모든 Stage를 지나는 데이터를 줄인다.
- 넓은 연산 전에 필요한 컬럼만 선택한다.
- 작은 테이블은 브로드캐스트 조인으로 큰 쪽 셔플을 없앤다.
- 가능하면 연산들 사이에 같은 파티셔닝을 유지해 재셔플을 피한다.

---

## 17. Adaptive Query Execution

AQE는 **완료된 셔플 Stage의 실제 통계**로 남은 쿼리 계획을 런타임에 다시 최적화한다. Spark 4에서 기본 활성(`spark.sql.adaptive.enabled=true`)이다.

정적 계획(Catalyst) → 셔플 Stage 실행 → 실제 셔플 크기 수집 → AQE가 남은 계획 재최적화

| 최적화 | 해결하는 문제 | 관련 설정 |
|---|---|---|
| 셔플 파티션 병합 | 작은 셔플 파티션이 너무 많아 Task 오버헤드 낭비 | `spark.sql.adaptive.coalescePartitions.enabled` |
| 스큐 조인 처리 | 거대한 파티션 하나가 Stage 전체를 지연 → 쪼개서 처리 | `spark.sql.adaptive.skewJoin.enabled` |
| 동적 조인 전략 전환 | 실제로는 작아서 브로드캐스트했어야 할 Sort Merge Join을 전환 | `spark.sql.adaptive.autoBroadcastJoinThreshold` |

```python
grouped_df = transactions_df.groupBy("product").count()
grouped_df.collect()
print(spark.conf.get("spark.sql.shuffle.partitions"))   # 설정값 200
print(count_partitions(grouped_df))                     # 실제로는 훨씬 적음
```

집계 결과가 작으면 AQE가 설정된 200개를 소수의 파티션으로 병합한다. 실행된 계획(SQL / DataFrame 화면)에서 **`AQEShuffleRead`** 노드가 coalesced로 표시된다. 계획 상단의 `AdaptiveSparkPlan isFinalPlan=true`는 AQE가 확정한 최종 계획이라는 뜻이다(`explain()`을 실행 전에 부르면 `isFinalPlan=false`인 초기 계획이 나온다).

`spark.sql.shuffle.partitions`는 AQE 아래에서 최종 값이 아니라 **시작점(상한)** 이다. 예전에 손으로 하던 셔플 파티션 수 고정, 스큐 키 수동 분할(salting), 조인 순서 강제 같은 작업의 상당 부분을 AQE가 대신한다. 할 일은 Spark UI에서 AQE가 동작했는지 확인하고, AQE가 도울 수 없는 영역(초기 읽기 레이아웃, 테이블 파일 상태, 캐싱 결정)에만 직접 개입하는 것이다.

> Databricks는 `spark.sql.shuffle.partitions`를 `auto`로 두어 초기 셔플 파티션 수까지 자동으로 정하는 옵션(자동 최적화 셔플)도 제공한다.

---

## 18. 조인 전략

조인은 셔플 비용이 몰리는 지점이다. Spark가 고르는 조인 전략은 다른 거의 모든 계획 결정보다 성능에 큰 영향을 준다.

| 전략 | 사용 조건 | 비용 |
|---|---|---|
| **Broadcast Hash Join** | 한쪽이 브로드캐스트 임계값 이하 | 작은 쪽을 모든 Executor에 복사. 큰 쪽은 **셔플도 정렬도 없이** 제자리에서 조인 |
| **Sort Merge Join** | 양쪽 모두 큼 | 양쪽 셔플 + 정렬. 비싼 기본값 |
| Shuffle Hash Join | 한쪽이 적당히 작음 | 양쪽 셔플, 작은 쪽을 해시 테이블로 |

```python
from pyspark.sql.functions import broadcast, sum

franchises_df = spark.read.table("samples.bakehouse.sales_franchises")

revenue_by_city_df = (transactions_df
    .join(broadcast(franchises_df), "franchiseID")
    .groupBy("city")
    .agg(sum("totalPrice").alias("revenue")))

revenue_by_city_df.explain()   # BroadcastHashJoin, 작은 쪽에만 BroadcastExchange
```

- 자동 브로드캐스트 임계값은 `spark.sql.autoBroadcastJoinThreshold`(기본 10MB)다. 이보다 작다고 추정되면 힌트 없이도 브로드캐스트한다.
- `broadcast()` 힌트는 **작은 쪽**에 건다. 통계가 부정확해 옵티마이저가 잘못 판단할 때 특히 중요하다.
- AQE는 실제 크기를 본 뒤 계획된 Sort Merge Join을 런타임에 Broadcast Join으로 바꿀 수 있다.
- 조인 전에 필요한 컬럼만 선택하고, Stages 화면의 Task 시간 분포로 조인 키 스큐를 확인한다.

> 주의: 브로드캐스트 대상은 드라이버로 모았다가 모든 Executor에 복사되므로 메모리에 충분히 들어가야 한다. 큰 테이블에 힌트를 강제하면 드라이버나 Executor 메모리 부족이 날 수 있다.

---

## 19. DataFrame 캐싱

캐싱은 계산된 DataFrame을 Executor 메모리(와 디스크)에 보관해 반복 사용 시 재계산을 건너뛴다.

| 캐싱이 도움 되는 경우 | 캐싱이 해로운 경우 |
|---|---|
| 같은 변환 결과에 여러 액션 수행 | 한 번만 쓰는 DataFrame |
| 비싼 상류 변환(조인, 집계)을 여러 출력이 공유 | 원본 테이블을 그대로 읽는 경우 (Delta 캐시, 디스크 캐시가 이미 도움) |
| 대화형 분석, 반복적인 ML | 메모리가 빠듯한 클러스터 |

```python
from pyspark.sql.functions import sum, avg, desc

enriched_df = (transactions_df.join(franchises_df, "franchiseID")
    .select("transactionID", "product", "quantity", "totalPrice",
            "paymentMethod", "city", "country", "size"))

enriched_df.cache()                                    # 표시만 한다 (지연)
print(f"Rows cached: {enriched_df.count()}")           # 첫 액션이 캐시를 채운다

display(enriched_df.groupBy("country").agg(sum("totalPrice").alias("revenue")))
display(enriched_df.groupBy("size").agg(avg("totalPrice").alias("avg_ticket")))
display(enriched_df.groupBy("product").count().orderBy(desc("count")).limit(5))

enriched_df.unpersist()                                # 다 쓰면 해제
```

- `cache()`는 **지연 평가**다. 호출 순간에는 아무것도 저장되지 않고, 첫 액션이 계산할 때 채워진다. 그래서 `count()`로 명시적으로 채우는 경우가 많다.
- DataFrame의 `cache()`는 `persist(StorageLevel.MEMORY_AND_DISK)`와 같다. 다른 저장 수준은 `persist()`로 지정한다.
- 캐시된 데이터를 읽는 쿼리의 계획에는 **`InMemoryTableScan`** 이 나타난다. Storage 화면에서 크기와 메모리 캐시 비율(Fraction Cached)을 본다.
- `count()`는 모든 파티션을 계산하므로 캐시를 완전히 채운다. `limit()`이나 `show()`처럼 일부 파티션만 계산하는 액션은 캐시를 일부만 채울 수 있다.

> 주의: 캐싱은 공짜가 아니다. 캐시 블록은 셔플, 실행 메모리와 경쟁하므로 한 번만 쓰는 DataFrame을 캐시하면 오히려 느려진다. 신중히 캐시하고, Storage 화면에서 확인하고, 다 쓰면 `unpersist()`한다. 표준 접근 모드나 서버리스에서는 `cache()`/`persist()` 지원이 제한될 수 있다.

---

## 20. Catalyst와 실행 계획 읽기

### 20-1. Catalyst가 자동으로 하는 일

Catalyst는 SQL이든 DataFrame이든 모든 쿼리를 실행 전에 최적화된 물리 계획으로 다시 쓴다.

| 최적화 | 내용 |
|---|---|
| 조건자 푸시다운 (predicate pushdown) | 필터를 스캔 단계로 내려 소스에서 읽는 데이터를 줄임 |
| 컬럼 프루닝 (column pruning) | 컬럼형 형식에서 참조된 컬럼만 읽음 |
| 필터 결합 | 여러 필터를 하나의 연산으로 |
| 동적 파일 프루닝 (dynamic file pruning) | 런타임에 조인 결과를 이용해 Delta 테이블 파일을 건너뜀 |

### 20-2. 부주의한 쿼리도 효율적으로 바뀐다

```python
careless_df = (transactions_df
    .join(franchises_df, "franchiseID")        # 전부 조인하고
    .filter(col("country") == "US")            # 마지막에 필터
    .filter(col("paymentMethod") == "visa")
    .select("transactionID", "product", "totalPrice", "city"))

careless_df.explain("formatted")
```

코드에서는 필터가 맨 뒤에 있지만 계획에서는 각 테이블 **스캔 단계**에 필터가 붙어 있다(`PushedFilters` 또는 스캔 연산자의 필터 조건). 스캔은 쿼리가 참조하는 몇 개 컬럼만 읽는다(`ReadSchema`). 그래서 **코드가 아니라 계획이 보여 주는 것을 최적화해야 한다.** 손으로 최적화하기 전에 `explain()`부터 본다.

### 20-3. explain 모드

| 호출 | 보여 주는 것 |
|---|---|
| `df.explain()` | 물리 계획 |
| `df.explain("formatted")` | 개요 + 연산자별 세부 (읽기 가장 쉬움) |
| `df.explain("extended")` | 파싱된 논리, 분석된 논리, 최적화된 논리, 물리 계획 |
| `df.explain("cost")` | 논리 계획 + 통계(크기 추정) |
| `df.explain("codegen")` | 생성된 코드 |

SQL에서는 `EXPLAIN FORMATTED SELECT ...`를 쓴다.

### 20-4. 계획에서 찾을 것

| 연산자 | 의미 |
|---|---|
| `Exchange hashpartitioning` / `RoundRobinPartitioning` | 셔플 |
| `BroadcastExchange` + `BroadcastHashJoin` | 브로드캐스트 조인 |
| `SortMergeJoin` (+ 양쪽 `Sort`, `Exchange`) | 정렬 병합 조인 |
| `HashAggregate` (partial / final) | 셔플 전후 2단계 집계 |
| `Coalesce` | 셔플 없는 파티션 축소 |
| `InMemoryTableScan` | 캐시에서 읽음 |
| `AQEShuffleRead` | AQE가 셔플 파티션을 병합하거나 분할 |
| `PushedFilters`, `ReadSchema` | 푸시다운된 필터, 읽는 컬럼 |
| `PhotonScan`, `PhotonGroupingAgg` 등 | Photon이 실행하는 연산자 |

Photon은 같은 Catalyst 계획을 더 빠르게 실행하는 벡터화 네이티브 엔진이다. 연산자 이름이 Photon으로 바뀔 뿐 최적화 원칙은 같다.

---

## 21. 모니터링 툴킷과 진단 절차

모든 최적화 주장은 측정으로 검증한다.

| 도구 | 제공 정보 |
|---|---|
| Spark UI Jobs / Stages | 시간이 쓰인 곳, Shuffle Read/Write, Spill, Task 스큐 |
| Spark UI SQL / DataFrame | 연산자별 행 수와 시간이 표시된 실행 계획 |
| Spark UI Storage | 캐시된 항목과 사용 메모리 |
| Spark UI Executors | 리소스 사용, GC 시간, Executor별 실패 Task |
| `explain()` | 실행 전 계획: 조인 전략, 푸시다운 필터 |
| `DESCRIBE DETAIL` / `DESCRIBE HISTORY` | 테이블 파일 레이아웃, 작업 지표 |
| 구조화된 로깅 | JSON 드라이버/Executor 로그를 Spark로 쿼리 |
| 쿼리 프로필 | SQL 웨어하우스, 서버리스에서의 연산자별 지표 |

### 진단 절차

1. **SQL / DataFrame 화면**에서 느린 쿼리를 찾고, 그 안에서 시간이 가장 많이 든 연산자를 찾는다.
2. 해당 **Stage**로 내려가 Shuffle Read/Write, Spill, Task 시간 분포(최댓값 vs 중앙값)를 본다.
3. 원인을 분류한다.

| 관찰 | 가능한 원인 | 대응 |
|---|---|---|
| 스캔 Task가 매우 많고 짧음, `numFiles` 큼 | 작은 파일 | `OPTIMIZE`, 자동 압축, 예측 최적화 |
| 필터가 있는데 스캔 행 수가 그대로 | 데이터 스키핑 실패 | Liquid 클러스터링 키 조정 |
| 최대 Task 시간이 중앙값보다 훨씬 김 | 스큐 | AQE 스큐 조인 확인, 키 정리 |
| Shuffle이 입력보다 큼 | 늦은 필터/프로젝션, 큰 쪽 셔플 | 일찍 필터/선택, 브로드캐스트 |
| Spill, GC 시간 큼 | 파티션이 너무 큼, 메모리 부족 | 파티션 늘림, 캐시 정리, 메모리 큰 노드 |
| 같은 조인/스캔이 반복 | 재계산 | 캐시 |

4. 그다음에만 코드나 설정을 바꾸고, **바꾼 뒤 반드시 다시 측정한다.**

---

## 22. 종합 예제: 거래 분석 워크로드 튜닝

```python
from pyspark.sql.functions import col, spark_partition_id, broadcast, sum, avg, desc

# 1) 기준선 기록
transactions_df = spark.read.table("samples.bakehouse.sales_transactions")
franchises_df = spark.read.table("samples.bakehouse.sales_franchises")

def count_partitions(df):
    return df.select(spark_partition_id().alias("partition_id")).distinct().count()

print(f"Default parallelism:     {sc.defaultParallelism}")
print(f"Shuffle partitions conf: {spark.conf.get('spark.sql.shuffle.partitions')}")
print(f"Initial partitions:      {count_partitions(transactions_df)}")

# 2) 파티셔닝 제어: 균등 8개 → 셔플 없이 2개
card_df = transactions_df.filter(col("totalPrice") >= 50).repartition(8)
compact_df = card_df.coalesce(2)

# 3) 계획 읽기: 결제 수단 필터가 transactions 스캔으로 내려갔는지 확인
gift_card_df = (transactions_df
    .join(franchises_df, "franchiseID")
    .filter(col("paymentMethod") == "gift card")
    .select("transactionID", "product", "totalPrice", "city"))
gift_card_df.explain("formatted")

# 4) 작은 테이블 브로드캐스트: 지역(district)별 매출
district_revenue_df = (transactions_df
    .join(broadcast(franchises_df), "franchiseID")
    .groupBy("district")
    .agg(sum("totalPrice").alias("revenue")))
district_revenue_df.explain()                 # BroadcastHashJoin 확인
display(district_revenue_df.orderBy(desc("revenue")))

# 5) 공유 DataFrame 캐시 → 분석 → 해제
enriched_df = transactions_df.join(franchises_df, "franchiseID")
enriched_df.cache()
enriched_df.count()                           # Storage 화면에서 캐시 확인
display(enriched_df.groupBy("country").agg(sum("totalPrice").alias("revenue")))
display(enriched_df.groupBy("size").agg(avg("totalPrice").alias("avg_ticket")))
enriched_df.unpersist()                       # Storage 화면에서 사라짐 확인
```

| 단계 | 확인할 것 |
|---|---|
| 기준선 | 코어 수, 셔플 파티션 설정, 초기 파티션 수 |
| 파티셔닝 | `repartition`은 `Exchange RoundRobinPartitioning`, `coalesce`는 `Coalesce`만 |
| 계획 | 필터가 조인 뒤가 아니라 transactions 스캔에 있음 |
| 브로드캐스트 | franchises 쪽에만 `BroadcastExchange`, transactions는 셔플 안 됨 |
| 캐시 | 분석 쿼리 계획에 `InMemoryTableScan`, Storage 화면에 항목 표시 후 해제 시 사라짐 |

---

## 23. 모범 사례

- Spark UI로 Job 진행, Stage 지표, 셔플, 스토리지, Executor 상태를 추적한다.
- 가능한 한 일찍, 자주 필터하고 필요한 컬럼만 선택한다.
- 내장 함수를 우선 쓰고, 커스텀 로직이 불가피하면 행 단위 Python UDF 대신 Pandas UDF를 쓴다.
- 셔플 파티션 수는 AQE에 맡기고, 특정 레이아웃 목표가 있을 때만 `repartition`한다.
- 작은 조인 입력은 브로드캐스트하고 계획에서 전략을 확인한다.
- 재사용하는 DataFrame만 캐시하고, Storage 화면에서 확인하고, 다 쓰면 `unpersist()`한다.
- `OPTIMIZE`와 Liquid 클러스터링(또는 예측 최적화)으로 테이블을 건강하게 유지한다. 엔진 튜닝으로는 단편화된 테이블을 고칠 수 없다.
- 변경 전후를 같은 방법으로 측정해 효과를 검증한다.

---

## 24. 핵심 정리 치트시트

### 플랫폼

| 키워드 | 핵심 |
|---|---|
| 레이크하우스 | 웨어하우스 구조 + 레이크 확장성, 개방형 형식 ACID |
| 메달리온 | 브론즈(원시, 레이크) → 실버(정제) → 골드(비즈니스 집계) |
| Unity Catalog | 거버넌스: 접근 제어, 리니지, 감사, 탐색. 성능 최적화는 아님 |
| DBR 17.3 LTS | Apache Spark 4.0 |
| Spark UI | 클래식 컴퓨트에서만. 서버리스는 쿼리 프로필 |
| Spark 4 | ANSI 기본, VARIANT, SQL 스크립팅, Spark Connect, 구조화 로깅, AQE 기본 |

### Delta Lake

| 키워드 | 핵심 |
|---|---|
| `_delta_log` | 순서 있는 JSON 커밋. ACID, 버전, 시간 여행의 근거 |
| 삭제 벡터 | 행 단위 삭제 표시로 DML 비용 감소, OPTIMIZE가 정리 |
| 버전 | 커밋된 작업 하나 = 버전 하나 (행 수 무관) |
| `DESCRIBE DETAIL` | numFiles, sizeInBytes, clusteringColumns |
| `DESCRIBE HISTORY` | version, operation, operationMetrics |
| 시간 여행 | `VERSION AS OF`, `TIMESTAMP AS OF` (조회) |
| `RESTORE` | 롤백, 새 커밋으로 기록 |
| `MERGE` | ON 일치 → MATCHED, 불일치 → NOT MATCHED, 한 커밋 |
| `OPTIMIZE` | 작은 파일 압축 |
| `VACUUM` | 보존 기간(기본 7일) 지난 미참조 파일 영구 삭제, 시간 여행 끊김, `DRY RUN` |
| 레이아웃 | Liquid 클러스터링 `CLUSTER BY` / `CLUSTER BY AUTO` 권장, 파티셔닝과 ZORDER는 레거시 |
| Iceberg | UC 관리형 Iceberg, UniForm으로 상호 운용 |

### 최적화

| 키워드 | 핵심 |
|---|---|
| 시간 소비 | 컴퓨트, 데이터 이동, I/O, 조정 |
| Task | 파티션당 하나 |
| 좁은 변환 | 파티션 수 유지, 스큐 생길 수 있음 |
| `repartition` | 전체 셔플, 균등, `Exchange` |
| `coalesce` | 셔플 없이 축소, 불균형 허용, `Coalesce` |
| 파티션 크기 | 약 100~200MB, 수 ≥ 코어, 50ms 미만 Task는 과다 |
| 셔플 | 가장 비쌈, Stage 경계, Shuffle Read/Write와 Spill 확인 |
| AQE | 파티션 병합, 스큐 조인, 조인 전략 전환. `AQEShuffleRead` |
| 셔플 파티션 설정 | AQE 아래서는 시작점(상한) |
| 조인 | Broadcast Hash(작은 쪽 복사, 큰 쪽 셔플 없음), Sort Merge(기본), Shuffle Hash |
| 브로드캐스트 | `broadcast(small)`, `autoBroadcastJoinThreshold` 10MB |
| 캐시 | 지연, 첫 액션이 채움, `InMemoryTableScan`, Storage 화면, `unpersist()` |
| Catalyst | 푸시다운, 컬럼 프루닝, 필터 결합, 동적 파일 프루닝 |
| explain | 기본, formatted, extended, cost, codegen |
| 진단 절차 | SQL 화면 → Stage 지표 → 원인 분류 → 변경 → 재측정 |

---

## 25. 자가 점검 문제

> 답을 가리고 먼저 생각해본 뒤 펼쳐보자.

**Q1.** 메달리온 아키텍처에서 레이크하우스의 데이터 레이크에 해당하는 계층은?
<details><summary>정답</summary>

브론즈 계층이다. 이력을 포함한 소스 데이터의 동일 복사본으로, 원시 데이터를 추가 위주로 보관한다. 실버는 정제, 표준화된 데이터, 골드는 비즈니스 수준 집계다.
</details>

**Q2.** Unity Catalog의 핵심 기능이 아닌 것은? 접근 제어, 리니지, 감사, 쿼리 성능 최적화 중 고르면?
<details><summary>정답</summary>

쿼리 성능 최적화다. Unity Catalog는 접근 제어, 리니지, 감사, 탐색을 맡는 거버넌스 계층이고, 성능은 Catalyst, AQE, Photon 같은 엔진과 테이블 유지 관리 기능이 담당한다.
</details>

**Q3.** Databricks Runtime에 대한 올바른 설명은?
<details><summary>정답</summary>

Spark와 주변 구성 요소를 사전 패키징하고 테스트한 조합으로, Photon 같은 Databricks 성능 최적화를 포함한다. 수동 구성이 필요 없고 ML 등 다양한 워크로드 유형을 지원하며, 각 릴리스가 특정 Spark 버전(17.3 LTS는 Spark 4.0)을 고정한다.
</details>

**Q4.** Spark 성능 분석에 클래식 컴퓨트를 쓰는 이유는?
<details><summary>정답</summary>

서버리스 컴퓨트는 클러스터와 함께 Spark UI를 노출하지 않기 때문이다. Job, Stage, Task, 실행 계획 지표를 직접 보려면 클래식 클러스터가 필요하다. 서버리스에서는 쿼리 프로필을 쓴다.
</details>

**Q5.** Spark 4에서 기본 동작이 바뀐 것과 그 영향은?
<details><summary>정답</summary>

ANSI SQL 모드가 기본으로 켜졌다. 잘못된 형 변환과 산술 오버플로가 조용히 NULL을 반환하지 않고 오류를 내므로, 관대한 처리가 필요하면 try_cast 같은 try_ 함수를 쓴다. AQE는 계속 기본 활성이고 DataFrame이 기본 API로 유지된다.
</details>

**Q6.** 구조화된 로깅이 모니터링에 주는 이점은?
<details><summary>정답</summary>

Spark 4 로그가 JSON 형식이라 DataFrame으로 읽어 같은 엔진으로 오류와 경고를 필터, 집계할 수 있다. Spark UI를 대체하는 것이 아니라 보완한다.
</details>

**Q7.** Job, Stage, Task는 각각 언제 생기는가?
<details><summary>정답</summary>

액션마다 Job이 생기고, Job은 셔플 경계에서 Stage로 나뉘며, 각 Stage는 파티션당 하나의 Task를 병렬로 실행한다.
</details>

**Q8.** 같은 집계를 SQL과 DataFrame API로 쓰면 성능이 다른가?
<details><summary>정답</summary>

다르지 않다. 둘 다 같은 물리 계획으로 컴파일되어 같은 종류의 Job을 만들므로 모니터링과 최적화 기법도 똑같이 적용된다.
</details>

**Q9.** 쿼리별 연산자와 행 수를 보는 데 가장 유용한 Spark UI 화면은? 캐시 상태는 어디서 보는가?
<details><summary>정답</summary>

SQL / DataFrame 화면이 실행된 계획과 연산자별 행 수, 시간을 보여 준다. 캐시된 DataFrame과 메모리 사용은 Storage 화면에서 본다.
</details>

**Q10.** 오픈 테이블 형식이 해결하는 전통적 데이터 레이크의 문제는?
<details><summary>정답</summary>

트랜잭션 보장 부재로 인한 불완전 데이터, 스키마 강제 부재, 작은 파일 성능 문제, 품질 제어 부재, 대규모 메타데이터 관리 어려움이다. Delta Lake와 Iceberg는 ACID, 버전 관리, 스키마 진화를 제공한다.
</details>

**Q11.** Delta Lake에서 ACID와 시간 여행을 가능하게 하는 구성 요소는?
<details><summary>정답</summary>

_delta_log 트랜잭션 로그다. 모든 변경을 순서 있는 커밋으로 기록하고, 읽기는 로그에서 요청 버전의 파일 목록을 구한다. Parquet 파일은 데이터만 보관한다.
</details>

**Q12.** 삭제 벡터는 무엇이며 어떤 이점이 있는가?
<details><summary>정답</summary>

UPDATE, DELETE, MERGE가 파일 전체를 다시 쓰는 대신 파일 안의 개별 행을 삭제된 것으로 표시하는 기능이다. 행 수준 DML 비용을 크게 줄이며, 이후 OPTIMIZE가 변경을 깨끗한 파일로 다시 쓴다.
</details>

**Q13.** Databricks에서 Delta 테이블을 만들 때 USING DELTA가 필요한가?
<details><summary>정답</summary>

필요 없다. Delta Lake가 기본 테이블 형식이라 CREATE TABLE이나 saveAsTable은 형식 지정 없이 Delta 테이블을 만든다.
</details>

**Q14.** DESCRIBE DETAIL과 DESCRIBE HISTORY는 각각 무엇을 보여 주는가?
<details><summary>정답</summary>

DESCRIBE DETAIL은 numFiles, sizeInBytes, clusteringColumns 같은 현재 물리 상태를, DESCRIBE HISTORY는 version, operation, operationMetrics 같은 트랜잭션 로그의 버전 이력을 보여 준다.
</details>

**Q15.** CREATE, UPDATE, DELETE를 차례로 실행하면 버전은 몇 개인가? 수천 행을 바꾼 UPDATE는 버전을 여러 개 만드는가?
<details><summary>정답</summary>

버전 0(CREATE), 1(UPDATE), 2(DELETE)로 세 개다. 커밋된 작업 하나는 영향받은 행 수와 무관하게 정확히 한 버전을 만든다.
</details>

**Q16.** 시간 여행과 RESTORE의 차이는?
<details><summary>정답</summary>

시간 여행(VERSION AS OF, TIMESTAMP AS OF)은 과거 상태를 조회만 한다. RESTORE는 테이블 자체를 과거 상태로 되돌리며 그 롤백이 새 버전으로 커밋된다.
</details>

**Q17.** RESTORE TABLE t TO VERSION AS OF 1을 실행하면 트랜잭션 로그에는 무슨 일이 일어나는가?
<details><summary>정답</summary>

이력 위에 새 RESTORE 버전이 커밋된다. 이후 버전이 지워지거나 로그가 다시 쓰이지 않으므로 롤백된 작업을 포함한 전체 이력이 남아 감사 가능성이 유지된다.
</details>

**Q18.** 변경 피드를 기존 테이블에 한 번에 업데이트와 삽입으로 적용하는 작업은?
<details><summary>정답</summary>

MERGE INTO다. ON 조건으로 소스와 타깃을 맞춰 일치하면 WHEN MATCHED(UPDATE), 일치하지 않으면 WHEN NOT MATCHED(INSERT)를 하나의 원자적 트랜잭션으로 적용한다. Spark SQL에는 UPSERT 문이 없다.
</details>

**Q19.** MERGE의 업데이트와 삽입은 몇 개의 버전으로 커밋되는가?
<details><summary>정답</summary>

하나다. 모든 업데이트와 삽입이 한 버전으로 함께 커밋되어, 읽는 쪽은 부분적으로 반영된 결과를 보지 않는다.
</details>

**Q20.** MERGE에서 소스에 같은 키의 행이 둘 있으면 어떻게 되는가?
<details><summary>정답</summary>

타깃 한 행에 여러 소스 행이 일치해 MERGE가 오류를 낸다. 소스를 키 기준으로 먼저 중복 제거해야 한다.
</details>

**Q21.** OPTIMIZE와 VACUUM의 목적은 각각 무엇인가?
<details><summary>정답</summary>

OPTIMIZE는 많은 작은 파일을 더 적고 큰 파일로 다시 써서 파일 열기와 메타데이터 오버헤드를 줄인다. VACUUM은 로그가 더 이상 참조하지 않고 보존 기간이 지난 파일을 영구 삭제해 스토리지를 회수한다.
</details>

**Q22.** VACUUM이 시간 여행에 미치는 영향과 기본 보존 기간은?
<details><summary>정답</summary>

보존 기간보다 오래된 버전이 참조하는 파일을 삭제하므로 그 버전은 더 이상 조회하거나 복원할 수 없다. 기본 보존 기간은 7일(168시간)이며 더 짧게 하면 안전 검사가 막는다. DRY RUN으로 삭제 대상을 미리 본다.
</details>

**Q23.** 예측 최적화는 무엇을 하는가?
<details><summary>정답</summary>

Unity Catalog 관리형 테이블에 대해 사용 패턴을 보고 OPTIMIZE, VACUUM, 통계 수집을 자동으로 실행한다. 최적화된 쓰기와 자동 압축도 새 테이블에서 기본 활성이라 수동 유지 관리 부담이 줄어든다.
</details>

**Q24.** 새 Unity Catalog 테이블에 권장되는 데이터 레이아웃 전략은? 레거시로 분류되는 것은?
<details><summary>정답</summary>

Liquid 클러스터링(CLUSTER BY) 또는 CLUSTER BY AUTO가 권장된다. 증분적으로 클러스터링하고 테이블 재작성 없이 키를 바꿀 수 있다. Hive 스타일 파티셔닝과 ZORDER BY는 레거시다.
</details>

**Q25.** 기존 테이블에 Liquid 클러스터링을 적용하고 확인하는 방법은?
<details><summary>정답</summary>

ALTER TABLE t CLUSTER BY (col)로 키를 지정한 뒤 OPTIMIZE를 실행해 데이터를 클러스터링한다. DESCRIBE DETAIL의 clusteringColumns로 확인한다.
</details>

**Q26.** UniForm은 무엇을 가능하게 하는가?
<details><summary>정답</summary>

Delta 테이블이 Iceberg 메타데이터도 노출하게 해 Iceberg 클라이언트가 같은 위치에서 직접 읽을 수 있게 한다. Unity Catalog는 관리형 Iceberg 테이블도 지원해 두 형식이 상호 운용된다.
</details>

**Q27.** filter() 같은 좁은 변환은 파티션 수를 바꾸는가?
<details><summary>정답</summary>

바꾸지 않는다. 각 출력 파티션이 입력 파티션 하나에만 의존하기 때문이다. 하지만 파티션마다 남는 행 수가 달라져 스큐가 생길 수 있다.
</details>

**Q28.** repartition과 coalesce의 차이와 실행 계획에서의 모습은?
<details><summary>정답</summary>

repartition은 전체 셔플로 균등한 파티션을 만들며 Exchange(RoundRobinPartitioning 또는 hashpartitioning)가 나타난다. coalesce는 셔플 없이 기존 파티션을 합쳐 수를 줄이고 불균형을 받아들이며, 계획에 Exchange 없이 Coalesce만 나타난다.
</details>

**Q29.** 파티션 수를 2개로 줄일 때 repartition 대신 coalesce를 쓰는 이유는? coalesce(1)의 함정은?
<details><summary>정답</summary>

coalesce는 전체 셔플의 네트워크 비용을 피하기 때문이다. 다만 셔플 경계가 없어 앞의 계산과 같은 Stage로 묶이므로, 무거운 계산 뒤에 coalesce(1)을 붙이면 그 계산 전체가 Task 하나로 실행될 수 있다.
</details>

**Q30.** 셔플 파티션 크기 지침과 파티션이 너무 많다는 신호는?
<details><summary>정답</summary>

파티션당 약 100~200MB를 목표로 하고 파티션 수를 코어 수 이상으로 유지한다. Spark UI에서 Task가 약 50ms 미만으로 매우 짧으면 파티션이 너무 많다는 신호다.
</details>

**Q31.** 셔플이 비싼 이유와 Stage 화면에서 볼 네 가지 지표는?
<details><summary>정답</summary>

데이터를 직렬화해 디스크에 쓰고 네트워크로 보낸 뒤 다시 읽으며, Stage 경계라 앞 Stage가 끝나야 다음이 시작하기 때문이다. Shuffle Read, Shuffle Write, Spill (Memory), Spill (Disk)를 본다.
</details>

**Q32.** 실제 통계로 런타임에 쿼리 계획을 재최적화하는 기능과 그 세 가지 최적화는?
<details><summary>정답</summary>

Adaptive Query Execution이다. 작은 셔플 파티션 병합, 거대 파티션을 쪼개는 스큐 조인 처리, Sort Merge Join을 Broadcast Join으로 바꾸는 동적 조인 전략 전환을 한다.
</details>

**Q33.** AQE가 켜져 있을 때 spark.sql.shuffle.partitions=200의 의미는? AQE 동작은 어떻게 확인하는가?
<details><summary>정답</summary>

최종 파티션 수가 아니라 시작점(상한)이다. AQE가 실제 크기에 맞춰 병합한다. 실행된 계획에서 coalesced로 표시된 AQEShuffleRead 노드로 확인한다.
</details>

**Q34.** Broadcast Hash Join이 Sort Merge Join보다 훨씬 싼 이유는?
<details><summary>정답</summary>

작은 쪽을 모든 Executor에 복사하므로 큰 쪽을 셔플하거나 정렬하지 않고 제자리에서 조인하기 때문이다.
</details>

**Q35.** broadcast() 힌트는 어느 쪽에 거는가? 힌트가 특히 필요한 경우는?
<details><summary>정답</summary>

작은 테이블 쪽에 건다. 작은 테이블은 임계값(spark.sql.autoBroadcastJoinThreshold, 기본 10MB) 이하면 자동으로 브로드캐스트되지만, 통계가 부정확해 옵티마이저가 잘못 판단할 때 힌트가 중요하다.
</details>

**Q36.** 브로드캐스트 조인이 적용됐는지 계획에서 확인하는 방법은?
<details><summary>정답</summary>

explain() 결과에 BroadcastHashJoin이 있고, 작은 쪽에만 BroadcastExchange가 붙으며 큰 쪽에는 조인을 위한 Exchange가 없는지 본다.
</details>

**Q37.** df.cache()를 호출하고 액션을 실행하지 않으면 무슨 일이 일어나는가?
<details><summary>정답</summary>

아무것도 캐시되지 않는다. cache()는 DataFrame을 표시만 하는 지연 연산이고, 첫 액션(예: count())이 계산할 때 Executor 메모리에 채워진다.
</details>

**Q38.** 캐싱이 항상 성능을 높이는가? 캐시 사용 후 할 일은?
<details><summary>정답</summary>

아니다. 캐시 블록은 셔플, 실행 메모리와 경쟁하므로 한 번만 쓰는 DataFrame을 캐시하면 오히려 느려진다. 다 쓰면 unpersist()로 메모리를 반환한다.
</details>

**Q39.** 캐시가 실제로 쓰였는지 확인하는 방법은?
<details><summary>정답</summary>

Storage 화면에서 캐시된 DataFrame과 크기, 메모리 캐시 비율을 보고, 분석 쿼리의 실행 계획에 원본 스캔과 조인 대신 InMemoryTableScan이 있는지 본다.
</details>

**Q40.** 코드에서 필터를 조인 뒤에 써도 성능 손해가 크지 않은 이유는?
<details><summary>정답</summary>

Catalyst가 조건자 푸시다운으로 필터를 스캔 단계로 내리고 컬럼 프루닝으로 참조된 컬럼만 읽게 계획을 다시 쓰기 때문이다. 계획의 PushedFilters와 ReadSchema로 확인한다.
</details>

**Q41.** Catalyst가 자동으로 하는 최적화 네 가지는?
<details><summary>정답</summary>

조건자 푸시다운, 컬럼 프루닝, 필터 결합, Delta 테이블에서 런타임에 조인 기반으로 파일을 건너뛰는 동적 파일 프루닝이다.
</details>

**Q42.** explain()의 formatted와 extended 모드는 무엇을 보여 주는가?
<details><summary>정답</summary>

formatted는 계획 개요와 연산자별 세부를 읽기 쉽게 보여 주고, extended는 파싱, 분석, 최적화된 논리 계획과 물리 계획을 모두 보여 준다.
</details>

**Q43.** Photon 런타임에서 계획이 어떻게 달라지며 최적화 원칙도 바뀌는가?
<details><summary>정답</summary>

연산자가 PhotonScan, PhotonGroupingAgg처럼 표시된다. Photon은 같은 Catalyst 계획을 더 빠르게 실행하는 벡터화 네이티브 엔진이므로 최적화 원칙은 그대로 적용된다.
</details>

**Q44.** 느린 워크로드를 진단하는 권장 절차는?
<details><summary>정답</summary>

SQL / DataFrame 화면에서 느린 쿼리와 느린 연산자를 찾고, 해당 Stage의 셔플, Spill, Task 시간 분포를 확인한 뒤, 원인에 맞게 코드나 설정을 바꾸고 반드시 다시 측정한다.
</details>

**Q45.** 스캔 Task가 매우 많고 짧으며 DESCRIBE DETAIL의 numFiles가 크기에 비해 크다면 원인과 대응은?
<details><summary>정답</summary>

작은 파일 문제다. 엔진 튜닝으로는 해결되지 않으므로 OPTIMIZE로 압축하고, 자동 압축과 예측 최적화를 활용해 재발을 막는다.
</details>

**Q46.** spark_partition_id()로 파티션 수를 셀 때의 한계는?
<details><summary>정답</summary>

행이 있는 파티션만 세므로 빈 파티션은 빠진다. 클래식 전용 접근 모드에서는 df.rdd.getNumPartitions()로 계획상 파티션 수를 볼 수 있지만, Spark Connect 기반 환경에서는 RDD API를 쓸 수 없다.
</details>

---

## 26. 참고 링크

- Databricks Runtime 17.3 LTS 릴리스 노트: https://docs.databricks.com/aws/en/release-notes/runtime/17.3lts
- Spark UI로 진단하기: https://docs.databricks.com/aws/en/optimizations/spark-ui-guide/
- 쿼리 프로필: https://docs.databricks.com/aws/en/sql/user/queries/query-profile
- Delta Lake 개요: https://docs.databricks.com/aws/en/delta/
- Delta 테이블 이력과 시간 여행: https://docs.databricks.com/aws/en/delta/history
- MERGE INTO: https://docs.databricks.com/aws/en/sql/language-manual/delta-merge-into
- RESTORE: https://docs.databricks.com/aws/en/sql/language-manual/delta-restore
- DESCRIBE DETAIL: https://docs.databricks.com/aws/en/delta/table-details
- 삭제 벡터: https://docs.databricks.com/aws/en/delta/deletion-vectors
- OPTIMIZE: https://docs.databricks.com/aws/en/delta/optimize
- VACUUM: https://docs.databricks.com/aws/en/delta/vacuum
- 예측 최적화: https://docs.databricks.com/aws/en/optimizations/predictive-optimization
- Liquid 클러스터링: https://docs.databricks.com/aws/en/delta/clustering
- UniForm (Iceberg 읽기): https://docs.databricks.com/aws/en/delta/uniform
- Adaptive Query Execution: https://docs.databricks.com/aws/en/optimizations/aqe
- 조인 힌트와 브로드캐스트: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-qry-select-hints
- 동적 파일 프루닝: https://docs.databricks.com/aws/en/optimizations/dynamic-file-pruning
- Photon: https://docs.databricks.com/aws/en/compute/photon
- Apache Spark 성능 튜닝: https://spark.apache.org/docs/latest/sql-performance-tuning.html
- Apache Spark 모니터링 (Web UI): https://spark.apache.org/docs/latest/web-ui.html
- PySpark DataFrame.explain: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.explain.html
- PySpark DataFrame.cache / persist: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.persist.html
- Spark ANSI 호환: https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html
- Spark 구조화된 로깅: https://spark.apache.org/docs/latest/configuration.html#structured-logging
