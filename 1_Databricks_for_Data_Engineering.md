# Databricks 데이터 엔지니어링 기초 — 핵심 개념 정리

---

## 목차

1. [Databricks Data Intelligence Platform](#1-databricks-data-intelligence-platform)
2. [Compute](#2-compute)
3. [노트북 기초](#3-노트북-기초)
4. [Unity Catalog](#4-unity-catalog)
5. [파일 읽기와 테이블 생성 (read_files, CTAS)](#5-파일-읽기와-테이블-생성-read_files-ctas)
6. [Delta Lake와 DML](#6-delta-lake와-dml)
7. [버전 이력과 시간 여행](#7-버전-이력과-시간-여행)
8. [데이터 수집 방법 비교](#8-데이터-수집-방법-비교)
9. [메달리온 아키텍처](#9-메달리온-아키텍처)
10. [Lakeflow Jobs](#10-lakeflow-jobs)
11. [Spark Declarative Pipelines (구 DLT)](#11-spark-declarative-pipelines-구-dlt)
12. [명령어 치트시트](#12-명령어-치트시트)
13. [제품 명칭 변경 정리](#13-제품-명칭-변경-정리)
14. [자격증 준비 가이드](#14-자격증-준비-가이드)
15. [자가 점검 문제](#15-자가-점검-문제)
16. [참고 링크](#16-참고-링크)

---

## 1. Databricks Data Intelligence Platform

### 1-1. 개념

- **Lakehouse** 아키텍처 위에 **AI**를 결합한 플랫폼.
  - 데이터 웨어하우스의 장점: 신뢰성, 성능, 거버넌스
  - 데이터 레이크의 장점: 저렴한 스토리지, 모든 데이터 형식, ML 지원
- 핵심은 **하나의 거버넌스 기반(Unity Catalog)** 위에서 데이터 엔지니어링, SQL 분석(BI), ML/AI를 모두 처리한다는 점.

### 1-2. 플랫폼 계층 구조 (아래에서 위로)

| 계층 | 구성 요소 | 역할 |
|---|---|---|
| 스토리지 | 클라우드 오브젝트 스토리지 (S3 / ADLS / GCS) | 실제 데이터 파일 저장 |
| 테이블 포맷 | **Delta Lake**, Apache Iceberg | 파일 위에 ACID 트랜잭션·버전 관리 제공 |
| 거버넌스 | **Unity Catalog** | 권한, 리니지, 감사, 검색 |
| 워크로드 | **Lakeflow** (Connect · Jobs · Declarative Pipelines), Databricks SQL | 수집, 변환, 오케스트레이션, 분석 |
| AI | Genie Code, AI/BI 대시보드, Genie, Mosaic AI | AI 보조 개발, 자연어 분석, ML |

### 1-3. Control Plane vs Compute Plane

| 구분 | 실행 위치 | 포함 요소 |
|---|---|---|
| **Control Plane** | Databricks 계정 | 웹 애플리케이션, 노트북 소스, Job 스케줄러, 클러스터 관리, UC 메타스토어 |
| **Classic Compute Plane** | **고객의** 클라우드 계정 | 고객 네트워크(VPC/VNet) 내 클러스터 VM |
| **Serverless Compute Plane** | **Databricks의** 클라우드 계정 | 서버리스 노트북/Job/파이프라인/SQL Warehouse |

- 데이터 자체는 고객의 클라우드 스토리지에 저장된다.

### 1-4. Genie Code

- Databricks에 내장된 AI 코딩 도우미 (구 Databricks Assistant).
- 자연어로 SQL/Python 코드 **작성**, 코드 **설명**, 오류 **디버깅**, **자동완성** 지원.

---

## 2. Compute

### 2-1. Compute 유형

| 유형 | 용도 | 특징 |
|---|---|---|
| **All-purpose (Interactive) Cluster** | 노트북 대화형 개발·탐색 | 여러 사용자 공유 가능, 단가 높음 |
| **Jobs Compute (Job Cluster)** | 예약된 Job 실행 | Job 시작 시 생성, 종료 시 삭제 → **운영 Job에 비용 효율적** |
| **Serverless** (노트북 / Job / 파이프라인) | 모든 워크로드 | 인프라 관리 불필요, 즉시 시작, 클러스터 대기 없음 |
| **SQL Warehouse** | SQL 쿼리, BI, 대시보드 | SQL 워크로드에 특화 |
| **Instance Pool** | 클러스터 시작 가속 | 유휴 VM을 미리 확보 |

### 2-2. SQL Warehouse 3종

| 유형 | 실행 위치 | 활성화된 최적화 | 사용 시점 |
|---|---|---|---|
| **Serverless** (권장) | Databricks 계정 | **Photon + Predictive IO + Intelligent Workload Management** | 대부분의 경우 기본 선택. 가장 빠른 시작, 최소 운영 부담 |
| **Pro** | 고객 클라우드 계정 | Photon + Predictive IO | 네트워크/규정 준수 요건으로 서버리스를 쓸 수 없을 때 |
| **Classic** | 고객 클라우드 계정 | Photon만 | 레거시. 최신 최적화 미적용 |

> **암기 팁:** 최적화 기능 개수 Serverless(3) > Pro(2) > Classic(1). Intelligent Workload Management는 서버리스에만 있다.

### 2-3. 알아둘 용어

- **Photon:** C++로 작성된 벡터화 쿼리 엔진. SQL과 DataFrame 연산을 가속.
- **Access Mode** (Unity Catalog 사용 시 중요)
  - **Standard** (구 Shared): 여러 사용자가 공유, 사용자 간 격리
  - **Dedicated** (구 Single user): 한 사용자/그룹 전용. ML 런타임, RDD API 등이 필요할 때
- **Autoscaling / Auto termination:** 작업량에 따라 워커 수 자동 조절 / 유휴 시 자동 종료 → 비용 최적화의 기본

---

## 3. 노트북 기초

### 3-1. 다국어 지원과 Magic Command

노트북은 **기본 언어**를 갖되, 셀마다 다른 언어를 섞어 쓸 수 있다.

| Magic Command | 기능 |
|---|---|
| `%sql` / `%python` / `%scala` / `%r` | 해당 언어로 셀 실행 |
| `%md` | Markdown 문서 셀 (`%md-sandbox`는 HTML/CSS 렌더링 허용) |
| `%run ./경로` | **다른 노트북을 현재 노트북 안에서 실행** → 그 노트북의 변수·함수를 그대로 사용 가능 (공통 설정 스크립트에 주로 사용) |
| `%sh` | 드라이버 노드에서 셸 명령 실행 |
| `%pip install 패키지` | 노트북 범위 Python 라이브러리 설치 |
| `%fs` | `dbutils.fs` 축약 |

> `%run` **vs** `dbutils.notebook.run()`
>
> | | `%run` | `dbutils.notebook.run(path, timeout, args)` |
> |---|---|---|
> | 실행 방식 | 같은 컨텍스트에서 실행 | 별도 실행(임시 Job) |
> | 변수/함수 공유 | O 공유됨 | X 공유 안 됨 |
> | 인자 전달 | X | O |
> | 반환값 | X | O `dbutils.notebook.exit(값)` |

### 3-2. Python에서 SQL 사용하기

```python
# SQL 결과를 Python 값으로 받기
trip_count = spark.sql("SELECT COUNT(*) AS total FROM samples.nyctaxi.trips").collect()[0]["total"]
print(f"There are {trip_count:,} trips.")

# Python 변수를 SQL에 넣기 → 반드시 f-string 사용
spark.sql(f"LIST '/Volumes/{my_catalog}/{my_schema}/myfiles/'").display()
```

- `spark`: 노트북에 기본 제공되는 `SparkSession` 객체
- `display(df)` / `df.display()`: 결과를 표·차트로 렌더링
- 주의: `spark.sql("""... {my_catalog} ...""")`처럼 `f`**를 빠뜨리면 변수가 치환되지 않고** 문자열 `{my_catalog}`가 그대로 들어가 오류가 난다. 아주 흔한 실수.

### 3-3. dbutils 주요 기능

```python
dbutils.fs.ls("/Volumes/cat/sch/vol/")            # 파일 목록
dbutils.widgets.text("env", "dev")                 # 노트북 파라미터(위젯) 정의
dbutils.widgets.get("env")                         # 파라미터 값 읽기
dbutils.secrets.get(scope="my-scope", key="pw")    # 시크릿 조회 (출력 시 [REDACTED]로 가려짐)
dbutils.jobs.taskValues.set(key="cnt", value=10)   # Job 태스크 간 값 전달
```

### 3-4. 세션 정보 확인 함수

```sql
SELECT current_user(), current_catalog(), current_schema();
SELECT current_date(), current_timestamp();
```

---

## 4. Unity Catalog

### 4-1. 3단계 네임스페이스

| 계층 | 설명 | 예시 |
|---|---|---|
| **Metastore** | 최상위 컨테이너. 계정 수준, 리전당 1개 | — |
| **Catalog** | 가장 넓은 데이터 단위. 환경별(dev/staging/prod) 또는 사업부별로 구분 | `prod` |
| **Schema** (= Database) | 관련 객체 묶음. 프로젝트·팀·도메인 단위 | `prod.sales` |
| **Table / View / Volume / Function / Model** | 실제 데이터 객체 | `prod.sales.orders` |

- 테이블 전체 이름: `catalog.schema.table` (예: `samples.nyctaxi.trips`)
- 볼륨 경로: `/Volumes/<catalog>/<schema>/<volume>/<path>`
- 스키마 안에는 테이블뿐 아니라 **뷰, 볼륨, 함수, ML 모델**도 들어간다.
- Unity Catalog는 객체의 **생성자, 생성 시각, 접근 권한**을 자동으로 추적한다.

```sql
SHOW CATALOGS;
SHOW SCHEMAS IN my_catalog;
SHOW TABLES;                                   -- 현재 스키마의 테이블
SHOW VOLUMES;                                  -- 현재 스키마의 볼륨
USE CATALOG my_catalog;
USE SCHEMA my_schema;
LIST '/Volumes/my_catalog/my_schema/myfiles/'; -- 볼륨 안의 파일 목록
```

### 4-2. Table vs Volume

| | Table | Volume |
|---|---|---|
| 저장 대상 | 정형화된, 쿼리 가능한 데이터 | 테이블로 만들기 전의 **원시 파일** (CSV, JSON, 이미지 등) |
| 접근 방법 | SQL (`SELECT`) | 파일 경로 (`/Volumes/...`), `read_files`, `LIST` |
| 비유 | DB 테이블 | 권한 관리가 적용된 폴더 |

### 4-3. Managed vs External

| 구분 | Managed (관리형) | External (외부) |
|---|---|---|
| 데이터 위치 | Unity Catalog가 관리하는 스토리지 위치 | 사용자가 지정한 `LOCATION` |
| `DROP TABLE` 시 | **메타데이터 + 데이터 파일 모두 삭제** (7일 내 `UNDROP` 가능) | **메타데이터만 삭제, 파일은 남음** |
| 생성 문법 | `CREATE TABLE t (...)` | `CREATE TABLE t (...) LOCATION 's3://...'` |
| 권장 여부 | O 기본 권장 (자동 최적화 혜택) | 기존 데이터 위치를 그대로 써야 할 때 |

- 볼륨도 **Managed Volume / External Volume** 두 가지가 있다.
- 외부 스토리지 접근 구조: **Storage Credential**(클라우드 인증 정보) → **External Location**(경로 + 크리덴셜) → External Table / External Volume

### 4-4. 권한 관리 (GRANT / REVOKE)

```sql
-- 분석가 그룹에게 골드 테이블 조회만 허용하는 전형적인 패턴
GRANT USE CATALOG ON CATALOG prod             TO `analysts`;
GRANT USE SCHEMA  ON SCHEMA  prod.sales       TO `analysts`;
GRANT SELECT      ON TABLE   prod.sales.revenue_gold TO `analysts`;

REVOKE SELECT ON TABLE prod.sales.revenue_gold FROM `analysts`;
SHOW GRANTS ON TABLE prod.sales.revenue_gold;
ALTER TABLE prod.sales.revenue_gold OWNER TO `data_eng_team`;
```

| 권한 | 의미 |
|---|---|
| `USE CATALOG` / `USE SCHEMA` | 하위 객체에 접근하기 위한 **필수 전제 권한** — 없으면 테이블 `SELECT` 권한이 있어도 조회 불가! |
| `SELECT` | 읽기 |
| `MODIFY` | INSERT / UPDATE / DELETE / MERGE |
| `CREATE TABLE` / `CREATE SCHEMA` | 객체 생성 |
| `READ VOLUME` / `WRITE VOLUME` | 볼륨 파일 읽기/쓰기 |
| `ALL PRIVILEGES` | 모든 권한 |
| `MANAGE` / **OWNER** | 권한 부여, 소유권 관리 |

- 권한은 **상속**된다: 카탈로그에 `SELECT`를 부여하면 하위 모든 스키마·테이블에 적용.
- 실무 원칙: 분석가에게는 **Gold에 SELECT**, Bronze 접근은 제한.
- 세밀한 보안: **Row Filter**(행 수준), **Column Mask**(열 수준), **Dynamic View**(`is_account_group_member()` 활용)

### 4-5. 거버넌스 기능

| 기능 | 설명 | 활용 |
|---|---|---|
| **Lineage (리니지)** | 소스 파일 → Bronze → Silver → Gold로 이어지는 데이터 흐름과, 관련된 노트북·Job을 **자동 수집** (컬럼 수준까지) | 문제가 생긴 테이블을 소스까지 역추적, 변경 영향도 분석 |
| **Permissions (권한)** | 객체별 SELECT / MODIFY / MANAGE 보유자 | 접근 제어 |
| **Insights (사용 현황)** | 최근 쿼리 활동, 자주 쿼리하는 사용자 | 실제로 쓰이는 테이블 파악 |
| **Audit (감사)** | 누가 언제 무엇에 접근했는지 기록 | 컴플라이언스 |

- **System Tables**로 감사·리니지·비용을 SQL로 조회할 수 있다: `system.access.audit`, `system.access.table_lineage`, `system.billing.usage` 등.

### 4-6. information_schema와 DESCRIBE

```sql
-- 테이블별 컬럼 수 조회
SELECT table_name, COUNT(*) AS column_count
FROM information_schema.columns
WHERE table_schema = 'my_schema'
GROUP BY table_name;
```

- 각 카탈로그에 `information_schema`가 자동 제공 (`tables`, `columns`, `table_privileges` 등).
- `DESCRIBE TABLE t` : 컬럼과 타입
- `DESCRIBE TABLE EXTENDED t` : 위치, 소유자, 속성 등 상세 정보
- `DESCRIBE DETAIL t` : 파일 수, 크기, 위치, 포맷
- `DESCRIBE HISTORY t` : 버전 이력 (7장)

---

## 5. 파일 읽기와 테이블 생성 (read_files, CTAS)

### 5-1. `read_files` — 테이블을 만들지 않고 파일 조회

```sql
-- 기본: 형식과 스키마를 자동 추론
SELECT * FROM read_files('/Volumes/my_catalog/my_schema/myfiles/employees.csv');

-- 형식 옵션을 명시
SELECT * FROM read_files(
  '/Volumes/my_catalog/my_schema/myfiles/employees.csv',
  format      => 'csv',   -- 자동 감지 대신 형식 명시
  header      => true,    -- 첫 행은 데이터가 아닌 컬럼명
  inferSchema => true     -- 각 컬럼의 데이터 타입 자동 감지
);
```

- 파일 하나 또는 **디렉터리 전체**를 읽을 수 있다.
- 지원 포맷: csv, json, parquet, avro, orc, text, binaryFile, xml 등
- 그 밖의 유용한 옵션: `sep`(구분자), `schemaHints`(일부 컬럼 타입 지정), `multiLine`(여러 줄 JSON)
- 형식 옵션은 헤더가 없거나, 비표준 구분자를 쓰거나, 데이터 타입이 모호한 파일에서 특히 중요하다.

### 5-2. `_rescued_data` 컬럼

- `read_files`(와 Auto Loader)가 **자동으로 추가**하는 컬럼.
- 추론된 스키마에 **맞지 않는 값**(잘못된 타입, 추가 필드, 형식 오류)을 **버리거나 에러를 내는 대신 이 컬럼에 보관**한다.
- 데이터가 모두 정상이면 `null`.
- 의미: **데이터 수집 중 유실을 막는 안전장치.** 실무에서는 `_rescued_data IS NOT NULL`인 행을 모니터링하거나 격리한다.
- 깔끔한 테이블이 필요하면 `SELECT *` 대신 **필요한 컬럼만 명시적으로 SELECT**해 제외한다.

### 5-3. CTAS (CREATE TABLE AS SELECT)

```sql
CREATE TABLE IF NOT EXISTS employees_clean AS
SELECT ID, FirstName, Country, Role          -- _rescued_data 제외
FROM read_files(
  '/Volumes/my_catalog/my_schema/myfiles/employees.csv',
  format => 'csv', header => true, inferSchema => true
);
```

- 파일 읽기와 테이블 생성을 **한 번에** 수행.
- 스키마는 SELECT 결과에서 **자동 추론**된다. CTAS에서는 컬럼 타입을 직접 선언할 수 없으므로 필요하면 `CAST`를 사용.
- Databricks의 **기본 테이블 포맷은 Delta** → 생성 즉시 트랜잭션 로그가 생겨 ACID, 시간 여행, 스키마 강제가 가능해진다.
- SQL에서 문자열 결합은 `||` : `'/Volumes/' || my_catalog || '/' || my_schema || '/myfiles/'`

> **테이블이 이미 있을 때의 동작**
>
> | 문법 | 테이블이 이미 있으면 |
> |---|---|
> | `CREATE TABLE t AS ...` | **오류** |
> | `CREATE TABLE IF NOT EXISTS t AS ...` | **아무것도 하지 않음** (데이터 변화 없음, 오류 없음) |
> | `CREATE OR REPLACE TABLE t AS ...` | **내용 교체**. 히스토리는 유지되고 새 버전으로 기록됨 (DROP 후 CREATE와 다름!) |

---

## 6. Delta Lake와 DML

### 6-1. Delta Lake의 원리

**Delta 테이블 = Parquet 데이터 파일 +** `_delta_log` **트랜잭션 로그**

```
my_table/
  _delta_log/
    00000000000000000000.json            -- Version 0 (CREATE)
    00000000000000000001.json            -- Version 1 (WRITE)
    00000000000000000010.checkpoint.parquet   -- 10커밋마다 체크포인트
  part-00000-xxxx.snappy.parquet         -- 실제 데이터
  part-00001-yyyy.snappy.parquet
```

- 일반 데이터 레이크(CSV/Parquet 파일)는 **행 단위 수정이 불가능** → 레코드 하나를 고치려 해도 파일 전체를 다시 써야 한다. Delta는 트랜잭션 로그로 이 문제를 해결.
- **기존 데이터 파일은 절대 수정하지 않는다 (immutable).** 변경 시 새 파일을 쓰고, 로그에 "어떤 파일이 추가/제거되었는지"를 기록 → 이것이 **버전 관리와 시간 여행의 원리**.
- 로그에는 작업 유형, 타임스탬프, 사용자, 추가/삭제된 파일 정보가 기록된다.

**ACID 보장**

| 속성 | 의미 |
|---|---|
| **A**tomicity (원자성) | DML은 완전히 성공하거나 완전히 롤백. 반쯤 쓰인 상태가 남지 않음 |
| **C**onsistency (일관성) | 스키마·제약 조건을 위반하는 쓰기는 거부 |
| **I**solation (격리성) | 낙관적 동시성 제어(Optimistic Concurrency Control)로 동시 쓰기 충돌 처리 |
| **D**urability (지속성) | 커밋된 데이터는 클라우드 스토리지에 영구 저장 |

### 6-2. 기본 DML

```sql
-- INSERT: 새 행 추가
INSERT INTO employees (ID, FirstName, Country, Role) VALUES
  (5, 'Maria', 'Brazil', 'Data Engineer'),
  (6, 'Aiden', 'Australia', 'Data Analyst');

-- UPDATE: 조건에 맞는 행의 값 변경
UPDATE employees SET Role = 'Senior Data Engineer' WHERE FirstName = 'Maria';

-- DELETE: 조건에 맞는 행 삭제 (현재 버전에서만 사라지고 이전 버전에는 남아 있음)
DELETE FROM employees WHERE FirstName = 'Aiden';
```

- 표준 SQL 문법 (PostgreSQL, MySQL, SQL Server와 동일).
- 모든 DML은 **원자적**이고, 실행할 때마다 테이블의 **새 버전**이 생성된다.
- UPDATE/DELETE로 바뀐 원래 값은 손실되지 않고 이전 버전에 남아 있다.

### 6-3. MERGE INTO (Upsert)

```sql
MERGE INTO employees AS t                    -- 대상 테이블
USING employee_updates AS s                  -- 소스 (변경분)
ON t.ID = s.ID                               -- 매칭 조건
WHEN MATCHED AND s.is_deleted = true THEN DELETE
WHEN MATCHED THEN UPDATE SET *               -- 키가 있으면 업데이트
WHEN NOT MATCHED THEN INSERT *               -- 키가 없으면 삽입
WHEN NOT MATCHED BY SOURCE THEN DELETE;      -- 소스에 없는 대상 행 삭제 (선택)
```

- **중복 없이 적재**하려면 `WHEN NOT MATCHED THEN INSERT` *만 사용 → "insert-only merge"
- 주의: 소스에 같은 키가 여러 개 있으면 오류 → 소스를 먼저 중복 제거해야 한다.
- CDC(Change Data Capture) 처리, SCD Type 1/2 구현의 기본 도구.

### 6-4. 그 밖의 Delta 핵심 기능

| 기능 | 문법 / 설명 |
|---|---|
| **스키마 강제 (Schema Enforcement)** | 테이블 스키마와 맞지 않는 쓰기는 **거부** |
| **스키마 진화 (Schema Evolution)** | `.option("mergeSchema", "true")`, `MERGE WITH SCHEMA EVOLUTION` → 새 컬럼 자동 추가 |
| **제약 조건** | `ALTER TABLE t ADD CONSTRAINT c CHECK (amount > 0);`, `NOT NULL` |
| **OPTIMIZE** | 작은 파일들을 큰 파일로 합침 (small file problem 해결) |
| **Z-ORDER** | `OPTIMIZE t ZORDER BY (col)` — 데이터 스키핑 향상 (레거시 방식) |
| **Liquid Clustering** (권장) | `CREATE TABLE t ... CLUSTER BY (col)` / `CLUSTER BY AUTO` — 파티셔닝과 Z-ORDER의 **현대적 대체** |
| **Predictive Optimization** | Managed 테이블에 대해 OPTIMIZE / VACUUM / ANALYZE를 **자동 실행**. 그래서 히스토리에 내가 실행하지 않은 `OPTIMIZE` 버전이 끼어 있을 수 있다 |
| **Deletion Vectors** | DELETE/UPDATE 시 파일을 다시 쓰지 않고 "삭제 표시"만 → 쓰기 성능 향상 |
| **Change Data Feed (CDF)** | `delta.enableChangeDataFeed = true` 설정 후 `table_changes('t', 2)`로 행 단위 변경분 조회 |
| **CLONE** | `DEEP CLONE`(데이터까지 복사) / `SHALLOW CLONE`(메타데이터만 복사, 테스트용) |

---

## 7. 버전 이력과 시간 여행

### 7-1. `DESCRIBE HISTORY`

```sql
DESCRIBE HISTORY employees;
```

| 주요 컬럼 | 의미 |
|---|---|
| `version` | 0(최초 생성)부터 시작, 작업마다 +1 |
| `timestamp` | 작업 시각 |
| `operation` | CREATE TABLE AS SELECT, WRITE(INSERT), UPDATE, DELETE, MERGE, COPY INTO, OPTIMIZE 등 |
| `userName` | 수행자 — **사람이 한 변경인지 Job이 한 변경인지 구분** 가능 |
| `operationParameters` | 조건절 등 작업 파라미터 |
| `operationMetrics` | 영향받은 행 수, 추가/제거 파일 수 등 |
| `job`, `notebook` | 어떤 Job/노트북에서 실행되었는지 |

- **사용 시점:** 감사(누가·언제·무엇을), 예상치 못한 데이터 디버깅, 복원할 버전 번호 찾기
- DML(INSERT/UPDATE/DELETE/MERGE)뿐 아니라 DDL(CREATE, ALTER)도 버전을 만든다.
- **실제 데이터 변경이 없는 작업은 새 버전을 만들지 않는다** (예: 0행을 적재한 COPY INTO).
- 주의: 자동 OPTIMIZE가 버전을 차지할 수 있으므로 "INSERT는 1번, UPDATE는 2번"처럼 **추측하지 말고 반드시 히스토리로 버전 번호를 확인**한다.

### 7-2. 이전 버전 조회 (Time Travel)

```sql
SELECT * FROM employees VERSION AS OF 0;                      -- 버전 번호로
SELECT * FROM employees TIMESTAMP AS OF '2026-11-16 09:00:00'; -- 시각으로
SELECT * FROM employees@v0;                                   -- 버전 축약 문법
SELECT * FROM employees@20261116090000000;                    -- 시각 축약 문법 (yyyyMMddHHmmssSSS)
```

- 시간 여행은 **읽기 전용** — 현재 테이블은 변하지 않는다.
- 일반 `SELECT`는 항상 **최신 버전**을 읽는다.
- 활용: **감사**(무엇이 언제 바뀌었는지 증명), **디버깅**(대시보드 수치가 이상하면 어제 버전과 비교), **복구**(실수로 지운 데이터 되살리기)

### 7-3. 버전 비교 패턴

```sql
-- 행 수 비교
SELECT 'Current' AS version, COUNT(*) AS row_count FROM employees
UNION ALL
SELECT 'Version 0', COUNT(*) FROM employees VERSION AS OF 0;

-- 실제 내용 비교 (현재에만 있는 행)
SELECT * FROM employees
EXCEPT
SELECT * FROM employees VERSION AS OF 0;
```

> 주의: **행 수 비교만으로는 부족하다.** 1행을 추가하고 1행을 삭제하거나 값을 UPDATE하면 행 수는 같지만 내용은 다르다. 내용 비교는 `EXCEPT`로.

### 7-4. RESTORE — 실수 복구

```sql
RESTORE TABLE employees TO VERSION AS OF 1;
RESTORE TABLE employees TO TIMESTAMP AS OF '2026-11-16 09:00:00';
```

- RESTORE 자체도 **새 버전**으로 기록된다 (히스토리가 지워지지 않음).
- 일부 행만 되살리려면: `INSERT INTO t SELECT * FROM t VERSION AS OF n WHERE ...` 또는 MERGE.

**삭제 사고 대응 절차**

1. `DESCRIBE HISTORY t` → `operation = DELETE`인 버전 n 확인
2. `SELECT * FROM t VERSION AS OF n-1` → 삭제 전 상태 확인
3. `RESTORE TABLE t TO VERSION AS OF n-1` 또는 필요한 행만 다시 INSERT

### 7-5. VACUUM과 보존 기간

```sql
VACUUM employees;                   -- 보존 기간(기본 7일)이 지난, 더 이상 참조되지 않는 파일 삭제
VACUUM employees RETAIN 168 HOURS;  -- 보존 기간 명시
VACUUM employees DRY RUN;           -- 삭제될 파일 미리보기
```

| 테이블 속성 | 기본값 | 의미 |
|---|---|---|
| `delta.deletedFileRetentionDuration` | **7일** | VACUUM이 삭제하지 않고 남겨두는 데이터 파일 보존 기간 |
| `delta.logRetentionDuration` | **30일** | 트랜잭션 로그(히스토리) 보존 기간 |

- **VACUUM 후에는 보존 기간 이전 버전으로 시간 여행 불가** (참조하던 데이터 파일이 사라졌으므로).
- 주의: 히스토리 목록은 30일간 보여도, VACUUM이 실행됐다면 **실제로 조회 가능한 기간은 7일**일 수 있다.
- 7일 미만으로 VACUUM하려면 안전장치(`spark.databricks.delta.retentionDurationCheck.enabled = false`)를 꺼야 하며, 실행 중인 읽기/쓰기를 깨뜨릴 위험이 있다.
- 개인정보 삭제 요청(GDPR 등) 시: DELETE만으로는 이전 버전에 데이터가 남는다 → **VACUUM까지 해야 물리적으로 삭제**된다.

---

## 8. 데이터 수집 방법 비교

**Lakeflow Connect**는 Databricks의 데이터 수집 기능 전체를 묶는 이름이다. 파일 업로드, COPY INTO, Auto Loader, SaaS·DB용 관리형 커넥터를 포함한다.

### 8-1. 방법별 비교

| 방법 | 방식 | 재실행 안전성 | 처리한 파일 추적 | 적합한 상황 |
|---|---|---|---|---|
| **CTAS** | SQL로 **새 테이블 생성** | 일회성 | X | 일회성 생성, 컬럼 선택·변환, 재현성 필요 |
| **파일 업로드 (UI)** | 코드 없이 파일 업로드 → 형식 감지·스키마 추론 후 테이블 생성 | X (수동) | X | 받은 스프레드시트를 빠르게 쿼리해야 할 때. 재현 불가 |
| **COPY INTO** | SQL로 **기존 테이블에 추가** | O 멱등 | O | 정기적으로 파일이 쌓이는 디렉터리, 수천 개 규모 |
| **Auto Loader** | Structured Streaming (`cloudFiles`) | O 멱등 | O (체크포인트) | **수백만 파일**, 지속 도착, 저지연, 스키마 진화 |
| **Streaming Table** | `CREATE STREAMING TABLE ... STREAM read_files()` | O | O (내부적으로 Auto Loader) | 선언적 파이프라인의 Bronze |
| **관리형 커넥터** | 설정 기반 | O | O | Salesforce, SQL Server, Workday 등 SaaS/DB 수집 |

> **CTAS = 처음부터 테이블을 "만들기"**, **COPY INTO = 기존 테이블에 새 데이터를 "추가하기"**
> 코드 기반 방법(CTAS, COPY INTO)은 반복 실행·파이프라인 포함·Git 버전 관리가 가능하다는 점이 수동 업로드와의 핵심 차이.

### 8-2. COPY INTO

```sql
-- 1단계: 스키마를 정의한 빈 테이블 생성 (COPY INTO는 기존 테이블에만 적재)
CREATE TABLE IF NOT EXISTS employees_bronze (
  ID INT, FirstName STRING, Country STRING, Role STRING
);

-- 2단계: 디렉터리의 모든 파일 적재
COPY INTO employees_bronze
FROM '/Volumes/my_catalog/my_schema/myfiles/'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true');
-- 결과: num_affected_rows = 6 (파일 2개의 행 합계)

-- 3단계: 같은 명령을 다시 실행 → num_affected_rows = 0, 새 버전도 생성되지 않음
```

```python
# Python 변수로 경로를 만들 때
result = spark.sql(f"""
  COPY INTO employees_bronze
  FROM '/Volumes/{my_catalog}/{my_schema}/myfiles/'
  FILEFORMAT = CSV
  FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
""")
result.display()
```

**멱등성(Idempotency)**: 같은 작업을 여러 번 실행해도 결과가 한 번 실행한 것과 같은 성질. COPY INTO는 이미 처리한 파일을 기억하고 건너뛰므로 **일정에 따라 반복 실행해도 데이터가 중복되지 않는다.**

**자주 쓰는 추가 옵션**

```sql
COPY INTO my_table
FROM (SELECT col1, CAST(col2 AS INT) AS col2 FROM '/Volumes/cat/sch/landing/')  -- 적재하며 변환
FILEFORMAT = JSON
PATTERN = '2026-11-*.json'                   -- 글롭 패턴으로 파일 필터
FORMAT_OPTIONS ('multiLine' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true',        -- 새 컬럼 등장 시 스키마 진화
              'force' = 'true');             -- 이미 처리한 파일도 강제 재적재 (멱등성 해제!)
```

**제한 사항**

- 주의: 파일을 **경로(이름) 기준**으로 추적한다 → 같은 이름으로 내용만 바꿔 덮어쓴 파일은 **다시 처리하지 않는다.**
- 대규모·저지연 시나리오에는 **Auto Loader / Streaming Table**이 권장된다.

### 8-3. Auto Loader

```python
(spark.readStream
    .format("cloudFiles")                                               # Auto Loader의 표식
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/Volumes/cat/sch/chk/schema") # 추론된 스키마와 변경 이력 저장
    .option("header", "true")
    .load("/Volumes/cat/sch/landing/")
 .writeStream
    .option("checkpointLocation", "/Volumes/cat/sch/chk/bronze")        # 처리 진행 상태 저장
    .trigger(availableNow=True)                                         # 쌓인 파일만 처리하고 종료
    .toTable("cat.sch.bronze"))
```

| 개념                               | 설명                                                                                                                           |
| -------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| **파일 감지 모드**                     | Directory listing(기본, 디렉터리를 나열해 새 파일 탐지) / File notification(클라우드 이벤트·큐 사용, 대규모용)                                            |
| `cloudFiles.schemaEvolutionMode` | `addNewColumns`(기본: 새 컬럼 발견 시 스트림이 실패하고 스키마를 갱신 → 재시작하면 반영) / `rescue`(새 컬럼을 `_rescued_data`로) / `failOnNewColumns` / `none` |
| `cloudFiles.schemaHints`         | 일부 컬럼의 타입을 직접 지정                                                                                                             |
| **Trigger**                      | `availableNow=True`(증분 배치처럼 동작 후 종료) / `processingTime='1 minute'`(계속 실행)                                                    |
| **Checkpoint**                   | 처리 위치를 저장 → 장애 후 재시작해도 **정확히 한 번(exactly-once)** 처리                                                                          |

> **COPY INTO vs Auto Loader 선택 기준**
>
> - 파일 수천 개 수준, 간단한 SQL → **COPY INTO**
> - 파일 수백만 개, 지속적으로 도착, 스키마가 바뀜 → **Auto Loader**

---

## 9. 메달리온 아키텍처

### 9-1. 레이어 정의

데이터를 세 개의 레이어로 나누고, 레이어를 거칠수록 품질과 구조가 향상된다.

**원시 데이터 → Bronze → Silver → Gold → 비즈니스 활용**

| 레이어 | 역할 | 처리 내용 | 대표 구현 |
|---|---|---|---|
| **Bronze** | 데이터 착륙 지점(landing zone). **"진실의 원천"** | 수신된 그대로 저장. 변환·필터 없음. 품질 문제(누락, 잘못된 타입, 중복)도 그대로 | COPY INTO, Auto Loader, Streaming Table |
| **Silver** | 정제·보강. **분석의 기반** | 컬럼명·값 표준화, 타입 변환, 중복 제거, 잘못된 레코드 필터, 감사 컬럼(처리 시각) 추가 | `CREATE OR REPLACE TABLE AS SELECT`, MERGE, Materialized View |
| **Gold** | 비즈니스 즉시 활용 | 특정 질문에 맞춘 집계·필터·조인. 대시보드와 분석가가 사용 | `INSERT OVERWRITE`, Materialized View |

**왜 3개 레이어인가?**

1. **원본 보존과 재처리:** 변환 로직에 버그가 있으면 Silver 로직만 고쳐 Bronze에서 다시 실행하면 된다. 원본은 절대 손실되지 않는다.
2. **관심사 분리:** 수집 / 변환 / 소비를 분리해 각각 독립적으로 디버깅·관리.
3. **재사용:** 하나의 Silver에서 재무용(매출 집계), HR용(부서별 인원), 운영용(SLA 지표) Gold를 여러 개 만든다.
4. **접근 제어:** 분석가는 Gold만, Bronze는 제한.

- 고정된 규칙은 아니다: ML 피처용 "Platinum" 레이어를 추가하거나, Gold 없이 Silver를 직접 쿼리하기도 한다. 핵심 원칙은 **수집·변환·소비의 분리**.
- 데이터 엔지니어가 가장 많은 시간을 쓰는 레이어는 **Silver**.
- Bronze·Silver는 공유 인프라, Gold는 특정 팀·대시보드·보고서 전용으로 만드는 경우가 많다.

### 9-2. 구현 예시

```sql
-- Bronze: 빈 테이블 + COPY INTO (원시 그대로)
CREATE TABLE IF NOT EXISTS employees_bronze (
  ID INT, FirstName STRING, Country STRING, Role STRING
);
COPY INTO employees_bronze
FROM '/Volumes/my_catalog/my_schema/myfiles/'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true');

-- Silver: 표준화 + 감사 컬럼 + 행 번호
CREATE OR REPLACE TABLE employees_silver AS
SELECT
  ID,
  FirstName,
  UPPER(Country)      AS Country,              -- 값 표준화
  UPPER(Role)         AS Role,
  current_timestamp() AS processed_timestamp,  -- 감사 추적: 언제 처리되었나
  current_date()      AS processed_date,
  ROW_NUMBER() OVER (ORDER BY ID) AS row_number  -- 윈도우 함수
FROM employees_bronze;

-- Gold: 임시 뷰에 집계 로직 정의 → 스키마가 고정된 테이블에 INSERT OVERWRITE
CREATE OR REPLACE TEMP VIEW temp_total_roles AS
SELECT Role, COUNT(*) AS TotalEmployees
FROM employees_silver
GROUP BY Role;

CREATE TABLE IF NOT EXISTS total_roles_gold (Role STRING, TotalEmployees INT);

INSERT OVERWRITE total_roles_gold
SELECT * FROM temp_total_roles;
```

### 9-3. `INSERT INTO` vs `INSERT OVERWRITE` vs `CREATE OR REPLACE`

| 명령 | 동작 | 스키마 | 용도 |
|---|---|---|---|
| `INSERT INTO` | 기존 데이터에 **추가** | 기존 스키마 강제 | 누적 적재 (주의: 재실행 시 중복 위험) |
| `INSERT OVERWRITE` | 데이터 **전체 교체** | **기존 스키마 유지**, 다르면 실패 | Gold 정기 갱신 — 테이블 정의·권한을 그대로 유지 |
| `CREATE OR REPLACE TABLE ... AS` | 테이블 **정의와 데이터 모두 교체** | 새 스키마로 변경 가능 | Silver 전체 재계산 |

- 세 가지 모두 히스토리에 새 버전을 남긴다 → 시간 여행 가능 (DROP 후 CREATE는 히스토리가 끊김).
- Gold는 **스키마·권한을 한 번 정의**하고 내용만 주기적으로 갱신하는 `INSERT OVERWRITE` 패턴이 일반적.

### 9-4. 뷰(View)의 종류

| 종류 | 문법 | 범위 / 수명 |
|---|---|---|
| **Temp View** | `CREATE TEMP VIEW` | **현재 세션(노트북)** 에서만. 세션 종료 시 소멸. 카탈로그에 등록되지 않음 |
| **Global Temp View** | `CREATE GLOBAL TEMP VIEW` → `global_temp.v`로 조회 | 같은 클러스터의 모든 세션. 클러스터 재시작 시 소멸 |
| **View (영구)** | `CREATE VIEW` | Unity Catalog에 등록, 권한 관리 가능. 조회할 때마다 재계산 |
| **Materialized View** | `CREATE MATERIALIZED VIEW` | 결과를 **저장**하고 갱신(refresh)하는 방식 |

---

## 10. Lakeflow Jobs

### 10-1. 핵심 개념

- **Job** = 하나 이상의 **Task**를 **의존성**과 **일정**에 따라 실행하는 워크플로. "무엇을, 어떤 순서로, 언제" 실행할지 정의한다.
- Task 유형: 노트북, SQL, Python 스크립트, 선언적 파이프라인, dbt, JAR 등
- 태스크 간 의존 관계는 **DAG**(Directed Acyclic Graph, 방향성 비순환 그래프)로 표현된다.
- 노트북을 직접 실행하는 것은 **개발·탐색용**, Job은 누가 실행 버튼을 누르지 않아도 자동으로 도는 **운영용**. 노트북 코드는 그대로 재사용한다.

**예시: 2-태스크 메달리온 파이프라인**

| Task | 내용 | 의존 대상 | 실행 조건 |
|---|---|---|---|
| `Setup-Bronze` | 볼륨 준비 + COPY INTO로 Bronze 적재 | — | — |
| `Silver-Gold` | Bronze → Silver 변환, Silver → Gold 집계 | `Setup-Bronze` | All succeeded |

→ Bronze가 실패하면 Silver/Gold는 **오래되거나 누락된 데이터로 실행되지 않는다.** 이것이 운영 파이프라인이 안정성을 유지하는 방식.

- 각 Task에 지정하는 것: 이름, 유형, 소스 경로, **Compute**(Serverless/Job cluster), **Depends on**, **Run if**
- 실행 상태 흐름: `Pending → Running → Succeeded / Failed`
- Job이 수행한 변경은 테이블 히스토리의 `userName`/`job` 컬럼으로 사람이 한 변경과 구분된다.

### 10-2. 트리거

| 트리거 | 설명 |
|---|---|
| **Scheduled** | Cron 일정 (매시간, 매일 06:00 등. Quartz cron 문법) |
| **File arrival** | 지정한 위치(볼륨·외부 위치)에 새 파일이 도착하면 실행 |
| **Table update** | 특정 테이블이 업데이트되면 실행 |
| **Continuous** | 항상 실행 상태 유지 (종료되면 즉시 재시작) |
| Manual | 수동 실행 / API 호출 |

### 10-3. Run if 조건 (의존성 실행 조건)

| 조건 | 의미 | 쓰임 |
|---|---|---|
| `All succeeded` (기본) | 모든 상위 태스크 성공 | 일반적인 순차 처리 |
| `At least one succeeded` | 하나 이상 성공 | 병렬 소스 중 일부만 성공해도 진행 |
| `None failed` | 실패 없음 (스킵은 허용) | 조건부 분기 이후 |
| `All done` | 성공·실패 무관, 모두 완료 | 정리(cleanup) 작업 |
| `At least one failed` | 하나 이상 실패 | 실패 알림, 롤백 태스크 |
| `All failed` | 모두 실패 | 전체 실패 대응 |

### 10-4. 운영 기능

| 기능 | 설명 |
|---|---|
| **Retries** | 태스크별 재시도 횟수·간격 |
| **Repair run** | 실패한 실행에서 **실패한 태스크와 그 하위 태스크만** 재실행 (전체 재실행 불필요) |
| **Timeout / 실행 시간 경고** | 너무 오래 걸리는 실행 감지 |
| **Notifications** | 시작/성공/실패 시 이메일, Slack, Teams, Webhook 알림 |
| **Job / Task parameters** | `{{job.id}}`, `{{job.start_time.iso_date}}` 같은 동적 값 참조 |
| **Task values** | `dbutils.jobs.taskValues.set/get`으로 태스크 간 값 전달 |
| **If/else 태스크, For each 태스크** | 조건 분기, 반복 실행 |
| **Run as** | Job 실행 주체. **Service Principal 권장** (담당자 퇴사 시에도 Job이 깨지지 않도록) |
| **Max concurrent runs** | 동시 실행 개수 제한 |
| **Databricks Asset Bundles (DABs)** | Job·파이프라인을 YAML 코드로 정의하고 CI/CD로 dev → prod 배포 |

---

## 11. Spark Declarative Pipelines (구 DLT)

### 11-1. 명령형 vs 선언형

| | 명령형 (테이블 생성 + COPY INTO + Job) | 선언형 (SDP) |
|---|---|---|
| 작성하는 것 | **"어떻게(How)"** — 먼저 이것, 그다음 저것 | **"무엇(What)"** — 이 데이터로 이런 테이블들을 원한다 |
| 실행 순서 | Job 태스크 의존성으로 직접 관리 | 테이블 간 참조 관계를 분석해 **자동 결정** |
| 증분 처리 | COPY INTO로 직접 구현 | Streaming Table이 자동 추적 |
| 인프라 | Compute 직접 선택 | 자동 관리 |
| 오류 처리 | 직접 구현 | 실패 시 영향받는 하위 테이블이 오래된 데이터로 갱신되지 않도록 차단 |
| 데이터 품질 | 검사 코드를 직접 작성 | **Expectations** 내장, 자동 추적 |
| 리니지 | 실행 기록 기반 | SQL 정의로부터 자동 구성 |

### 11-2. 핵심 객체

| 객체 | 문법 | 동작 | 주 용도 |
|---|---|---|---|
| **Streaming Table** | `CREATE OR REFRESH STREAMING TABLE` | **증분 처리** — 새 데이터만 처리하고 처리한 것을 추적 (append-only 소스) | **Bronze** (수집) |
| **Materialized View** | `CREATE OR REFRESH MATERIALIZED VIEW` | 결과를 저장, 파이프라인 실행 시 소스로부터 **재계산** (가능하면 증분 갱신) | **Silver / Gold** (변환·집계) |
| Temporary View | `CREATE TEMPORARY VIEW` | 파이프라인 내부에서만 사용, 카탈로그에 게시되지 않음 | 중간 단계 |

### 11-3. 전체 파이프라인 = SQL 3개

```sql
-- Bronze: Streaming Table. STREAM 키워드 = 처리한 파일을 추적하는 증분 처리 (내부적으로 Auto Loader)
CREATE OR REFRESH STREAMING TABLE employees_bronze_sdp
COMMENT "Raw employee data ingested from CSV files."
AS SELECT *
FROM STREAM read_files(
  '/Volumes/${my_catalog}/${my_schema}/myfiles/',   -- ${...} = 파이프라인 설정(configuration) 값 참조
  format => 'csv', header => true, inferSchema => true
);

-- Silver: Materialized View + 데이터 품질 규칙
CREATE OR REFRESH MATERIALIZED VIEW employees_silver_sdp (
  CONSTRAINT valid_id   EXPECT (ID IS NOT NULL),
  CONSTRAINT valid_name EXPECT (FirstName IS NOT NULL)
)
COMMENT "Cleaned and enriched employee data."
AS SELECT ID, FirstName, Country, UPPER(Role) AS Role,
          current_timestamp() AS processed_timestamp,
          current_date()      AS processed_date
FROM employees_bronze_sdp;

-- Gold: Materialized View (집계)
CREATE OR REFRESH MATERIALIZED VIEW total_roles_gold_sdp
COMMENT "Employee count by role."
AS SELECT Role, COUNT(*) AS TotalEmployees
FROM employees_silver_sdp
GROUP BY Role;
```

| 단계 | 명령형 | 선언형 |
|---|---|---|
| Bronze | CREATE TABLE + COPY INTO (2단계) | STREAMING TABLE (1단계) |
| Silver | CREATE OR REPLACE TABLE AS SELECT | MATERIALIZED VIEW + Expectations |
| Gold | TEMP VIEW + CREATE TABLE + INSERT OVERWRITE (3단계) | MATERIALIZED VIEW (1단계) |
| 오케스트레이션 | Job 2태스크 + 의존성 | 자동 (Silver가 Bronze를, Gold가 Silver를 읽는다는 것을 스스로 파악) |

- 주의: 선언적 파이프라인의 소스 코드는 노트북에서 셀 단위로 **직접 실행할 수 없다.** ETL 파이프라인으로 등록해 파이프라인 엔진이 실행한다.
- 파이프라인 설정 항목: 게시 대상 **카탈로그·스키마**, **소스 코드 경로**, **configuration**(키-값, SQL에서 `${key}`로 참조), compute

### 11-4. Expectations (데이터 품질 규칙)

| 위반 시 동작 | 문법 | 결과 |
|---|---|---|
| **경고 (기본)** | `EXPECT (cond)` | 위반 행도 **테이블에 포함**, 위반 건수만 기록 |
| **행 삭제** | `EXPECT (cond) ON VIOLATION DROP ROW` | 위반 행은 **버리고** 진행, 기록 |
| **업데이트 실패** | `EXPECT (cond) ON VIOLATION FAIL UPDATE` | 위반 시 **해당 플로가 실패** (트랜잭션 롤백) |

- 주의: `ON VIOLATION WARN`이라는 문법은 **없다.** 경고 모드는 `ON VIOLATION` 절을 생략하는 것.
- 통과/실패 건수는 파이프라인의 데이터 품질 지표와 **Event Log**(`event_log()` 테이블 함수로 SQL 조회)에 기록된다.
- **격리(quarantine) 패턴:** 반대 조건으로 별도 테이블을 만들어 불량 레코드를 보관한다.

### 11-5. 심화 개념

| 개념 | 설명 |
|---|---|
| **Triggered vs Continuous** | 한 번 갱신하고 종료 vs 계속 실행 |
| **Development vs Production 모드** | 개발: 클러스터 재사용, 재시도 없음(빠른 디버깅) / 운영: 실패 시 재시도, 클러스터 재시작 |
| **Full refresh** | Streaming Table의 상태를 초기화하고 처음부터 재처리 |
| **AUTO CDC** (구 APPLY CHANGES INTO) | CDC 피드를 받아 SCD Type 1/2 자동 처리. `KEYS`(키), `SEQUENCE BY`(순서 보장 컬럼), `STORED AS SCD TYPE 2` |
| **Python API** | `from pyspark import pipelines as dp` → `@dp.table`, `@dp.materialized_view`, `@dp.expect_or_drop(...)` (구 `import dlt`, `@dlt.table`) |
| **Event Log** | 파이프라인 진행 상황, 데이터 품질, 리니지 기록 |

---

## 12. 명령어 치트시트

```sql
-- ===== 탐색 =====
SELECT current_user(), current_catalog(), current_schema();
SHOW CATALOGS; SHOW SCHEMAS; SHOW TABLES; SHOW VOLUMES;
LIST '/Volumes/cat/sch/vol/';
DESCRIBE TABLE EXTENDED t;  DESCRIBE DETAIL t;  DESCRIBE HISTORY t;

-- ===== 파일 미리보기 =====
SELECT * FROM read_files('/Volumes/.../file.csv', format => 'csv', header => true, inferSchema => true);

-- ===== 생성 =====
CREATE TABLE IF NOT EXISTS t (id INT, name STRING);                -- 빈 테이블 (스키마 명시)
CREATE TABLE IF NOT EXISTS t AS SELECT ... FROM read_files(...);   -- CTAS
CREATE OR REPLACE TABLE t AS SELECT ...;                           -- 교체 (히스토리 유지)
CREATE OR REPLACE TEMP VIEW v AS SELECT ...;                       -- 세션 임시 뷰

-- ===== 적재 / 수정 =====
INSERT INTO t VALUES (...);                -- 추가
INSERT OVERWRITE t SELECT ...;             -- 전체 교체 (스키마 유지)
UPDATE t SET c = v WHERE ...;
DELETE FROM t WHERE ...;
MERGE INTO t USING s ON ... WHEN MATCHED THEN ... WHEN NOT MATCHED THEN ...;
COPY INTO t FROM '/path/' FILEFORMAT = CSV FORMAT_OPTIONS (...) COPY_OPTIONS (...);

-- ===== 시간 여행 / 유지보수 =====
SELECT * FROM t VERSION AS OF 3;   SELECT * FROM t@v3;
SELECT * FROM t TIMESTAMP AS OF '2026-11-16';
RESTORE TABLE t TO VERSION AS OF 3;
OPTIMIZE t;   VACUUM t;   VACUUM t RETAIN 168 HOURS;

-- ===== 권한 =====
GRANT SELECT ON TABLE t TO `group`;   REVOKE SELECT ON TABLE t FROM `group`;   SHOW GRANTS ON TABLE t;

-- ===== 선언적 파이프라인 =====
CREATE OR REFRESH STREAMING TABLE b AS SELECT * FROM STREAM read_files(...);
CREATE OR REFRESH MATERIALIZED VIEW s (CONSTRAINT c EXPECT (x IS NOT NULL) ON VIOLATION DROP ROW)
AS SELECT ... FROM b;
```

---

## 13. 제품 명칭 변경 정리

Databricks는 제품 이름이 자주 바뀐다. 문서·블로그·커뮤니티 글에 옛 이름이 섞여 나오므로 대응 관계를 알아두자.

| 현재 이름 | 이전 이름 |
|---|---|
| **Lakeflow Jobs** | Databricks Workflows, Jobs |
| **Spark Declarative Pipelines (SDP)** / Lakeflow Declarative Pipelines | **Delta Live Tables (DLT)** |
| `CREATE OR REFRESH STREAMING TABLE` | `CREATE STREAMING LIVE TABLE` |
| `MATERIALIZED VIEW` (파이프라인) | `LIVE TABLE` |
| `AUTO CDC` | `APPLY CHANGES INTO` |
| `pyspark.pipelines` (`dp`) | `import dlt` |
| **Lakeflow Connect** | (수집 기능 통합 브랜드) |
| **Genie Code** | Databricks Assistant |
| Standard / Dedicated access mode | Shared / Single user access mode |
| Catalog Explorer | Data Explorer |
| (참고) Hive Metastore, `hive_metastore` 카탈로그 | Unity Catalog 이전의 레거시 메타스토어 (2단계 네임스페이스) |

---

## 14. 자격증 준비 가이드

> 주의: 시험 구성·문항 수·비용은 수시로 개편된다. 응시 전 [공식 Certification 페이지](https://www.databricks.com/learn/training/certification)의 **최신 Exam Guide**를 반드시 확인할 것.

### 14-1. Associate vs Professional

| 항목 | Data Engineer **Associate** | Data Engineer **Professional** |
|---|---|---|
| 대상 | 입문~초급 (약 6개월 경험) | 숙련 (1년 이상 실무 경험 권장) |
| 성격 | "이 기능이 무엇이고 어떻게 쓰는가" | "운영 환경에서 **어떤 설계·최적화·디버깅**이 맞는가" |
| 형식 | 객관식, 온라인 감독 | 객관식 (시나리오형·코드 해석 비중 높음), 온라인 감독 |
| 규모 (대략) | 약 45문항 / 90분 | 약 60문항 / 120분 |
| 코드 언어 | SQL + Python(PySpark) | 동일, 코드 비중 더 높음 |

> 최종 목표가 Professional이어도 **Associate를 먼저** 취득하는 것을 권장한다. Associate 범위가 Professional의 기초 전제다.

### 14-2. 이 문서가 다룬 범위와 추가 학습 필요 영역

| 시험 주제 | 이 문서 | 추가로 공부할 것 |
|---|---|---|
| 플랫폼 / Compute | 충분 | 클러스터 정책, 런타임 버전 |
| Unity Catalog | 일부 | Row filter·Column mask 실습, Delta Sharing, Lakehouse Federation |
| Delta Lake | 충분 | MERGE 심화(SCD Type 2), 동시성 충돌, CDF 실습 |
| 데이터 수집 | 일부 | Auto Loader 심화, Structured Streaming(트리거, 체크포인트, 워터마크, 상태 저장 연산) |
| 메달리온 아키텍처 | 충분 | 스트리밍 기반 메달리온, 중복 제거 패턴 |
| Lakeflow Jobs | 일부 | 파라미터·Task values 실습, Asset Bundles(CI/CD) |
| Declarative Pipelines | 일부 | AUTO CDC, Event Log 분석, Python API |
| 성능 최적화 / 모니터링 | 없음 | Spark UI, Query Profile, 셔플·스큐·스필, System Tables, 비용 최적화 |
| 데이터 모델링 / 보안 / 테스트 | 없음 | SCD, PII 처리, 단위 테스트, 디버깅 |

### 14-3. Professional 시험 주요 영역 (대략적 분류)

1. **Python·SQL 데이터 처리 코드 개발** — PySpark DataFrame API, UDF, 스트리밍
2. **데이터 수집·획득** — Auto Loader, Lakeflow Connect, CDC
3. **데이터 변환·정제·품질** — MERGE, 중복 제거, Expectations
4. **데이터 공유·연합** — Delta Sharing, Lakehouse Federation
5. **모니터링·알림** — System Tables, Event Log, Job 알림, Lakehouse Monitoring
6. **비용·성능 최적화** — Liquid Clustering, Photon, 파일 크기, Spark UI 해석
7. **보안·컴플라이언스** — 권한, Row filter/Column mask, PII, 시크릿
8. **데이터 거버넌스** — Unity Catalog 설계, 리니지, 감사
9. **디버깅·배포** — Repair run, Asset Bundles, CI/CD, 테스트
10. **데이터 모델링** — SCD, 메달리온 설계, 정규화/비정규화

### 14-4. 학습 전략

1. **Databricks Free Edition**에서 이 문서의 코드를 처음부터 직접 작성해 실행해보기
2. 공식 문서의 **Best practices** 페이지 정독 (Delta Lake, Structured Streaming, Jobs)
3. 13장의 명칭 변경 숙지 — 문제에 옛 이름이 나와도 당황하지 않기
4. 시험 직전: 공식 Exam Guide의 영역별 체크리스트와 Practice Exam으로 약점 보완

---

## 15. 자가 점검 문제

> 답을 가리고 먼저 생각해본 뒤 펼쳐보자.

**Q1.** Unity Catalog의 네임스페이스를 넓은 단위부터 쓰고, 테이블과 볼륨의 차이를 설명하라.

정답

Metastore → Catalog → Schema → Table/Volume (테이블 이름은 `catalog.schema.table` 3단계). 테이블은 정형·쿼리 가능한 데이터(SQL로 접근), 볼륨은 테이블로 만들기 전의 원시 파일(CSV, JSON 등)을 경로로 관리하는 권한 적용 폴더.

**Q2.** `read_files`로 CSV를 읽었더니 `_rescued_data` 컬럼에 값이 들어 있다. 무슨 의미인가?

정답

해당 행의 일부 값이 추론된 스키마와 맞지 않았다(타입 불일치, 추가 필드, 형식 오류). 데이터를 버리거나 오류를 내는 대신 이 컬럼에 보관한 것. 정상이면 null.

**Q3.** `CREATE TABLE IF NOT EXISTS`, `CREATE OR REPLACE TABLE`, 옵션 없는 `CREATE TABLE`은 테이블이 이미 있을 때 각각 어떻게 동작하는가?

정답

IF NOT EXISTS: 아무것도 하지 않음. OR REPLACE: 내용을 교체하고 새 버전으로 기록(히스토리 유지). 옵션 없음: 오류.

**Q4.** 버전 0의 데이터를 조회하는 문법 3가지는? 버전 번호는 어떻게 확인해야 하는가?

정답

`VERSION AS OF 0`, `@v0`, `TIMESTAMP AS OF '<시각>'`. 자동 OPTIMIZE(Predictive Optimization) 등이 버전을 차지할 수 있으므로 추측하지 말고 `DESCRIBE HISTORY`로 확인.

**Q5.** 팀원이 실수로 행을 삭제했다. 확인하고 복구하는 절차는?

정답

① `DESCRIBE HISTORY t`로 DELETE 버전 n 확인 → ② `VERSION AS OF n-1`로 삭제 전 상태 확인 → ③ `RESTORE TABLE t TO VERSION AS OF n-1` 또는 삭제된 행만 `INSERT INTO t SELECT ... FROM t VERSION AS OF n-1 WHERE ...`. 단 VACUUM으로 파일이 지워졌다면 불가.

**Q6.** VACUUM을 기본 설정으로 실행한 후 10일 전 버전으로 시간 여행이 가능한가?

정답

불가능. VACUUM 기본 보존 기간은 7일이라 그 이전 버전이 참조하던 데이터 파일이 삭제된다. 로그는 30일간 보존되므로 히스토리 목록에는 보일 수 있지만 데이터 조회는 실패한다.

**Q7.** 행 수가 같은 두 버전은 내용도 같다고 볼 수 있는가?

정답

아니다. 추가 1건 + 삭제 1건, 또는 UPDATE는 행 수를 바꾸지 않는다. 내용 비교는 `EXCEPT` 등으로 해야 한다.

**Q8.** CTAS와 COPY INTO의 차이와 각각의 사용 시점은?

정답

CTAS: 새 테이블을 만드는 일회성 작업, 스키마는 쿼리 결과에서 추론 → 일회성 생성, 컬럼 선택·변환. COPY INTO: 기존 테이블에 추가하고 처리한 파일을 추적해 멱등 → 정기적으로 새 파일이 도착하는 디렉터리의 증분 적재.

**Q9.** COPY INTO를 두 번 실행했더니 두 번째는 `num_affected_rows = 0`이었다. 이유와 히스토리 변화는?

정답

이미 처리한 파일을 경로 기준으로 기억하고 건너뛰었다(멱등성). 데이터 변경이 없으므로 새 버전도 생기지 않는다.

**Q10.** 파트너가 같은 파일명 `daily.csv`를 매일 내용만 바꿔 덮어쓴다. COPY INTO로 괜찮은가?

정답

문제가 있다. 경로 기준 추적이므로 덮어쓴 파일은 재처리하지 않는다. 파일명에 날짜를 넣도록 협의하거나, `COPY_OPTIONS ('force' = 'true')`(멱등성 포기, 중복 주의)를 쓰거나, Auto Loader의 `cloudFiles.allowOverwrites` 등을 검토한다.

**Q11.** 수백만 개의 JSON 파일이 계속 도착하고 스키마가 가끔 바뀐다. 무엇을 쓰는가?

정답

Auto Loader(`format("cloudFiles")`, `cloudFiles.schemaLocation`, 스키마 진화 모드) 또는 선언적 파이프라인의 Streaming Table(`STREAM read_files`). 대규모라면 File notification 모드도 고려.

**Q12.** 데이터를 최종 테이블에 바로 적재하지 않고 Bronze/Silver/Gold를 거치는 이유는?

정답

원본(Bronze)을 보존해 변환 버그 시 재처리 가능, 수집·변환·소비의 분리로 디버깅·관리 용이, 하나의 Silver에서 여러 Gold 재사용, 레이어별 접근 제어(분석가는 Gold만).

**Q13.** Gold 테이블을 매일 갱신할 때 `CREATE OR REPLACE` 대신 `INSERT OVERWRITE`를 쓰는 이유는?

정답

테이블 정의(스키마)와 권한 설정을 한 번만 해두고 내용만 교체하기 위해. INSERT OVERWRITE는 기존 스키마를 강제하므로 실수로 스키마가 바뀌는 것도 막아준다.

**Q14.** Job에서 Silver 태스크를 Bronze 태스크에 의존시키고 Run if를 `All succeeded`로 두는 이유는? 실패 알림 태스크와 정리 태스크는 각각 어떤 조건을 쓰는가?

정답

Bronze가 실패하면 Silver/Gold가 오래되거나 누락된 데이터로 실행되지 않도록. 실패 알림은 `At least one failed`, 성공·실패 무관 정리 작업은 `All done`.

**Q15.** Job이 중간 태스크에서 실패했다. 처음부터 다시 돌리지 않고 복구하는 기능은?

정답

Repair run — 실패한 태스크와 그 하위 태스크만 재실행한다.

**Q16.** 선언적 파이프라인에서 Streaming Table과 Materialized View는 각각 어디에 쓰는가?

정답

Streaming Table: append-only 소스의 증분 수집, Bronze. Materialized View: 변환·집계 결과 저장, 소스로부터 재계산(가능하면 증분), Silver/Gold.

**Q17.** Expectation 위반 시 동작 3가지와 문법은?

정답

경고(기본, 행 유지·기록): `EXPECT (cond)` / 행 삭제: `EXPECT (cond) ON VIOLATION DROP ROW` / 실패: `EXPECT (cond) ON VIOLATION FAIL UPDATE`.

**Q18.** Managed 테이블과 External 테이블을 각각 DROP하면?

정답

Managed: 메타데이터와 데이터 파일 모두 삭제(7일 내 UNDROP 가능). External: 메타데이터만 삭제, 스토리지의 파일은 남는다.

**Q19.** 분석가 그룹에 `prod.sales.revenue` 테이블 SELECT 권한만 줬는데 조회가 안 된다. 원인은?

정답

상위 객체에 대한 `USE CATALOG`(prod)와 `USE SCHEMA`(prod.sales) 권한이 없기 때문.

**Q20.** `%run`과 `dbutils.notebook.run()`의 차이는?

정답

`%run`은 같은 컨텍스트에서 실행해 변수·함수를 공유한다(인자 전달 불가). `dbutils.notebook.run()`은 별도로 실행되어 변수 공유가 없고, 파라미터 전달과 `dbutils.notebook.exit()`를 통한 반환값 수신이 가능하다.

**Q21.** Serverless, Pro, Classic SQL Warehouse의 차이는?

정답

Serverless: Databricks 계정에서 실행, Photon + Predictive IO + Intelligent Workload Management, 가장 빠른 시작(권장). Pro: 고객 계정에서 실행, Photon + Predictive IO, 네트워크·규정 준수 요건이 있을 때. Classic: Photon만, 레거시.

---

## 16. 참고 링크

### 공식 문서 (AWS 기준, Azure/GCP는 경로 동일)

- Delta Lake: https://docs.databricks.com/aws/en/delta/
- Unity Catalog: https://docs.databricks.com/aws/en/data-governance/unity-catalog/
- Lakeflow Connect (수집 개요): https://docs.databricks.com/aws/en/ingestion/overview
- Auto Loader: https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/
- COPY INTO: https://docs.databricks.com/aws/en/sql/language-manual/delta-copy-into
- Lakeflow Jobs: https://docs.databricks.com/aws/en/jobs/
- Spark Declarative Pipelines: https://docs.databricks.com/aws/en/ldp/
- DLT 명칭 변경 안내: https://docs.databricks.com/aws/en/ldp/where-is-dlt
- 메달리온 아키텍처: https://docs.databricks.com/aws/en/lakehouse/medallion
- Genie Code: https://docs.databricks.com/aws/en/genie-code/
