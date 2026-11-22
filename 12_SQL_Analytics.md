# Databricks SQL 분석 — 핵심 개념 정리

---

## 목차

1. [분석가가 일하는 플랫폼 구조](#1-분석가가-일하는-플랫폼-구조)
2. [Unity Catalog와 3단계 네임스페이스](#2-unity-catalog와-3단계-네임스페이스)
3. [스키마 안의 데이터 객체](#3-스키마-안의-데이터-객체)
4. [데이터를 찾는 세 가지 방법](#4-데이터를-찾는-세-가지-방법)
5. [카탈로그 탐색기로 보는 메타데이터](#5-카탈로그-탐색기로-보는-메타데이터)
6. [보안: 최소 권한, 주체, 소유권, 권한](#6-보안-최소-권한-주체-소유권-권한)
7. [공유 데이터: Marketplace와 OpenSharing](#7-공유-데이터-marketplace와-opensharing)
8. [SQL로 환경 탐색하기](#8-sql로-환경-탐색하기)
9. [데이터 수집에서 분석가의 역할, Delta Lake, 메달리온](#9-데이터-수집에서-분석가의-역할-delta-lake-메달리온)
10. [파일을 직접 가져오는 방법](#10-파일을-직접-가져오는-방법)
11. [경로로 파일 조회와 read_files](#11-경로로-파일-조회와-read_files)
12. [COPY INTO](#12-copy-into)
13. [스트리밍 테이블](#13-스트리밍-테이블)
14. [Databricks SQL과 SQL 웨어하우스](#14-databricks-sql과-sql-웨어하우스)
15. [SQL 편집기와 노트북](#15-sql-편집기와-노트북)
16. [데이터 탐색과 변환 SQL](#16-데이터-탐색과-변환-sql)
17. [필터, 집계, 서브쿼리, CTE](#17-필터-집계-서브쿼리-cte)
18. [PIVOT과 윈도우 함수](#18-pivot과-윈도우-함수)
19. [뷰의 종류](#19-뷰의-종류)
20. [information_schema로 메타데이터 조회](#20-information_schema로-메타데이터-조회)
21. [종합 예: 브론즈에서 골드까지](#21-종합-예-브론즈에서-골드까지)
22. [핵심 정리 치트시트](#22-핵심-정리-치트시트)
23. [자가 점검 문제](#23-자가-점검-문제)
24. [참고 링크](#24-참고-링크)

---

## 1. 분석가가 일하는 플랫폼 구조

| 계층 | 구성 | 역할 |
|---|---|---|
| **작업 공간** | Databricks Workspace (카탈로그 탐색기, SQL 편집기, 노트북) | 데이터를 찾고, 쿼리하고, 공유하는 곳 |
| **거버넌스** | Unity Catalog (권한, 리니지, 검색) | 누가 무엇을 볼 수 있는지 결정하고, 데이터 출처를 추적하고, 자산을 찾을 수 있게 만든다 |
| **기반** | 클라우드(AWS, Azure, GCP) 위의 레이크하우스, 오픈 포맷 | 사용자 클라우드 계정의 오픈 스토리지에 데이터를 저장. 다른 도구도 같은 데이터를 읽을 수 있어 특정 플랫폼에 종속되지 않는다 |

분석가의 일은 두 질문에서 시작한다. **데이터가 어디 있고 믿을 수 있는가**, 그리고 **누가 쓸 수 있는가**. Databricks에서는 둘 다 Unity Catalog가 답한다.

---

## 2. Unity Catalog와 3단계 네임스페이스

Unity Catalog는 모든 데이터와 AI 자산을 위한 통합 거버넌스 계층이다. 데이터를 중첩된 컨테이너로 구성한다.

| 수준 | 의미 |
|---|---|
| **메타스토어** | 최상위 컨테이너. 관리자가 관리하며 그 아래 모든 메타데이터와 접근 규칙을 가진다. 분석가가 직접 다룰 일은 드물다 |
| **카탈로그** | 주소의 첫 부분. 관련 스키마를 묶는다. 보통 팀, 프로젝트, 환경별로 하나 |
| **스키마** (데이터베이스) | 주소의 두 번째 부분. 서로 관련된 테이블과 뷰를 묶는다 |
| **데이터 객체** | 주소의 세 번째 부분. 테이블, 뷰, 볼륨, 함수, 모델 |

모든 객체는 `catalog.schema.object` 형태의 고유한 주소를 가진다(예: `sales_catalog.au_sales.orders`). 두 스키마에 같은 이름의 테이블이 있어도 전체 주소로 모호함이 사라진다.

추가로 알아 둘 것:

- `information_schema`: Unity Catalog의 모든 카탈로그에 자동으로 생기는 SQL 표준 스키마. 볼 권한이 있는 객체를 설명하는 뷰 모음이다.
- 레거시 `hive_metastore`: 이전 방식의 메타스토어. 하위 호환을 위해 남아 있지만 Databricks는 모든 객체를 Unity Catalog로 관리하길 권장한다.

---

## 3. 스키마 안의 데이터 객체

| 객체 | 설명 | 세부 |
|---|---|---|
| **테이블** | 행과 열로 구성된 구조화된 데이터. 분석의 핵심 | 관리형 또는 외부 |
| **뷰** | 하나 이상의 테이블에 대해 저장된 쿼리. 데이터 복사본이 아니다. 읽기 전용 | 표준, 임시, 구체화, 동적, 메트릭 뷰 |
| **볼륨** | 클라우드 객체 스토리지 위치를 나타내는 논리적 저장 공간. **파일**을 관리 | CSV, JSON, 텍스트, 이미지 등 비표 형식 데이터 |
| **함수** | 값이나 행 집합을 반환하는 저장된 로직 | 한 번 작성해 팀 전체가 재사용 |

### 관리형 테이블 vs 외부 테이블

| | 관리형 (Managed) | 외부 (External) |
|---|---|---|
| 데이터 관리 | Databricks가 데이터 파일까지 관리 | 사용자가 관리하는 스토리지 경로를 가리킴 |
| `DROP TABLE` 시 | 데이터도 함께 삭제 | 메타데이터만 삭제, 원본 파일은 남음 |
| 권장 | **기본 권장** | 기존 외부 데이터를 그대로 써야 할 때 |

---

## 4. 데이터를 찾는 세 가지 방법

| 방법 | 특징 | 적합한 상황 |
|---|---|---|
| **카탈로그 탐색기** | 시각적으로 카탈로그 → 스키마 → 객체를 펼쳐 보고 컬럼, 샘플 데이터, 설명, 권한을 확인 | 전체 구조를 둘러보며 데이터를 이해할 때 |
| **지능형 검색** | 정확한 이름이 아니라 비즈니스 용어로 검색. 객체의 설명, 주석, 태그를 활용 | 개념은 아는데 정확한 이름을 모를 때 |
| **프로그래밍 방식** | `SHOW CATALOGS`, `SHOW SCHEMAS`, `SHOW TABLES` 같은 SQL | 정확하고 반복 가능한 목록이 필요할 때, 스크립트로 자동화할 때 |

지능형 검색의 품질은 문서화 품질에 달려 있다. 내가 소유한 객체에는 명확한 설명과 주석을 붙여 둔다.

---

## 5. 카탈로그 탐색기로 보는 메타데이터

테이블 하나를 열면 다음 정보를 볼 수 있다.

| 영역 | 내용 |
|---|---|
| 개요 | 컬럼 목록, 소유자, 유형, 데이터 소스, 인기도, 최종 수정, 크기. 태그와 행 필터 추가. **AI가 제안하는 테이블 설명과 컬럼 주석** |
| 샘플 데이터 | 실제 행 미리보기 |
| 세부 정보 | 관리형/외부 여부, 클라우드 저장 위치, 생성자 |
| 권한 | 현재 권한 확인, 다른 주체에게 권한 부여 |
| 이력 | 테이블 버전 이력. 설명이나 주석 추가도 새 버전으로 기록된다 |
| 리니지 | 업스트림/다운스트림 테이블과 원본 파일. 그래프로 시각화 |
| 인사이트 | 최근 30일간 자주 실행된 쿼리와 사용자 |
| 품질 | 데이터 품질과 통계 분포 변화를 추적하는 모니터 생성 |

- 테이블 설명을 수락하면 이력에 `SET TBLPROPERTIES` 작업으로, 컬럼 주석은 컬럼 변경 작업으로 새 버전이 남는다.
- `CREATE TABLE AS SELECT`로 만든 테이블의 첫 버전(Version 0)은 그 작업으로 기록된다.
- 리니지는 테이블이 어떤 파일이나 테이블로부터 만들어졌는지 보여 주므로, 데이터를 믿을 수 있는지 판단하는 근거가 된다.

---

## 6. 보안: 최소 권한, 주체, 소유권, 권한

### 6-1. 최소 권한 원칙

각 사용자나 시스템에 **필요한 최소한의 권한만, 필요한 기간 동안만** 부여한다. 권한이 넓게 퍼질수록 우발적 노출과 오용 위험이 커진다.

| 실천 습관 | 내용 |
|---|---|
| 역할 기반 접근 | 요청마다 개별 부여하지 않고 역할에 필요한 권한을 기준으로 결정 |
| 모니터링과 감사 | 권한을 정기적으로 검토하고 더 이상 필요 없는 권한은 회수 |
| 알 필요성 | 실제로 필요한 사람에게, 필요한 기간에만 공유하는 것을 기본으로 |

### 6-2. 세 가지 주체 (Principal)

주체는 권한을 받을 수 있는 모든 ID다.

| 주체 | 의미 | 식별/용도 |
|---|---|---|
| **사용자** | 플랫폼에 접근하는 사람 | 이메일 주소 |
| **서비스 주체** | 사람이 아닌 행위자 | 자동화 작업, 예약 프로세스, 서드파티 연결. 운영 작업은 개인이 아닌 서비스 주체로 실행하는 경우가 많다 |
| **어카운트 그룹** | 사용자와 서비스 주체의 묶음 | 그룹 단위로 한 번에 권한 관리 |

모범 사례는 **개인이 아니라 그룹에** 소유권과 권한을 주는 것이다. 구성원이 역할을 바꿔도 접근이 끊기지 않는다.

### 6-3. 소유권

- 기본적으로 객체를 **생성한 주체가 소유자**가 되며, 다른 사람에게 권한을 주는 것을 포함한 완전한 제어권을 가진다.
- 소유권은 계층을 따라 내려간다. 카탈로그 소유자는 다른 사람이 만든 것을 포함해 그 안의 스키마와 객체에 대한 권한을 가진다.
- 노트북이나 쿼리 같은 워크스페이스 객체의 소유자는 워크스페이스 관리자만 바꿀 수 있다.

### 6-4. 두 종류의 객체, 두 가지 공유 방식

| 객체 종류 | 예 | 공유 방식 |
|---|---|---|
| **워크스페이스 객체** | 노트북, 쿼리, 대시보드, SQL 파일 | 권한 수준으로 공유 (아래 표) |
| **Unity Catalog 데이터 객체** | 테이블, 뷰, 볼륨 | `GRANT`로 `SELECT`, `MODIFY` 같은 세분화된 권한 부여 |

워크스페이스 객체의 권한 수준은 누적된다.

| 수준 | 할 수 있는 일 |
|---|---|
| **CAN VIEW** | 보기, 결과 읽기, 복사본 만들기 |
| **CAN RUN** | 위 + 실행해서 결과 갱신 |
| **CAN EDIT** | 위 + 내용 변경 |
| **CAN MANAGE** | 위 + 권한 변경과 삭제. 신중히 부여 |

```sql
-- 데이터 객체 권한 부여 예
GRANT USE CATALOG ON CATALOG sales_catalog TO `analysts`;
GRANT USE SCHEMA ON SCHEMA sales_catalog.au_sales TO `analysts`;
GRANT SELECT ON TABLE sales_catalog.au_sales.orders TO `analysts`;
SHOW GRANTS ON TABLE sales_catalog.au_sales.orders;
```

테이블을 읽으려면 테이블의 `SELECT`뿐 아니라 상위 카탈로그의 `USE CATALOG`, 스키마의 `USE SCHEMA`도 필요하다.

### 6-5. 분석가에게 의미하는 것

| 상황 | 할 일 |
|---|---|
| 내가 만든 자산 | 누가 보고 쓸지 직접 결정. 데이터 객체는 `GRANT`, 워크스페이스 객체는 공유 |
| 남이 소유한 자산이 필요 | **권한이 없는 객체는 아예 보이지 않는다.** 필요해지기 전에 소유자에게 공유를 요청 |
| 지속적 관리 | 내가 관리하는 객체의 권한을 주기적으로 검토하고, 개인보다 그룹을 사용 |

---

## 7. 공유 데이터: Marketplace와 OpenSharing

- **Databricks Marketplace**는 데이터셋, 노트북, 모델 등을 공유받을 수 있는 곳이다. 공급자(예: Databricks)가 올린 공유를 받으면 내 메타스토어에 **공유 카탈로그**로 나타난다.
- 공유 카탈로그의 유형은 **OpenSharing**(Delta Sharing 기반)으로 표시되며, 받은 공유 목록 아래에 보인다.
- 공유 카탈로그는 **읽기 전용**이다. `SELECT`는 할 수 있지만 쓰기는 할 수 없다.
- 바로 접근할 수 없는 공유는 접근 요청을 보낼 수 있다.
- `samples` 카탈로그처럼 Databricks가 소유한 기본 제공 카탈로그도 있다(예: `samples.nyctaxi.trips`).

---

## 8. SQL로 환경 탐색하기

```sql
SHOW CATALOGS;                                  -- 볼 수 있는 카탈로그
SELECT current_catalog(), current_schema();     -- 현재 기본 카탈로그와 스키마
SHOW SCHEMAS;                                   -- 현재 카탈로그의 스키마
SHOW SCHEMAS IN samples;                        -- 특정 카탈로그의 스키마
SHOW TABLES;                                    -- 현재 스키마의 테이블 (뷰 포함)
SHOW TABLES IN samples.nyctaxi;
SHOW VIEWS;                                     -- 뷰만
SHOW VOLUMES;                                   -- 볼륨
LIST '/Volumes/my_catalog/retail/backup';       -- 볼륨 안의 파일 목록
```

### 기본 카탈로그와 스키마

- 기본 카탈로그와 스키마가 설정되어 있으면 그 안의 객체는 이름만으로 참조할 수 있다(`SELECT * FROM ca_orders`).
- 다른 카탈로그의 객체는 전체 3단계 이름이 필요하다. `SELECT * FROM trips`는 기본 위치에 `trips`가 없으면 오류다.

```sql
USE CATALOG samples;
USE SCHEMA nyctaxi;
SELECT * FROM trips LIMIT 10;     -- 이제 이름만으로 동작

-- 카탈로그 이름을 변수나 문자열로 동적으로 넘길 때
USE CATALOG IDENTIFIER(my_catalog);
USE SCHEMA retail;
```

- `IDENTIFIER()`는 문자열이나 변수를 객체 이름으로 해석한다.
- `LIST`는 볼륨이나 외부 위치의 파일을 나열한다. Unity Catalog 관리형 테이블의 내부 파일은 나열하지 않는다.

---

## 9. 데이터 수집에서 분석가의 역할, Delta Lake, 메달리온

### 9-1. 역할 분담

| 역할 | 하는 일 |
|---|---|
| 데이터 엔지니어 | 대규모 수집 파이프라인 구축과 일정 관리. Spark Declarative Pipelines 같은 도구로 자동화 |
| 데이터 분석가 | 엔지니어가 준비한 테이블을 쿼리. 필요할 때 목표값 스프레드시트나 조회 목록 같은 **작은 보조 파일만 직접** 가져옴 |

직접 수집은 예외적인 경우다. 지속적이거나 대규모인 데이터는 엔지니어링 팀과 일찍 협의해 처음부터 제대로 수집되게 한다.

### 9-2. Delta Lake

Databricks는 테이블을 Delta Lake 형식으로 저장한다. Unity Catalog 관리형 테이블은 다음 두 가지로 이루어진다.

| 구성 | 내용 |
|---|---|
| 데이터 파일 | 컬럼 기반 Parquet 파일 |
| 트랜잭션 로그 | 모든 변경과 버전을 기록한 JSON 로그. 파일 모음을 신뢰할 수 있는 테이블로 만드는 핵심 |

| 로그가 주는 기능 | 내용 |
|---|---|
| ACID 트랜잭션 | 여러 사용자가 동시에 읽고 써도 일관성 유지 |
| 완전한 DML | `INSERT`, `UPDATE`, `DELETE`, `MERGE` |
| 시간 여행 | 이전 버전을 조회하거나 복원 |
| 스키마 강제와 진화 | 잘못된 데이터 유입을 막고, 필요할 때 의도적으로 스키마 변경 |

Delta Lake는 오픈 소스라 특정 벤더에 종속되지 않는다.

### 9-3. 메달리온 아키텍처

| 레이어 | 데이터 | 특징 |
|---|---|---|
| **브론즈** (원시) | 소스에서 온 그대로 | 손실 없이 보관. 로직이 바뀌거나 버그가 있으면 다시 처리할 수 있다 |
| **실버** (정제) | 필터링, 정제, 조인, 보강, 스키마 적용 | 여러 프로젝트가 공유하는 단일 진실 공급원 |
| **골드** (큐레이션) | 집계된 비즈니스용 테이블 | 특정 보고서, 대시보드, 분석에 맞춤. **분석가가 주로 쿼리하는 곳** |

실제 흐름은 직선이 아니라 여러 테이블로 갈라지고 합쳐지는 경우가 많다. Unity Catalog가 전 과정의 리니지를 추적한다.

---

## 10. 파일을 직접 가져오는 방법

| 방법 | 특징 | 적합한 경우 |
|---|---|---|
| **파일 업로드 UI** | 작은 CSV 등을 업로드해 바로 테이블로 만들거나 볼륨에 저장. 구분자, 헤더, 컬럼 유형 등 옵션 지정 가능 | 이메일로 받은 목록 같은 일회성 작업 |
| **`read_files`** | 쿼리 안에서 볼륨이나 경로의 파일을 테이블 형태로 읽는 SQL 함수 | 저장 전에 내용을 확인하거나 SQL의 유연성이 필요할 때 |
| **`COPY INTO`** | 파일을 테이블에 적재하는 SQL 명령. **멱등**: 다시 실행해도 이미 적재한 파일은 건너뜀 | 같은 파일이 여러 번 처리될 수 있는 반복 적재 |

파일 업로드 UI는 두 갈래다.

- **볼륨에 업로드**: 파일 자체를 볼륨에 저장. 이후 SQL로 읽어 테이블을 만든다.
- **업로드로 테이블 생성**: 업로드하면서 바로 관리형 테이블을 만든다.

조직의 보안 정책에 따라 파일 다운로드나 업로드가 막혀 있을 수 있다.

---

## 11. 경로로 파일 조회와 read_files

### 11-1. 경로로 직접 조회

테이블을 만들기 전에 원본 파일의 구조(구분자, 헤더 유무, 전체 형태)를 먼저 확인한다.

```sql
-- 원시 텍스트 그대로 (한 줄 = 한 행)
SELECT * FROM text.`/Volumes/my_catalog/retail/backup/au_products.csv`;

-- JSON을 기본 옵션으로 테이블처럼
SELECT * FROM json.`/Volumes/dbacademy_retail/v01/retail-pipeline/customers/stream_json/` LIMIT 10;

-- 그 밖에 csv., parquet. 등 형식 접두사 사용 가능
```

경로는 백틱으로 감싼다. 폴더 경로를 주면 그 안의 파일을 모두 읽는다.

### 11-2. read_files

`read_files`는 지정 위치의 파일을 읽어 테이블 형태로 반환하는 **테이블 반환 함수(TVF)** 다. JSON, CSV, XML, TEXT, BINARYFILE, PARQUET, AVRO, ORC를 지원한다.

```sql
CREATE OR REPLACE TABLE au_products AS
SELECT *
FROM read_files(
  '/Volumes/my_catalog/retail/backup/au_products.csv',
  format => 'csv',
  header => true
);
```

| 옵션/동작 | 의미 |
|---|---|
| `format` | 파일 형식. 생략하면 추론 |
| `header => true` | CSV 첫 행이 헤더 |
| `schema => '...'` | 스키마 직접 지정 |
| 스키마 미지정 시 | 대상 파일 전체를 읽어 통합 스키마를 추론. 대용량이면 비효율적 |
| `_rescued_data` 컬럼 | 스키마와 맞지 않는 데이터를 버리지 않고 담아 두는 컬럼. 기본으로 추가된다 |
| 폴더 경로 | 파일 이름 없이 폴더를 주면 관련 파일을 모두 읽는다 |

```sql
-- 대용량 파일은 스키마를 명시해 추론 비용을 없앤다
CREATE OR REPLACE TABLE au_products_with_schema AS
SELECT *
FROM read_files(
  '/Volumes/my_catalog/retail/backup/au_products.csv',
  format => 'csv',
  header => true,
  schema => 'productid STRING, productname STRING, listprice DOUBLE'
);

-- JSON 폴더 전체
CREATE OR REPLACE TABLE customers_lab AS
SELECT * FROM read_files('/Volumes/dbacademy_retail/v01/retail-pipeline/customers/stream_json/', format => 'json');
```

경로 문자열은 `'/Volumes/' || my_catalog || '/retail/backup/au_products.csv'`처럼 연결해 동적으로 만들 수 있다.

---

## 12. COPY INTO

파일 위치의 데이터를 Delta 테이블로 적재한다. **재시도 가능하고 멱등**이며, 이미 적재한 파일은 건너뛴다(적재 후 수정된 파일도 다시 읽지 않는다).

```sql
-- 1) 스키마를 정한 빈 테이블
CREATE TABLE au_products_copy_into (
  productid STRING,
  productname STRING,
  listprice DOUBLE
);

-- 2) 적재
COPY INTO au_products_copy_into
FROM '/Volumes/my_catalog/retail/backup'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true');
```

같은 `COPY INTO`를 다시 실행하면 `num_affected_rows`, `num_inserted_rows`가 0이다.

```sql
-- 스키마 없이 빈 테이블을 만들고, 적재하면서 스키마를 병합
CREATE TABLE customers_lab;
COPY INTO customers_lab
FROM '/Volumes/dbacademy_retail/v01/retail-pipeline/customers/stream_json/'
FILEFORMAT = JSON
COPY_OPTIONS ('mergeSchema' = 'true');
```

| 절 | 의미 |
|---|---|
| `FROM` | 볼륨 경로 또는 외부 위치 |
| `FILEFORMAT` | CSV, JSON, PARQUET 등 |
| `FORMAT_OPTIONS` | 형식별 옵션 (헤더, 스키마 추론 등) |
| `COPY_OPTIONS` | 적재 동작 옵션 (`mergeSchema`, `force` 등) |

> `COPY INTO`는 레거시로 분류된다. 클라우드 객체 스토리지의 증분 수집에는 **스트리밍 테이블**(`CREATE STREAMING TABLE`)이 더 확장성 있고 안정적인 권장 대안이다.

### read_files vs COPY INTO

| | `CREATE TABLE AS SELECT ... read_files` | `COPY INTO` |
|---|---|---|
| 실행 결과 | 전체를 다시 읽어 테이블 생성/교체 | 새 파일만 추가 적재 |
| 멱등성 | `CREATE OR REPLACE`면 결과는 같지만 매번 전체 재처리 | 이미 적재한 파일 자동 건너뜀 |
| 테이블 준비 | 쿼리가 테이블을 만든다 | 대상 테이블을 먼저 만든다 |

---

## 13. 스트리밍 테이블

스트리밍 테이블은 소스에 **새로 들어온 데이터만 증분으로** 처리하는 테이블이다. 내부적으로 Spark Declarative Pipeline이 만들어지며, Unity Catalog와 Databricks SQL(서버리스 파이프라인)에서 지원된다.

```sql
CREATE OR REFRESH STREAMING TABLE emp_silver_streaming
SCHEDULE EVERY 1 HOUR                 -- 선택: 자동 갱신 일정
AS
SELECT *, current_timestamp() AS IngestDateTime
FROM STREAM emp_bronze_raw;           -- STREAM 키워드: 증분으로 읽기

-- 일정을 기다리지 않고 즉시 갱신
REFRESH STREAMING TABLE emp_silver_streaming;

DESCRIBE HISTORY emp_silver_streaming;
```

- 이력에는 파이프라인 설정 작업과 `STREAMING UPDATE` 작업이 기록된다.
- 소스에 2행이 추가된 뒤 갱신하면 `operationMetrics.numOutputRows = 2`, `outputMode = Append`로 새 행만 추가된 것을 확인할 수 있다.
- 클라우드 스토리지 파일을 직접 스트리밍하려면 `FROM STREAM read_files('경로', format => 'json')` 형태를 쓴다.

---

## 14. Databricks SQL과 SQL 웨어하우스

### 14-1. Databricks SQL

레이크하우스 위에 구축된 서버리스 데이터 웨어하우스로, AI가 엔진에 내장되어 있다.

| 특징 | 내용 |
|---|---|
| AI 기반 최적화 | 서버리스 compute가 데이터 규모와 쿼리 복잡도에 맞춰 자동 확장, 수동 튜닝 없이 자동 최적화 |
| 지능형 경험 | 데이터와 비즈니스 용어를 이해하는 자연어 지원 |
| 통합 아키텍처 | 수집, 변환, 쿼리를 한 플랫폼에서. SQL과 AI 기능을 함께 사용 |
| 분석 기능 | ANSI SQL, 완전한 SQL 편집기, 쿼리 일정과 알림, 배치와 스트리밍을 하나의 거버넌스 플랫폼에서 |

### 14-2. SQL 웨어하우스 유형

SQL 웨어하우스는 SQL 쿼리에 최적화된 compute다.

| 유형 | Photon | Predictive I/O | Intelligent Workload Management | 사용 시점 |
|---|---|---|---|---|
| **서버리스** | 예 | 예 | 예 | **권장 기본값** |
| **Pro** | 예 | 예 | 아니오 | 리전에서 서버리스를 쓸 수 없거나 커스텀 네트워크 요구로 제한될 때 |
| **Classic** | 예 | 아니오 | 아니오 | 둘 다 쓸 수 없을 때의 마지막 선택 |

| 기능 | 의미 |
|---|---|
| **Photon** | SQL 실행을 가속하는 벡터화 쿼리 엔진 |
| **Predictive I/O** | 선택적 스캔(조회, 필터)을 빠르게 |
| **Intelligent Workload Management** | AI로 쿼리를 라우팅하고 실제 수요에 따라 웨어하우스를 확장/축소 |

### 14-3. 서버리스를 권장하는 이유

| 이점 | 내용 |
|---|---|
| 즉각적이고 탄력적 | 몇 초 만에 준비, 수요에 따라 자동 확장/축소 |
| 최소한의 관리 | 용량 계획, 패치, 업그레이드, 튜닝을 Databricks가 처리 |
| 낮은 총비용 | 과도한 프로비저닝과 유휴 compute 비용이 없고 쓴 만큼만 지불 |

웨어하우스는 보통 관리자가 미리 만들어 두고, 분석가는 알맞은 웨어하우스에 연결하기만 한다.

---

## 15. SQL 편집기와 노트북

### 15-1. 노트북의 SQL

- 노트북은 SQL 웨어하우스나 서버리스 compute에 연결해 SQL 셀을 실행한다. 마크다운 설명과 함께 쓰기 좋다.
- 셀 **포커스 모드**로 셀을 크게 펼쳐 작성하고, 한 셀에서 여러 문장의 결과를 각각 확인할 수 있다.
- 결과에서 바로 시각화를 만들 수 있다.

### 15-2. 통합 SQL 편집기

다른 SQL 도구에 익숙한 개발자에게 맞는 SQL 전용 작성 환경이다.

| 기능 | 내용 |
|---|---|
| 웨어하우스와 기본 위치 선택 | 연결할 웨어하우스와 쿼리의 기본 카탈로그/스키마 지정 |
| 선택 실행 | 선택한 문장만 실행. 여러 문장을 실행하면 결과를 순서대로 넘겨 볼 수 있다 |
| 결과 활용 | 결과 다운로드, 복사, 시각화 생성 |
| **쿼리 파라미터** | `:param_name` 형태로 쿼리에 파라미터를 넣고 위젯으로 값 입력 |
| AI 자연어 필터 | 결과를 "amount less than 1000" 같은 자연어로 필터링 |
| 일정과 공유 | 저장한 SQL 파일에 일정 설정, 다른 사용자와 공유 |
| 성능 확인 | 쿼리 실행 후 성능 정보와 쿼리 프로필 확인 |
| 생산성 | 코드 접기, 명령 팔레트, 테마, 실시간 협업, 버전 이력, 워크스페이스 파일 브라우저 |
| AI 어시스턴트 | 자연어로 SQL 작성, 오류 진단, 최적화 제안 |

```sql
-- 파라미터 예: 위젯에서 my_state_value 값을 입력 (기본값 'SA')
SELECT *
FROM aus_opportunities
WHERE state = :my_state_value;
```

AI 어시스턴트의 제안은 직접 결과를 확인한 뒤 쓴다.

---

## 16. 데이터 탐색과 변환 SQL

### 16-1. 테이블 구조 확인

```sql
SELECT * FROM aus_orders LIMIT 10;
DESCRIBE TABLE aus_orders;            -- 컬럼 이름, 유형, 주석
DESCRIBE TABLE EXTENDED aus_orders;   -- + 생성 시간, 관리형/외부, 소유자, 위치 등
SELECT DISTINCT productid FROM aus_orders;
```

### 16-2. 변환 단계를 쌓아 실버 테이블 만들기

변환은 `LIMIT`으로 로직을 검증한 뒤 전체에 적용한다.

```sql
CREATE OR REPLACE TABLE aus_orders_silver AS
SELECT
  ord.orderid                       AS orders_id,       -- 컬럼 이름 정리
  ord.customerid                    AS customer_id,
  ord.productid                     AS product_id,
  prod.productname                  AS product_name,    -- 조회 테이블에서 가져옴
  ord.quantity,
  ord.orderamt                      AS order_amt,
  ROUND(order_amt / quantity, 2)    AS price_per_item,  -- 계산 컬럼
  UPPER(ord.salesrep)               AS sales_rep,       -- 값 표준화
  'Australia'                       AS order_country,   -- 상수 컬럼
  ord.orderdate                     AS order_date,
  YEAR(order_date)                  AS order_year,
  MONTH(order_date)                 AS order_month,
  DAY(order_date)                   AS order_day,
  DAYNAME(order_date)               AS order_dayofweek,
  QUARTER(order_date)               AS order_quarter,
  CONCAT(order_year, '-Q', order_quarter) AS order_year_quarter,
  CASE
    WHEN order_amt <= 250000 THEN 'Low'
    WHEN order_amt < 1000000 THEN 'Middle'
    ELSE 'High'
  END                               AS order_amt_category
FROM aus_orders ord
  INNER JOIN au_products_lookup prod
  ON ord.productid = prod.productid;
```

- 같은 `SELECT` 안에서 앞서 정의한 별칭(`order_amt`, `order_year`)을 뒤 컬럼에서 바로 참조할 수 있다(**측면 컬럼 별칭**, lateral column alias). 표준 SQL이나 다른 DB에서는 안 될 수 있다.
- `CASE WHEN`은 위에서부터 첫 번째로 참인 조건의 값을 반환한다.
- 그 밖의 날짜 형식은 `date_format`으로 만든다.

### 16-3. 조인 종류

| 조인 | 반환 |
|---|---|
| `INNER JOIN` | 양쪽에서 일치하는 행만 |
| `LEFT JOIN` | 왼쪽 전체 + 오른쪽 일치 행 (없으면 NULL) |
| `RIGHT JOIN` | 오른쪽 전체 + 왼쪽 일치 행 |
| `FULL JOIN` | 양쪽 전체, 불일치는 NULL |

원본 행을 하나도 잃지 않아야 하면 `LEFT JOIN`을 쓴다(예: 주 약어를 전체 주 이름으로 보강하되 고객 행은 모두 유지).

### 16-4. 자주 쓰는 변환 함수

| 함수 | 예 | 결과 |
|---|---|---|
| `UPPER`, `LOWER` | `UPPER(salesrep)` | 대소문자 표준화 |
| `ROUND` | `ROUND(x, 2)` | 소수 둘째 자리 |
| `CONCAT` | `CONCAT(y, '-Q', q)` | `2020-Q1` |
| `YEAR`, `MONTH`, `DAY`, `QUARTER`, `DAYNAME` | `DAYNAME(d)` | 날짜 구성 요소 |
| `from_unixtime` | `from_unixtime(1632417981)` | Unix 초 → 타임스탬프 문자열 |
| `date_format` | `date_format(ts, 'yyyy-MM-dd HH:mm:ss')` | 원하는 형식 문자열 |
| `substring_index` | `substring_index('peter@example.org', '@', -1)` | `example.org` (음수는 오른쪽부터) |

리니지는 이렇게 만든 실버 테이블이 어떤 업스트림 테이블에서 왔는지 자동으로 보여 준다.

---

## 17. 필터, 집계, 서브쿼리, CTE

```sql
-- 필터와 정렬
SELECT sales_rep, orders_id, customer_id, product_name, order_amt
FROM aus_orders_silver
WHERE sales_rep = 'RAVI' AND customer_id = 'NS7598'
ORDER BY order_amt DESC;

-- 요약 통계
SELECT COUNT(*) AS total_orders,
       max(order_amt) AS max_amt, avg(order_amt) AS avg_amt, min(order_amt) AS min_amt
FROM aus_orders_silver;

-- 그룹 집계 (그룹 조건 필터는 HAVING)
SELECT product_name, ROUND(SUM(order_amt), 2) AS total_order_amount
FROM aus_orders_silver
GROUP BY product_name
HAVING SUM(order_amt) > 1000000
ORDER BY total_order_amount DESC;
```

| 절 | 필터 시점 |
|---|---|
| `WHERE` | 그룹화 **전**, 개별 행 |
| `HAVING` | 그룹화 **후**, 집계 결과 |
| `QUALIFY` | 윈도우 함수 계산 **후** |

### 17-1. 스칼라 서브쿼리

```sql
-- 영업 담당자별 매출과 전체 대비 비율
SELECT
  sales_rep,
  ROUND(SUM(order_amt), 2) AS sales_rep_total_sales,
  ROUND(sales_rep_total_sales / (SELECT SUM(order_amt) FROM aus_orders_silver), 2) AS percentage_of_sales
FROM aus_orders_silver
GROUP BY sales_rep
ORDER BY percentage_of_sales DESC;
```

### 17-2. CTE (공통 테이블 표현식)

`WITH`로 문장 안에서 여러 번 참조할 수 있는 임시 결과 집합을 정의한다. 쿼리를 모듈화해 가독성, 디버깅, 유지 보수가 쉬워진다.

```sql
WITH TotalSalesQry AS (
  SELECT ROUND(SUM(order_amt), 2) AS total
  FROM aus_orders_silver
)
SELECT
  sales_rep,
  ROUND(SUM(order_amt), 2) AS sales_rep_total_sales,
  ROUND(sales_rep_total_sales / (SELECT total FROM TotalSalesQry), 2) AS percentage_of_sales
FROM aus_orders_silver
GROUP BY sales_rep
ORDER BY percentage_of_sales DESC;
```

### 17-3. 시각화

쿼리 결과에서 바로 막대, 선 등 시각화를 만든다. 축 레이블을 깔끔하게 하려면 쿼리에서 백틱 별칭(`` AS `Total Amount Sold` ``)을 쓰거나 시각화 설정에서 이름을 바꾼다.

```sql
SELECT
  order_year_quarter AS `Year Quarter`,
  product_name       AS `Product Name`,
  ROUND(SUM(order_amt), 2) AS `Total Amount Sold`
FROM aus_orders_silver
GROUP BY order_year_quarter, product_name
ORDER BY order_year_quarter, product_name;
```

---

## 18. PIVOT과 윈도우 함수

### 18-1. PIVOT

행 값을 열로 바꿔 보고서 형태로 재구성한다.

```sql
WITH SourceTable AS (
  SELECT sales_rep, order_amt, order_year
  FROM aus_orders_silver
)
SELECT *
FROM SourceTable
PIVOT (
  ROUND(SUM(order_amt), 2)
  FOR order_year IN (2021, 2022, 2023, 2024)
);
```

| sales_rep | 2021 | 2022 | 2023 | 2024 |
|---|---|---|---|---|
| RAVI | 5,436,493.42 | 90,624,752.06 | ... | ... |

- 집계 함수와 `FOR <열> IN (값 목록)`을 지정한다. 목록에 있는 값만 열이 된다.
- 소스에 남은 나머지 컬럼(`sales_rep`)이 그룹 기준이 되므로, 피벗 전에 필요한 컬럼만 고른다. CTE 대신 서브쿼리로 써도 결과는 같다.

### 18-2. 윈도우 함수

결과 행을 줄이지 않고 현재 행과 관련된 행 집합에 대해 계산한다. 순위, 누적 합계, 이동 평균 등에 쓴다.

```sql
-- 영업 담당자별 주문 금액 순위
SELECT
  sales_rep,
  order_amt,
  RANK() OVER (PARTITION BY sales_rep ORDER BY order_amt DESC) AS sale_rank
FROM aus_orders_silver;

-- 담당자별 상위 3건만
SELECT
  sales_rep,
  order_amt,
  RANK() OVER (PARTITION BY sales_rep ORDER BY order_amt DESC) AS sale_rank
FROM aus_orders_silver
QUALIFY sale_rank <= 3;
```

| 구성 | 역할 |
|---|---|
| `OVER` | 함수가 동작할 윈도우 정의 |
| `PARTITION BY` | 그룹을 나눈다. 그룹이 바뀌면 순위가 다시 시작 |
| `ORDER BY` | 윈도우 안의 정렬 |
| `QUALIFY` | 윈도우 함수 결과로 필터. `WHERE`로는 윈도우 함수 결과를 필터할 수 없다 |

| 순위 함수 | 동점 처리 |
|---|---|
| `ROW_NUMBER()` | 동점이어도 1, 2, 3 고유 번호 |
| `RANK()` | 동점은 같은 순위, 다음 순위는 건너뜀 (1, 1, 3) |
| `DENSE_RANK()` | 동점은 같은 순위, 건너뛰지 않음 (1, 1, 2) |

```sql
-- 누적 합계 예
SELECT order_date, order_amt,
       SUM(order_amt) OVER (ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM aus_orders_silver;
```

---

## 19. 뷰의 종류

| 뷰 | 저장 위치 | 데이터 | 갱신 | 주 용도 |
|---|---|---|---|---|
| **표준 뷰** | 스키마 (영구) | 저장 안 함, 쿼리 텍스트만 | 조회할 때마다 실행 → 항상 최신 | 가벼운 쿼리의 재사용과 공유 |
| **임시 뷰** | 세션 (비영구) | 저장 안 함 | 조회할 때마다 실행 → 최신 | 세션 안의 탐색, 중간 결과 |
| **구체화된 뷰** | 스키마 (영구) | 결과를 **미리 계산해 저장** | 수동 `REFRESH` 또는 일정 → 마지막 갱신 시점 | 비싼 집계의 성능 최적화 |
| **동적 뷰** | 스키마 (영구) | 저장 안 함 | 조회 시 실행, 사용자에 따라 결과가 다름 | 열/행 수준 보안, 마스킹 |
| **메트릭 뷰** | 스키마 (영구) | 정의만 | 조회 시 계산 | 차원과 측정값을 한 번 정의하는 시맨틱 레이어 |

`SHOW TABLES`는 뷰도 함께 보여 주고, `SHOW VIEWS`는 뷰만 `namespace`, `viewName`, `isTemporary`, `isMaterialized`와 함께 보여 준다.

### 19-1. 표준 뷰

```sql
CREATE OR REPLACE VIEW employees_vw AS
SELECT *, 'Standard View' AS ViewType
FROM employees;

DESCRIBE TABLE EXTENDED employees_vw;   -- Type, View Text, View Query Output Columns
```

| 장점 | 고려 사항 |
|---|---|
| 조회할 때마다 최신 데이터 | 조회마다 쿼리가 실행되어, 비싼 쿼리를 여러 번 참조하면 느려진다 |
| 스키마에 저장되어 공유 가능 | 잠깐 필요한 것도 스키마 객체로 남는다 |
| 추가 저장 공간 불필요 | 결과를 미리 계산해 두지 않는다 |

### 19-2. 임시 뷰

```sql
CREATE OR REPLACE TEMPORARY VIEW employees_temp_vw AS
SELECT *, 'Temp View' AS ViewType
FROM employees;
```

| 유형 | 범위 |
|---|---|
| `TEMPORARY` | 만든 세션에서만 보이고 세션이 끝나면 삭제 |
| `GLOBAL TEMPORARY` | 시스템 스키마 `global_temp`에 연결. **SQL 웨어하우스에서는 지원되지 않는다** |

- `SHOW VIEWS`에서 `namespace`가 비어 있고 `isTemporary = true`.
- 웨어하우스가 종료되거나 클러스터에서 분리 후 재연결하면, 또는 다른 노트북에서는 사용할 수 없다.
- 임시 뷰도 데이터를 저장하지 않으므로 조회할 때마다 최신 데이터를 가져온다. 다른 사용자나 세션과는 공유되지 않는다.

### 19-3. 구체화된 뷰 (Materialized View)

Unity Catalog가 관리하는 객체로, 소스의 결과를 **미리 계산해 저장**한다. 조회 시 매번 다시 계산하지 않고 **마지막 갱신 시점의 데이터**를 보여 준다. Databricks SQL의 구체화된 뷰는 서버리스 Spark Declarative Pipeline 위에서 동작하므로 워크스페이스가 서버리스 파이프라인을 지원해야 한다.

```sql
CREATE OR REPLACE MATERIALIZED VIEW employees_mv AS
SELECT *, 'Materialized View' AS ViewType
FROM employees;

-- 소스에 행이 추가되어도 조회 결과는 그대로 → 갱신 필요
REFRESH MATERIALIZED VIEW employees_mv;

-- 생성 시 일정 지정
CREATE OR REPLACE MATERIALIZED VIEW employees_mv_scheduled
  SCHEDULE EVERY 1 HOUR
AS
SELECT *, 'Materialized View' AS ViewType
FROM employees;

DESCRIBE EXTENDED employees_mv_scheduled;   -- 갱신 정보: 마지막 갱신, 갱신 유형, 상태, 일정
```

| 장점 | 고려 사항 |
|---|---|
| 비싼 쿼리, 집계를 미리 계산해 빠르게 조회 | 결과를 저장하므로 저장 공간 사용 |
| 실시간까지는 아니어도 주기적(매일, 매시간) 갱신이면 되는 경우에 적합 | 갱신 주기만큼 데이터가 오래될 수 있다 |
| | 갱신 전략(전체 재계산 또는 증분) 관리 필요 |

갱신 정보의 갱신 유형은 전체를 다시 계산했는지(`RECOMPUTED` 등), 증분 갱신했는지를 보여 준다.

### 19-4. 동적 뷰

`is_account_group_member()`로 조회하는 사람의 그룹을 확인해 같은 뷰라도 사람마다 다른 결과를 보여 준다.

```sql
SELECT is_account_group_member('admins');   -- 현재 사용자가 admins 그룹인지

-- 열 수준: admins가 아니면 이름을 가림
CREATE OR REPLACE VIEW employees_redact_name_vw AS
SELECT
  EmployeeID,
  CASE WHEN is_account_group_member('admins') THEN FirstName ELSE 'Redacted' END AS FirstName,
  Department
FROM employees;

-- 행 수준: admins가 아니면 IT 부서 행만
CREATE OR REPLACE VIEW employees_filter_vw AS
SELECT *
FROM employees
WHERE CASE
        WHEN is_account_group_member('admins') THEN TRUE
        ELSE Department = 'IT'
      END;
```

- 동적 뷰는 **뷰를 통해 읽는 사용자만** 보호한다. 기반 테이블 자체의 직접 접근은 막지 않으므로, 사용자에게는 뷰 권한만 주고 테이블 권한은 주지 않는다.
- 동적 뷰는 모든 환경과 compute 구성에서 지원되지는 않으므로 요구 사항을 확인한다.
- 테이블 자체에 거는 행 필터와 열 마스크, 태그 기반 정책(ABAC)도 같은 목적의 대안이다.

### 19-5. 메트릭 뷰

테이블 위에 **시맨틱 레이어**를 정의하는 Unity Catalog 객체다.

- "총 매출", "활성 고객 수" 같은 비즈니스 측정값(measure)과 분석 차원(dimension)을 한 번 정의한다.
- 쿼리, 대시보드, AI/BI Genie 전반에서 같은 정의를 써서 수치가 일치하는 **단일 진실 공급원**이 된다.
- 소스, 차원, 측정값을 나열하는 YAML 사양으로 정의하며, 조회할 때는 측정값을 `MEASURE()`로 집계한다.
- 같은 측정값을 여러 보고서에서 재사용하고 SQL 복사·붙여넣기 대신 인증된 정의가 필요할 때 쓴다.

---

## 20. information_schema로 메타데이터 조회

`information_schema`는 각 카탈로그에 있는 SQL 표준 스키마로, 사용자가 볼 권한이 있는 객체를 설명하는 뷰를 제공한다.

```sql
-- 카탈로그의 모든 뷰
SELECT * FROM information_schema.views;

-- 특정 스키마의 뷰와 정의
SELECT table_name, view_definition, is_materialized
FROM information_schema.views
WHERE table_schema = 'retail';

-- 그 밖에 자주 쓰는 뷰
SELECT * FROM information_schema.tables  WHERE table_schema = 'retail';
SELECT * FROM information_schema.columns WHERE table_name = 'aus_orders';
```

| 컬럼 (views) | 의미 |
|---|---|
| `table_schema` | 뷰가 속한 스키마 |
| `view_definition` | 뷰의 쿼리 텍스트 |
| `is_materialized` | 구체화된 뷰 여부 |

세션에만 있는 임시 뷰는 `information_schema`에 나타나지 않는다.

---

## 21. 종합 예: 브론즈에서 골드까지

시나리오: 볼륨에 쌓인 고객 JSON 파일을 실버 테이블로 정제하고, 주별 고객 수를 매일 갱신되는 골드 구체화된 뷰로 만든다.

```sql
-- 1) 브론즈: JSON 파일 적재 (스키마 병합)
DROP TABLE IF EXISTS customers_lab_bronze;
CREATE TABLE customers_lab_bronze;
COPY INTO customers_lab_bronze
FROM '/Volumes/dbacademy_retail/v01/retail-pipeline/customers/stream_json/'
FILEFORMAT = JSON
COPY_OPTIONS ('mergeSchema' = 'true');

-- 2) 탐색: 주 약어의 고유값 (NULL 포함 여부 확인)
SELECT DISTINCT state AS distinct_state
FROM customers_lab_bronze
ORDER BY distinct_state;

-- 3) 실버: 타임스탬프 변환, 도메인 추출, 주 이름 보강 (고객 행은 모두 유지)
CREATE OR REPLACE TABLE customer_lab_silver AS
SELECT
  cust.customer_id,
  cust.name,
  cust.address,
  cust.city,
  cust.email,
  cust.operation,
  cust.zip_code,
  cust.state,
  date_format(from_unixtime(cust.timestamp), 'yyyy-MM-dd HH:mm:ss') AS sign_up_date,
  substring_index(cust.email, '@', -1) AS email_domain,
  st.state_name
FROM customers_lab_bronze cust
  LEFT JOIN state_lookup st ON cust.state = st.state_abbreviation;

-- 4) 검증: 기대 결과 테이블과 비교해 차이가 0행인지
SELECT * FROM customer_lab_silver_expected
EXCEPT
SELECT * FROM customer_lab_silver;

-- 5) 골드: 매일 갱신되는 구체화된 뷰
CREATE OR REPLACE MATERIALIZED VIEW customers_per_state_gold
SCHEDULE EVERY 1 DAY
AS
SELECT state_name, count(*) AS total_customers
FROM customer_lab_silver
GROUP BY state_name
ORDER BY total_customers DESC;

-- 6) 갱신 정보 확인
DESCRIBE EXTENDED customers_per_state_gold;
```

- `EXCEPT`는 첫 쿼리에만 있는 행을 돌려준다. 두 테이블이 같다면 0행이다(완전한 비교는 양방향으로 확인한다).
- 주 약어가 NULL인 고객은 LEFT JOIN 덕분에 실버에 남고, 골드에서 `state_name`이 NULL인 그룹으로 집계된다.

---

## 22. 핵심 정리 치트시트

### 탐색과 거버넌스

| 키워드 | 핵심 |
|---|---|
| 플랫폼 계층 | 워크스페이스(작업) → Unity Catalog(거버넌스) → 레이크하우스(저장) |
| 네임스페이스 | 메타스토어 → `catalog.schema.object` |
| 객체 | 테이블(관리형/외부), 뷰, 볼륨(파일), 함수 |
| 관리형 vs 외부 | DROP 시 데이터 삭제 / 파일 유지. 관리형이 기본 권장 |
| 찾는 방법 | 카탈로그 탐색기, 지능형 검색(설명 품질에 의존), SQL |
| 카탈로그 탐색기 | 개요, 샘플, 세부 정보, 권한, 이력, 리니지, 인사이트(30일), 품질 |
| 주체 | 사용자, 서비스 주체, 어카운트 그룹. 그룹에 권한을 주는 것이 모범 사례 |
| 소유권 | 생성자가 소유. 계층을 따라 내려감 |
| 워크스페이스 객체 권한 | CAN VIEW < CAN RUN < CAN EDIT < CAN MANAGE |
| 데이터 객체 권한 | `GRANT SELECT/MODIFY`, 상위의 `USE CATALOG`, `USE SCHEMA` 필요 |
| 보이지 않음 | 권한 없는 객체는 아예 안 보인다 |
| 공유 카탈로그 | Marketplace, 유형 OpenSharing, 읽기 전용 |

### SQL과 수집

| 키워드 | 핵심 |
|---|---|
| 탐색 SQL | `SHOW CATALOGS/SCHEMAS/TABLES/VIEWS/VOLUMES`, `LIST`, `current_catalog()` |
| 기본 위치 | `USE CATALOG`, `USE SCHEMA`, `IDENTIFIER()` |
| Delta Lake | Parquet + 트랜잭션 로그. ACID, DML, 시간 여행, 스키마 강제/진화 |
| 메달리온 | 브론즈(원시) → 실버(정제) → 골드(비즈니스용, 분석가가 쿼리) |
| 경로 조회 | `` SELECT * FROM text.`경로` ``, `json.`, `csv.` |
| read_files | TVF, `format`, `header`, `schema`, `_rescued_data`, 폴더 경로 가능 |
| COPY INTO | 멱등, 이미 적재한 파일 건너뜀, `mergeSchema`. 레거시 |
| 스트리밍 테이블 | `CREATE OR REFRESH STREAMING TABLE ... FROM STREAM`, `REFRESH STREAMING TABLE` |

### Databricks SQL

| 키워드 | 핵심 |
|---|---|
| 웨어하우스 유형 | 서버리스(Photon + Predictive I/O + IWM, 권장), Pro(IWM 없음), Classic(Photon만) |
| 서버리스 이점 | 즉시·탄력, 최소 관리, 낮은 총비용 |
| SQL 편집기 | 선택 실행, 다중 결과, `:파라미터`, 자연어 필터, 일정, 공유, 성능 확인 |
| 측면 컬럼 별칭 | 같은 SELECT에서 앞 별칭 재사용 |
| 필터 시점 | WHERE(행) → HAVING(그룹) → QUALIFY(윈도우) |
| CTE | `WITH 이름 AS (...)`, 가독성과 재사용 |
| PIVOT | `PIVOT (집계 FOR 열 IN (값...))` |
| 윈도우 | `RANK() OVER (PARTITION BY ... ORDER BY ...)` + `QUALIFY` |

### 뷰

| 키워드 | 핵심 |
|---|---|
| 표준 뷰 | 쿼리만 저장, 항상 최신, 매번 실행 |
| 임시 뷰 | 세션 한정, 항상 최신. GLOBAL TEMPORARY는 SQL 웨어하우스 미지원 |
| 구체화된 뷰 | 결과 저장, 마지막 갱신 시점, `REFRESH`, `SCHEDULE EVERY`, 서버리스 파이프라인 기반 |
| 동적 뷰 | `is_account_group_member()`로 열 가림, 행 필터 |
| 메트릭 뷰 | 차원과 측정값의 시맨틱 레이어, YAML 정의 |
| information_schema | `views`, `tables`, `columns`. 임시 뷰는 안 나옴 |

---

## 23. 자가 점검 문제

> 답을 가리고 먼저 생각해본 뒤 펼쳐보자.

**Q1.** Databricks 플랫폼의 세 계층(작업 공간, 거버넌스, 기반)은 각각 어떤 역할을 하는가?
<details><summary>정답</summary>

워크스페이스는 카탈로그 탐색기, SQL 편집기, 노트북으로 데이터를 찾고 쿼리하는 곳이고, Unity Catalog는 권한, 리니지, 검색을 담당하는 거버넌스 계층이며, 클라우드 위의 레이크하우스는 오픈 포맷으로 데이터를 저장하는 기반이다.
</details>

**Q2.** Unity Catalog의 3단계 네임스페이스는 무엇이며, 전체 이름을 쓰는 이유는?
<details><summary>정답</summary>

catalog.schema.object 형태다. 메타스토어 아래 카탈로그, 스키마, 데이터 객체 순으로 중첩된다. 여러 스키마에 같은 이름의 테이블이 있어도 전체 주소로 모호함 없이 지정할 수 있다.
</details>

**Q3.** 스키마 안의 네 가지 데이터 객체와 각각의 용도는?
<details><summary>정답</summary>

행과 열로 된 데이터인 테이블, 저장된 쿼리인 뷰, CSV나 JSON 같은 파일을 관리하는 볼륨, 값이나 행 집합을 반환하는 재사용 로직인 함수다.
</details>

**Q4.** 관리형 테이블과 외부 테이블을 DROP하면 각각 어떻게 되는가?
<details><summary>정답</summary>

관리형 테이블은 Databricks가 데이터까지 관리하므로 데이터 파일도 함께 삭제된다. 외부 테이블은 사용자가 관리하는 스토리지를 가리키기만 하므로 메타데이터만 지워지고 원본 파일은 남는다. 관리형이 기본 권장이다.
</details>

**Q5.** 데이터를 찾는 세 가지 방법과 각각 적합한 상황은?
<details><summary>정답</summary>

전체 구조를 둘러볼 때는 카탈로그 탐색기, 정확한 이름 없이 비즈니스 용어로 찾을 때는 지능형 검색, 정확하고 반복 가능한 목록이 필요할 때는 SHOW 같은 SQL을 쓴다.
</details>

**Q6.** 지능형 검색이 잘 동작하려면 무엇이 필요한가?
<details><summary>정답</summary>

검색이 객체의 설명, 주석, 태그를 활용하므로 객체에 명확한 설명과 주석이 붙어 있어야 한다. 소유한 객체를 잘 문서화하는 것이 검색 품질을 좌우한다.
</details>

**Q7.** 테이블에 AI 제안 설명을 수락하면 테이블 이력에 어떤 변화가 생기는가?
<details><summary>정답</summary>

새 버전이 생기며 작업은 SET TBLPROPERTIES로 기록된다. 메타데이터 변경도 Delta 테이블의 버전으로 남는다.
</details>

**Q8.** 카탈로그 탐색기의 리니지와 인사이트는 각각 무엇을 보여 주는가?
<details><summary>정답</summary>

리니지는 테이블이 어떤 업스트림 테이블이나 파일로부터 만들어졌고 어디로 흘러가는지 보여 주고, 인사이트는 최근 30일간 그 테이블에 자주 실행된 쿼리와 사용자를 보여 준다.
</details>

**Q9.** 최소 권한 원칙과 이를 실천하는 세 가지 습관은?
<details><summary>정답</summary>

필요한 최소 권한만 필요한 기간에만 주는 원칙이다. 역할 기반으로 권한을 정하고, 정기적으로 모니터링하고 감사해 불필요한 권한을 회수하며, 알 필요가 있는 사람에게만 필요한 기간 동안 공유한다.
</details>

**Q10.** 세 가지 주체 유형은? 권한을 개인 대신 그룹에 주는 이유는?
<details><summary>정답</summary>

사람인 사용자, 자동화와 서드파티 연결 같은 사람이 아닌 행위자인 서비스 주체, 이들의 묶음인 어카운트 그룹이다. 그룹에 주면 한 번에 관리할 수 있고 구성원이 역할을 바꿔도 접근이 끊기지 않는다.
</details>

**Q11.** 워크스페이스 객체와 Unity Catalog 데이터 객체는 공유 방식이 어떻게 다른가?
<details><summary>정답</summary>

노트북, 쿼리, 대시보드 같은 워크스페이스 객체는 CAN VIEW, CAN RUN, CAN EDIT, CAN MANAGE 같은 권한 수준으로 공유하고, 테이블, 뷰, 볼륨 같은 데이터 객체는 소유자가 GRANT로 SELECT, MODIFY 같은 세분화된 권한을 준다.
</details>

**Q12.** CAN RUN과 CAN MANAGE의 차이는?
<details><summary>정답</summary>

권한은 누적된다. CAN RUN은 보기와 복사에 더해 실행해서 결과를 갱신할 수 있고, CAN MANAGE는 편집까지 포함해 권한 변경과 삭제까지 할 수 있다.
</details>

**Q13.** 동료가 만든 테이블이 카탈로그 탐색기에 보이지 않는 가장 흔한 이유와 대처는?
<details><summary>정답</summary>

권한이 없는 객체는 아예 표시되지 않기 때문이다. 필요해지기 전에 소유자에게 공유(GRANT)를 요청한다. 테이블 SELECT 외에 상위 카탈로그와 스키마의 USE 권한도 필요하다.
</details>

**Q14.** Marketplace로 받은 공유 카탈로그의 특징은?
<details><summary>정답</summary>

받은 공유 아래에 OpenSharing 유형의 카탈로그로 나타나며 읽기 전용이다. SELECT는 할 수 있지만 쓰기는 할 수 없다.
</details>

**Q15.** 기본 카탈로그와 스키마를 확인하고 바꾸는 SQL은? 기본 위치 밖의 테이블을 이름만으로 조회하면 어떻게 되는가?
<details><summary>정답</summary>

SELECT current_catalog(), current_schema()로 확인하고 USE CATALOG, USE SCHEMA로 바꾼다. 기본 위치에 없는 테이블을 이름만으로 조회하면 찾지 못해 오류가 나므로 전체 3단계 이름을 쓴다.
</details>

**Q16.** `IDENTIFIER()`는 언제 쓰는가?
<details><summary>정답</summary>

문자열이나 변수에 담긴 값을 객체 이름으로 해석해야 할 때 쓴다. 예를 들어 사용자마다 다른 카탈로그 이름을 변수로 받아 USE CATALOG IDENTIFIER(my_catalog)처럼 쓴다.
</details>

**Q17.** `LIST` 문은 무엇을 나열하며, 무엇은 나열하지 않는가?
<details><summary>정답</summary>

볼륨이나 외부 위치 경로의 파일을 나열한다. Unity Catalog 관리형 테이블의 내부 데이터 파일은 나열하지 않는다.
</details>

**Q18.** Delta Lake 테이블을 이루는 두 가지와, 트랜잭션 로그가 제공하는 기능은?
<details><summary>정답</summary>

Parquet 데이터 파일과 JSON 트랜잭션 로그다. 로그 덕분에 ACID 트랜잭션, INSERT/UPDATE/DELETE/MERGE, 이전 버전 조회와 복원(시간 여행), 스키마 강제와 진화가 가능하다.
</details>

**Q19.** 메달리온 아키텍처의 세 레이어와 분석가가 주로 쓰는 레이어는?
<details><summary>정답</summary>

원시 데이터를 그대로 보관하는 브론즈, 정제와 조인과 보강을 거친 단일 진실 공급원인 실버, 집계된 비즈니스용 테이블인 골드다. 분석가는 주로 골드를 쿼리한다.
</details>

**Q20.** 분석가가 파일을 직접 가져오는 세 가지 방법과 각각 적합한 경우는?
<details><summary>정답</summary>

일회성 작은 파일은 파일 업로드 UI, 저장 전에 내용을 확인하거나 SQL로 유연하게 읽을 때는 read_files, 같은 파일이 여러 번 처리될 수 있는 반복 적재에는 멱등인 COPY INTO를 쓴다.
</details>

**Q21.** 테이블을 만들기 전에 `` SELECT * FROM text.`경로` ``로 파일을 먼저 보는 이유는?
<details><summary>정답</summary>

원본 파일의 구분자, 첫 행이 헤더인지, 전체 구조를 확인해 read_files나 COPY INTO의 옵션을 올바르게 정하기 위해서다. json., csv. 같은 접두사로 형식에 맞게 읽어 볼 수도 있다.
</details>

**Q22.** `read_files`에서 스키마를 지정하지 않으면 어떻게 동작하며, 대용량에서 문제는?
<details><summary>정답</summary>

대상 파일 전체를 읽어 통합 스키마를 추론한다. 모든 파일을 읽어야 하므로 대용량에서는 비효율적이라 schema 옵션으로 직접 지정하는 것이 낫다.
</details>

**Q23.** `read_files` 결과에 생기는 `_rescued_data` 컬럼의 역할은?
<details><summary>정답</summary>

스키마와 맞지 않는 데이터를 버리지 않고 담아 두는 컬럼이다. 데이터 손실 없이 나중에 문제를 확인할 수 있다.
</details>

**Q24.** 같은 `COPY INTO`를 두 번 실행하면 어떻게 되는가? 스키마 없이 만든 테이블에 적재하려면?
<details><summary>정답</summary>

이미 적재한 파일을 건너뛰므로 두 번째 실행은 num_inserted_rows가 0이다(멱등). 스키마 없이 CREATE TABLE로 빈 테이블을 만든 경우 COPY_OPTIONS ('mergeSchema' = 'true')로 적재하면서 스키마를 병합한다.
</details>

**Q25.** COPY INTO보다 권장되는 증분 수집 방법은?
<details><summary>정답</summary>

스트리밍 테이블(CREATE OR REFRESH STREAMING TABLE)이다. COPY INTO는 레거시로 분류되며, 스트리밍 테이블이 더 확장성 있고 안정적인 증분 수집을 제공한다.
</details>

**Q26.** 스트리밍 테이블은 어떻게 증분 처리하며, 일정 전에 즉시 반영하려면?
<details><summary>정답</summary>

FROM STREAM 소스로 정의하면 갱신할 때 소스에 새로 들어온 행만 처리해 추가한다(내부적으로 Spark Declarative Pipeline). SCHEDULE EVERY로 자동 갱신을 걸 수 있고, 즉시 반영하려면 REFRESH STREAMING TABLE을 실행한다.
</details>

**Q27.** 세 가지 SQL 웨어하우스 유형의 기능 차이는?
<details><summary>정답</summary>

서버리스는 Photon, Predictive I/O, Intelligent Workload Management를 모두 갖고, Pro는 Photon과 Predictive I/O만, Classic은 Photon만 갖는다. 서버리스가 권장이고, Pro는 리전이나 네트워크 제약 시, Classic은 최후의 선택이다.
</details>

**Q28.** Photon, Predictive I/O, Intelligent Workload Management는 각각 무엇을 하는가?
<details><summary>정답</summary>

Photon은 SQL 실행을 빠르게 하는 벡터화 쿼리 엔진, Predictive I/O는 조회와 필터 같은 선택적 스캔을 빠르게 하는 기능, Intelligent Workload Management는 AI로 쿼리를 라우팅하고 수요에 따라 웨어하우스를 확장, 축소하는 기능이다.
</details>

**Q29.** 서버리스 SQL 웨어하우스를 권장하는 세 가지 이유는?
<details><summary>정답</summary>

몇 초 만에 준비되고 수요에 따라 자동 확장되는 탄력성, 패치와 업그레이드와 튜닝을 Databricks가 맡는 최소한의 관리, 유휴와 과잉 프로비저닝 비용이 없는 낮은 총비용이다.
</details>

**Q30.** SQL 편집기에서 쿼리 파라미터는 어떻게 쓰는가?
<details><summary>정답</summary>

쿼리에 `:my_state_value`처럼 콜론 접두사 파라미터를 넣고, 파라미터 이름, 위젯 레이블, 유형, 기본값을 설정해 위젯에서 값을 입력해 실행한다.
</details>

**Q31.** 같은 SELECT 안에서 앞에서 만든 별칭을 바로 참조하는 기능은 무엇이라 하는가?
<details><summary>정답</summary>

측면 컬럼 별칭(lateral column alias)이다. 예를 들어 orderamt AS order_amt를 정의한 뒤 같은 SELECT에서 ROUND(order_amt/quantity, 2)처럼 쓸 수 있다. 다른 SQL 엔진에서는 지원되지 않을 수 있다.
</details>

**Q32.** Unix 타임스탬프를 읽을 수 있는 날짜로 바꾸고, 이메일에서 도메인을 뽑는 함수는?
<details><summary>정답</summary>

date_format(from_unixtime(timestamp), 'yyyy-MM-dd HH:mm:ss')로 날짜 문자열을 만들고, substring_index(email, '@', -1)로 마지막 @ 뒤의 도메인을 얻는다.
</details>

**Q33.** 주 약어를 전체 이름으로 보강할 때 INNER JOIN 대신 LEFT JOIN을 쓰는 이유는?
<details><summary>정답</summary>

조회 테이블에 없는 약어나 NULL 약어를 가진 고객 행도 버리지 않고 모두 유지하기 위해서다. INNER JOIN은 일치하지 않는 행을 제거한다.
</details>

**Q34.** WHERE, HAVING, QUALIFY는 각각 언제 필터하는가?
<details><summary>정답</summary>

WHERE는 그룹화 전 개별 행을, HAVING은 GROUP BY 후 집계 결과를, QUALIFY는 윈도우 함수 계산 후 그 결과를 필터한다. 윈도우 함수 결과는 WHERE로 필터할 수 없다.
</details>

**Q35.** CTE를 쓰는 이점은?
<details><summary>정답</summary>

WITH로 이름 붙인 임시 결과 집합을 문장 안에서 여러 번 참조할 수 있어, 서브쿼리를 분리해 쿼리를 모듈화하고 가독성, 디버깅, 유지 보수를 쉽게 한다.
</details>

**Q36.** 영업 담당자별로 연도를 열로 펼친 매출표를 만드는 SQL 연산자와 구문은?
<details><summary>정답</summary>

PIVOT이다. 필요한 컬럼만 고른 소스에 대해 PIVOT (ROUND(SUM(order_amt), 2) FOR order_year IN (2021, 2022, 2023, 2024))을 쓰면 남은 컬럼(sales_rep)별로 연도가 열이 된다.
</details>

**Q37.** 영업 담당자별 상위 3건 주문을 구하는 쿼리 구조는? RANK와 DENSE_RANK의 차이는?
<details><summary>정답</summary>

RANK() OVER (PARTITION BY sales_rep ORDER BY order_amt DESC)로 순위를 매기고 QUALIFY sale_rank <= 3으로 거른다. RANK는 동점 뒤 순위를 건너뛰고(1, 1, 3), DENSE_RANK는 건너뛰지 않는다(1, 1, 2).
</details>

**Q38.** 표준 뷰, 임시 뷰, 구체화된 뷰는 소스 테이블에 행이 추가된 뒤 조회하면 각각 무엇을 보여 주는가?
<details><summary>정답</summary>

표준 뷰와 임시 뷰는 조회할 때마다 저장된 쿼리를 실행하므로 최신 행을 보여 준다. 구체화된 뷰는 미리 계산해 저장한 결과를 보여 주므로 REFRESH 전까지는 마지막 갱신 시점의 행만 보여 준다.
</details>

**Q39.** 임시 뷰의 범위와 GLOBAL TEMPORARY 뷰의 제약은?
<details><summary>정답</summary>

TEMPORARY 뷰는 만든 세션에서만 보이고 세션이 끝나거나 웨어하우스가 종료되면 사라지며 다른 노트북이나 사용자와 공유되지 않는다. GLOBAL TEMPORARY 뷰는 global_temp 스키마에 연결되지만 SQL 웨어하우스에서는 지원되지 않는다.
</details>

**Q40.** 구체화된 뷰가 적합한 경우와 단점은? 일정을 거는 방법은?
<details><summary>정답</summary>

비싼 집계를 미리 계산해 빠르게 조회하고 싶고 실시간이 아닌 주기적 갱신으로 충분할 때 적합하다. 결과를 저장해 공간을 쓰고 갱신 주기만큼 데이터가 오래될 수 있으며 갱신 전략을 관리해야 한다. 생성 시 SCHEDULE EVERY 1 DAY처럼 일정을 걸고, DESCRIBE EXTENDED의 갱신 정보로 확인한다.
</details>

**Q41.** 동적 뷰로 열 수준과 행 수준 보안을 구현하는 방법은? 주의할 점은?
<details><summary>정답</summary>

is_account_group_member('admins')를 CASE에 넣어, 열 수준은 그룹이 아니면 값을 'Redacted'로 바꾸고, 행 수준은 WHERE에서 그룹이 아니면 Department = 'IT' 같은 조건만 통과시킨다. 뷰를 통해 읽는 사람만 보호하므로 기반 테이블 권한은 주지 않아야 한다.
</details>

**Q42.** 메트릭 뷰는 무엇을 해결하는가?
<details><summary>정답</summary>

총 매출 같은 측정값과 분석 차원을 YAML로 한 번 정의하는 시맨틱 레이어로, 쿼리, 대시보드, Genie 전반에서 같은 정의를 써 수치가 일치하는 단일 진실 공급원을 만든다. SQL 복사·붙여넣기로 정의가 갈라지는 문제를 막는다.
</details>

**Q43.** `information_schema.views`로 알 수 있는 것은? 임시 뷰도 나오는가?
<details><summary>정답</summary>

볼 권한이 있는 뷰의 스키마(table_schema), 쿼리 정의(view_definition), 구체화 여부(is_materialized)를 알 수 있다. 세션에만 존재하는 임시 뷰는 나오지 않는다.
</details>

**Q44.** 두 테이블이 같은지 SQL로 검증하는 방법은?
<details><summary>정답</summary>

SELECT ... FROM A EXCEPT SELECT ... FROM B로 A에만 있는 행을 구한다. 0행이면 A의 모든 행이 B에 있다. 완전히 같은지 보려면 반대 방향도 확인한다.
</details>

---

## 24. 참고 링크

- Unity Catalog: https://docs.databricks.com/aws/en/data-governance/unity-catalog/
- 카탈로그 탐색기: https://docs.databricks.com/aws/en/catalog-explorer/
- Unity Catalog 권한: https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/
- information_schema: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-information-schema
- information_schema VIEWS: https://docs.databricks.com/aws/en/sql/language-manual/information-schema/views
- Databricks Marketplace: https://docs.databricks.com/aws/en/marketplace/
- Delta Lake: https://docs.databricks.com/aws/en/delta/
- 데이터 수집 개요: https://docs.databricks.com/aws/en/ingestion/
- 경로로 데이터 조회: https://docs.databricks.com/aws/en/query#query-data-by-path
- read_files: https://docs.databricks.com/aws/en/sql/language-manual/functions/read_files
- COPY INTO: https://docs.databricks.com/aws/en/sql/language-manual/delta-copy-into
- LIST: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-aux-list
- CREATE STREAMING TABLE: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-streaming-table
- REFRESH (구체화된 뷰, 스트리밍 테이블): https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-refresh-full
- Databricks SQL: https://docs.databricks.com/aws/en/sql/
- SQL 웨어하우스: https://docs.databricks.com/aws/en/compute/sql-warehouse/
- SQL 편집기: https://docs.databricks.com/aws/en/sql/user/sql-editor/
- 쿼리 파라미터: https://docs.databricks.com/aws/en/sql/user/queries/query-parameters
- 내장 함수: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-functions-builtin
- JOIN: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-qry-select-join
- PIVOT: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-qry-select-pivot
- 윈도우 함수: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-window-functions
- QUALIFY: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-qry-select-qualify
- 뷰 개요: https://docs.databricks.com/aws/en/views/
- 구체화된 뷰: https://docs.databricks.com/aws/en/views/materialized
- CREATE MATERIALIZED VIEW: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-materialized-view
- 동적 뷰: https://docs.databricks.com/aws/en/views/dynamic
- 메트릭 뷰: https://docs.databricks.com/aws/en/metric-views/
- 사용자 정의 함수: https://docs.databricks.com/aws/en/udf/
- 쿼리 프로필: https://docs.databricks.com/aws/en/sql/user/queries/query-profile
