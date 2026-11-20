# Unity Catalog로 구현하는 데이터 거버넌스 — 핵심 개념 정리

---

## 목차

1. [데이터 거버넌스와 Unity Catalog](#1-데이터-거버넌스와-unity-catalog)
2. [Unity Catalog 아키텍처](#2-unity-catalog-아키텍처)
3. [보안 객체(Securable) 계층 구조](#3-보안-객체securable-계층-구조)
4. [관리형 vs 외부: 테이블과 Volume](#4-관리형-vs-외부-테이블과-volume)
5. [외부 시스템 연결을 위한 기본 객체](#5-외부-시스템-연결을-위한-기본-객체)
6. [Volumes: 파일 거버넌스](#6-volumes-파일-거버넌스)
7. [Functions, Models, Feature Tables](#7-functions-models-feature-tables)
8. [ID(Identity)와 관리 역할](#8-ididentity와-관리-역할)
9. [권한 모델: 소유권, ACL, GRANT/REVOKE](#9-권한-모델-소유권-acl-grantrevoke)
10. [권한 종류와 상속](#10-권한-종류와-상속)
11. [자주 쓰는 권한 시나리오](#11-자주-쓰는-권한-시나리오)
12. [권한 확인과 문제 해결](#12-권한-확인과-문제-해결)
13. [세분화된 접근 제어 개요](#13-세분화된-접근-제어-개요)
14. [Dynamic View](#14-dynamic-view)
15. [네이티브 Row Filter와 Column Mask](#15-네이티브-row-filter와-column-mask)
16. [태그: 일반 태그와 Governed Tag](#16-태그-일반-태그와-governed-tag)
17. [ABAC: 태그 기반 정책](#17-abac-태그-기반-정책)
18. [Data Classification](#18-data-classification)
19. [자산 검색 (Discovery)](#19-자산-검색-discovery)
20. [데이터 리니지](#20-데이터-리니지)
21. [OpenSharing (Delta Sharing)과 Marketplace](#21-opensharing-delta-sharing과-marketplace)
22. [종합 예제: 급여 데이터 보호](#22-종합-예제-급여-데이터-보호)
23. [핵심 정리 치트시트](#23-핵심-정리-치트시트)
24. [자가 점검 문제](#24-자가-점검-문제)
25. [참고 링크](#25-참고-링크)

---

## 1. 데이터 거버넌스와 Unity Catalog

### 1-1. 데이터 거버넌스란

데이터 거버넌스는 "누가, 어떤 데이터에, 어떤 방식으로 접근할 수 있고, 그 데이터가 어디서 와서 어디로 가는지"를 관리하는 일이다. 크게 네 영역으로 나눌 수 있다.

| 영역 | 내용 |
|---|---|
| 보안 | 액세스 제어, 감사(audit) |
| 협업 | 검색(discovery), 안전한 데이터 공유 |
| 품질 | 리니지, 품질 모니터링 |
| 관리 | 비용 제어, 비즈니스 시맨틱(메트릭 정의 등) |

### 1-2. Unity Catalog(UC)

Unity Catalog는 Databricks의 **모든 데이터 및 AI 자산을 위한 통합 거버넌스 레이어**이다.

- **통합(Unified)**: 위 네 영역을 하나의 레이어에서 처리한다. 대상도 테이블에 그치지 않고 파일, ML 모델, 함수, 노트북, 대시보드까지 포함한다.
- **개방(Open)**: Delta Lake, Apache Iceberg, Parquet, CSV, JSON 등 개방형 형식을 지원한다. Unity REST API, Iceberg REST Catalog API, OpenSharing을 통해 외부 엔진과 도구도 UC가 관리하는 데이터에 접근할 수 있다. 오픈 소스(UC OSS)로도 공개되어 있다.
- **중앙화(Centralized)**: 보안 제어의 기준을 개별 클러스터에서 **데이터 객체 자체**로 옮긴다. 어떤 compute로 접근하든 같은 권한 검사를 거친다.

기존 카탈로그(예: Hive Metastore)는 테이블 형식 데이터의 메타데이터만 관리했고, 권한도 워크스페이스나 클러스터 단위로 흩어져 있었다. UC는 이 한계를 해결한다.

### 1-3. 개발자 리소스

- UC 오픈 소스는 OpenAPI 사양, OSS 서버, REST API, SDK(Java, Python, Go)를 제공한다.
- 운영 환경에서는 Terraform, Databricks CLI, REST API 같은 코드형 인프라(IaC)로 UC 객체와 권한을 관리하는 것이 권장된다. 감사 가능하고 반복 가능한 배포가 되기 때문이다.
- **자격 증명 벤딩(credential vending)**: 외부 엔진이 UC 관리 데이터를 읽을 때, UC가 클라우드 스토리지용 **단기(short-lived) 토큰**을 발급한다. 장기 자격 증명을 외부에 넘기지 않아도 된다.

---

## 2. Unity Catalog 아키텍처

### 2-1. UC 도입 전 vs 후

| 구분 | UC 도입 전 | UC 도입 후 |
|---|---|---|
| 메타스토어 | 워크스페이스마다 별도(Hive Metastore) | 여러 워크스페이스가 하나의 UC 메타스토어 공유 |
| 사용자/그룹 관리 | 워크스페이스마다 별도 | 어카운트 수준에서 한 번 관리 |
| 액세스 제어 | 워크스페이스, 클러스터별로 분산 | 메타스토어에서 중앙 관리 |
| 외부 데이터베이스 | 개별 연결 | Connection + Foreign Catalog로 통합 |
| 결과 | 사일로, 관리 부담, 보안 위험 | 단일 거버넌스 레이어 |

### 2-2. 메타스토어(Metastore)

- UC 객체 계층의 **최상위 컨테이너**이자, 액세스 제어의 중앙 제어 지점이다.
- **클라우드 리전당 하나**를 만드는 것이 원칙이다. 같은 리전의 여러 워크스페이스를 하나의 메타스토어에 연결(attach)한다. 하나의 워크스페이스는 하나의 메타스토어에만 연결된다.
- 보통 **어카운트 관리자**가 생성한다.
- 핵심 원칙은 **메타데이터와 데이터의 분리**이다.
  - 메타데이터와 권한 정보: Databricks Control Plane에서 관리
  - 실제 데이터 파일: 고객의 클라우드 스토리지(S3, ADLS, GCS)에 저장
- 메타스토어가 관장하는 네 가지 기능
  1. 외부 스토리지 액세스 (Storage Credential, External Location)
  2. 카탈로그 (객체 계층의 진입점)
  3. Lakehouse Federation (외부 DB를 옮기지 않고 그 자리에서 쿼리)
  4. OpenSharing (조직 간 안전한 데이터 공유)
- 메타스토어 수준에 관리형 스토리지 위치(managed storage location)를 지정할 수 있으며, 이는 선택 사항이다. 카탈로그나 스키마 수준에서 따로 지정할 수도 있다.

### 2-3. 3단계 네임스페이스

UC의 모든 데이터 객체는 `catalog.schema.object` 세 단계로 참조한다.

| 구분 | 형식 | 예시 |
|---|---|---|
| 전통적 SQL (2단계) | `schema.table` | `SELECT * FROM sales.orders` |
| Unity Catalog (3단계) | `catalog.schema.object` | `SELECT * FROM prod.sales.orders` |

3단계가 주는 이점은 다음과 같다.

- 카탈로그로 환경(prod/dev), 도메인, 사업부를 명확히 분리할 수 있다.
- 객체 이름이 모호하지 않게 해석된다.
- 모든 접근이 명확한 보안 컨텍스트(어느 카탈로그의 어느 스키마인지) 안에서 이뤄진다.

현재 컨텍스트를 지정하면 이름을 줄여 쓸 수 있다.

```sql
USE CATALOG prod;
USE SCHEMA sales;
SELECT * FROM orders;   -- prod.sales.orders

-- 현재 컨텍스트 확인
SELECT current_catalog(), current_schema(), current_user();
```

---

## 3. 보안 객체(Securable) 계층 구조

### 3-1. 계층

- **Metastore**
  - **Catalog**
    - **Schema** (= Database)
      - Table (관리형 / 외부)
      - View, Materialized View, Metric View
      - Volume (관리형 / 외부)
      - Function
      - Model (등록된 ML 모델)
      - 기타: Service, Secret, Skill, 스키마 수준 Connection
  - Storage Credential, Service Credential
  - External Location
  - Connection (Lakehouse Federation용)
  - Share, Recipient, Provider (OpenSharing용)

### 3-2. 각 객체 설명

| 객체 | 설명 |
|---|---|
| **Catalog** | 3단계 네임스페이스의 첫 단계. 가장 큰 조직 경계로, 도메인, 환경(prod/staging), 사업부별로 데이터를 나눈다. |
| **Schema (Database)** | 카탈로그 안에서 관련 자산을 논리적으로 묶는 단위. 애플리케이션, 도메인 팀, 샌드박스 단위로 권한을 관리하는 경계가 된다. |
| **Table** | 구조화된 데이터의 기본 저장 단위. 보통 Delta 형식이며 관리형 또는 외부 테이블이다. |
| **View** | 저장된 쿼리 정의. 복잡한 로직을 추상화하거나, 민감한 행과 열을 숨기는 보안 도구로 쓴다. |
| **Volume** | 비정형, 반정형 파일에 대한 접근을 관리하는 컨테이너. 관리형 또는 외부 Volume이다. |
| **Function** | SQL 또는 Python으로 작성한 사용자 정의 함수(UDF). 행 필터와 컬럼 마스크 로직의 기반이 된다. |
| **Model** | MLflow에 등록된 ML 모델. UC가 모델 레지스트리 역할을 한다. |
| **Metric View** | 테이블, 뷰, 쿼리 위에 재사용 가능한 메트릭(지표) 정의를 두는 읽기 전용 객체. 일반 뷰처럼 쿼리하며, 지표 로직을 중앙에서 관리해 노트북, 대시보드, 보고서에서 일관되게 쓴다. |
| **Service** | 모델 서빙, 모델 공급자, MCP 서버, 에이전트 같은 호출 가능한 AI 자산. UC 권한으로 AI 트래픽 접근을 제어한다. |
| **Secret** | 비밀번호, API 토큰 같은 민감한 값. 실제 값을 노출하지 않고 코드나 다른 UC 객체에서 참조한다. |
| **Skill** | 재사용 가능한 에이전트 기능을 패키지로 만든 객체. 다른 UC 자산과 같은 방식으로 공유, 검색, 권한 관리한다. |
| **Connection** | 외부 시스템 접속에 필요한 엔드포인트와 자격 증명. Lakehouse Federation, 관리형 수집, JDBC, HTTP 호출 등에 쓴다. 스키마 수준 HTTP Connection(`catalog.schema.connection`)은 상위 스키마의 권한을 상속한다. |

> 주의: Service, Secret, Skill, 스키마 수준 Connection은 최근에 추가된 객체 유형이다. 기능 상태(Preview/GA)는 시점에 따라 다를 수 있다.

---

## 4. 관리형 vs 외부: 테이블과 Volume

### 4-1. 관리형 테이블 vs 외부 테이블

| 항목 | 관리형 (MANAGED) | 외부 (EXTERNAL, 비관리형) |
|---|---|---|
| 데이터 수명 주기 | UC가 메타데이터와 데이터 파일 모두 관리 | UC는 메타데이터와 권한만 관리, 파일은 고객이 관리 |
| `DROP TABLE` | 메타데이터와 데이터 모두 삭제 (일정 기간 `UNDROP` 가능) | 메타데이터만 삭제, 데이터 파일은 남음 |
| 생성 구문 | `CREATE TABLE c.s.t (...)` | `CREATE TABLE c.s.t (...) LOCATION '<경로>'` |
| 데이터 위치 | 관리형 스토리지 위치 중 **스키마 → 카탈로그 → 메타스토어** 순으로 먼저 발견되는 곳 | `LOCATION`으로 지정한 경로 |
| 성능 최적화 | Predictive Optimization 등 자동 최적화 지원 | 수동 관리 |
| 지원 형식 | Delta, Iceberg | Delta, Iceberg, CSV, JSON, Avro, Parquet, ORC, Text |
| 장점 | 새 기능, 성능, 단순함, 더 엄격한 접근 제어 | 스토리지 명명 규칙 유지, 외부 리더/라이터와 호환, 쉬운 마이그레이션 |

```sql
-- 관리형 테이블
CREATE TABLE prod.sales.orders (order_id BIGINT, amount DECIMAL(10,2));

-- 외부 테이블 (External Location 아래 경로여야 함)
CREATE TABLE prod.sales.orders_ext (order_id BIGINT, amount DECIMAL(10,2))
LOCATION 's3://my-bucket/sales/orders';

-- 테이블 유형 확인 (Type: MANAGED / EXTERNAL)
DESCRIBE TABLE EXTENDED prod.sales.orders;

-- 실수로 삭제한 관리형 테이블 복구
UNDROP TABLE prod.sales.orders;
```

> 주의: 특별한 이유(외부 도구가 같은 파일을 직접 읽고 써야 함, 기존 스토리지 구조를 유지해야 함 등)가 없다면 **관리형 테이블이 기본 권장**이다.

### 4-2. 관리형 Volume vs 외부 Volume

| 항목 | 관리형 Volume | 외부 Volume |
|---|---|---|
| 파일 위치 | UC가 지정한 관리형 스토리지 경로 | 기존 External Location 아래 경로 |
| 삭제 시 | 파일도 삭제 | 파일은 남음 |
| 용도 | 새로 만드는 파일 저장 공간 | 이미 있는 클라우드 경로에 데이터 이동 없이 UC 권한과 감사를 적용 |

```sql
CREATE VOLUME prod.raw.landing;                                    -- 관리형
CREATE EXTERNAL VOLUME prod.raw.legacy LOCATION 's3://bucket/legacy';  -- 외부
```

---

## 5. 외부 시스템 연결을 위한 기본 객체

| 대상 | 객체 | 역할 |
|---|---|---|
| 파일 (클라우드 스토리지) | **Storage Credential** | 스토리지 접근용 인증 정보 (AWS IAM Role, Azure Managed Identity / Service Principal 등) |
| | **Service Credential** | 클라우드 네이티브 서비스(스토리지 외) 접근용 자격 증명 |
| | **External Location** | Storage Credential + 스토리지 경로의 조합. 외부 테이블, 외부 Volume, 관리형 스토리지 위치가 이 아래에 만들어진다. |
| 데이터베이스 | **Connection** | 외부 DB 접속 정보와 자격 증명 (MySQL, PostgreSQL, Snowflake 등) |
| | **Foreign Catalog** | Connection을 통해 외부 DB를 UC 카탈로그처럼 보이게 한 것. UC 테이블과 함께 쿼리할 수 있다 (Lakehouse Federation). |

```sql
-- External Location 생성 (Storage Credential이 먼저 있어야 함)
CREATE EXTERNAL LOCATION sales_landing
URL 's3://my-bucket/landing'
WITH (STORAGE CREDENTIAL my_cred);

-- 해당 위치의 파일 읽기 권한
GRANT READ FILES ON EXTERNAL LOCATION sales_landing TO `data_engineers`;
```

---

## 6. Volumes: 파일 거버넌스

### 6-1. 왜 Volume이 필요한가

거버넌스는 테이블만이 아니라 **파일**에도 적용되어야 한다. Volume은 파일 접근에 UC 권한과 감사를 적용한다. 플랫폼에서 다루는 모든 파일을 Volume에 등록하는 것이 모범 사례다.

Volume에 두는 데이터 예시는 다음과 같다.

- ML용 비정형 데이터: 이미지, 오디오, 동영상, PDF
- ML 학습, 검증, 테스트용 반정형 데이터셋
- 수집 직후의 원시 파일, 탐색용 임시 파일
- 여러 워크스페이스에서 쓰는 라이브러리, 설정 파일
- 운영 파일: 로그, 체크포인트

> 주의: 테이블은 Volume 안에 등록하는 것이 아니라 관리형/외부 위치에 등록한다. Volume은 테이블이 읽어 들일 원시 파일에 대한 안전한 접근 경로다.

### 6-2. 경로와 접근 방법

경로 형식은 `/Volumes/<catalog>/<schema>/<volume>/<경로>` 이다.

```python
# dbutils
dbutils.fs.ls("/Volumes/prod/raw/landing/")

# Spark
df = spark.read.json("/Volumes/prod/raw/landing/events/")
```

```sql
-- SQL
SELECT * FROM read_files('/Volumes/prod/raw/landing/events/', format => 'json');
LIST '/Volumes/prod/raw/landing/';
```

```bash
# POSIX 스타일 (셸)
ls /Volumes/prod/raw/landing/
```

### 6-3. Volume 권한

| 작업 | 필요한 권한 |
|---|---|
| 파일 읽기 | `READ VOLUME` (+ 상위 `USE CATALOG`, `USE SCHEMA`) |
| 파일 쓰기/삭제 | `WRITE VOLUME` (+ 상위 `USE CATALOG`, `USE SCHEMA`) |
| Volume 생성 | 스키마에 `CREATE VOLUME` (외부 Volume은 External Location에 `CREATE EXTERNAL VOLUME`도 필요) |

Volume의 파일 작업도 UC 감사와 리니지에 기록된다.

---

## 7. Functions, Models, Feature Tables

### 7-1. Functions (UDF)

- SQL 또는 Python으로 작성한 함수를 스키마에 등록하고 UC가 관리한다.
- 함수 생성에는 스키마의 `CREATE FUNCTION` 권한이 필요하고, 호출에는 `EXECUTE` 권한이 필요하다.
- Python UDF는 조건부 해싱, 복잡한 비식별화처럼 GDPR, HIPAA 등 규정 준수에 필요한 로직을 구현할 때 유용하다.
- 행 필터와 컬럼 마스크의 로직은 이 UDF로 구현한다.

```sql
CREATE OR REPLACE FUNCTION prod.util.roll_dice()
RETURNS INT
LANGUAGE SQL
RETURN CAST(floor(rand() * 6) + 1 AS INT);

DESCRIBE FUNCTION EXTENDED prod.util.roll_dice;
GRANT EXECUTE ON FUNCTION prod.util.roll_dice TO `analysts`;
```

### 7-2. Models (MLflow Model Registry)

- UC는 통합 MLflow Model Registry 역할을 한다. 모델은 `catalog.schema.model` 이름으로 저장되므로, 여러 워크스페이스에서 같은 모델을 공유하고 관리할 수 있다.
- 모델의 버전 관리, 배포, 보안, 리니지(어떤 데이터와 피처로 학습했는지)가 같은 거버넌스 체계 안에서 관리된다.
- UC에 등록하는 모델 버전에는 반드시 **모델 시그니처**(입출력 스키마)가 있어야 한다.
- 배포 단계는 별칭(alias)으로 표현한다. 예: 버전 1에 `@prod` 별칭 부여.
- 필요한 권한
  - 모델 등록: 카탈로그에 `USE CATALOG`, 스키마에 `USE SCHEMA`와 `CREATE MODEL`
  - 새 버전 생성: 모델 소유자이거나 `CREATE MODEL VERSION` 권한 보유
  - 모델 사용(로드, 추론): `EXECUTE`

```python
import mlflow
mlflow.set_registry_uri("databricks-uc")
mlflow.register_model("runs:/<run_id>/model", "prod.ml.turbine_maintenance")
```

### 7-3. Feature Tables

- UC에서 **기본 키(Primary Key)가 있는 Delta 또는 Iceberg 테이블**은 모두 피처 테이블로 쓸 수 있다.
- 피처 테이블도 일반 테이블처럼 스키마 아래에 등록되고, 같은 권한, 검색, 리니지 체계를 따른다.
- 생성에 필요한 권한: 상위 카탈로그에 `USE CATALOG`, 스키마에 `USE SCHEMA`와 `CREATE TABLE`.

```sql
ALTER TABLE prod.ml.persons
ADD CONSTRAINT persons_pk PRIMARY KEY (first_name, last_name);
```

> UC의 기본 키 제약은 정보성(informational) 제약이다. 중복을 막아 주지는 않으므로 데이터 품질은 별도로 보장해야 한다.

---

## 8. ID(Identity)와 관리 역할

### 8-1. 주체(Principal)의 종류

| 주체 | 설명 |
|---|---|
| **User** | 사람 사용자 (이메일로 식별) |
| **Group** | 사용자, 서비스 주체, 다른 그룹의 묶음. 권한은 개인이 아닌 그룹에 주는 것이 원칙 |
| **Service Principal** | 자동화(Job, CI/CD, 외부 앱)를 위한 기계 ID. 애플리케이션 ID(UUID)로 식별 |

### 8-2. ID 페더레이션(Identity Federation)

- 사용자, 그룹, 서비스 주체를 **어카운트 수준에서 한 번** 관리하고, 각 워크스페이스에는 할당만 한다.
- 어카운트 콘솔에서 직접 만들거나, Microsoft Entra ID, Okta 같은 **IdP에서 SCIM으로 자동 동기화**할 수 있다.
- UC 권한 부여는 어카운트 수준 ID를 대상으로 한다.

> 주의: 워크스페이스에서만 만든 그룹(워크스페이스 로컬 그룹)은 레거시다. UC 권한 부여에는 **어카운트 그룹**을 사용해야 한다. 함수에서 그룹 멤버십을 검사할 때도 `is_account_group_member()`가 권장되며, `is_member()`는 워크스페이스 로컬 그룹을 기준으로 검사한다.

### 8-3. 관리 역할

Databricks는 관리 권한을 네 범위로 나눠 한 사람이 모든 것을 관리하지 않아도 되게 한다(분산 거버넌스).

| 역할 | 주요 책임 | 권장 담당 |
|---|---|---|
| **Account Admin** | UC 활성화, 메타스토어 생성, 어카운트 수준 사용자와 그룹 관리, IdP 연동, 워크스페이스 생성과 관리, Storage Credential 생성 | 플랫폼 운영 또는 중앙 거버넌스 팀 |
| **Workspace Admin** | 개별 워크스페이스의 접근 제어, 설정, 기능 관리. 클러스터와 SQL Warehouse 접근 권한 관리(비용과 보안을 위해 compute 생성 권한을 제한하는 경우가 많음). 어카운트 사용자와 그룹을 워크스페이스에 할당 | IT, 플랫폼, DevOps 팀 (서비스 주체 정기 감사 포함) |
| **Metastore Admin** | 메타스토어의 모든 객체 관리, 카탈로그를 워크스페이스에 바인딩, 카탈로그 생성과 삭제, 권한 부여, 소유권 이전, External Location 생성, OpenSharing 관계 설정 | 플랫폼 운영 또는 중앙 거버넌스 팀 (소유권은 데이터 소유자에게 위임) |
| **Marketplace Admin** | Marketplace 공급자 프로필 관리, 공개/비공개 리스팅 생성과 관리, 리스팅에 쓸 Share 생성 | 데이터 공유, 수익화 전략 담당 팀 |

### 8-4. 소유권 체인

```text
Metastore Admin
  → Catalog Owner   : 카탈로그와 그 안의 스키마, 스키마 객체 관리
    → Schema Owner  : 스키마와 그 안의 객체 관리
      → Table / View Owner : 개별 데이터 / AI 객체 관리
```

- 카탈로그 소유권은 사업부(BU) 거버넌스 팀 또는 중앙 거버넌스 팀에 위임하는 것이 일반적이다.
- **메타스토어 관리자 지정은 선택 사항**이다. 지정하지 않으면 어카운트 관리자가 그 역할을 대신한다.
- 소유자는 개인보다 **그룹**으로 지정하는 것이 좋다. 담당자가 퇴사하거나 이동해도 관리가 끊기지 않는다.

```sql
ALTER CATALOG prod OWNER TO `prod_data_owners`;
ALTER SCHEMA prod.sales OWNER TO `sales_data_team`;
ALTER TABLE prod.sales.orders OWNER TO `sales_data_team`;
```

### 8-5. 카탈로그-워크스페이스 바인딩

기본적으로 카탈로그는 같은 메타스토어에 연결된 모든 워크스페이스에서 보인다. **워크스페이스 바인딩**을 설정하면 특정 카탈로그를 지정한 워크스페이스에서만 접근할 수 있게 제한할 수 있다. 예를 들어 `prod` 카탈로그는 운영 워크스페이스에서만 쓸 수 있게 한다. 권한(GRANT)이 있어도 바인딩되지 않은 워크스페이스에서는 접근할 수 없다.

---

## 9. 권한 모델: 소유권, ACL, GRANT/REVOKE

### 9-1. 기본 원칙

- **소유자는 암묵적으로 모든 권한(ALL PRIVILEGES에 해당)을 가진다.** 객체를 만든 주체가 기본 소유자가 된다.
- 다른 주체는 **명시적으로 권한을 받아야만** 접근할 수 있다. 기본값은 접근 불가(deny by default)이다.
- 이 구조 덕분에 도메인 소유자가 자기 자산의 접근을 직접 관리하는 분산 거버넌스가 가능하다.

예시: 테이블 `t`

| 주체 | 권한 | 근거 |
|---|---|---|
| `jdoe@company.com` | 모든 권한 | 소유자 (암묵적) |
| `analysts` 그룹 | `SELECT` | 명시적 부여 |
| `dbadmins` 그룹 | `ALL PRIVILEGES` | 명시적 부여 |

### 9-2. ACL (Access Control List)

ACL은 **주체(누가) - 권한(무엇을) - 보안 객체(어디에)** 를 연결한다.

- 주체: 사용자, 그룹, 서비스 주체
- 권한: `SELECT`, `MODIFY`, `CREATE TABLE`, `USE CATALOG`, `EXECUTE`, `BROWSE` 등
- 보안 객체: 메타스토어, 카탈로그, 스키마, 테이블, 뷰, Volume, 함수, 모델, External Location, Connection, Share 등

권한 검사는 사용한 compute 종류와 관계없이 **항상 중앙 메타스토어의 메타데이터를 기준으로** 수행된다.

### 9-3. GRANT / REVOKE (ANSI SQL DCL)

```sql
-- 문법
GRANT <privilege> ON <securable_type> <securable_name> TO `<principal>`;
REVOKE <privilege> ON <securable_type> <securable_name> FROM `<principal>`;

-- 예시
GRANT SELECT ON TABLE iot.events TO `engineers`;
REVOKE SELECT ON TABLE iot.events FROM `engineers`;

-- 여러 권한을 한 번에
GRANT USE SCHEMA, SELECT ON SCHEMA prod.sales TO `analysts`;

-- 서비스 주체는 애플리케이션 ID로 지정
GRANT SELECT ON TABLE prod.sales.orders TO `a1b2c3d4-...-service-principal-uuid`;
```

> 주의: 주체 이름에 `@`, `-`, 공백 등이 들어가므로 **백틱(`)으로 감싸야** 한다.

### 9-4. 권한 관리 방법 세 가지

| 방법 | 설명 | 적합한 상황 |
|---|---|---|
| SQL | 노트북이나 SQL 에디터에서 `GRANT` / `REVOKE` 실행 | 일상 관리, 스크립트화 |
| Catalog Explorer (UI) | 화면에서 권한 확인과 변경 | 탐색, 빠른 확인 |
| 프로그래밍 방식 | Databricks CLI, Terraform, REST API, SDK | **운영 환경 권장**: 감사 가능하고 반복 가능한 배포 |

### 9-5. UC 권한 모델의 특징

- UC에는 `DENY`가 없다. 권한은 "부여됨"과 "부여 안 됨" 두 상태뿐이며, `REVOKE`는 이전에 준 권한을 회수할 뿐이다.
- **상속된 권한은 하위 객체에서 따로 회수할 수 없다.** 스키마에 `SELECT`를 줬다면, 그 스키마의 특정 테이블만 빼고 싶을 때 테이블에서 `REVOKE`해도 효과가 없다. 이 경우 권한을 더 좁은 범위로 다시 설계하거나, 행 필터, 컬럼 마스크, 뷰로 제한해야 한다.
- 권한은 주체가 속한 그룹을 통해서도 전달된다. 사용자의 실제 권한은 "직접 받은 권한 + 모든 소속 그룹이 받은 권한 + 상위 객체에서 상속된 권한"의 합이다.

---

## 10. 권한 종류와 상속

### 10-1. 주요 권한

| 권한 | 대상 | 의미 |
|---|---|---|
| `USE CATALOG` | 카탈로그 | 카탈로그 안의 객체에 접근하기 위한 전제 조건 (자체로는 데이터 접근 불가) |
| `USE SCHEMA` | 스키마 | 스키마 안의 객체에 접근하기 위한 전제 조건 |
| `BROWSE` | 카탈로그 등 | 데이터를 읽지 않고 **메타데이터만 볼** 수 있게 함 (검색, 리니지 그래프 노출) |
| `SELECT` | 테이블, 뷰 | 데이터 읽기 |
| `MODIFY` | 테이블 | `INSERT`, `UPDATE`, `DELETE`, `MERGE` 등 데이터 변경 |
| `EXECUTE` | 함수, 모델 | 함수 호출, 모델 사용 |
| `READ VOLUME` / `WRITE VOLUME` | Volume | Volume 파일 읽기 / 쓰기 |
| `READ FILES` / `WRITE FILES` | External Location | 외부 위치의 파일을 직접 읽기 / 쓰기 |
| `CREATE CATALOG` | 메타스토어 | 카탈로그 생성 |
| `CREATE SCHEMA` | 카탈로그 | 스키마 생성 |
| `CREATE TABLE` / `CREATE VOLUME` / `CREATE FUNCTION` / `CREATE MODEL` / `CREATE MATERIALIZED VIEW` | 스키마 | 해당 객체 생성 |
| `CREATE EXTERNAL TABLE` / `CREATE EXTERNAL VOLUME` | External Location | 그 위치에 외부 테이블 / 외부 Volume 생성 |
| `CREATE CONNECTION`, `CREATE EXTERNAL LOCATION`, `CREATE STORAGE CREDENTIAL` | 메타스토어 | 연결, 외부 위치, 자격 증명 생성 |
| `CREATE SHARE`, `CREATE RECIPIENT`, `CREATE PROVIDER` | 메타스토어 | OpenSharing 객체 생성 |
| `USE SHARE`, `USE RECIPIENT`, `USE PROVIDER`, `SET SHARE PERMISSION` | 메타스토어 | OpenSharing 객체 조회와 관리 |
| `USE MARKETPLACE ASSETS` | 메타스토어 | Marketplace 자산 사용 |
| `MANAGE` | 대부분의 객체 | 소유자가 아니어도 그 객체의 권한 관리, 이름 변경, 삭제 가능 |
| `ALL PRIVILEGES` | 대부분의 객체 | 해당 객체에 적용 가능한 모든 권한 |

### 10-2. 권한 상속

권한은 **위에서 아래로 상속**된다. 카탈로그나 스키마에 준 권한은 그 안의 **기존 객체와 앞으로 만들어질 객체 모두**에 적용된다.

| 모델 | 부여 방법 | 장점 | 단점 |
|---|---|---|---|
| 명시적 (Explicit) | 테이블마다 `SELECT` 부여 | 가장 세밀한 제어 | 테이블 10개면 부여도 10번. 관리 부담 큼 |
| 스키마에서 상속 | 스키마에 `SELECT` 부여 | 스키마의 모든 기존, 미래 테이블과 뷰에 자동 적용 | 스키마 안 일부 테이블만 제외하기 어려움 |
| 카탈로그에서 상속 | 카탈로그에 `SELECT` 부여 | 가장 간단 | 범위가 가장 넓음 |

- 상속은 **권한이 부여된 경계 안에서만** 적용된다. 스키마 A에 준 `SELECT`가 인접한 스키마 B로 번지지 않는다. 이것이 도메인 간 격리를 보장한다.
- 상위 객체에 `SELECT`를 상속받았더라도 `USE CATALOG`, `USE SCHEMA`는 여전히 필요하다. 둘 다 같은 레벨에서 같이 주는 경우가 많다.

```sql
-- 분석가 그룹에 sales 스키마 전체 읽기 권한 (현재와 미래의 테이블 모두)
GRANT USE CATALOG ON CATALOG prod TO `analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA prod.sales TO `analysts`;
```

---

## 11. 자주 쓰는 권한 시나리오

### 11-1. 테이블 조회

세 가지가 **모두** 있어야 한다. 하나라도 빠지면 쿼리가 실패한다.

| 레벨 | 필요한 권한 |
|---|---|
| 카탈로그 | `USE CATALOG` |
| 스키마 | `USE SCHEMA` |
| 테이블 | `SELECT` (직접 또는 스키마/카탈로그에서 상속) |

- `SELECT`는 실제 행을 읽는 "작업 권한"이고, `USE CATALOG`와 `USE SCHEMA`는 네임스페이스를 따라가 객체를 찾기 위한 "통과 권한"이다.

### 11-2. 테이블 생성

| 레벨 | 필요한 권한 |
|---|---|
| 카탈로그 | `USE CATALOG` |
| 스키마 | `USE SCHEMA` + `CREATE TABLE` |

- `CREATE TABLE`도 카탈로그 수준에서 주면 그 안의 모든 스키마로 상속된다.
- 새 **카탈로그**를 만들려면 메타스토어에 `CREATE CATALOG`가 필요하다.
- 외부 테이블을 만들려면 추가로 External Location에 `CREATE EXTERNAL TABLE`이 필요하다.

### 11-3. 기타 시나리오

| 작업 | 필요한 권한 |
|---|---|
| 테이블에 데이터 쓰기 | `USE CATALOG` + `USE SCHEMA` + `MODIFY` (`MERGE`처럼 읽기를 동반하면 `SELECT`도) |
| 뷰 조회 | `USE CATALOG` + `USE SCHEMA` + 뷰에 `SELECT` (기반 테이블 권한은 필요 없음) |
| 뷰 생성 | 스키마에 `CREATE TABLE` + 뷰가 참조하는 테이블에 `SELECT` |
| 함수 호출 | `USE CATALOG` + `USE SCHEMA` + 함수에 `EXECUTE` |
| 모델 등록 | `USE CATALOG` + `USE SCHEMA` + `CREATE MODEL` |
| Volume 파일 읽기 | `USE CATALOG` + `USE SCHEMA` + `READ VOLUME` |
| 메타데이터만 탐색 | 카탈로그에 `BROWSE` |

> 뷰 조회 시 사용자는 기반 테이블 권한이 없어도 된다. **뷰 소유자**가 기반 테이블에 접근할 수 있으면 된다. Dynamic View 보안 패턴이 이 원리에 기반한다.

---

## 12. 권한 확인과 문제 해결

### 12-1. 권한과 객체 정보 확인

```sql
-- 객체에 부여된 권한 확인
SHOW GRANTS ON TABLE prod.sales.orders;
SHOW GRANTS ON SCHEMA prod.sales;
SHOW GRANTS ON VIEW prod.sales.orders_secure;

-- 특정 주체가 받은 권한만 확인
SHOW GRANTS `analysts` ON SCHEMA prod.sales;

-- 함수 정의 확인
DESCRIBE FUNCTION EXTENDED prod.sec.redact_ssn;

-- 테이블 상세 (소유자, 유형, 위치, 행 필터/컬럼 마스크 등)
DESCRIBE TABLE EXTENDED prod.sales.orders;

-- 현재 사용자, 그룹 멤버십 확인
SELECT current_user(),
       is_account_group_member('analysts') AS in_analysts;
```

### 12-2. INFORMATION_SCHEMA로 권한 조회

각 카탈로그에는 `information_schema` 스키마가 있고, 메타스토어 전체를 보는 `system.information_schema`도 있다. 조회 결과에는 **본인이 볼 권한이 있는 객체만** 나온다.

```sql
-- 테이블 권한 목록
SELECT grantee, table_schema, table_name, privilege_type
FROM prod.information_schema.table_privileges
WHERE table_schema = 'sales';

-- 스키마, 카탈로그 권한
SELECT * FROM prod.information_schema.schema_privileges;
SELECT * FROM system.information_schema.catalog_privileges;
```

### 12-3. 자주 만나는 오류

| 상황 | 원인 | 해결 |
|---|---|---|
| `PERMISSION_DENIED` / `INSUFFICIENT_PERMISSIONS` | `SELECT`, `USE SCHEMA`, `USE CATALOG` 중 하나 이상 없음 | 세 단계 권한을 모두 확인 |
| 테이블이 존재하지 않는다는 오류 | 객체가 실제로 없거나, `BROWSE`/`USE` 권한이 없어 보이지 않음 | 권한 문제와 미존재를 구분해서 확인 |
| 테이블에서 `REVOKE`했는데 여전히 조회됨 | 스키마나 카탈로그 수준에서 상속된 권한, 또는 그룹을 통해 받은 권한 | 상위 레벨과 그룹 멤버십 확인 |
| 권한은 있는데 특정 워크스페이스에서만 접근 불가 | 카탈로그-워크스페이스 바인딩 | 바인딩 설정 확인 |

> 테이블 권한을 회수해도, 사용자가 그 테이블을 기반으로 한 **뷰**에 `SELECT`가 있으면 뷰를 통한 조회는 계속 가능하다(뷰 소유자 권한으로 실행되므로). 이는 의도된 동작이다.

---

## 13. 세분화된 접근 제어 개요

객체 단위 권한(GRANT)만으로는 "같은 테이블인데 사람마다 보이는 행, 열을 다르게" 할 수 없다. 이를 위한 것이 세분화된 접근 제어(Fine-Grained Access Control)이다.

### 13-1. 세 가지 사용 사례

| 사용 사례 | 내용 | 예시 |
|---|---|---|
| **컬럼 접근 제한** | 권한 없는 사용자에게 컬럼 값을 숨김 (컬럼은 있지만 값이 가려짐) | `pay_rate`를 `NULL` 또는 `CONFIDENTIAL`로 |
| **행 필터링** | 사용자나 그룹에 따라 레코드 자체를 결과에서 제외 | 미국 담당자는 `country = 'US'` 행만 |
| **데이터 마스킹** | 값을 변환하거나 일부만 가림 | 이메일은 도메인만 보이게: `*****@databricks.com` |

세 가지 모두 **쿼리를 실행하는 사용자, 서비스 주체, 그룹 멤버십에 따라 조건부로** 적용한다. 민감도별로 테이블 사본을 따로 만들 필요가 없다.

### 13-2. 세 가지 구현 방식

| 방식 | 정책 위치 | 특징 |
|---|---|---|
| **Dynamic View** | 뷰 정의 안 | 익숙하고 유연함. 뷰 객체가 늘어나 관리 부담. 원본 테이블에 직접 접근하면 우회됨 |
| **네이티브 Row Filter / Column Mask** | 테이블 메타데이터에 함수 연결 | 쿼리 경로와 관계없이 항상 적용. 엔터프라이즈 거버넌스에 권장 |
| **ABAC (속성 기반 접근 제어)** | 카탈로그/스키마 수준 정책 + 태그 | 태그가 붙은 모든 테이블, 컬럼에 자동 적용. 테이블별 설정을 하나의 정책으로 통합 |

### 13-3. 조건 판단에 쓰는 내장 함수

| 함수 | 반환 | 설명 |
|---|---|---|
| `current_user()` | STRING | 쿼리를 실행하는 사용자의 이메일 (서비스 주체는 애플리케이션 ID) |
| `is_account_group_member('g')` | BOOLEAN | 현재 사용자가 **어카운트 그룹** `g`의 멤버인지 (권장) |
| `is_member('g')` | BOOLEAN | 현재 사용자가 **워크스페이스 로컬 그룹** `g`의 멤버인지 (레거시) |

> 주의: 그룹 이름은 대소문자를 구분할 수 있으므로 정확히 입력해야 한다.

---

## 14. Dynamic View

### 14-1. 개념과 적용 방법

Dynamic View는 기반 테이블 위에 정의한 뷰로, 데이터를 사용자에게 보여 주기 전에 **조건부 로직으로 필터링하거나 마스킹**한다.

적용 순서는 다음과 같다.

1. 기반 테이블 위에 조건부 로직이 들어간 뷰를 만든다.
2. 사용자에게 **뷰에만** `SELECT`를 준다.
3. 사용자의 **기반 테이블 직접 접근 권한은 회수**한다. 이제 모든 조회가 뷰를 거친다.

뷰 소유자가 기반 테이블에 접근할 수 있으므로, 사용자는 테이블 권한 없이도 뷰를 통해 걸러진 데이터만 볼 수 있다.

### 14-2. 예시

```sql
-- 컬럼 마스킹 + 행 필터링을 함께 적용한 Dynamic View
CREATE OR REPLACE VIEW prod.hr.employees_secure AS
SELECT
  employee_id,
  name,
  -- 이메일: HR 그룹만 전체, 나머지는 사용자명 부분을 가림
  CASE WHEN is_account_group_member('hr_dept') THEN email
       ELSE regexp_replace(email, '^[^@]+', '*****')
  END AS email,
  -- 급여: HR 그룹만 실제 값
  CASE WHEN is_account_group_member('hr_dept') THEN pay_rate
       ELSE NULL
  END AS pay_rate,
  country
FROM prod.hr.employees
-- 행 필터: 관리자는 전체, 미국 담당 그룹은 미국 행만
WHERE is_account_group_member('admins')
   OR (is_account_group_member('us_managers') AND country = 'US');

GRANT SELECT ON VIEW prod.hr.employees_secure TO `us_managers`;
REVOKE SELECT ON TABLE prod.hr.employees FROM `us_managers`;
```

### 14-3. 재사용 가능한 마스킹 함수를 뷰에서 쓰기

마스킹 로직을 UDF로 만들어 두면 여러 뷰에서 재사용할 수 있다.

```sql
CREATE OR REPLACE FUNCTION prod.sec.redact_ssn(ssn STRING)
RETURNS STRING
COMMENT 'payroll 그룹 외에는 SSN 끝 4자리만 노출'
RETURN CASE
  WHEN is_account_group_member('payroll') THEN ssn
  ELSE 'XXX-XX-' || right(ssn, 4)
END;

CREATE OR REPLACE VIEW prod.hr.salaries_secure AS
SELECT salary_id, employee_name, prod.sec.redact_ssn(ssn) AS ssn, region
FROM prod.hr.salaries;
```

뷰 위에 다시 뷰를 쌓아 마스킹과 행 필터를 계층적으로 적용할 수도 있다. 예를 들어 `salaries_secure`(마스킹) 위에 `WHERE filter_region(region)`을 더한 `salaries_multi_secure`(마스킹 + 행 필터)를 만들 수 있다.

### 14-4. 한계

- 접근 패턴마다 뷰를 만들어야 해서 객체 수와 관리 부담이 늘어난다.
- 누군가 기반 테이블에 직접 권한을 가지고 있으면 뷰의 보호를 우회할 수 있다.
- 그래서 엔터프라이즈 거버넌스에는 테이블 자체에 정책을 붙이는 네이티브 방식이 권장된다.

---

## 15. 네이티브 Row Filter와 Column Mask

### 15-1. 개념

필터와 마스크 함수를 **테이블 메타데이터에 직접 연결**한다. 노트북, SQL Warehouse, BI 도구, 그 테이블을 읽는 뷰 등 어떤 경로로 테이블을 조회하든 항상 같은 정책이 적용된다. 테이블을 복제하거나 구조를 바꿀 필요가 없다.

| 구분 | Row Filter | Column Mask |
|---|---|---|
| 함수 반환 타입 | `BOOLEAN` (TRUE인 행만 보임) | **마스킹 대상 컬럼과 같은 타입** |
| 함수 인자 | 필터 판단에 쓸 컬럼들 | 첫 번째 인자는 마스킹할 컬럼 값, 이후 인자로 다른 컬럼 전달 가능 (`USING COLUMNS`) |
| 연결 단위 | 테이블당 하나 | 컬럼당 하나 |

### 15-2. 예시: 직원 디렉터리 테이블

직원 디렉터리(`employee_id`, `name`, `contact_info`, `country`, `tin`(납세자 ID), `pay_rate`)를 보호하는 예다.

```sql
-- 1) 컬럼 마스크 함수: HR 그룹 외에는 가림 (반환 타입 = 컬럼 타입)
CREATE OR REPLACE FUNCTION prod.sec.redact_tin(tin STRING)
RETURNS STRING
RETURN CASE WHEN is_account_group_member('hr_dept') THEN tin ELSE '***-**-****' END;

CREATE OR REPLACE FUNCTION prod.sec.redact_pay_rate(pay_rate DECIMAL(10,2))
RETURNS DECIMAL(10,2)
RETURN CASE WHEN is_account_group_member('hr_dept') THEN pay_rate ELSE NULL END;

-- 2) 행 필터 함수: 관리자는 전체, 나머지는 미국 행만
CREATE OR REPLACE FUNCTION prod.sec.filter_us(country STRING)
RETURNS BOOLEAN
RETURN is_account_group_member('admins') OR country = 'US';

-- 3) 테이블에 연결
ALTER TABLE prod.hr.directory ALTER COLUMN tin      SET MASK prod.sec.redact_tin;
ALTER TABLE prod.hr.directory ALTER COLUMN pay_rate SET MASK prod.sec.redact_pay_rate;
ALTER TABLE prod.hr.directory SET ROW FILTER prod.sec.filter_us ON (country);

-- 해제
ALTER TABLE prod.hr.directory ALTER COLUMN tin DROP MASK;
ALTER TABLE prod.hr.directory DROP ROW FILTER;
```

테이블 생성 시점에 바로 지정할 수도 있다.

```sql
CREATE TABLE prod.hr.directory (
  employee_id STRING,
  name        STRING,
  country     STRING,
  tin         STRING MASK prod.sec.redact_tin,
  pay_rate    DECIMAL(10,2) MASK prod.sec.redact_pay_rate
)
WITH ROW FILTER prod.sec.filter_us ON (country);
```

다른 컬럼 값을 참고해 마스킹할 때는 `USING COLUMNS`를 쓴다.

```sql
-- 미국 직원의 tin만 가리기
CREATE OR REPLACE FUNCTION prod.sec.mask_tin_by_country(tin STRING, country STRING)
RETURNS STRING
RETURN CASE WHEN country = 'US' AND NOT is_account_group_member('hr_dept')
            THEN '***' ELSE tin END;

ALTER TABLE prod.hr.directory
  ALTER COLUMN tin SET MASK prod.sec.mask_tin_by_country USING COLUMNS (country);
```

### 15-3. 필요한 권한과 주의 사항

- 필터나 마스크를 설정하려면 테이블 소유자이거나 `MANAGE` 권한이 있어야 하고, 함수에 `EXECUTE` 권한이 필요하다.
- 테이블을 조회하는 사용자는 **필터와 마스크 함수에 대한 `EXECUTE` 권한이 없어도** 된다. 정책은 자동으로 적용된다.
- 마스크 함수의 반환 타입이 컬럼 타입과 다르면 마스크로 연결할 수 없다. 예를 들어 `DECIMAL` 컬럼에 `'CONFIDENTIAL'`(STRING)을 반환하는 함수는 Dynamic View에서는 쓸 수 있어도 네이티브 마스크로는 쓸 수 없다.
- 행 필터나 컬럼 마스크가 걸린 테이블은 compute 유형과 런타임 버전에 따라 읽기 제약이 있을 수 있다. 서버리스, SQL Warehouse, 표준(공유) 접근 모드에서는 지원된다.

### 15-4. Dynamic View vs 네이티브 Row/Column Security

| 항목 | Dynamic View | 네이티브 Row Filter / Column Mask |
|---|---|---|
| 정책 위치 | 별도 뷰 객체 | 기반 테이블 메타데이터 |
| 적용 범위 | 그 뷰를 통한 조회만 | 테이블에 대한 모든 조회 |
| 우회 가능성 | 테이블 직접 권한이 있으면 우회 | 우회 불가 |
| 객체 관리 | 접근 패턴마다 뷰 추가 | 함수 재사용, 테이블에 연결 |
| 사용자가 쿼리하는 대상 | 뷰 이름 | 원래 테이블 이름 |
| 권장 | 레거시 패턴, 특수 변환이 필요할 때 | **권장 방식** |

---

## 16. 태그: 일반 태그와 Governed Tag

### 16-1. 태그란

태그는 UC 객체(카탈로그, 스키마, 테이블, 뷰, 컬럼, Volume, 모델 등)에 붙이는 **키-값 메타데이터**이다. 소유자, 목적, 민감도, 비용 센터처럼 비즈니스 맥락을 기술 메타데이터에 연결해 검색, 분류, 정책 적용에 쓴다.

### 16-2. 일반 태그 vs Governed Tag

| 항목 | 일반 태그 (비관리, 비즈니스 메타데이터 태그) | Governed Tag (관리형 태그) |
|---|---|---|
| 정의 위치 | 없음. 누구나 자유롭게 키와 값을 만듦 | **어카운트 수준 태그 정책**으로 키와 허용 값을 미리 정의 |
| 생성 주체 | `APPLY TAG` 권한이 있는 사용자 | 어카운트 관리자 또는 워크스페이스 관리자 (UI, API로 정의) |
| 값 | 자유 텍스트 | **허용된 값만** 사용 가능. 다른 값을 쓰면 오류 |
| 할당 권한 | `APPLY TAG` | 해당 태그 정책에 대한 `ASSIGN` 권한 |
| ABAC 정책에서 사용 | 불가 | **가능** |
| 용도 | 담당자, 프로젝트, 보존 기간 등 팀 메타데이터 | PII 분류, 민감도, 환경, 비용 센터 등 통제가 필요한 분류 |

### 16-3. Governed Tag 상세

**태그 정책이 하는 일**

- 사용할 수 있는 키와 값을 모두 통제해 일관성을 유지하고, 비슷한 태그가 난립하는 것을 막는다.
- 어떤 사용자가 특정 태그를 객체에 붙일 수 있는지 통제한다.
- 어떤 태그를 ABAC 정책에서 참조할 수 있는지 결정한다.
- 태그 정책 관리와 태그 할당 모두 UI, API, SQL을 지원한다.

**권한 모델**

| 권한 | 의미 |
|---|---|
| `CREATE` | 새 태그 정책 생성 |
| `MANAGE` | 태그 정책 수정, 삭제, 권한 관리 |
| `ASSIGN` | 정의된 Governed Tag를 객체에 할당 |

- 권한은 개별 태그 정책 단위로 주거나 어카운트 전체에 줄 수 있다.
- 기본값: 어카운트 관리자는 `MANAGE`와 `CREATE`, 워크스페이스 관리자는 `CREATE`를 가진다.
- 정책을 정의하는 사람(관리자)과 태그를 붙이는 사람(데이터 스튜어드, 도메인 소유자, `ASSIGN`만 필요)을 분리해 **직무 분리**를 지킨다.
- 태그 지정 권한은 데이터 접근 권한과 별개다.

**기존 태그와의 관계**

- Governed Tag는 기존 UC 태그 시스템 위에 만들어져 기존 태그를 그대로 보존한다.
- 어떤 키(예: `pii`)로 태그 정책을 정의하면, 그 키를 가진 **기존 태그가 자동으로 정책의 관리 대상**이 된다.
- 태그 키와 값은 **대소문자를 구분**한다.

**제약**

- Governed Tag 정의 자체를 노트북이나 SQL에서 생성, 이름 변경, 삭제할 수는 없다. 관리자 권한으로 UI 또는 API에서 한다.
- 허용 값이 아닌 값으로 할당하면 런타임 오류가 난다. 예를 들어 `pii`의 허용 값이 `ssn`, `email`, `address`라면 `pii = 'true'`는 실패한다.

**Governed Tag 체계 예시**

| 키 | 허용 값 | 붙이는 대상 예시 |
|---|---|---|
| `pii` | `ssn`, `email`, `address` | `employees.ssn` 컬럼에 `pii = ssn` |
| `classification` | `salary_masked`, `confidential`, `internal`, `public` | `employees.salary` 컬럼에 `confidential` |
| `geo_access` | `region_filter` | 행 필터 기준이 되는 `employees.region` 컬럼 |
| `environment` | `production`, `staging`, `development` | 카탈로그 |
| `data_quality` | `certified`, `experimental`, `deprecated` | 인증된 뷰 |
| `cost_center` | `engineering`, `hr`, `finance`, `marketing`, `sales` | 스키마 |

**사용 사례**

| 영역 | 내용 |
|---|---|
| 접근 관리 | ABAC 정책에서 참조할 태그를 결정. 접근 제한에 쓰는 태그에 통제를 더함 |
| 비용 귀속 | 예산 정책(Budget Policy)과 연동해 비용 모니터링 태그를 일관되게 사용 |
| 시스템 태그 | Databricks 시스템 태그로 자산 인증(certified), 사용 중단(deprecated), 자동 분류, 워크스페이스 수준 태그 지원 |
| 자동화 | 규칙과 워크플로로 태그 할당을 프로그래밍 방식으로 대규모 자동화 |

### 16-4. 태그 SQL

```sql
-- 카탈로그, 스키마, 테이블, 뷰, Volume에 태그 설정
SET TAG ON CATALOG prod `environment` = `production`;
SET TAG ON SCHEMA prod.hr `cost_center` = `hr`;
SET TAG ON TABLE prod.hr.employees `data_steward` = `hr_team`;
SET TAG ON VIEW prod.hr.hr_master `data_quality` = `certified`;

-- 태그 제거
UNSET TAG ON TABLE prod.hr.employees `data_steward`;

-- ALTER 문법 (여러 개를 한 번에)
ALTER TABLE prod.hr.employees SET TAGS ('contains_pii' = 'true', 'retention_policy' = '7_years');
ALTER TABLE prod.hr.employees UNSET TAGS ('retention_policy');

-- 컬럼 태그는 ALTER TABLE ... ALTER COLUMN 사용
ALTER TABLE prod.hr.employees ALTER COLUMN ssn    SET TAGS ('pii' = 'ssn');
ALTER TABLE prod.hr.employees ALTER COLUMN email  SET TAGS ('pii' = 'email');
ALTER TABLE prod.hr.employees ALTER COLUMN region SET TAGS ('geo_access' = 'region_filter');
ALTER TABLE prod.hr.employees ALTER COLUMN salary SET TAGS ('classification' = 'confidential');
```

> 컬럼은 독립된 보안 객체가 아니라 테이블의 일부이므로 `SET TAG ON COLUMN`이 아니라 `ALTER TABLE ... ALTER COLUMN ... SET TAGS`를 쓴다.

같은 키에 새 값을 덮어쓸 때 안전하게 처리하려면, 기존 태그를 조회해 있는 키만 먼저 해제한 뒤 설정한다.

```python
desired_tags = {"department": "finance", "sensitivity": "high"}

rows = spark.sql("""
  SELECT tag_name FROM prod.information_schema.table_tags
  WHERE schema_name = 'payroll' AND table_name = 'salaries'
""").collect()
existing = {r["tag_name"] for r in rows}

for key, value in desired_tags.items():
    if key in existing:   # 이미 있는 키만 해제 (없는 키 UNSET 시 오류 방지)
        spark.sql(f"UNSET TAG ON TABLE prod.payroll.salaries `{key}`")
    spark.sql(f"SET TAG ON TABLE prod.payroll.salaries `{key}` = `{value}`")
```

### 16-5. 태그 조회 (INFORMATION_SCHEMA)

| 뷰 | 내용 |
|---|---|
| `information_schema.catalog_tags` | 카탈로그 태그 |
| `information_schema.schema_tags` | 스키마 태그 |
| `information_schema.table_tags` | 테이블, 뷰 태그 |
| `information_schema.column_tags` | 컬럼 태그 |
| `information_schema.volume_tags` | Volume 태그 |

```sql
-- 테이블 수준 + 컬럼 수준 태그를 한 번에
SELECT table_name, tag_name, tag_value, 'table' AS scope
FROM prod.information_schema.table_tags
WHERE schema_name = 'hr'
UNION ALL
SELECT table_name, tag_name, tag_value, 'column:' || column_name
FROM prod.information_schema.column_tags
WHERE schema_name = 'hr'
ORDER BY table_name, scope, tag_name;
```

---

## 17. ABAC: 태그 기반 정책

### 17-1. 개념

ABAC(Attribute-Based Access Control)는 **Governed Tag를 속성으로 사용하는 중앙 정책**이다.

- "`pii` 태그가 붙은 모든 컬럼을 마스킹한다" 같은 규칙을 카탈로그나 스키마 수준에 **한 번** 정의한다.
- 그 범위 안에서 태그가 일치하는 모든 테이블과 컬럼에 행 필터나 컬럼 마스크가 **자동으로** 적용된다.
- 새 테이블이 생겨도 태그만 붙이면 정책이 따라붙는다. 테이블마다 `SET MASK`를 할 필요가 없다.

| 정책 유형 | 내용 |
|---|---|
| 세분화된 정책 | 행 필터(RLS), 컬럼 마스크(CLM)를 카탈로그/스키마/테이블 범위에 적용 |
| 거친(coarse) 정책 | 태그 기반 GRANT/DENY (향후 지원 예정으로 소개됨) |

### 17-2. 구문 예시

```sql
-- prod 카탈로그 전체: pii 태그가 붙은 컬럼을 마스킹 (관리자 제외)
CREATE POLICY mask_pii_columns
ON CATALOG prod
COMMENT 'pii 태그 컬럼 마스킹'
COLUMN MASK prod.sec.mask_string
TO `account users`
EXCEPT `admins`
FOR TABLES
MATCH COLUMNS has_tag('pii') AS c
ON COLUMN c;

-- geo_access 태그 컬럼을 기준으로 행 필터 적용
CREATE POLICY region_row_filter
ON SCHEMA prod.hr
ROW FILTER prod.sec.filter_region
TO `account users`
FOR TABLES
MATCH COLUMNS has_tag_value('geo_access', 'region_filter') AS r
USING COLUMNS (r);
```

- `has_tag('키')`: 해당 키의 태그가 있는 컬럼
- `has_tag_value('키', '값')`: 해당 키와 값이 일치하는 컬럼

> 주의: ABAC는 비교적 최근 기능이라 구문과 지원 범위가 바뀌어 왔다(초기 Preview에서는 `SET RULE ... WHEN COLUMN tagged(...)` 형태도 소개되었다). 실제로 쓸 때는 공식 문서의 현재 구문을 확인해야 한다.

### 17-3. 세 방식 비교 정리

| 질문 | Dynamic View | Row Filter / Column Mask | ABAC |
|---|---|---|---|
| 정책을 어디에 정의하나 | 뷰마다 | 테이블/컬럼마다 | 카탈로그/스키마에 한 번 |
| 대상 선택 방식 | 뷰 SQL에 직접 작성 | 테이블에 직접 연결 | 태그 일치 |
| 새 테이블 추가 시 | 새 뷰 작성 | 새로 연결 | 태그만 붙이면 자동 |
| 확장성 | 낮음 | 중간 | 높음 |

---

## 18. Data Classification

- AI(에이전트 기반)로 UC 테이블 전반의 **민감 데이터를 자동 감지**하고 태그를 제안하거나 붙이는 기능이다.
- 12가지 이상의 PII 유형과 지역별, 도메인별 패턴을 다루는 내장 분류기를 제공하며, 조직 고유 규칙을 위한 **사용자 정의 분류기**도 지원한다.
- 새 테이블과 컬럼은 보통 **24시간 이내**에 스캔된다. 재스캔은 새 컬럼, 새 테이블, 스키마 변경처럼 바뀐 부분만 **증분으로** 수행해 비용을 줄인다.
- 결과는 검토 UI와 시스템 테이블 **`system.data_classification.results`** 에서 확인한다.
- 검토 후 태그(예: `class.email_address`)를 자동 적용하고, 그 태그를 쓰는 ABAC 정책("`class.email_address` 컬럼은 마스킹")으로 바로 보호까지 연결할 수 있다.

```sql
SELECT * FROM system.data_classification.results
WHERE catalog_name = 'prod';
```

> 주의: Data Classification과 ABAC는 Preview로 소개된 기능이다. 상태는 시점에 따라 다를 수 있다.

---

## 19. 자산 검색 (Discovery)

### 19-1. 검색의 기본 원칙

- 사용자는 **자신이 볼 권한이 있는 자산만** 검색 결과에서 본다. 검색에도 UC 권한이 그대로 적용된다.
- 데이터를 읽을 권한은 주지 않고 존재만 알려 주고 싶을 때 `BROWSE` 권한을 쓴다. 사용자가 자산을 발견한 뒤 접근을 요청하는 흐름에 유용하다.
- 테이블 이름을 외우는 대신 태그로 "인증된 자산", "PII가 있는 자산", "HR 소유 자산"을 찾는 것이 거버넌스 워크플로의 핵심이다.

### 19-2. 워크스페이스 검색에서 태그로 찾기

| 검색어 | 결과 |
|---|---|
| `tag:pii` | `pii` 태그가 붙은 모든 자산 |
| `tag:pii:email` | `pii = email`인 자산 |
| `tag:data_quality:certified` | 인증된 자산 |
| `tag:cost_center:hr` | HR 비용 센터 자산 |

검색 동작 규칙은 다음과 같다.

- 형식은 `tag:<키>` 또는 `tag:<키>:<값>`이다.
- 태그 검색 대상은 테이블, 뷰, 모델, Volume, 대시보드, Genie Space, 노트북이다. **카탈로그, 스키마, 컬럼은 태그 검색 대상이 아니다.** 그래서 스키마에만 붙은 태그로는 결과가 나오지 않을 수 있다.
- 키와 값 모두 **정확한 문자열 일치**이다(`pii:SSN`이 아니라 `pii:ssn`).
- Governed Tag도 검색에서는 일반 태그와 똑같이 동작한다. UI에서는 잠금 표시로 구분된다.

### 19-3. SQL로 검색하기

```sql
-- 민감도가 높은 테이블과 뷰
SELECT table_name, tag_name, tag_value
FROM prod.information_schema.table_tags
WHERE schema_name = 'hr' AND tag_name = 'sensitivity' AND tag_value = 'high';

-- 인증되었거나 HR 비용 센터인 자산
SELECT table_name, tag_name, tag_value
FROM prod.information_schema.table_tags
WHERE schema_name = 'hr'
  AND ((tag_name = 'data_quality' AND tag_value = 'certified')
    OR (tag_name = 'cost_center'  AND tag_value = 'hr'));

-- 민감한 Governed Tag가 붙은 모든 컬럼 (마스킹 대상 파악)
SELECT table_name, column_name, tag_name, tag_value
FROM prod.information_schema.column_tags
WHERE schema_name = 'hr'
  AND tag_name IN ('pii', 'classification', 'geo_access')
ORDER BY table_name, column_name;
```

SQL 검색은 자동화된 거버넌스 파이프라인이나 감사 보고서에 특히 유용하다.

---

## 20. 데이터 리니지

### 20-1. 자동화된 리니지

- 클러스터나 SQL Warehouse에서 쿼리가 실행될 때 **런타임에 자동으로** 리니지가 캡처된다. 별도 설정이나 코드가 필요 없다.
- **테이블 수준과 컬럼 수준**까지 추적한다.
- 언어(SQL, Python, Scala, R)와 관계없이 캡처된다.
- 대상: 테이블, 뷰, 컬럼, 대시보드, Job(Workflow), 노트북, 피처 테이블, 파일, 외부 소스, 모델, Lakeflow Spark Declarative Pipelines.
- UC의 공통 권한 모델을 그대로 따른다.
- 활용: 규정 준수 증명, 영향 분석(이 컬럼을 바꾸면 어디가 깨지는가), 근본 원인 분석(이 값은 어디서 왔는가).

### 20-2. 작동 흐름

1. 코드(ETL Job, 임시 쿼리, 파이프라인)가 클러스터나 SQL Warehouse에서 실행된다.
2. **리니지 서비스**가 compute가 내보낸 로그를 분석하고, 선언적 파이프라인의 메타데이터를 가져온다.
3. 테이블 수준과 컬럼 수준 리니지를 조립한다.
4. 결과를 UI에 그래프로 보여 준다.
5. API로 내보내 Alation, Collibra, Microsoft Purview 같은 **외부 카탈로그**와 연동할 수 있다.

### 20-3. 리니지와 권한

- 리니지 그래프에도 권한이 적용된다. 사용자가 특정 객체에 대한 `BROWSE`나 `SELECT` 권한이 없으면 그 노드는 **숨겨지거나 마스킹되어** 표시된다.
- 따라서 전체 흐름을 보려면 관련 객체들의 권한이 필요하다.

### 20-4. 리니지 시스템 테이블

```sql
-- employees 테이블에서 파생된 모든 대상 (다운스트림)
SELECT source_table_full_name, target_table_full_name,
       source_type, target_type, entity_type, event_time
FROM system.access.table_lineage
WHERE source_table_full_name = 'prod.hr.employees'
ORDER BY event_time DESC;

-- 컬럼 수준 리니지: ssn 컬럼이 흘러간 곳
SELECT target_table_full_name, target_column_name
FROM system.access.column_lineage
WHERE source_table_full_name = 'prod.hr.employees'
  AND source_column_name = 'ssn';
```

> 주의: 시스템 테이블은 실시간이 아니라 지연을 두고 갱신된다. 방금 실행한 작업의 리니지가 바로 보이지 않을 수 있다.

예를 들어 `employees`에 PII 컬럼이 있다는 것을 태그로 찾았다면, 컬럼 리니지로 그 PII가 흘러간 모든 다운스트림 뷰와 모델을 찾아 함께 보호할 수 있다. 태그와 리니지는 이렇게 연결된다.

### 20-5. External Lineage

- Databricks 밖의 자산(Tableau 대시보드, Salesforce 객체 등)을 UC에 **외부 메타데이터 객체**로 등록하고, UC의 테이블, 뷰, 모델, 경로와 연결해 엔드투엔드 리니지를 완성한다.
- 업스트림/다운스트림 관계와 컬럼 수준 관계까지 수동으로 추가할 수 있고, 런타임 리니지와 함께 그래프에 표시된다.
- 필요 권한: 메타스토어에 `CREATE EXTERNAL METADATA`, 외부 메타데이터 객체에 `MODIFY`, 연결할 UC 객체에 적절한 읽기/쓰기 권한.

---

## 21. OpenSharing (Delta Sharing)과 Marketplace

### 21-1. OpenSharing 개요

- OpenSharing(Delta Sharing 기반의 개방형 공유 프로토콜)은 데이터를 **복제하지 않고** 파트너, 고객, 내부 다른 팀과 실시간 데이터를 안전하게 공유한다.
- 받는 쪽이 Databricks가 아니어도 된다(개방형 프로토콜).
- 공유 객체는 모두 메타스토어에서 관리되며, 공유된 데이터에도 UC 정책과 감사가 적용된다.

### 21-2. 구성 요소

| 객체 | 설명 |
|---|---|
| **Share** | 공유할 테이블, 뷰, Volume, 모델 등을 묶은 읽기 전용 컬렉션 (제공자 측) |
| **Recipient** | 공유를 받는 대상 (제공자 측에서 정의) |
| **Provider** | 나에게 데이터를 공유한 조직 (수신자 측에서 보임) |

접근 흐름은 다음과 같다.

1. 수신자 클라이언트가 OpenSharing 서버에 요청한다.
2. UC의 액세스 제어가 요청을 검사한다.
3. 승인되면 클라우드 스토리지(컨테이너/버킷)에 접근할 수 있는 **단기 토큰**이 발급되고, 승인되지 않으면 거부된다.

```sql
-- 제공자 측
CREATE SHARE sales_share;
ALTER SHARE sales_share ADD TABLE prod.sales.orders;
CREATE RECIPIENT partner_a;                       -- (Databricks 간 공유 시 USING ID 지정)
GRANT SELECT ON SHARE sales_share TO RECIPIENT partner_a;
```

### 21-3. 공유 관련 권한

| 권한 | 의미 |
|---|---|
| `CREATE SHARE` / `CREATE RECIPIENT` / `CREATE PROVIDER` | 각 객체 생성 |
| `USE PROVIDER` | 받은 공유(인바운드) 전체 보기 |
| `USE RECIPIENT` | 보낸 공유(아웃바운드)의 수신자 보기 |
| `USE SHARE` | Share 보기 |
| `SET SHARE PERMISSION` | Share에 대한 수신자 접근 관리 |

메타스토어 관리자는 이 권한들을 모두 가지며, 다른 사용자에게 위임할 수 있다.

### 21-4. Databricks Marketplace

- OpenSharing 위에 만들어진 데이터 교환 장터다.
- 공급자는 공급자 프로필을 만들고, 공개 또는 비공개 **리스팅**으로 데이터 상품, 노트북, 모델 등을 게시한다.
- 소비자는 `USE MARKETPLACE ASSETS` 권한으로 리스팅을 받아 쓴다.
- 공급자 측 관리는 Marketplace Admin이 담당한다.

---

## 22. 종합 예제: 급여 데이터 보호

지금까지의 내용을 한 흐름으로 묶은 예제다. 급여 데이터를 만들고, 태그를 달고, 권한을 주고, 마스킹과 필터링을 적용하고, 검증한다.

### 22-1. 객체 생성과 태그

```sql
-- 1) 카탈로그, 스키마 생성
CREATE CATALOG IF NOT EXISTS finance COMMENT '재무 및 급여 데이터';
CREATE SCHEMA IF NOT EXISTS finance.payroll COMMENT '급여, 보너스 데이터';

SET TAG ON CATALOG finance `department` = `finance`;
SET TAG ON SCHEMA finance.payroll `sensitivity` = `high`;

-- 2) 테이블 생성
CREATE OR REPLACE TABLE finance.payroll.salaries (
  salary_id     INT           COMMENT '급여 레코드 ID',
  employee_name STRING        COMMENT '직원 이름',
  ssn           STRING        COMMENT '사회보장번호 (PII)',
  email         STRING        COMMENT '이메일',
  base_salary   DECIMAL(10,2) COMMENT '기본 연봉',
  region        STRING        COMMENT '근무 지역'
) COMMENT '민감한 급여 및 PII 데이터';

SET TAG ON TABLE finance.payroll.salaries `contains_pii` = `true`;
ALTER TABLE finance.payroll.salaries ALTER COLUMN ssn SET TAGS ('pii' = 'ssn');

INSERT INTO finance.payroll.salaries VALUES
  (1, 'Nancy Gibson',   '666-84-2234', 'nancy.gibson@example.com',   84500.00, 'Americas'),
  (2, 'Nobu Yagawa',    '921-40-2534', 'nobu.yagawa@example.com',    98000.00, 'Asia Pacific'),
  (3, 'Ines Drechsler', '966-25-0367', 'ines.drechsler@example.com', 156500.00, 'Europe Middle East');
```

### 22-2. 객체 권한 부여

```sql
GRANT USE CATALOG ON CATALOG finance        TO `payroll_analysts`;
GRANT USE SCHEMA  ON SCHEMA  finance.payroll TO `payroll_analysts`;
GRANT SELECT      ON TABLE   finance.payroll.salaries TO `payroll_analysts`;
-- salary_bonuses 테이블에는 SELECT를 주지 않음 → 조회 시 INSUFFICIENT_PERMISSIONS
```

### 22-3. 마스킹, 필터 함수와 보안 뷰 (Dynamic View 방식)

```sql
CREATE OR REPLACE FUNCTION finance.payroll.redact_ssn(ssn STRING)
RETURNS STRING
RETURN CASE WHEN is_account_group_member('payroll_admins') THEN ssn
            ELSE 'XXX-XX-' || right(ssn, 4) END;

CREATE OR REPLACE FUNCTION finance.payroll.redact_email(email STRING)
RETURNS STRING
RETURN CASE WHEN is_account_group_member('payroll_admins') THEN email
            ELSE regexp_replace(email, '^[^@]+', '*****') END;

CREATE OR REPLACE FUNCTION finance.payroll.redact_base_salary(base_salary DECIMAL(10,2))
RETURNS STRING
RETURN CASE WHEN is_account_group_member('payroll_admins') THEN CAST(base_salary AS STRING)
            ELSE 'CONFIDENTIAL' END;

CREATE OR REPLACE FUNCTION finance.payroll.filter_region(region STRING)
RETURNS BOOLEAN
RETURN is_account_group_member('payroll_admins')
    OR (is_account_group_member('americas_team') AND region = 'Americas');

-- 1단계 뷰: 마스킹
CREATE OR REPLACE VIEW finance.payroll.salaries_secure AS
SELECT salary_id, employee_name,
       finance.payroll.redact_ssn(ssn)                 AS ssn,
       finance.payroll.redact_email(email)             AS email,
       finance.payroll.redact_base_salary(base_salary) AS base_salary,
       region,
       current_user()      AS accessed_by,
       current_timestamp() AS accessed_at
FROM finance.payroll.salaries;

-- 2단계 뷰: 마스킹 + 행 필터
CREATE OR REPLACE VIEW finance.payroll.salaries_multi_secure AS
SELECT * FROM finance.payroll.salaries_secure
WHERE finance.payroll.filter_region(region);

GRANT SELECT ON VIEW finance.payroll.salaries_secure       TO `payroll_analysts`;
GRANT SELECT ON VIEW finance.payroll.salaries_multi_secure TO `payroll_analysts`;
```

### 22-4. 검증과 회수

```sql
-- 함수 정의와 권한 확인
DESCRIBE FUNCTION finance.payroll.filter_region;
SHOW GRANTS ON VIEW finance.payroll.salaries_secure;
SHOW GRANTS ON TABLE finance.payroll.salaries;

-- 원본 테이블 직접 접근 회수 → 이제 뷰를 통해서만 조회 가능
REVOKE SELECT ON TABLE finance.payroll.salaries FROM `payroll_analysts`;

-- 정리: 카탈로그와 그 안의 모든 객체 삭제
DROP CATALOG IF EXISTS finance CASCADE;
```

결과는 다음과 같다.

| 사용자 | `salaries` 직접 조회 | `salaries_secure` | `salaries_multi_secure` |
|---|---|---|---|
| `payroll_admins` 멤버 (테이블 권한 있음) | 전체 원본 | 전체 원본 | 전체 행 |
| `payroll_analysts` + `americas_team` | 회수 후 실패 | SSN, 이메일, 급여 마스킹 | Americas 행만, 마스킹 상태 |
| `payroll_analysts`만 | 회수 후 실패 | 마스킹 | 0행 |

> 같은 요구 사항을 네이티브 방식으로 구현하려면 뷰 대신 `ALTER TABLE ... ALTER COLUMN ssn SET MASK ...`와 `SET ROW FILTER`를 원본 테이블에 직접 걸면 된다. 이 경우 원본 테이블 권한을 회수할 필요가 없다.

> 문제 해결 팁: "테이블이 없다"는 결과가 나오면 객체가 실제로 없는 경우(NOT_FOUND)와 권한이 없어 안 보이는 경우(PERMISSION_DENIED)를 구분해야 한다. 먼저 `SHOW TABLES IN finance.payroll`로 존재 여부를 확인하고, 그다음 `SELECT`를 시도하면 원인을 나눌 수 있다.

---

## 23. 핵심 정리 치트시트

### 구조

| 키워드 | 핵심 |
|---|---|
| 메타스토어 | 리전당 하나, 최상위 컨테이너, 메타데이터(Control Plane)와 데이터(고객 스토리지) 분리 |
| 네임스페이스 | `catalog.schema.object` |
| 관리형 테이블 | DROP 시 데이터도 삭제(UNDROP 가능), 위치는 스키마 → 카탈로그 → 메타스토어 순 |
| 외부 테이블 | `LOCATION` 지정, DROP 시 메타데이터만 삭제 |
| Volume | 파일 거버넌스, `/Volumes/c/s/v/...`, `READ VOLUME` / `WRITE VOLUME` |
| External Location | Storage Credential + 경로 |
| Foreign Catalog | Connection으로 외부 DB를 카탈로그처럼 (Lakehouse Federation) |

### 권한

| 키워드 | 핵심 |
|---|---|
| 기본 원칙 | 소유자는 모든 권한, 나머지는 명시적 부여 필요, DENY 없음 |
| 테이블 조회 | `USE CATALOG` + `USE SCHEMA` + `SELECT` |
| 테이블 생성 | `USE CATALOG` + `USE SCHEMA` + `CREATE TABLE` |
| 뷰 조회 | 뷰에 `SELECT`만 (기반 테이블 권한 불필요) |
| 상속 | 상위에 준 권한은 기존 + 미래의 하위 객체에 적용, 부여 경계 밖으로는 안 번짐 |
| 상속 권한 회수 | 하위 객체에서 따로 회수 불가 |
| `BROWSE` | 메타데이터만 보기 (검색, 리니지 노출) |
| `MANAGE` | 소유자가 아니어도 권한 관리, 이름 변경, 삭제 |
| 그룹 함수 | `is_account_group_member()` 권장, `is_member()`는 워크스페이스 로컬 그룹 |

### 역할

| 역할 | 핵심 |
|---|---|
| Account Admin | 메타스토어, 워크스페이스, ID, Storage Credential |
| Workspace Admin | 워크스페이스 내부 설정, compute 접근 |
| Metastore Admin | UC 객체 전체, 소유권 이전, 카탈로그 바인딩 (지정은 선택) |
| Marketplace Admin | 공급자 프로필, 리스팅 |

### 세분화된 접근 제어

| 방식 | 핵심 |
|---|---|
| Dynamic View | 뷰에 CASE/WHERE 로직 + 테이블 직접 권한 회수 |
| Row Filter | BOOLEAN 함수, `ALTER TABLE ... SET ROW FILTER f ON (col)` |
| Column Mask | 컬럼과 같은 타입을 반환하는 함수, `ALTER COLUMN c SET MASK f` |
| ABAC | `CREATE POLICY ... MATCH COLUMNS has_tag(...)`, 카탈로그/스키마에 한 번 정의 |

### 태그, 검색, 리니지

| 키워드 | 핵심 |
|---|---|
| 일반 태그 | `APPLY TAG`, 자유 값 |
| Governed Tag | 어카운트 수준 정책, 허용 값만, `ASSIGN` 권한, ABAC에 사용 |
| 태그 문법 | 객체: `SET TAG ON <type> <name> \`k\` = \`v\``, 컬럼: `ALTER TABLE ... ALTER COLUMN ... SET TAGS` |
| 태그 조회 | `information_schema.{catalog,schema,table,column}_tags` |
| 태그 검색 | `tag:키:값`, 카탈로그/스키마/컬럼은 대상 아님, 정확 일치 |
| 리니지 | 런타임 자동 캡처, 테이블 + 컬럼 수준, 권한 없는 노드는 숨김 |
| 리니지 테이블 | `system.access.table_lineage`, `system.access.column_lineage` |
| Data Classification | 자동 PII 감지, `system.data_classification.results` |
| OpenSharing | Share / Recipient / Provider, 단기 토큰, 복제 없음 |

---

## 24. 자가 점검 문제

> 답을 가리고 먼저 생각해본 뒤 펼쳐보자.

**Q1.** Unity Catalog 메타스토어는 어떤 단위로 만들며, 어떤 원칙으로 메타데이터와 데이터를 다루는가?
<details><summary>정답</summary>

클라우드 리전당 하나를 만들고, 같은 리전의 여러 워크스페이스가 공유한다. 메타데이터와 권한은 Databricks Control Plane에, 실제 데이터는 고객의 클라우드 스토리지에 두어 둘을 분리한다.
</details>

**Q2.** 관리형 테이블과 외부 테이블에서 `DROP TABLE`의 결과는 어떻게 다른가?
<details><summary>정답</summary>

관리형 테이블은 메타데이터와 데이터 파일이 모두 삭제된다(일정 기간 `UNDROP` 가능). 외부 테이블은 메타데이터만 삭제되고 데이터 파일은 원래 위치에 남는다.
</details>

**Q3.** 관리형 테이블을 만들 때 데이터 파일은 어디에 저장되는가?
<details><summary>정답</summary>

관리형 스토리지 위치를 스키마 → 카탈로그 → 메타스토어 순서로 찾아, 가장 먼저 지정된 위치에 저장한다.
</details>

**Q4.** 사용자가 `prod.sales.orders`를 조회하려면 최소한 어떤 권한이 필요한가?
<details><summary>정답</summary>

`prod` 카탈로그에 `USE CATALOG`, `prod.sales` 스키마에 `USE SCHEMA`, 테이블에 `SELECT`(직접 또는 상위에서 상속)가 필요하다. 하나라도 없으면 실패한다.
</details>

**Q5.** 스키마에 `SELECT`를 부여한 뒤, 그 스키마에 새 테이블이 생기면 사용자가 바로 조회할 수 있는가?
<details><summary>정답</summary>

그렇다. 상위 객체에 준 권한은 기존 객체뿐 아니라 앞으로 만들어질 하위 객체에도 상속된다(`USE CATALOG`, `USE SCHEMA`도 있다는 전제).
</details>

**Q6.** 스키마에서 `SELECT`를 상속받은 사용자에 대해, 특정 테이블에서만 `REVOKE SELECT`를 실행하면 어떻게 되는가?
<details><summary>정답</summary>

효과가 없다. 상속된 권한은 하위 객체에서 따로 회수할 수 없고, UC에는 DENY도 없다. 권한 범위를 다시 설계하거나 행 필터, 마스크, 뷰로 제한해야 한다.
</details>

**Q7.** 새 테이블을 만들려면 어떤 권한이 필요한가? 새 카탈로그는?
<details><summary>정답</summary>

테이블: `USE CATALOG` + `USE SCHEMA` + 스키마에 `CREATE TABLE`. 카탈로그: 메타스토어에 `CREATE CATALOG`.
</details>

**Q8.** 객체 소유자는 어떤 권한을 가지며, 소유권은 어떻게 바꾸는가?
<details><summary>정답</summary>

소유자는 그 객체에 대한 모든 권한을 암묵적으로 가지고 다른 주체에게 권한을 부여할 수 있다. `ALTER TABLE ... OWNER TO \`그룹\``처럼 변경하며, 소유자는 개인보다 그룹으로 지정하는 것이 좋다.
</details>

**Q9.** `BROWSE` 권한의 용도는 무엇인가?
<details><summary>정답</summary>

데이터를 읽지 않고 메타데이터만 볼 수 있게 한다. 검색 결과와 리니지 그래프에 자산이 보이게 해서, 사용자가 자산을 발견하고 접근을 요청할 수 있게 한다.
</details>

**Q10.** Account Admin과 Metastore Admin의 책임 차이는?
<details><summary>정답</summary>

Account Admin은 어카운트 전체(메타스토어 생성, 워크스페이스 생성, 사용자/그룹과 IdP 연동, Storage Credential)를 관리한다. Metastore Admin은 메타스토어 안의 UC 객체(카탈로그 생성, 권한 부여, 소유권 이전, 카탈로그-워크스페이스 바인딩, External Location, 공유)를 관리한다.
</details>

**Q11.** Workspace Admin은 주로 무엇을 관리하는가?
<details><summary>정답</summary>

개별 워크스페이스의 접근 제어, 설정, 기능, 클러스터와 SQL Warehouse 접근 권한(비용과 보안을 위해 compute 생성 제한), 어카운트 사용자와 그룹의 워크스페이스 할당을 관리한다.
</details>

**Q12.** ID 페더레이션이란?
<details><summary>정답</summary>

사용자, 그룹, 서비스 주체를 어카운트 수준에서 한 번 관리하고 각 워크스페이스에 할당하는 모델이다. IdP에서 SCIM으로 동기화할 수 있다.
</details>

**Q13.** 함수에서 그룹 멤버십을 확인할 때 `is_account_group_member()`와 `is_member()`의 차이는?
<details><summary>정답</summary>

`is_account_group_member()`는 어카운트 그룹을, `is_member()`는 워크스페이스 로컬 그룹을 기준으로 검사한다. UC 환경에서는 어카운트 그룹 기반인 `is_account_group_member()`가 권장된다.
</details>

**Q14.** 세분화된 접근 제어의 세 가지 사용 사례는?
<details><summary>정답</summary>

컬럼 접근 제한(값 숨김), 행 필터링(레코드 제외), 데이터 마스킹(값 일부 가림)이다.
</details>

**Q15.** Dynamic View로 보안을 적용할 때 반드시 함께 해야 하는 작업은?
<details><summary>정답</summary>

사용자의 기반 테이블 직접 접근 권한을 회수해야 한다. 그래야 모든 조회가 뷰를 거친다. 사용자는 뷰에 대한 `SELECT`만 있으면 되고, 뷰 소유자가 기반 테이블에 접근할 수 있으면 된다.
</details>

**Q16.** 네이티브 Row Filter / Column Mask가 Dynamic View보다 권장되는 이유는?
<details><summary>정답</summary>

정책이 테이블 메타데이터에 직접 붙어서 쿼리 경로와 관계없이 항상 적용되고 우회할 수 없다. 접근 패턴마다 뷰를 만들 필요가 없고, 사용자는 원래 테이블 이름 그대로 조회한다.
</details>

**Q17.** Row Filter 함수와 Column Mask 함수의 반환 타입 조건은?
<details><summary>정답</summary>

Row Filter는 BOOLEAN을 반환하고(TRUE인 행만 보임), Column Mask는 마스킹 대상 컬럼과 같은 타입을 반환해야 한다.
</details>

**Q18.** 테이블에 행 필터와 컬럼 마스크를 연결하는 SQL은?
<details><summary>정답</summary>

`ALTER TABLE t SET ROW FILTER f ON (col);`, `ALTER TABLE t ALTER COLUMN c SET MASK m;` 해제는 `DROP ROW FILTER`, `ALTER COLUMN c DROP MASK`이다.
</details>

**Q19.** 일반 태그와 Governed Tag의 차이는?
<details><summary>정답</summary>

일반 태그는 `APPLY TAG` 권한으로 누구나 자유로운 키와 값을 붙인다. Governed Tag는 어카운트 수준 태그 정책으로 키와 허용 값이 정해져 있고, `ASSIGN` 권한이 있어야 붙일 수 있으며, ABAC 정책에서 참조할 수 있다.
</details>

**Q20.** 허용 값이 `ssn, email, address`인 Governed Tag `pii`에 `pii = 'true'`를 할당하면?
<details><summary>정답</summary>

허용 값이 아니므로 런타임 오류가 발생한다.
</details>

**Q21.** 컬럼에 태그를 붙이는 문법은 테이블과 어떻게 다른가?
<details><summary>정답</summary>

테이블, 뷰, 스키마, 카탈로그는 `SET TAG ON <type> <name> \`k\` = \`v\``를 쓰고, 컬럼은 독립 보안 객체가 아니므로 `ALTER TABLE t ALTER COLUMN c SET TAGS ('k' = 'v')`를 쓴다.
</details>

**Q22.** 어떤 키로 태그 정책을 새로 정의하면, 그 키를 가진 기존 태그는 어떻게 되는가?
<details><summary>정답</summary>

자동으로 해당 태그 정책의 관리 대상(Governed)이 된다. 기존 태그 이력은 보존된다.
</details>

**Q23.** ABAC의 장점은 무엇인가?
<details><summary>정답</summary>

카탈로그나 스키마 수준에 정책을 한 번 정의하면, 일치하는 Governed Tag가 붙은 모든 테이블과 컬럼에 행 필터나 컬럼 마스크가 자동 적용된다. 새 테이블도 태그만 붙이면 보호되므로 테이블별 설정이 필요 없다.
</details>

**Q24.** 워크스페이스 검색에서 `tag:cost_center:hr`로 검색했는데 결과가 없다. 가능한 원인은?
<details><summary>정답</summary>

태그가 카탈로그나 스키마에만 붙어 있는 경우(카탈로그, 스키마, 컬럼은 태그 검색 대상이 아님), 태그가 붙은 객체를 볼 권한이 없는 경우, 키나 값의 대소문자가 다른 경우(정확 일치)이다.
</details>

**Q25.** 모든 PII 태그 컬럼을 SQL로 찾으려면 어떤 뷰를 조회하는가?
<details><summary>정답</summary>

`<catalog>.information_schema.column_tags`에서 `tag_name = 'pii'` 조건으로 조회한다.
</details>

**Q26.** UC 리니지는 어떻게 캡처되며, 어느 수준까지 추적하는가?
<details><summary>정답</summary>

클러스터나 SQL Warehouse에서 실행될 때 런타임에 자동으로 캡처되며, 언어와 관계없이 테이블 수준과 컬럼 수준까지 추적한다.
</details>

**Q27.** 리니지 그래프에서 일부 노드가 보이지 않는 이유는?
<details><summary>정답</summary>

해당 객체에 대한 `BROWSE`나 `SELECT` 권한이 없으면 리니지 노드가 숨겨지거나 마스킹된다.
</details>

**Q28.** 리니지를 SQL로 조회할 수 있는 시스템 테이블은?
<details><summary>정답</summary>

`system.access.table_lineage`(테이블 수준)와 `system.access.column_lineage`(컬럼 수준)이다. 실시간이 아니라 지연을 두고 갱신된다.
</details>

**Q29.** Tableau 대시보드처럼 Databricks 밖의 자산을 리니지에 포함하려면?
<details><summary>정답</summary>

External Lineage로 외부 메타데이터 객체를 등록하고 UC 객체와 관계를 연결한다. 메타스토어에 `CREATE EXTERNAL METADATA` 권한 등이 필요하다.
</details>

**Q30.** OpenSharing의 Share, Recipient, Provider는 각각 무엇이며, 수신자는 어떻게 데이터에 접근하는가?
<details><summary>정답</summary>

Share는 공유할 자산 묶음, Recipient는 공유받는 대상(제공자 측 정의), Provider는 나에게 공유한 조직(수신자 측에서 보임)이다. 수신자 요청이 UC 접근 제어를 통과하면 스토리지 접근용 단기 토큰이 발급되며, 데이터는 복제되지 않는다.
</details>

**Q31.** UC에 모델 버전을 등록할 때 반드시 있어야 하는 것은?
<details><summary>정답</summary>

모델 시그니처(입출력 스키마)이다. 등록에는 `USE CATALOG`, `USE SCHEMA`, `CREATE MODEL` 권한이 필요하다.
</details>

**Q32.** 어떤 테이블이 피처 테이블로 쓰일 수 있는가?
<details><summary>정답</summary>

UC에 있는, 기본 키가 정의된 Delta 또는 Iceberg 테이블은 모두 피처 테이블로 쓸 수 있다.
</details>

---

## 25. 참고 링크

- Unity Catalog 개요: https://docs.databricks.com/aws/en/data-governance/unity-catalog/
- 보안 객체와 권한: https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/privileges
- 권한 관리: https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/
- 관리형 테이블: https://docs.databricks.com/aws/en/tables/managed
- 외부 테이블: https://docs.databricks.com/aws/en/tables/external
- Volumes: https://docs.databricks.com/aws/en/volumes/
- External Location과 Storage Credential: https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/
- Lakehouse Federation: https://docs.databricks.com/aws/en/query-federation/
- 관리 역할: https://docs.databricks.com/aws/en/admin/
- ID 관리: https://docs.databricks.com/aws/en/admin/users-groups/
- Row Filter와 Column Mask: https://docs.databricks.com/aws/en/tables/row-and-column-filters
- Dynamic View: https://docs.databricks.com/aws/en/views/dynamic
- ABAC: https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/
- 태그: https://docs.databricks.com/aws/en/database-objects/tags
- Governed Tags: https://docs.databricks.com/aws/en/admin/governed-tags/
- Data Classification: https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification
- 데이터 리니지: https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-lineage
- 시스템 테이블: https://docs.databricks.com/aws/en/admin/system-tables/
- INFORMATION_SCHEMA: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-information-schema
- Delta Sharing (OpenSharing): https://docs.databricks.com/aws/en/delta-sharing/
- Databricks Marketplace: https://docs.databricks.com/aws/en/marketplace/
