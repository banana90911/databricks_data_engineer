# 레이크하우스 데이터 모델링 전략 — 핵심 개념 정리

---

## 목차

1. [메달리온 아키텍처와 모델링의 위치](#1-메달리온-아키텍처와-모델링의-위치)
2. [데이터 웨어하우스 기초](#2-데이터-웨어하우스-기초)
3. [세 가지 고전적 모델링 접근 방식](#3-세-가지-고전적-모델링-접근-방식)
4. [모델링 추상화 계층: CDM, LDM, PDM](#4-모델링-추상화-계층-cdm-ldm-pdm)
5. [Inmon: Corporate Information Factory](#5-inmon-corporate-information-factory)
6. [정규화: 3NF에 도달하는 방법](#6-정규화-3nf에-도달하는-방법)
7. [ERM과 Delta의 제약 조건](#7-erm과-delta의-제약-조건)
8. [Inmon 구현 예제: 3NF EDW와 종속 마트](#8-inmon-구현-예제-3nf-edw와-종속-마트)
9. [Kimball: 스타 스키마와 팩트 테이블](#9-kimball-스타-스키마와-팩트-테이블)
10. [차원 테이블과 대리 키](#10-차원-테이블과-대리-키)
11. [SCD(Slowly Changing Dimension)](#11-scdslowly-changing-dimension)
12. [Kimball 구현 예제: 스타 스키마와 SCD2 MERGE](#12-kimball-구현-예제-스타-스키마와-scd2-merge)
13. [스타 스키마 vs 스노우플레이크 스키마](#13-스타-스키마-vs-스노우플레이크-스키마)
14. [Data Vault 2.0: Hub, Link, Satellite](#14-data-vault-20-hub-link-satellite)
15. [해시 키와 로드 패턴](#15-해시-키와-로드-패턴)
16. [Data Vault 아키텍처와 구현 예제](#16-data-vault-아키텍처와-구현-예제)
17. [현대적 골드 레이어의 세 기둥](#17-현대적-골드-레이어의-세-기둥)
18. [모델링 접근 방식 결합](#18-모델링-접근-방식-결합)
19. [데이터 제품](#19-데이터-제품)
20. [종합 시나리오: 하나의 소스, 네 팀의 요구](#20-종합-시나리오-하나의-소스-네-팀의-요구)
21. [핵심 정리 치트시트](#21-핵심-정리-치트시트)
22. [자가 점검 문제](#22-자가-점검-문제)
23. [참고 링크](#23-참고-링크)

---

## 1. 메달리온 아키텍처와 모델링의 위치

### 1-1. 세 계층의 역할

데이터는 소스 시스템 → 브론즈 → 실버 → 골드 순으로 흐르고, 골드에서 BI, ML, 데이터 제품으로 분기한다.

| 계층 | 역할 | 특징 |
|---|---|---|
| **브론즈** | 원시 수집 (수집 계층) | 소스 형태 그대로 저장. 스키마 변경에 대비해 STRING, VARIANT, BINARY로 유지하기도 함. **추가 전용(append-only)**, 수정, 삭제하지 않음. 모든 하위 계층을 처음부터 재구성할 수 있는 원천 |
| **실버** | 정제 + 통합 (모델링 계층) | 타입 변환, 중복 제거, null 처리, 조인. 검증된 **비집계** 데이터. 브론즈의 소스 변동성과 골드의 소비 요구를 분리하는 완충 지점 |
| **골드** | 소비 준비 완료 (서빙 계층) | 특정 소비자(BI, ML, 데이터 제품)에 맞게 집계, 보강. 단일 계층이 아니라 목적별 구조의 집합 |

### 1-2. 각 모델링 방식이 놓이는 곳

| 계층 | 모델링 | 이유 |
|---|---|---|
| 브론즈 | 모델링 미적용 | 유일한 설계 목표는 소스 충실도. 여기서 모델링하면 모든 미래 사용 사례를 미리 알아야 하고, 모델 변경 시 원시 이력을 잃을 위험이 있음 |
| 실버 | **3NF / ERM (Inmon)**, **Raw Vault (Data Vault)** | 둘 다 **통합 지향** 패턴. 목표는 여러 소스를 하나의 일관된 모델로 표준화하는 것이지 쿼리 성능이 아님 |
| 골드 | **스타 스키마 (Kimball)**, **Business Vault + 마트**, 특성 테이블, 메트릭 뷰, 데이터 제품 | 모두 **소비 지향** 패턴. 특정 소비자의 쿼리 패턴을 중심으로 설계 |

### 1-3. 모델링을 지원하는 Delta Lake와 Unity Catalog 기능

| 기능 | 모델링에서의 용도 |
|---|---|
| `MERGE INTO` (ACID) | SCD Type 2 업데이트, Data Vault Hub/Link/Satellite 로드 |
| `GENERATED ALWAYS AS IDENTITY` | 별도 시퀀스 없이 대리 키 자동 생성 |
| 정보성 `PRIMARY KEY` / `FOREIGN KEY` | 모델 의도 문서화, ER 다이어그램 렌더링, 옵티마이저 힌트 |
| 시간 여행 (`VERSION AS OF`) | 감사, 로드 실패 후 롤백, 재현 가능한 ML 특성 스냅샷 |
| 3단계 네임스페이스 | 환경별 카탈로그, 계층/도메인별 스키마, 엔티티/마트별 테이블 |
| 리니지 | 브론즈 → 실버 → 골드 흐름을 자동 캡처 |
| 통합 거버넌스 | 테이블, 뷰, 볼륨, 모델, 특성 테이블에 하나의 GRANT/REVOKE 모델 |

---

## 2. 데이터 웨어하우스 기초

### 2-1. OLTP vs OLAP

| 항목 | OLTP (트랜잭션 처리) | OLAP (분석 처리) |
|---|---|---|
| 목적 | 비즈니스 이벤트 기록 | 패턴과 트렌드 분석 |
| 쿼리 | 소수 행, 인덱스 기반 포인트 조회 | 광범위한 스캔, 집계 |
| 스키마 | 정규화 (3NF) | 차원형, 광폭형, 볼트형 |
| 저장 | 행 기반 | 열 기반 (Parquet, Delta) |
| 일관성 | 행 수준, 실시간 | 최종 일관성 |
| 사용자 | 애플리케이션 | 분석가, BI, ML 파이프라인 |

- OLTP는 개별 행을 빠르게 쓰고 고치도록 설계되어 정규화로 쓰기 범위를 최소화한다.
- OLAP 쿼리는 적은 열로 많은 행을 집계하므로 열 기반 저장이 유리하다. `SUM(revenue)`는 그 열의 바이트만 읽는다.
- **Delta Lake는 OLAP 플랫폼**이다. 열 기반 Parquet 위에 ACID 트랜잭션을 더해 OLTP의 쓰기 안전성과 OLAP의 읽기 성능을 함께 제공한다.

### 2-2. 데이터 웨어하우스의 네 가지 속성 (Inmon, 1992)

| 속성 | 의미 | Databricks 대응 |
|---|---|---|
| **주제 지향 (Subject-oriented)** | 애플리케이션이 아니라 고객, 주문, 제품 같은 비즈니스 주제별로 구성 | 3단계 네임스페이스. 스키마를 비즈니스 도메인별로 구성 |
| **통합 (Integrated)** | 모든 소스가 하나의 명칭, 형식, 키 체계를 공유 | PK/FK 제약, 실버의 일관된 명명 규칙, 리니지 |
| **시간 가변 (Time-variant)** | 덮어쓰지 않고 변경 이력 보존 | Delta 시간 여행, SCD2, Satellite |
| **비휘발 (Non-volatile)** | 커밋된 데이터는 자동으로 덮어써지지 않음. 추가와 버전 관리 | Delta 트랜잭션 로그가 모든 변경을 새 버전으로 기록 |

---

## 3. 세 가지 고전적 모델링 접근 방식

### 3-1. 비교

| 항목 | Inmon (CIF) | Kimball (차원 모델링) | Data Vault 2.0 |
|---|---|---|---|
| 방향 | **하향식 (Top-down)** | **상향식 (Bottom-up)** | Hub / Link / Satellite |
| 핵심 | 통합 3NF 엔터프라이즈 모델이 종속 마트에 데이터 공급 | 비즈니스 프로세스별 스타 스키마를 마트 단위로 구축하고 **일치된 차원(conformed dimension)** 으로 연결 | 해시 비즈니스 키, 삽입 전용 이력, 병렬 로드 |
| 장점 | 단일 진실 소스, 강력한 통합 | 빠른 제공, BI 친화적이고 직관적 | 완전한 감사 추적, 병렬 로드, 확장 용이 |
| 트레이드오프 | 가치 실현까지 리드 타임이 길고 하위 변경이 어려움 | 일치된 차원으로 통합해야 함, 감사 지향이 아님 | 관리할 아티팩트가 많고, 소비를 위해 Business Vault 필요 |
| 주 위치 | 실버 | 골드 | 실버 (Raw Vault) + 골드 (Business Vault) |

> 세 방식은 **상호 배타적이지 않다.** 가장 흔한 레이크하우스 패턴은 3NF 또는 Raw Vault 실버가 Kimball 골드에 데이터를 공급하는 형태다.

### 3-2. 접근 방식 선택 기준

세 가지 요인이 선택을 좌우한다.

| 요인 | 기울어지는 방향 |
|---|---|
| **감사 부하** (전체 이력, 규제 요건) | Data Vault |
| **소스 변동성** (자주 바뀌는 소스가 많음, 여러 팀이 독립적으로 로드) | Data Vault |
| **소비자 기대** (BI 우선, 안정적인 소스) | Kimball |

의사결정 흐름은 다음과 같다.

1. 감사 부하나 규제 요건이 있는가? → 예: **Data Vault**
2. 아니라면, 변동성 높은 소스 시스템이 많은가? → 예: **Data Vault**
3. 아니라면, BI 우선이고 소스가 안정적인가? → 예: **Kimball**
4. 아니라면 → **혼합 (실버 3NF + 골드 스타)**

- 감사 요건을 가장 먼저 묻는 이유는 **사후에 적용하기 가장 어려운 제약**이기 때문이다. 실버에서 덮어쓴 이력은 골드 작업으로 복구할 수 없다.
- 팀 구조도 신호다. 소규모 BI 팀은 Kimball의 빠른 주기가, 대규모 플랫폼 팀은 Inmon이나 Data Vault의 넓은 구축이 맞는다.

---

## 4. 모델링 추상화 계층: CDM, LDM, PDM

어떤 접근 방식이든 모델은 세 단계의 추상화로 구체화된다. 별개의 프로젝트가 아니라 같은 모델을 점점 자세히 보는 관점이다.

| 계층 | 관점 | 내용 | 소유자 |
|---|---|---|---|
| **개념적 데이터 모델 (CDM)** | 비즈니스 뷰 | 핵심 엔티티(고객, 주문, 제품)와 관계만. 컬럼, 타입, 키 없음 | 비즈니스 이해관계자 |
| **논리적 데이터 모델 (LDM)** | 속성 뷰 | 속성, 키, 데이터 타입, 카디널리티(1:1, 1:N, M:N), 정규화 수준. **플랫폼 독립적** | 데이터 아키텍트 |
| **물리적 데이터 모델 (PDM)** | 구현 뷰 | `CREATE TABLE` DDL, PK/FK 제약, IDENTITY 컬럼, 파티셔닝, 클러스터링 | 데이터 엔지니어 |

진행 순서는 항상 CDM → LDM → PDM이다. 비즈니스가 어떤 엔티티와 관계가 중요한지 합의하기 전에 엔지니어가 물리 스키마를 확정하는 실패를 막아 준다.

---

## 5. Inmon: Corporate Information Factory

### 5-1. CIF 아키텍처

운영 소스 → 스테이징 → **엔터프라이즈 3NF 웨어하우스(EDW)** → 주제별 데이터 마트(재무, 마케팅, 운영)

- 추가로 **ODS(Operational Data Store)** 가 소스에서 직접 데이터를 받아 EDW를 우회한다. 전체 ETL 주기를 기다릴 수 없는 준실시간 대시보드용이다.
- EDW는 어떤 마트보다 먼저 설계되고, 통합 복잡성은 EDW에서 **한 번만** 처리된다.
- 모든 마트, ODS, 보고서가 하나의 EDW에서 파생되므로, **EDW를 한 번 고치면 모든 마트에 반영**된다.

| CIF 계층 | 메달리온 |
|---|---|
| 스테이징 | 브론즈 (변환 로직 없음) |
| 엔터프라이즈 3NF 웨어하우스 | 실버 (단일 진실 소스) |
| 데이터 마트 | 골드 (주제별 차원 모델) |

### 5-2. Inmon이 맞는 경우와 맞지 않는 경우

| 적합 | 부적합 |
|---|---|
| 소스가 많은 대규모 엔터프라이즈 (ERP, CRM, HR, 물류 등 통합을 한 번에) | 소규모 팀, 빠른 납품 필요 (리드 타임이 보통 수개월) |
| 강력한 거버넌스와 감사 (각 사실이 정확히 한 번 저장되어 리니지가 명확) | 변동성이 높은 소스 (스키마 변경이 3NF 전반과 하위 마트로 파급) |
| 마트 민첩성보다 안정성 우선 (재무와 마케팅이 항상 같은 숫자) | BI 전용 사용 사례 (통합 오버헤드만 추가) |

---

## 6. 정규화: 3NF에 도달하는 방법

### 6-1. 정규형 단계

**제3정규형(3NF)**: 모든 비키 컬럼이 기본 키에만 종속되고 중복이 없는 설계. 각 단계는 한 종류의 중복을 제거하며, 각 정규형은 다음 정규형의 전제 조건이다.

| 단계 | 조건 | 제거하는 것 |
|---|---|---|
| UNF (비정규형) | 시작점 | 반복 그룹, 다중 값 속성, 중복 행이 공존 |
| **1NF** | 모든 셀이 원자 값 하나, 각 행이 고유 | 반복 그룹 (값마다 별도 행으로 펼침) |
| **2NF** | 1NF + **부분 종속성 없음** | 복합 키의 일부에만 종속된 컬럼 → 그 부분 키를 키로 하는 별도 테이블로 |
| **3NF** | 2NF + **이행 종속성 없음** | 비키 컬럼이 다른 비키 컬럼에 종속되는 사슬 (A → B → C) → 별도 테이블로 |

### 6-2. 예제: 주문 스프레드시트

**UNF**: 한 셀에 여러 값

| OrderID | Customer | Items |
|---|---|---|
| 1001 | Acme Co | SKU-A x2 @ 9.99; SKU-B x1 @ 4.50 |
| 1002 | Globex | SKU-A x1 @ 9.99 |

**1NF**: 라인 항목당 한 행, 셀당 한 값 (복합 키 = OrderID + SKU)

| OrderID | Customer | SKU | Qty | Price |
|---|---|---|---|---|
| 1001 | Acme Co | SKU-A | 2 | 9.99 |
| 1001 | Acme Co | SKU-B | 1 | 4.50 |
| 1002 | Globex | SKU-A | 1 | 9.99 |

**2NF**: `Price`는 전체 키가 아니라 `SKU`에만 종속(부분 종속) → Product 테이블로 분리

- OrderLine(OrderID, Customer, SKU, Qty)
- Product(SKU, Price)

이제 SKU-A 가격이 바뀌면 Product의 한 행만 고치면 된다.

**3NF**: `CustomerName`은 OrderID → CustID → CustomerName의 이행 종속 → Customer 테이블로 분리

| 테이블 | 컬럼 |
|---|---|
| Order | OrderID (PK), CustID (FK) |
| Customer | CustID (PK), Name |
| OrderLine | OrderID (FK), SKU (FK), Qty |
| Product | SKU (PK), Price |

### 6-3. Delta Lake에서의 고려 사항

- Delta는 `ARRAY`, `MAP`, `STRUCT`를 지원하므로 1NF를 위반하는 것처럼 보일 수 있다. 반정형 브론즈에는 적합하지만 **3NF 실버 테이블에는 쓰지 않는다.**
- 3NF에서는 각 사실이 한 번만 저장되어, 업데이트가 오면 한 행만 바뀌고 `MERGE INTO` 중복 제거 로직이 단순해진다.

---

## 7. ERM과 Delta의 제약 조건

### 7-1. 엔티티-관계 모델링(ERM)

| 구성 요소 | 물리적 대응 | 설명 | 예 |
|---|---|---|---|
| **엔티티** | 테이블 | 추적이 필요한 고유한 비즈니스 객체. 반드시 고유 식별자(기본 키)가 있어야 함 | Customer, Order, Product |
| **속성** | 컬럼 | 엔티티에 관한 사실. 데이터 타입과 제약(NOT NULL 등)을 가짐 | Name, SKU, Price, Qty |
| **관계** | 외래 키 | 두 엔티티 간 연관. 자식 테이블의 컬럼이 부모의 기본 키를 참조. 카디널리티(1:1, 1:N, M:N)가 방향을 결정 | Customer places Order |

까마귀 발 표기법에서 `Customer ||--o{ Order`는 "고객 한 명이 0개 이상의 주문을 가진다"는 뜻이다. M:N 관계는 연결 테이블(예: OrderLine)로 풀어낸다.

### 7-2. 어떤 제약이 실제로 적용되는가

| 제약 | 동작 |
|---|---|
| `PRIMARY KEY` | **정보성.** 쓰기 시 검사하지 않음. 중복 키도 삽입됨 |
| `FOREIGN KEY` | **정보성.** 부모에 없는 값(고아 행)도 삽입됨 |
| `NOT NULL` | **적용됨.** null 삽입 거부. 추가할 때 기존 행도 검증 |
| `CHECK` | **적용됨.** 조건이 false인 행 거부. 결정론적 Boolean 식이어야 함. 추가할 때 기존 행도 검증 |

```sql
CREATE TABLE silver.customer (
  c_custkey INT NOT NULL,                       -- PK 컬럼은 반드시 NOT NULL
  c_name    STRING NOT NULL,                    -- 적용됨
  c_acctbal DECIMAL(12,2),
  CONSTRAINT pk_customer PRIMARY KEY (c_custkey) -- 정보성
);

CREATE TABLE silver.orders (
  o_orderkey INT NOT NULL,
  o_custkey  INT,
  CONSTRAINT pk_orders PRIMARY KEY (o_orderkey),
  CONSTRAINT fk_orders_customer FOREIGN KEY (o_custkey)
    REFERENCES main.silver.customer (c_custkey)   -- 정보성. 다른 스키마/카탈로그면 전체 이름 사용
);

-- CHECK 제약 추가 (적용됨)
ALTER TABLE silver.customer ADD CONSTRAINT chk_acctbal CHECK (c_acctbal >= -10000);
```

### 7-3. 그래도 PK/FK를 선언하는 이유

1. **모델 의도 문서화**: 어떤 컬럼이 키이고 무엇을 참조하는지 메타데이터로 남는다.
2. **ER 다이어그램**: Unity Catalog가 선언된 제약으로 엔티티-관계 다이어그램을 자동으로 그린다. 제약 정보는 `information_schema`의 뷰(예: `table_constraints`, `key_column_usage`, `referential_constraints`)로도 조회할 수 있어 다른 도구로 다이어그램을 생성할 수 있다.
3. **옵티마이저 최적화**: `RELY` 옵션으로 "파이프라인이 무결성을 보장한다"고 알리면 **조인 제거(join elimination)** 같은 재작성이 가능하다. 예: 두 테이블을 조인하지만 한쪽 컬럼을 전혀 쓰지 않으면 조인 자체를 생략.
4. **AI/BI, Genie**: 조인 경로를 자동으로 파악하는 데 쓰인다.

```sql
-- 파이프라인이 실제로 무결성을 보장할 때만 RELY 사용
CONSTRAINT pk_customer PRIMARY KEY (c_custkey) RELY
```

> 주의: `RELY`를 선언했는데 실제 데이터에 중복이나 고아 행이 있으면 **쿼리 결과가 틀릴 수 있다.** 실제 무결성은 파이프라인(중복 제거를 포함한 `MERGE INTO`, expectation 등)에서 보장해야 한다.

- PK/FK 제약에는 Unity Catalog가 필요하다. 제약을 추가하면 테이블의 Delta writer 프로토콜 버전이 올라간다.

---

## 8. Inmon 구현 예제: 3NF EDW와 종속 마트

TPC-H 데이터로 실버에 5개 엔티티의 EDW를 만들고, 골드에 종속 마트를 파생한다.

EDW 관계: REGION (1) → (N) NATION (1) → (N) CUSTOMER (1) → (N) ORDERS (1) → (N) LINEITEM

| 테이블 | PK | FK |
|---|---|---|
| `edw_region` | `r_regionkey` | 없음 (최상위) |
| `edw_nation` | `n_nationkey` | `n_regionkey` → region |
| `edw_customer` | `c_custkey` | `c_nationkey` → nation |
| `edw_orders` | `o_orderkey` | `o_custkey` → customer |
| `edw_lineitem` | (`l_orderkey`, `l_linenumber`) 복합 키 | `l_orderkey` → orders |

```sql
-- 부모부터 생성 (FK가 참조할 대상이 먼저 있어야 함), 삭제는 역순
CREATE TABLE silver.edw_region (
  r_regionkey INT NOT NULL,
  r_name      STRING NOT NULL,
  CONSTRAINT pk_region PRIMARY KEY (r_regionkey)
);

CREATE TABLE silver.edw_nation (
  n_nationkey INT NOT NULL,
  n_name      STRING NOT NULL,
  n_regionkey INT,
  CONSTRAINT pk_nation PRIMARY KEY (n_nationkey),
  CONSTRAINT fk_nation_region FOREIGN KEY (n_regionkey) REFERENCES silver.edw_region (r_regionkey)
);

-- ... edw_customer, edw_orders 동일 패턴 ...

CREATE TABLE silver.edw_lineitem (
  l_orderkey      INT NOT NULL,
  l_linenumber    INT NOT NULL,
  l_extendedprice DECIMAL(12,2),
  l_discount      DECIMAL(12,2),
  CONSTRAINT pk_lineitem PRIMARY KEY (l_orderkey, l_linenumber),
  CONSTRAINT fk_lineitem_orders FOREIGN KEY (l_orderkey) REFERENCES silver.edw_orders (o_orderkey)
);
```

정보성 FK 확인:

```sql
-- 존재하지 않는 고객의 주문도 성공적으로 삽입됨
INSERT INTO silver.edw_orders VALUES (99999999, 9999999, 'F', 0.00, current_date());

-- 안티 조인으로 고아 행 찾기 (무결성은 이렇게 파이프라인에서 검사)
SELECT COUNT(*) AS orphan_orders
FROM silver.edw_orders o
LEFT JOIN silver.edw_customer c ON o.o_custkey = c.c_custkey
WHERE c.c_custkey IS NULL;

-- NOT NULL은 적용되어 실패함
INSERT INTO silver.edw_customer (c_custkey, c_name) VALUES (NULL, 'Missing PK');
```

종속 마트는 **소스가 아니라 EDW에서만** 만든다.

```sql
CREATE OR REPLACE TABLE gold.mart_region_sales AS
SELECT r.r_name AS region,
       n.n_name AS nation,
       COUNT(DISTINCT o.o_orderkey) AS order_count,
       ROUND(SUM(l.l_extendedprice * (1 - l.l_discount)), 2) AS net_revenue
FROM silver.edw_region r
JOIN silver.edw_nation   n ON n.n_regionkey = r.r_regionkey
JOIN silver.edw_customer c ON c.c_nationkey = n.n_nationkey
JOIN silver.edw_orders   o ON o.o_custkey   = c.c_custkey
JOIN silver.edw_lineitem l ON l.l_orderkey  = o.o_orderkey
GROUP BY r.r_name, n.n_name;
```

---

## 9. Kimball: 스타 스키마와 팩트 테이블

### 9-1. 스타 스키마의 구성

| 구분 | 팩트 테이블 | 차원 테이블 |
|---|---|---|
| 내용 | 비즈니스 이벤트의 **측정값** | 측정값에 대한 **설명적 맥락** (누가, 무엇을, 어디서, 언제) |
| 형태 | 행 많음, 열 적음 | 행 적음, 열 많음 (넓음) |
| 키 | 차원 대리 키들을 FK로 보유 | 대리 기본 키 + 자연(비즈니스) 키 |
| 기타 | 설명 텍스트 없음 | SCD 컬럼으로 속성 변경 추적 |

예: `FCT_SALES`(dim_customer_key, dim_date_key, dim_product_key, dim_store_key, quantity, net_price, tax_amount, total_amount)가 중앙에 있고, `DIM_CUSTOMER`, `DIM_DATE`, `DIM_PRODUCT`, `DIM_STORE`가 둘러싼다. 모든 관계는 차원(1) → 팩트(N)다.

### 9-2. 그레인(Grain)

- **그레인 = 팩트 테이블 한 행이 나타내는 것.** 예: "판매 라인 항목 하나당 한 행".
- 그레인을 먼저 명시적으로 선언해야 유효한 차원과 측정값이 정해진다.
- 그레인을 섞으면(라인 항목과 주문 헤더를 한 테이블에) **이중 집계**가 발생한다.

### 9-3. 측정값의 가산성

| 유형 | 의미 | 예 |
|---|---|---|
| 가산 (additive) | 모든 차원에서 합산 가능 | 수량, 매출액 |
| 반가산 (semi-additive) | 일부 차원에서만 합산 가능. 보통 **시간축으로는 합산 불가** | 계좌 잔액, 재고량 (고객 간 합은 의미 있지만 1월+2월 잔액은 무의미) |
| 비가산 (non-additive) | 합산하면 안 됨 | 비율, 단가, 퍼센트 |

### 9-4. 네 가지 팩트 테이블 유형

| 유형 | 그레인 | 특징 | Delta 로드 방식 |
|---|---|---|---|
| **트랜잭션** | 이벤트당 한 행 (예: 주문 라인 항목) | 가장 일반적, 기본 선택. 기록 후 변경 없음 | 추가 전용, 날짜/고객 키로 Liquid 클러스터링 |
| **주기적 스냅샷** | 기간별 엔티티당 한 행 (예: 월별 계좌) | 이벤트가 없어도 행 생성. 잔액 같은 반가산 측정값 | 기간별 일괄 로드, 기간 키 기준 증분 덮어쓰기 |
| **누적 스냅샷** | 프로세스 인스턴스당 한 행 (예: 주문 1건) | 마일스톤(주문, 출고, 배송일)이 달성될 때마다 **제자리 업데이트**. 미달성 마일스톤은 NULL | 유일하게 UPDATE가 필요. 중복 제거 후 `MERGE INTO` |
| **팩트리스** | 이벤트 발생 기록 (예: 일별 학생 출석) | 숫자 측정값 없음. 행 자체가 사실, 개수는 행을 세어 구함 | 추가 전용 |

### 9-5. 일치된 차원(Conformed Dimension)과 드릴 어크로스

- `DIM_DATE`, `DIM_PRODUCT`처럼 여러 팩트(판매, 반품, 재고)가 공유하는 차원은 **같은 대리 키 값, 컬럼명, 비즈니스 규칙**을 가져야 한다.
- 그러면 분석가가 같은 날짜나 제품 축으로 여러 팩트를 나란히 비교(**드릴 어크로스**)할 수 있다.
- Kimball의 상향식 접근은 마트를 하나씩 만들되 이 일치된 차원으로 통합을 유지한다.

---

## 10. 차원 테이블과 대리 키

### 10-1. 차원 테이블의 네 가지 컬럼 그룹

| 그룹 | 예 | 설명 |
|---|---|---|
| **대리 키** | `dim_customer_key` | 시스템 생성 정수. 행 **버전**마다 다름. 팩트가 FK로 참조 |
| **비즈니스 키** | `customer_id` | 소스의 자연 식별자. 버전이 바뀌어도 고정 |
| **설명 속성** | `name`, `address`, `segment` | BI가 GROUP BY, 필터에 쓰는 컬럼 |
| **SCD 추적 컬럼** | `start_date`, `end_date`, `is_current` | 각 버전의 유효 기간 |

예: 고객 42가 이사한 경우

| dim_customer_key | customer_id | address | start_date | end_date | is_current |
|---|---|---|---|---|---|
| 7 | 42 | 100 Elm St | 2024-01-01 | 2024-05-31 | false |
| 18 | 42 | 200 Oak Ave | 2024-06-01 | NULL | true |

키 7을 참조하는 3월 팩트는 Elm St를, 키 18을 참조하는 7월 팩트는 Oak Ave를 보여 준다. **특정 시점 정확성**이 자동으로 보장된다.

### 10-2. 비즈니스 키를 차원 PK로 쓰지 않는 이유

1. **비즈니스 키는 바뀔 수 있다**: 인수합병으로 고객 ID가 재번호되거나 SKU가 재할당되면, 그 키를 참조하던 과거 팩트를 찾을 수 없게 된다.
2. **소스마다 키가 겹친다**: CRM의 고객 42와 청구 시스템의 계정 42는 다른 엔티티다. 대리 키는 웨어하우스가 만들어 전체에서 고유하다.
3. **버전 관리**: 같은 비즈니스 키의 여러 버전을 구분할 수 있어야 한다.
4. **조인 성능**: 수십억 행 팩트에서 BIGINT 하나가 복합 키나 긴 문자열보다 빠르다.

### 10-3. IDENTITY 컬럼

```sql
dim_customer_key BIGINT GENERATED ALWAYS AS IDENTITY
-- 시작값, 증가폭 지정 가능
dim_customer_key BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1)
```

| 형식 | 동작 |
|---|---|
| `GENERATED ALWAYS AS IDENTITY` | 항상 자동 생성. 사용자가 값을 지정할 수 없음 (권장) |
| `GENERATED BY DEFAULT AS IDENTITY` | 값을 지정하지 않으면 자동 생성, 지정하면 그 값 사용. 기존 웨어하우스에서 대리 키 값을 그대로 가져올 때만 사용 |

- IDENTITY 값은 할당 후 업데이트할 수 없고, IDENTITY 컬럼은 파티션 컬럼이 될 수 없다.
- 값은 고유하고 증가하지만 **연속적이라는 보장은 없다**(중간에 빈 번호가 생길 수 있다).
- IDENTITY 컬럼이 있는 테이블은 동시 쓰기 트랜잭션이 제한될 수 있다.

### 10-4. 특수 차원

| 차원 | 설명 |
|---|---|
| **역할 수행 차원 (Role-playing)** | 하나의 물리 차원을 다른 별칭으로 여러 번 조인. 예: `DIM_DATE`를 주문일, 출고일, 배송일 키로 세 번 사용 |
| **정크 차원 (Junk)** | 자연스러운 차원에 속하지 않는 저카디널리티 플래그(`order_type`, `is_online`)를 한 테이블로 모음. 팩트는 FK 하나만 보유 |
| **날짜 차원** | 연, 분기, 월, 요일, 휴일 여부 등을 미리 계산해 둔 Type 0 차원 |

---

## 11. SCD(Slowly Changing Dimension)

### 11-1. 네 가지 유형

SCD 유형은 **테이블이 아니라 속성 단위로** 선택한다. 한 차원에서 생년월일은 Type 0, 이메일은 Type 1, 주소는 Type 2를 쓰는 것이 일반적이다.

| 유형 | 전략 | 이력 | 사용 대상 |
|---|---|---|---|
| **Type 0** | 변경 무시, 원래 값 영구 보존 | 최초 값만 | 최초 가입일, 출생 국가, 생년월일 |
| **Type 1** | 제자리 덮어쓰기 (UPDATE) | 없음 | 오타 수정, 이메일 수정. 과거 팩트도 수정된 값으로 보이는 것이 의도된 동작 |
| **Type 2** | 기존 행을 닫고 새 대리 키로 새 행 추가 | 전체 | 주소, 세그먼트 등 이력이 중요한 속성 |
| **Type 3** | `previous_` 컬럼 추가 | 직전 값 하나만 | 직전 값 하나만 의미 있을 때. 다시 바뀌면 그 이전 값은 사라짐 |

Type 1 예 (이메일 오타 수정):

| customer_id | email |
|---|---|
| 42 | acme@gmial.com → acme@gmail.com (같은 행, 같은 키) |

Type 3 예:

| customer_id | address | previous_address |
|---|---|---|
| 42 | 200 Oak Ave | 100 Elm St |

### 11-2. SCD Type 2를 MERGE로 구현하는 원리

변경된 비즈니스 키마다 소스 행을 **두 번** 준비한다.

1. **OLD 행**: 대상의 현재 행과 매칭되어 그 행을 닫는다 (`WHEN MATCHED THEN UPDATE`: `is_current = false`, `end_date` 설정).
2. **NEW 행**: 의도적으로 매칭되지 않게 만들어 새 버전을 삽입한다 (`WHEN NOT MATCHED THEN INSERT`). IDENTITY가 새 대리 키를 할당한다.

하나의 `MERGE`가 한 트랜잭션으로 실행되므로, "이전 버전은 닫혔는데 새 버전은 아직 없는" 불완전한 상태가 생기지 않는다.

`MERGE` 절 정리:

| 절 | 실행 대상 | 허용 동작 |
|---|---|---|
| `WHEN MATCHED` | 소스와 대상이 매칭된 행 | UPDATE, DELETE |
| `WHEN NOT MATCHED [BY TARGET]` | 대상에 없는 소스 행 | INSERT |
| `WHEN NOT MATCHED BY SOURCE` | 소스에 없는 대상 행 | UPDATE, DELETE (예: 소스에서 사라진 레코드 만료 처리) |

> 주의: `MERGE`는 **대상 행 하나에 매칭되는 소스 행이 최대 하나**여야 한다. 소스에 같은 키가 중복되면 런타임 오류가 난다. 병합 전에 항상 소스를 중복 제거한다(`ROW_NUMBER()` 윈도우 함수, `GROUP BY` 등).

### 11-3. 팩트 로드와 조회

- 새 팩트를 로드할 때는 차원과 **`is_current = TRUE` 조건으로** 조인해 로드 시점에 유효한 버전의 대리 키를 가져온다.
- 이미 저장된 팩트를 조회할 때는 **대리 키로 직접** 조인한다. 팩트에 판매 시점의 대리 키가 들어 있으므로 과거 시점 정확성이 유지된다.
- 늦게 도착한 팩트는 이벤트 날짜가 버전의 `start_date`~`end_date` 사이에 드는 행과 조인한다.

```sql
-- 이벤트 날짜 기준으로 당시 유효했던 버전 찾기
JOIN gold.dim_customer dc
  ON f.customer_id = dc.customer_id
 AND f.order_date >= dc.start_date
 AND (dc.end_date IS NULL OR f.order_date <= dc.end_date)
```

> 선언적 파이프라인(SDP)을 쓴다면 `AUTO CDC INTO ... STORED AS SCD TYPE 2`가 위의 MERGE 로직(`__START_AT`, `__END_AT` 컬럼 관리, 순서 뒤바뀜 처리)을 자동으로 처리한다.

---

## 12. Kimball 구현 예제: 스타 스키마와 SCD2 MERGE

### 12-1. 실버 정제 테이블

TPC-H의 접두사 컬럼명(`c_custkey`)을 표준 이름(`customer_id`)으로 바꾸고, 공백 제거와 고정 정밀도 캐스팅을 한다.

```sql
CREATE OR REPLACE TABLE silver.refined_customer AS
SELECT c_custkey AS customer_id, TRIM(c_name) AS name, TRIM(c_address) AS address,
       c_nationkey AS nation_key, TRIM(c_phone) AS phone,
       CAST(c_acctbal AS DECIMAL(12,2)) AS acct_bal, TRIM(c_mktsegment) AS market_segment
FROM bronze.customer;
```

### 12-2. 골드 스타 스키마 DDL

```sql
-- 차원 (SCD2)
CREATE TABLE gold.DimCustomer (
  dim_customer_key BIGINT GENERATED ALWAYS AS IDENTITY,
  customer_id      INT,
  name             STRING,
  address          STRING,
  market_segment   STRING,
  start_date       DATE,
  end_date         DATE,
  is_current       BOOLEAN,
  CONSTRAINT pk_dim_customer PRIMARY KEY (dim_customer_key)
);

-- 차원 (Type 0 날짜 차원)
CREATE TABLE gold.DimDate (
  dim_date_key BIGINT GENERATED ALWAYS AS IDENTITY,
  full_date    DATE,
  year         INT,
  quarter      INT,
  month        INT,
  CONSTRAINT pk_dim_date PRIMARY KEY (dim_date_key)
);

-- 팩트 (그레인: 주문 1건)
CREATE TABLE gold.FactOrders (
  fact_orders_key  BIGINT GENERATED ALWAYS AS IDENTITY,
  order_id         INT,
  dim_customer_key BIGINT,
  dim_date_key     BIGINT,
  total_price      DECIMAL(12,2),
  CONSTRAINT pk_fact_orders PRIMARY KEY (fact_orders_key),
  CONSTRAINT fk_dim_customer FOREIGN KEY (dim_customer_key) REFERENCES gold.DimCustomer (dim_customer_key),
  CONSTRAINT fk_dim_date     FOREIGN KEY (dim_date_key)     REFERENCES gold.DimDate (dim_date_key)
);
```

> 날짜 차원은 실무에서 `20240115` 같은 스마트 키(YYYYMMDD 정수)를 쓰고, 주문에 등장한 날짜만이 아니라 전체 기간의 달력을 미리 생성해 두는 경우가 많다.

### 12-3. 초기 로드

```sql
-- 모든 고객을 현재 버전으로 (end_date NULL = 열린 상태)
INSERT INTO gold.DimCustomer (customer_id, name, address, market_segment, start_date, end_date, is_current)
SELECT customer_id, name, address, market_segment, CURRENT_DATE(), NULL, TRUE
FROM silver.refined_customer;

INSERT INTO gold.DimDate (full_date, year, quarter, month)
SELECT DISTINCT order_date, YEAR(order_date), QUARTER(order_date), MONTH(order_date)
FROM silver.refined_orders WHERE order_date IS NOT NULL;

-- 팩트: 현재 차원 버전의 대리 키를 가져옴
INSERT INTO gold.FactOrders (order_id, dim_customer_key, dim_date_key, total_price)
SELECT ro.order_id, dc.dim_customer_key, dd.dim_date_key, ro.total_price
FROM silver.refined_orders ro
JOIN gold.DimCustomer dc ON ro.customer_id = dc.customer_id AND dc.is_current = TRUE
JOIN gold.DimDate     dd ON ro.order_date  = dd.full_date;
```

### 12-4. 멱등성 있는 SCD Type 2 MERGE

```sql
MERGE INTO gold.DimCustomer t
USING (
  -- OLD 쌍둥이: 항상 생성. ON 절의 내용 비교가 멱등성을 보장
  SELECT *, 'OLD' AS row_type FROM incremental_customer_updates
  UNION ALL
  -- NEW 쌍둥이: 같은 내용의 현재 행이 이미 있으면 생성하지 않음
  SELECT i.*, 'NEW' AS row_type
  FROM incremental_customer_updates i
  WHERE NOT EXISTS (
    SELECT 1 FROM gold.DimCustomer x
    WHERE x.customer_id = i.customer_id
      AND x.is_current  = TRUE
      AND x.address     = i.address
  )
) s
  ON t.customer_id = s.customer_id
 AND t.is_current  = TRUE
 AND s.row_type    = 'OLD'          -- NEW 행은 절대 매칭되지 않음 → 항상 INSERT 경로
 AND t.address    <> s.address      -- 실제로 바뀐 경우에만 닫음
WHEN MATCHED THEN UPDATE SET
  t.is_current = FALSE,
  t.end_date   = CURRENT_DATE()
WHEN NOT MATCHED AND s.row_type = 'NEW' THEN INSERT
  (customer_id, name, address, market_segment, start_date, end_date, is_current)
VALUES
  (s.customer_id, s.name, s.address, s.market_segment, CURRENT_DATE(), NULL, TRUE);
```

| 장치 | 역할 |
|---|---|
| `s.row_type = 'OLD'`를 ON 절에 | NEW 행이 기존 행과 매칭되지 않게 해서 항상 삽입 경로로 보냄 |
| `t.address <> s.address` | 속성이 실제로 바뀐 경우에만 기존 행을 닫음 |
| 소스의 `NOT EXISTS` | 같은 내용의 현재 행이 있으면 NEW 행을 아예 만들지 않음 (`WHEN NOT MATCHED` 조건에는 서브쿼리를 쓸 수 없어 소스 단계에 둠) |
| 결과 | 같은 증분 데이터로 다시 실행해도 아무 변화가 없음 (**멱등성**) |

- 변경된 고객은 행이 2개(닫힌 이전 버전 + 새 현재 버전), 신규 고객은 현재 행 1개가 된다.
- 예제는 `address` 하나만 비교했지만, 실무에서는 모든 Type 2 속성을 비교하거나 **속성 전체의 해시(hash diff)** 하나를 비교한다.

### 12-5. 스타 스키마 쿼리

```sql
-- 시장 세그먼트별 총 지출 (팩트 → 차원 한 번 조인)
SELECT dc.market_segment, SUM(f.total_price) AS total_spent
FROM gold.FactOrders f
JOIN gold.DimCustomer dc ON f.dim_customer_key = dc.dim_customer_key
GROUP BY dc.market_segment;

-- 세그먼트, 분기별 매출
SELECT dc.market_segment, dd.year, dd.quarter, COUNT(*) AS orders, SUM(f.total_price) AS revenue
FROM gold.FactOrders f
JOIN gold.DimCustomer dc ON f.dim_customer_key = dc.dim_customer_key
JOIN gold.DimDate     dd ON f.dim_date_key     = dd.dim_date_key
GROUP BY dc.market_segment, dd.year, dd.quarter;
```

팩트가 대리 키를 참조하므로, 세그먼트 값은 각 주문이 로드될 당시 유효했던 값이다.

---

## 13. 스타 스키마 vs 스노우플레이크 스키마

| 항목 | 스타 스키마 | 스노우플레이크 스키마 |
|---|---|---|
| 구조 | 모든 차원이 팩트에 직접 연결 (한 번의 홉) | 계층을 별도 테이블로 정규화 (Customer → Nation → Region) |
| 계층 처리 | 차원에 평탄화 (DimCustomer가 nation, region 컬럼 보유) | 각 계층이 독립 엔티티 |
| 조인 수 | 적음 | 많음 (지역까지 3번 조인) |
| 저장 | 계층 값이 반복됨 | 계층 값이 한 번만 저장 |
| BI 친화성 | 높음 | 낮음 |

> "스노우플레이크"는 다이어그램 모양(눈송이)에서 온 일반 모델링 용어로, Snowflake사와는 무관하다.

### Delta에서는 스타 스키마가 유리한 이유

- **열 기반 저장**: 30개 컬럼 차원에서 2개 컬럼만 쓰는 쿼리는 그 2개만 읽는다. 넓은 차원이 I/O를 늘리지 않는다.
- **딕셔너리 인코딩**: 반복되는 문자열("EMEA" 등)의 저장 비용이 거의 없다.
- **브로드캐스트 조인**: 평탄화된 차원(수십만 행)은 실행자 메모리에 들어가 셔플 없이 조인된다. 스노우플레이크로 나누면 조인만 늘어난다.

스노우플레이크가 정당화되는 경우:

- 계층 엔티티가 **독립적인 생명주기**를 가질 때 (다른 팀이 다른 일정으로 관리하는 DIM_NATION)
- 수십억 행 규모의 차원에서 반복 문자열 절약이 조인 비용보다 클 때
- 보통은 일부 차원만 선택적으로 스노우플레이크화한다.

### 팩트 테이블 최적화

| 기법 | 설명 |
|---|---|
| 데이터 스키핑 | Delta는 기본적으로 처음 32개 컬럼의 min/max 통계를 수집. `WHERE dim_date_key BETWEEN ...` 범위 밖 파일은 읽지 않음 |
| Z-ORDER | `OPTIMIZE fct ZORDER BY (dim_date_key, dim_customer_key)`로 비슷한 값을 같은 파일에 배치 |
| **Liquid 클러스터링** (권장) | `CLUSTER BY`로 키 지정, 나중에 재작성 없이 `ALTER TABLE ... CLUSTER BY`로 변경 가능. 쏠림과 늦게 도착하는 데이터를 더 잘 처리. 새 팩트 테이블은 가장 자주 쓰는 필터 키로 Liquid 클러스터링 권장 |

```sql
CREATE TABLE gold.fct_sales (...) CLUSTER BY (dim_date_key, dim_customer_key);
OPTIMIZE gold.fct_sales;
```

---

## 14. Data Vault 2.0: Hub, Link, Satellite

### 14-1. Data Vault가 필요한 상황

| 상황 | Data Vault의 대응 | 다른 방식의 한계 |
|---|---|---|
| 감사가 필수 | 모든 행에 `load_ts`, `record_source`. 삭제, 덮어쓰기 없음 | Inmon 3NF는 새 소스 추가 시 기존 테이블 변경과 이력 재처리가 필요할 수 있음 |
| 소스가 자주 변경 | 새 소스는 새 Satellite만 추가. Hub, Link 변경 없음 | |
| 팀이 병렬로 작업 | 해시 키는 비즈니스 키에서 파생되므로 공유 시퀀스 없이 독립 계산 | Kimball SCD2 차원 로드는 엄격한 순서 의존성이 있음 |

핵심 아이디어는 **구조(비즈니스 키, 관계)와 맥락(설명 속성)의 분리**다.

### 14-2. 세 가지 테이블 유형

| 유형 | 담는 것 | 행 단위 | 필수 컬럼 |
|---|---|---|---|
| **Hub** | 비즈니스 키 (엔티티의 존재 등록) | 고유 비즈니스 키당 1행 | `hash_key`(PK), `business_key`, `load_ts`, `record_source` |
| **Link** | 2개 이상 Hub 간의 관계 | 고유한 부모 키 조합당 1행 | `composite_hash_key`(PK), 부모 `hash_key`들, `load_ts`, `record_source` |
| **Satellite** | 설명 속성 + 이력 | 속성이 바뀔 때마다 1행 (삽입 전용) | 부모 `hash_key`, `hash_diff`, 설명 속성, `load_ts`, `record_source`. PK = (`hash_key`, `load_ts`) |

- Hub에는 설명 속성이 없다. 비즈니스 개념당 Hub 하나 (소스가 몇 개든 고객 Hub는 하나).
- Link도 삽입 전용이다. 관계의 종료는 Link가 아니라 Link에 붙은 **Effectivity Satellite**로 모델링한다.
- Satellite는 **소스와 맥락별로** 나눈다. CRM의 주소와 리스크 시스템의 신용 정보는 같은 `customer_hk`를 참조하는 서로 다른 Satellite다. 한 소스의 스키마 변경은 그 Satellite에만 영향을 준다.

### 14-3. 예: 고객 42가 주문 1001을 생성

| 테이블 | 데이터 |
|---|---|
| H_Customer | (customer_hk = a3f7..., customer_id = 42) |
| H_Order | (order_hk = d41c..., order_id = 1001) |
| L_Customer_Order | (composite_hk = b8e2..., customer_hk = a3f7..., order_hk = d41c...) |
| S_Customer | (a3f7..., address = 100 Elm St, load_ts = 2024-01), (a3f7..., address = 200 Oak Ave, load_ts = 2024-06) |
| S_Order | (d41c..., amount = 1,250.00, status = F) |

- Hub는 엔티티당 한 행(존재만 기록), Satellite는 버전마다 한 행(이력 기록).
- ERP 같은 두 번째 소스가 추가되면, 같은 `customer_hk`를 참조하는 새 Satellite를 만들면 된다. 기존 테이블은 건드리지 않는다.

---

## 15. 해시 키와 로드 패턴

### 15-1. 두 가지 해시

MD5는 임의의 입력으로 고정 길이 32자 16진수 문자열을 만든다. 같은 입력은 항상 같은 출력을 낸다.

| 해시 | 입력 | 용도 | 변하는가 |
|---|---|---|---|
| **hash_key** | 비즈니스 키만 | 어떤 엔티티인지 식별. Hub(PK), Link(복합 PK), Satellite(FK) | 같은 키면 항상 같음 |
| **hash_diff** | 설명 속성 전체 | 무엇이 바뀌었는지 감지. Satellite에만 | 속성이 바뀌면 새 값 → 새 Satellite 행 |

### 15-2. "생성이 아니라 파생"이 병렬 로드를 가능하게 하는 이유

- 데이터베이스 시퀀스로 키를 만들면, Link와 Satellite가 키를 참조하기 전에 Hub가 먼저 삽입되어야 한다(직렬 의존성).
- 해시 키는 비즈니스 키에서 **계산**되므로, 어느 작업이든 언제 계산해도 같다. Hub, Link, Satellite를 **임의 순서로 동시에** 로드할 수 있다.

### 15-3. 해시 계산 규칙

```sql
-- hash_key: 비즈니스 키를 정규화한 뒤 해시 (대소문자, 공백 차이로 다른 해시가 나오지 않게)
md5(UPPER(TRIM(CAST(customer_id AS STRING)))) AS customer_hk

-- hash_diff: 설명 속성을 구분자로 연결 후 해시, NULL은 대체 값으로
md5(concat_ws('||',
    COALESCE(name, '^^'), COALESCE(address, '^^'), COALESCE(phone, '^^'))) AS hash_diff

-- Link의 복합 해시: 부모 해시 키들을 연결 후 해시
md5(concat_ws('||', customer_hk, order_hk)) AS customer_order_hk
```

- **구분자(`||`)는 필수**다. 없으면 `'12' + '3'`과 `'1' + '23'`이 모두 `'123'`이 되어 다른 속성 조합이 같은 해시로 처리된다.
- **NULL 처리**: `concat`은 인자 중 하나라도 NULL이면 결과 전체가 NULL이 된다. `concat_ws`는 NULL을 건너뛰지만, 그러면 NULL 위치가 달라도 같은 문자열이 될 수 있다. `COALESCE`로 대체 값을 넣는 것이 안전하다.
- 해시 함수로 MD5 대신 SHA-1, SHA-256(`sha2(x, 256)`)을 쓰기도 한다. 충돌 가능성은 더 낮고 저장 공간은 더 크다.

### 15-4. 로드 패턴

| 대상 | 패턴 | 결과 |
|---|---|---|
| Hub | 새 hash_key만 삽입 (`MERGE ... WHEN NOT MATCHED THEN INSERT`) | 이미 있는 키는 건너뜀 (멱등) |
| Link | 새 composite_hash_key만 삽입 | 멱등 |
| Satellite | (hash_key, hash_diff) 조합이 새로울 때만 삽입 | 속성이 같으면 건너뜀, 다르면 새 행 추가. 이전 행은 그대로 |

Satellite 변경 감지는 속성이 몇 개든 **hash_diff 컬럼 하나만 비교**하므로, 테이블이 넓어져도 로직과 비용이 일정하다.

> 엄밀한 Satellite 로드는 들어온 hash_diff를 **해당 키의 최신 Satellite 행**의 hash_diff와 비교한다. 값이 A → B → A로 되돌아가는 경우, "어느 버전에든 같은 hash_diff가 있으면 건너뛰기" 방식은 마지막 A 변경을 놓친다.

---

## 16. Data Vault 아키텍처와 구현 예제

### 16-1. 계층 구조

소스 → 수집 → 스테이징 → **Raw Vault (실버)** → **Business Vault (골드)** → **Information Mart (골드)** → 소비자

| 계층 | 역할 | 구성 |
|---|---|---|
| **Raw Vault** (실버) | 감사 계층. 소스가 보낸 그대로 기록. 비즈니스 규칙 없음 | Hub, Link, Satellite (병렬 로드) |
| **Business Vault** (골드) | Raw Vault를 수정하지 않고 위에 비즈니스 규칙 적용 | **PIT 테이블**, **Bridge 테이블**, 계산된 Satellite |
| **Information Mart** (골드) | BI 소비용 뷰 | 보통 Kimball 스타 스키마 또는 평면 비정규화 뷰 |

| Business Vault 구성 요소 | 설명 |
|---|---|
| **PIT (Point-In-Time) 테이블** | 특정 시점마다 각 Satellite의 유효 행(load_ts)을 미리 계산해 둔 테이블. 여러 Satellite를 시점 기준으로 조인할 때 비싼 범위 조인을 피함 |
| **Bridge 테이블** | 여러 Link를 거치는 경로를 미리 조인해 둔 테이블 |
| **계산된 Satellite** | 고객 생애 가치처럼 파생된 메트릭을 담는 Satellite |

Information Mart에서 차원은 "Hub + 최신 Satellite 행", 팩트는 "Link + 해당 Satellite"로 구성한다.

### 16-2. Delta와 잘 맞는 이유

- **ACID**: 별도의 Spark 작업이 Hub, Link, Satellite를 동시에 써도 부분 쓰기가 노출되지 않는다.
- **`MERGE ... WHEN NOT MATCHED THEN INSERT`**: 멱등한 Hub, Link 로드 패턴.
- **추가(append) 시맨틱**: 삽입 전용 Satellite와 일대일로 대응.
- **두 개의 감사 시간축**: Delta 시간 여행은 파일이 기록된 시점을, Satellite의 `load_ts`는 소스가 보고한 시점을 보여 준다.
- **스키마 진화**: `ALTER TABLE ADD COLUMNS`나 `mergeSchema`로 새 속성을 Satellite에 추가.
- 선언적 파이프라인에서는 세 테이블 유형 사이에 의존성이 없으므로 하나의 DAG에서 병렬로 로드된다.

### 16-3. Raw Vault DDL

```sql
CREATE TABLE silver.H_Customer (
  customer_hk    STRING NOT NULL COMMENT 'MD5(customer_id)',
  customer_id    INT    NOT NULL,
  load_timestamp TIMESTAMP NOT NULL,
  record_source  STRING,
  CONSTRAINT pk_h_customer PRIMARY KEY (customer_hk)
);

CREATE TABLE silver.L_Customer_Order (
  customer_order_hk STRING NOT NULL COMMENT 'MD5(customer_hk || order_hk)',
  customer_hk       STRING NOT NULL,
  order_hk          STRING NOT NULL,
  load_timestamp    TIMESTAMP NOT NULL,
  record_source     STRING,
  CONSTRAINT pk_l_customer_order PRIMARY KEY (customer_order_hk)
);

CREATE TABLE silver.S_Customer (
  customer_hk    STRING NOT NULL,
  hash_diff      STRING NOT NULL COMMENT '모든 설명 컬럼의 MD5',
  name           STRING,
  address        STRING,
  phone          STRING,
  acct_bal       DECIMAL(12,2),
  market_segment STRING,
  load_timestamp TIMESTAMP NOT NULL,
  record_source  STRING,
  CONSTRAINT pk_s_customer PRIMARY KEY (customer_hk, load_timestamp)  -- 버전 여러 개 저장
);
```

### 16-4. 로드

```sql
-- Hub: 새 비즈니스 키만
MERGE INTO silver.H_Customer AS t
USING (
  SELECT md5(CAST(customer_id AS STRING)) AS customer_hk, customer_id,
         current_timestamp() AS load_timestamp, 'CRM' AS record_source
  FROM silver.refined_customer
) AS s
ON t.customer_hk = s.customer_hk
WHEN NOT MATCHED THEN INSERT *;

-- Satellite: 속성이 바뀐 경우만 (삽입 전용)
MERGE INTO silver.S_Customer AS t
USING (
  SELECT md5(CAST(customer_id AS STRING)) AS customer_hk,
         md5(concat_ws('||', name, address, phone, CAST(acct_bal AS STRING), market_segment)) AS hash_diff,
         name, address, phone, acct_bal, market_segment,
         current_timestamp() AS load_timestamp, 'CRM' AS record_source
  FROM silver.refined_customer
) AS s
ON t.customer_hk = s.customer_hk AND t.hash_diff = s.hash_diff
WHEN NOT MATCHED THEN INSERT *;

-- Link: 새 고객-주문 쌍만
MERGE INTO silver.L_Customer_Order AS t
USING (
  SELECT md5(concat_ws('||', md5(CAST(customer_id AS STRING)), md5(CAST(order_id AS STRING)))) AS customer_order_hk,
         md5(CAST(customer_id AS STRING)) AS customer_hk,
         md5(CAST(order_id AS STRING))    AS order_hk,
         current_timestamp() AS load_timestamp, 'OMS' AS record_source
  FROM silver.refined_orders
) AS s
ON t.customer_order_hk = s.customer_order_hk
WHEN NOT MATCHED THEN INSERT *;
```

`MERGE` 대신 `INSERT INTO ... SELECT ... WHERE NOT EXISTS (...)`(안티 조인) 형태로 쓰면 더 가벼운 append 작업이 된다.

### 16-5. 골드의 비즈니스 뷰

분석가가 해시 키를 의식하지 않도록 볼트를 조인해 비즈니스 컬럼만 노출한다. Satellite는 키별 **최신 행**만 고른다.

```sql
CREATE OR REPLACE VIEW gold.BV_Customer_Order AS
WITH sc AS (
  SELECT * FROM silver.S_Customer
  QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_hk ORDER BY load_timestamp DESC) = 1
),
so AS (
  SELECT * FROM silver.S_Order
  QUALIFY ROW_NUMBER() OVER (PARTITION BY order_hk ORDER BY load_timestamp DESC) = 1
)
SELECT hc.customer_id,
       sc.name           AS customer_name,
       sc.market_segment AS customer_segment,
       ho.order_id,
       so.order_date,
       so.total_price,
       so.order_status
FROM silver.H_Customer       hc
JOIN sc                          ON hc.customer_hk = sc.customer_hk
JOIN silver.L_Customer_Order lco ON hc.customer_hk = lco.customer_hk
JOIN silver.H_Order          ho  ON lco.order_hk   = ho.order_hk
JOIN so                          ON ho.order_hk    = so.order_hk;
```

내부적으로는 5방향 조인이지만 분석가는 `GROUP BY customer_name`만 쓰면 된다. 감사자를 위한 골드 테이블이라면 `latest_load_ts`, `record_source` 같은 볼트 메타데이터를 함께 남기기도 한다.

### 16-6. Data Vault가 맞는 경우와 맞지 않는 경우

| 적합 | 부적합 |
|---|---|
| 규제 산업: 금융(BCBS 239), 의료(HIPAA), 보험(Solvency II), 정부 | 소규모 팀, 단일 소스 |
| 변동성 높은 소스가 많음 (소스별 Satellite로 흡수) | 감사가 필요 없는 BI 전용 |
| 도메인별 병렬 팀 운영 | 이력 관리가 필요 없음 |
| "언제 무엇을 알고 있었는가?" 질의 | |

- 오버헤드: 테이블이 많고, 쿼리 조인이 많으며, 학습 곡선이 가파르다.
- 경험칙: **규제 감사, 변동성 높은 다중 소스, 대규모 병렬 팀 납품 중 최소 두 가지**에 해당할 때 투자 가치가 있다.

---

## 17. 현대적 골드 레이어의 세 기둥

실버에서 통합된 데이터는 골드에서 세 가지 소비 경로로 나뉜다. 모두 Unity Catalog가 거버넌스, 리니지, 접근 제어를 맡는다.

| 기둥 | Databricks 요소 | 소비자 | 해결하는 문제 |
|---|---|---|---|
| ML | **특성 저장소 (Feature Store)** | ML 모델, 서빙 엔드포인트 | 학습/서빙 스큐 |
| 분석/BI | **메트릭 뷰 (Metric View)** | 대시보드, Genie, 분석가 | 보고서 간 지표 불일치 |
| 협업 | **OpenSharing (Delta Sharing)** | 파트너, 외부 팀, 규제 기관 | 오래된 내보내기, 통제 불가능한 접근 |

### 17-1. 특성 저장소

**학습/서빙 스큐**: 같은 특성(예: `avg_order_value`)을 학습 노트북과 서빙 레이어에서 따로 구현하면 null 처리 같은 작은 차이로 분포가 어긋나고, 모델 성능이 **조용히** 떨어진다.

| 특성 저장소 없이 | 특성 저장소 사용 |
|---|---|
| 특성 로직이 노트북, 학습 작업, 서빙에 각각 재구현됨 | 하나의 정의가 학습, 배치, 실시간 서빙에 모두 적용 |
| 학습과 서빙 간 드리프트 | 구조적으로 학습 = 서빙 보장 |
| 팀 간 재사용 불가 | UC의 일급 자산으로 탐색, 재사용 |

- **특성 테이블** = 기본 키가 선언된 UC 관리 Delta 테이블. 보통 엔티티당 한 행의 넓은 비정규화 테이블이다.
- 흐름: 실버 → 특성 엔지니어링 → 특성 테이블(골드) → 학습 / 배치 추론 / 온라인 스토어(실시간 서빙)

| API (`FeatureEngineeringClient`) | 역할 |
|---|---|
| `create_table(name, primary_keys, df, timestamp_keys=...)` | 특성 테이블 생성. `timestamp_keys`로 시점 정확 조인(point-in-time) 지원 |
| `write_table(name, df, mode='merge')` | 데이터 쓰기 (`merge`는 증분 업데이트) |
| `register_table(...)` | 기존 UC 테이블을 데이터 재작성 없이 특성 테이블로 선언 |
| `FeatureLookup(table_name, feature_names, lookup_key)` | 키로 특성을 조회하는 정의 |
| `create_training_set(df, feature_lookups, label)` | 레이블 DataFrame에 특성을 조인한 학습 세트 |
| `log_model(...)` | 특성 조회 정보를 모델 메타데이터로 기록 → 서빙 시 자동으로 최신 특성 조회 |

레거시 `FeatureStoreClient`는 더 이상 쓰지 않는다. UC에서는 기본 키가 있는 Delta/Iceberg 테이블이면 특성 테이블로 쓸 수 있다.

```python
from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

fe = FeatureEngineeringClient()
fe.create_table(name="main.gold.customer_features", primary_keys=["customer_id"],
                df=features_df, description="고객 단위 특성")

training_set = fe.create_training_set(
    df=labels_df,
    feature_lookups=[FeatureLookup(table_name="main.gold.customer_features",
                                   feature_names=["total_orders", "avg_order_value"],
                                   lookup_key="customer_id")],
    label="label")
```

| 특성 저장소가 적합 | 일반 골드 테이블로 충분 |
|---|---|
| 여러 모델이 같은 파생 데이터를 재사용 | 단일 일별 배치 모델 |
| 저지연 실시간 추론 필요 | 실시간 서빙 요구 없음 |
| 학습/서빙 동등성 감사 필요 | 팀 간 재사용 없음 |
| 팀 간 특성 검색이 중요 | |

골드 테이블로 시작하고, 두 번째 소비자나 실시간 서빙이 필요해지면 `register_table()`로 전환하면 된다.

### 17-2. 메트릭 뷰

**메트릭 드리프트**: 두 팀이 같은 `gold.fact_orders`로 "국가별 매출"을 만들었는데, 재무팀은 완료 주문을 제외하고 세일즈팀은 필터 없이 계산해 숫자가 다르다. 각 대시보드는 그 자체로는 맞아 보여서 나란히 놓기 전까지 드러나지 않는다.

메트릭 뷰는 메트릭을 **YAML로 한 번 정의**하는 UC 객체다.

```sql
CREATE OR REPLACE VIEW gold.revenue_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
source: gold.fact_orders
filter: order_status = 'O'
dimensions:
  - name: nation_name
    expr: nation_name
  - name: order_month
    expr: DATE_TRUNC('MONTH', order_date)
measures:
  - name: revenue
    expr: SUM(total_price)
  - name: order_count
    expr: COUNT(1)
$$;
```

```sql
-- 측정값은 MEASURE()로 조회
SELECT nation_name, MEASURE(revenue) AS revenue
FROM gold.revenue_metrics
GROUP BY nation_name;
```

| 구성 요소 | 의미 |
|---|---|
| `source` | 메트릭을 계산할 테이블, 뷰, 또는 SQL 쿼리. 리니지 포인터 |
| `filter` | 모든 쿼리에 적용되는 영구 행 필터. 소비자가 재정의할 수 없음 |
| `dimensions` | 유효한 분할 기준. 원시 컬럼이나 파생 식. `display_name`, `synonyms`로 Genie 자연어 매칭 개선 |
| `measures` | 이름 붙은 집계식. 소비자가 임의의 집계를 만들 수 없음 |

- 정의를 바꾸면 모든 대시보드, 노트북, Genie, 알림에 즉시 반영된다.
- 측정값이 다른 측정값을 참조할 수 있어(조합 가능성), "고객당 매출"을 "매출"과 "고객 수"로부터 도출할 수 있다.
- 자주 쓰는 메트릭은 구체화(materialization)해 증분 갱신하고, 쿼리를 자동으로 재작성하게 할 수 있다.
- 생성 조건: SQL Warehouse 또는 Databricks Runtime 17.3 이상, 소스 테이블 `SELECT`, 대상 스키마 `CREATE TABLE`과 `USE SCHEMA`.

| 메트릭 뷰가 적합 | 골드 뷰/테이블로 충분 |
|---|---|
| 여러 대시보드가 같은 KPI를 보고 | 단일 대시보드, 단일 작성자 |
| 비즈니스 사용자가 Genie로 질의 | 일회성 분석 |
| 메트릭 정의에 거버넌스와 버전 관리 필요 | 팀 간 공유 없음 |
| 재무, 규정 준수에서 일관된 계산 요구 | |

### 17-3. OpenSharing (Delta Sharing)

| 내보내기 방식 (CSV, SFTP, DB 복제) | OpenSharing |
|---|---|
| 보내는 순간부터 데이터가 오래됨 | 수신자가 실시간 데이터를 읽음 |
| 내보낸 뒤 접근 제어 불가 | 제공자가 언제든 접근 철회 가능 |
| 리니지, 감사 추적 없음 | UC에 전체 감사 추적 |

- 쿼리 시점에 클라우드 스토리지 Parquet 파일에 대한 **단기 사전 서명 URL**을 발급한다. 데이터는 수신자 compute로 일시적으로만 전달되고 영구 복사본이 생기지 않는다.
- Linux Foundation이 관리하는 개방형 프로토콜이라 수신자가 Databricks가 아니어도 된다(Python, Spark, pandas, Power BI, Tableau 등 클라이언트).
- **Databricks 간 공유**는 수신자의 메타스토어 ID(`cloud:region:uuid`)로 연결하며, 공유가 읽기 전용 카탈로그로 마운트된다. 자격 증명 파일을 주고받지 않는다.
- 같은 클라우드 리전 안의 공유는 전송 비용이 없고, 리전 간이나 클라우드 간에는 표준 전송 요금이 붙을 수 있다.

```sql
CREATE SHARE partner_share;
ALTER SHARE partner_share ADD TABLE gold.mart_revenue_by_nation;
CREATE RECIPIENT acme_corp;
GRANT SELECT ON SHARE partner_share TO RECIPIENT acme_corp;
```

| `ALTER SHARE` 옵션 | 효과 |
|---|---|
| `WITH HISTORY` | Delta 이력까지 공유 → 수신자가 시간 여행과 스트리밍 읽기 가능 |
| `PARTITION (col = value) AS alias` | 일부 파티션만 다른 이름으로 공유 |
| `ADD SCHEMA ...` | 스키마의 현재, 미래 테이블, 뷰, 볼륨을 자동 포함 |
| `CURRENT_RECIPIENT()` | 수신자 속성으로 행 범위 제한 (수신자마다 Share를 따로 만들 필요 없음) |

- Share 소유자는 포함된 모든 테이블에 `SELECT` 권한을 유지해야 한다.
- 공유할 수 있는 자산: 테이블, 스트리밍 테이블, 뷰, Materialized View, 볼륨, Python UDF, 노트북, AI 모델.
- **모델링 시 주의**: Share는 테이블을 있는 그대로 노출한다. 호환되지 않는 스키마 변경은 수신자를 깨뜨린다. 공유할 골드 테이블은 **안정적인 스키마, 명확한 컬럼명, 일관된 단위, 적절한 집계 수준**으로 설계한다.

| OpenSharing이 적합 | 직접 권한 부여로 충분 |
|---|---|
| 외부 파트너가 큐레이션된 데이터 필요 | 모든 소비자가 같은 UC 메타스토어에 있음 |
| 다른 메타스토어의 내부 팀 | GRANT와 뷰로 충분히 제어 가능 |
| 규제 보고에 감사 가능한 접근 필요 | 조직 간 공유 요건 없음 |
| 데이터 제품을 광범위하게 게시 | |

### 17-4. 기둥별 모델링 영향

| 기둥 | 골드 아티팩트 | 모델링 형태 |
|---|---|---|
| 특성 저장소 | 특성 테이블 (Delta + 메타데이터) | 엔티티 키를 가진 넓은 비정규화 테이블 |
| 메트릭 뷰 | 메트릭 뷰 (UC 내 YAML) | 명확히 명명된 측정값과 차원을 가진 스타 스키마. 복잡한 조인은 YAML이 아니라 골드 테이블에서 처리 |
| OpenSharing | Share + Recipient | 안정적인 스키마, 명확한 컬럼명, 적절한 집계 |

하나의 골드 테이블이 특성 테이블로 등록되고, 메트릭 뷰로 감싸지고, Share에 추가될 수 있다. 어떤 기둥을 쓸지는 데이터가 아니라 **소비 패턴**이 결정한다.

---

## 18. 모델링 접근 방식 결합

### 18-1. 하나의 메달리온에 함께 놓기

| 계층 | 구성 |
|---|---|
| 브론즈 | 원시 수집. 동일한 브론즈가 3NF 경로와 Raw Vault 경로 모두에 공급 (실버 방식을 바꿔도 재수집 불필요) |
| 실버 | 3NF 통합 (Inmon) 그리고/또는 Raw Vault (Data Vault) |
| 골드 | Business Vault → 스타 스키마 (Kimball) ← 3NF. 스타 스키마가 특성 저장소, 메트릭 뷰, OpenSharing에 공급 |

모든 아티팩트가 UC 관리 테이블이므로 리니지, 권한, 검색이 Hub든 차원이든 팩트든 똑같이 작동한다. 소비자에게는 실버의 모델링 방식이 보이지 않는다.

### 18-2. 세 가지 일반적인 패턴

| 패턴 | 구성 | 적합한 경우 | 트레이드오프 |
|---|---|---|---|
| **패턴 1** | 3NF 실버 → Kimball 골드 | 엔터프라이즈 통합이 중요하고 BI가 주 소비자인 잘 이해된 도메인 (리테일, 물류, HR) | 사전 모델링, 마트 구축 전 팀 간 엔티티 정의 합의 필요 |
| **패턴 2** | Data Vault 실버 → (Business Vault) → Kimball 골드 | 규제가 많고, 다중 소스이고, 감사가 많은 환경 (금융, 보험) | 구축, 유지할 아티팩트 증가 (Hub, Link, Satellite, 해시 키) |
| **패턴 3** | 실버에서 3NF + Data Vault를 도메인별로 혼용 | 도메인마다 요구가 다른 대규모 조직 (안정적인 제품 카탈로그는 3NF, 변동성 큰 텔레메트리는 Vault) | 두 실버 스타일에 걸친 명명 규칙과 거버넌스 표준 합의 필요 |

업계별 출발점: 리테일과 소비재는 패턴 1, 금융과 보험은 패턴 2, 수십 개 병원 시스템이 서로 다른 HL7/FHIR 스키마를 보내는 헬스케어는 Data Vault Satellite가 유리하다.

### 18-3. 선택 매트릭스

| 질문 | 예라면 |
|---|---|
| 작고 안정적이며 잘 이해된 도메인인가? | 3NF 실버 + Kimball 골드 (패턴 1) |
| 전사적 통합이 최우선인가? | 단일 진실 소스로서의 3NF 실버 (패턴 1) |
| 규제 대상, 다중 소스, 감사가 많은가? | Data Vault 실버 + Kimball 골드 (패턴 2) |
| 소스 시스템이 자주 바뀌는가? | Data Vault (새 Satellite로 흡수) (패턴 2) |
| 일부 도메인은 단순하고 일부는 복잡한가? | 도메인별 3NF + Data Vault 혼용 (패턴 3) |
| 팀마다 이력과 감사 요구 수준이 다른가? | 각 팀이 도메인에 맞는 방식 선택 (패턴 3) |
| 통합보다 빠른 BI 제공이 중요한가? | 브론즈에서 바로 Kimball 골드 (실버 통합 생략. 범위가 명확한 소규모 도메인에만 적합, 공유 표준 엔티티 레이어를 잃음) |

- 스키마에 `modeling_approach = data_vault` / `3nf` 같은 태그를 달면 두 방식에 걸쳐 검색과 영향 분석이 쉬워진다.

### 18-4. 두 경로, 같은 답

같은 브론즈에서 두 실버를 만들고 각각 골드를 도출해 "국가별 총 계좌 잔액"을 비교하면 결과가 같다.

```sql
-- 경로 1: 3NF 실버 → Kimball 골드 (비즈니스 키 조인)
CREATE OR REPLACE TABLE gold.kimball_revenue_by_nation AS
SELECT n.nation_name, r.region_name,
       COUNT(*) AS customer_count, SUM(c.acct_bal) AS total_acct_bal
FROM silver.inmon_customer c
JOIN silver.inmon_nation n ON c.nation_key = n.nation_key
JOIN silver.inmon_region r ON n.region_key = r.region_key
GROUP BY n.nation_name, r.region_name;

-- 경로 2: Raw Vault 실버 → 골드 (해시 키 조인, 볼트 메타데이터 유지)
CREATE OR REPLACE TABLE gold.vault_revenue_by_nation AS
SELECT hn.nation_hk, sn.nation_name, r.r_name AS region_name,
       COUNT(*) AS customer_count, SUM(sc.acct_bal) AS total_acct_bal,
       MAX(sc.load_ts) AS latest_load_ts, 'TPC-H' AS record_source
FROM silver.vault_S_Customer sc
JOIN silver.vault_H_Customer hc ON sc.customer_hk = hc.customer_hk
JOIN silver.vault_H_Nation   hn ON sc.nation_key  = hn.nation_key
JOIN silver.vault_S_Nation   sn ON hn.nation_hk   = sn.nation_hk
JOIN bronze.region           r  ON sn.region_key  = r.r_regionkey
GROUP BY hn.nation_hk, sn.nation_name, r.r_name;
```

| 비교 | 경로 1 (3NF) | 경로 2 (Vault) |
|---|---|---|
| 조인 키 | 읽기 쉬운 비즈니스 키 | MD5 해시 키 |
| 골드 컬럼 | 비즈니스 컬럼 4개 | 비즈니스 컬럼 + `nation_hk`, `latest_load_ts`, `record_source` |
| 결과 값 | 동일 | 동일 |

방식 선택은 결과의 정확성이 아니라 **감사, 속도, 유연성** 같은 팀의 요구로 결정한다. 리니지 그래프에서는 두 경로가 각각 추적된다.

---

## 19. 데이터 제품

### 19-1. 데이터를 제품으로 취급해야 하는 이유

잘 모델링된 테이블도 아무도 찾지 못하고, 신뢰하지 못하고, 소유자를 모르면 쓸모가 없다. 새 분석가는 모델링으로 답할 수 없는 질문을 만난다: 어떤 테이블이 표준인가, 소유자는 누구인가, 언제 갱신되었나, 반품이 포함되었나, 이 컬럼은 무슨 뜻인가. 답을 못 얻으면 작성자에게 묻거나(병목), 잘못 쓰거나(오류), 자기 복사본을 만든다(중복).

**데이터를 제품으로 취급**한다는 것은 데이터셋을 만드는 팀이 검색 가능성, 문서화, 신뢰성, 접근 제어까지 책임지는 것이다. 소비자는 별도 미팅 없이 데이터를 쓸 수 있어야 한다.

### 19-2. 중앙 웨어하우스 vs Data Mesh

| 중앙 웨어하우스 | Data Mesh |
|---|---|
| 중앙 팀이 모든 데이터 소유 | 도메인 팀이 자기 데이터 소유 |
| 하나의 모델로 모든 것 처리 | 통일된 인터페이스를 가진 도메인별 모델 |
| 파이프라인 = 배관 작업 | 파이프라인 = 제품의 일부 |
| 거버넌스는 나중에 | 처음부터 거버넌스 |

Data Mesh의 네 가지 원칙과 UC 대응:

| 원칙 | Unity Catalog 대응 |
|---|---|
| 도메인 소유권 | 3단계 네임스페이스 + `ALTER ... OWNER TO <그룹>` |
| 제품으로서의 데이터 | 태그, `COMMENT ON`, 자동 리니지, AI 생성 설명 |
| 셀프 서비스 플랫폼 | Catalog Explorer, UC REST API, OpenSharing, 모든 도구에서 같은 이름 |
| 연합 거버넌스 | 중앙 접근 제어. 한 번 부여한 권한이 모든 워크스페이스, 엔진에 일관 적용 |

> Data Mesh는 조직 모델이고 데이터 제품은 그 결과물이다. 완전한 Data Mesh를 도입하지 않아도 어떤 팀이든 기존 테이블에 소유권, 문서, SLA를 더해 제품으로 만들 수 있다.

### 19-3. DATSIS: 데이터 제품의 여섯 가지 속성

| 속성 | 해결하는 문제 | 기준 | Unity Catalog 기능 |
|---|---|---|---|
| **D**iscoverable (검색 가능) | 아무도 찾을 수 없다 | 카탈로그에 등록, 도메인, 갱신 주기, 소유자 태그 | 태그, 검색, AI 생성 설명 |
| **A**ddressable (주소 지정 가능) | 경로가 계속 바뀐다 | 하나의 안정적인 이름. 스토리지가 이동해도 불변 | `catalog.schema.table` |
| **T**rustworthy (신뢰 가능) | 최신인지, 정확한지 모른다 | 최신성/가용성 SLA, 쓰기 시점 품질 검사, 리니지, 감사 추적 | 리니지, 파이프라인 expectation, 시간 여행, 시스템 테이블(`system.access.audit`), 데이터 프로파일링 |
| **S**elf-describing (자기 설명적) | 컬럼 의미를 작성자에게 물어야 한다 | 모든 컬럼 설명, 테이블 설명에 그레인, 소유자, 갱신 주기, 샘플 미리보기 | `COMMENT ON TABLE/COLUMN`, AI 생성 설명 |
| **I**nteroperable (상호 운용) | 도메인 간 키가 맞지 않는다 | 합의된 키 규칙으로 조인. PK/FK로 조인 경로 노출 | 정보성 PK/FK, OpenSharing |
| **S**ecure (보안) | 도구마다 접근 권한이 다르다 | 한 곳에서 테이블, 행, 컬럼 수준 접근 관리. 민감 필드 마스킹 | GRANT, 행 필터, 컬럼 마스크, 단일 감사 로그 |

### 19-4. 테이블을 데이터 제품으로 만드는 여섯 단계

```sql
-- D: 검색 가능 (태그)
ALTER TABLE gold.kimball_revenue_by_nation
SET TAGS ('domain' = 'finance', 'refresh' = 'daily', 'owner' = 'finance-data-team');

-- A: 주소 지정 가능 → catalog.gold.kimball_revenue_by_nation 이름이 이미 어디서나 동작

-- T: 신뢰 가능 → expectation으로 쓰기 시점 품질, 리니지, 시간 여행, 감사 로그
SELECT * FROM system.access.audit WHERE action_name = 'getTable';

-- S: 자기 설명적 (그레인, 소유자, SLA를 주석에)
COMMENT ON TABLE gold.kimball_revenue_by_nation
IS 'Revenue by nation. Grain: 1 row/nation. Owner: finance-data-team. SLA: 06:00 UTC.';
COMMENT ON COLUMN gold.kimball_revenue_by_nation.total_acct_bal IS '국가별 계좌 잔액 합계 (USD)';

-- I: 상호 운용 (조인 경로 선언)
ALTER TABLE gold.kimball_revenue_by_nation ALTER COLUMN nation_name SET NOT NULL;
ALTER TABLE gold.kimball_revenue_by_nation ADD CONSTRAINT pk_nation PRIMARY KEY (nation_name);

-- S: 보안 (접근 권한, 필요하면 행 필터와 컬럼 마스크)
GRANT SELECT ON TABLE gold.kimball_revenue_by_nation TO `finance_analysts`;

-- 소유권은 개인이 아니라 그룹에. 스키마에 설정하면 도메인 단위 관리
ALTER SCHEMA gold OWNER TO `finance-data-team`;
```

- 태그는 객체당 최대 50개이며, 키는 대소문자를 구분한다. `information_schema.table_tags`, `column_tags`로 조회할 수 있다.
- **데이터 프로파일링**(품질 모니터)은 행 수, null 비율, 값 분포, 스키마 드리프트를 정기적으로 계산해 신뢰성을 유지한다. 모니터 유형은 스냅샷, 시계열, 추론(ML 모델 성능) 세 가지다.
- 외부 소비자에게는 OpenSharing으로 같은 데이터 제품 계약을 확장한다.
- 3NF 엔티티, Vault, 스타 스키마, 특성 테이블, 메트릭 뷰, Share 등 **모든 아티팩트가 데이터 제품 후보**다. 테이블은 이미 있고, 제품은 그 위에 더하는 것이다.
- **작게 시작한다**: 도메인 하나, 테이블 하나를 골라 소유권, 문서, 리니지, SLA를 더하고 확장한다.

---

## 20. 종합 시나리오: 하나의 소스, 네 팀의 요구

리테일 기업에서 네 팀이 같은 원시 데이터를 공유하며 서로 다른 것을 요구한다.

| 팀 | 요구 | 해법 | 위치 |
|---|---|---|---|
| 재무 | 무결성이 보장된 통합 Customer / Orders / Lineitem | Inmon 3NF EDW (PK/FK 선언) | 실버 |
| 규정 준수 | "언제 무엇을 알았는지" 증명할 수 있는 완전한 감사 이력 | Raw Vault (Hub, Link, Satellite). EDW와 나란히 | 실버 |
| BI | 세그먼트, 국가, 시간으로 자를 수 있는 스타 스키마, 세그먼트 변경 이력 보존 | Kimball 스타 스키마 (DimCustomer는 SCD2). **브론즈가 아니라 EDW에서** 도출 | 골드 |
| 데이터 사이언스 | BI가 이미 신뢰하는 숫자와 같은 고객 특성 | 특성 테이블. **스타 스키마에서** 집계 | 골드 |

핵심은 각 단계가 앞 단계를 소비한다는 점이다. EDW → 스타 스키마 → 특성 테이블로 이어지므로 BI와 ML이 **구조적으로 같은 숫자**를 공유한다.

```python
# 스타 스키마에서 특성 집계 (BI가 보는 숫자와 동일)
features_df = (spark.table("main.gold.FactOrders").alias("f")
    .join(spark.table("main.gold.DimCustomer").alias("dc"), "dim_customer_key")
    .where("dc.is_current")
    .groupBy("dc.customer_id", "dc.market_segment")
    .agg(F.count("*").alias("total_orders"),
         F.avg("f.total_price").alias("avg_order_value"),
         F.sum("f.total_price").alias("total_spending")))

fe.create_table(name="main.gold.customer_features", primary_keys=["customer_id"], df=features_df)
```

마지막으로 모든 아티팩트를 데이터 제품으로 게시한다.

```sql
COMMENT ON TABLE silver.edw_customer IS 'Integrated 3NF customer entity. Published by Finance.';
ALTER TABLE silver.edw_customer SET TAGS ('domain'='finance', 'data_product'='customer_master', 'layer'='silver');

ALTER TABLE gold.FactOrders SET TAGS ('domain'='sales', 'data_product'='orders_star', 'layer'='gold');
ALTER TABLE gold.customer_features SET TAGS ('domain'='data_science', 'data_product'='customer_features', 'layer'='gold');

-- 태그로 데이터 제품 검색
SELECT catalog_name, schema_name, table_name, tag_value
FROM system.information_schema.table_tags
WHERE tag_name = 'data_product';
```

어떤 모델링을 선택하든 세 가지는 변하지 않는다: **저장 형식(Delta)**, **거버넌스 플레인(Unity Catalog)**, **의도적으로 모델링된 하나의 테이블이 BI, ML, 애플리케이션을 함께 지원한다는 목표.**

---

## 21. 핵심 정리 치트시트

### 계층과 접근 방식

| 키워드 | 핵심 |
|---|---|
| 브론즈 | 원시, 추가 전용, 모델링 없음 |
| 실버 | 통합 지향: 3NF (Inmon), Raw Vault (Data Vault) |
| 골드 | 소비 지향: 스타 스키마 (Kimball), Business Vault, 특성 테이블, 메트릭 뷰 |
| OLTP / OLAP | 기록, 행 저장, 3NF / 분석, 열 저장, 차원형. Delta는 OLAP |
| Inmon 4속성 | 주제 지향, 통합, 시간 가변, 비휘발 |
| 선택 요인 | 감사 부하, 소스 변동성 → Data Vault. BI 우선, 안정 소스 → Kimball. 그 외 → 혼합 |
| CDM → LDM → PDM | 비즈니스(엔티티, 관계) → 아키텍트(키, 타입, 카디널리티) → 엔지니어(DDL) |

### Inmon

| 키워드 | 핵심 |
|---|---|
| CIF | 소스 → 스테이징 → 3NF EDW → 종속 마트. ODS는 EDW 우회 |
| 1NF / 2NF / 3NF | 원자 값 / 부분 종속 제거 / 이행 종속 제거 |
| ERM | 엔티티 = 테이블, 속성 = 컬럼, 관계 = FK |
| Delta 제약 | PK, FK = 정보성 / NOT NULL, CHECK = 적용 |
| RELY | 옵티마이저에 무결성 보장을 알림 (조인 제거). 실제 무결성이 없으면 결과 오류 |

### Kimball

| 키워드 | 핵심 |
|---|---|
| 그레인 | 팩트 한 행의 의미. 먼저 선언, 섞지 않기 |
| 팩트 4유형 | 트랜잭션, 주기적 스냅샷, 누적 스냅샷(유일하게 UPDATE), 팩트리스 |
| 가산성 | 가산 / 반가산(잔액, 시간축 합 불가) / 비가산(비율) |
| 대리 키 | `GENERATED ALWAYS AS IDENTITY`, 버전마다 새 값 |
| SCD | 0 무시, 1 덮어쓰기, 2 새 행, 3 이전 값 컬럼. 속성 단위로 선택 |
| SCD2 MERGE | OLD + NEW 쌍둥이 행, 소스 중복 제거 필수, 멱등성 장치 |
| 팩트 로드 | 새 팩트는 `is_current = TRUE`, 조회는 대리 키로 |
| 특수 차원 | 일치된 차원(드릴 어크로스), 역할 수행, 정크 |
| 스타 vs 스노우플레이크 | Delta에서는 스타 권장 (열 저장, 딕셔너리 인코딩, 브로드캐스트 조인) |
| 팩트 최적화 | Liquid 클러스터링 권장, Z-ORDER, 데이터 스키핑 |

### Data Vault

| 키워드 | 핵심 |
|---|---|
| Hub | 비즈니스 키, 엔티티당 1행 |
| Link | Hub 간 관계, 부모 키 조합당 1행, 종료는 Effectivity Satellite |
| Satellite | 설명 속성 + 이력, 삽입 전용, PK = (hash_key, load_ts), 소스별 분리 |
| hash_key / hash_diff | 비즈니스 키 해시(식별) / 속성 해시(변경 감지) |
| 해시 규칙 | 정규화(UPPER, TRIM), 구분자 `\|\|`, NULL은 COALESCE |
| 병렬 로드 | 키를 생성이 아닌 파생 → 순서 의존성 없음 |
| 계층 | Raw Vault(실버, 감사) → Business Vault(골드, PIT, Bridge, 계산 Satellite) → Information Mart(골드, 스타) |

### 골드와 데이터 제품

| 키워드 | 핵심 |
|---|---|
| 특성 저장소 | 학습/서빙 스큐 방지, PK 있는 UC 테이블, `FeatureEngineeringClient`, `FeatureLookup` |
| 메트릭 뷰 | 메트릭 드리프트 방지, `WITH METRICS LANGUAGE YAML`, source/filter/dimensions/measures, `MEASURE()` |
| OpenSharing | 복사 없는 실시간 공유, 단기 사전 서명 URL, Share/Recipient/Provider, `WITH HISTORY` |
| 결합 패턴 | 1: 3NF → Kimball, 2: Vault → Kimball, 3: 실버에서 혼용 |
| Data Mesh | 도메인 소유권, 제품으로서의 데이터, 셀프 서비스, 연합 거버넌스 |
| DATSIS | Discoverable, Addressable, Trustworthy, Self-describing, Interoperable, Secure |

---

## 22. 자가 점검 문제

> 답을 가리고 먼저 생각해본 뒤 펼쳐보자.

**Q1.** 메달리온의 각 계층에 어떤 모델링 방식이 놓이며, 그 이유는?
<details><summary>정답</summary>

브론즈는 모델링하지 않는다(소스 충실도가 목표, 원시 이력 보존). 실버에는 통합 지향인 Inmon 3NF와 Data Vault Raw Vault가, 골드에는 소비 지향인 Kimball 스타 스키마, Business Vault, 특성 테이블, 메트릭 뷰가 놓인다.
</details>

**Q2.** OLTP와 OLAP의 차이를 저장 방식, 스키마, 쿼리 관점에서 설명하라. Delta는 어느 쪽인가?
<details><summary>정답</summary>

OLTP는 행 기반 저장, 정규화(3NF), 소수 행 포인트 조회. OLAP는 열 기반 저장, 차원형/볼트형 스키마, 대량 스캔과 집계. Delta는 열 기반 Parquet 위에 ACID를 더한 OLAP 플랫폼이다.
</details>

**Q3.** Inmon이 정의한 데이터 웨어하우스의 네 가지 속성은?
<details><summary>정답</summary>

주제 지향(비즈니스 주제별 구성), 통합(단일 명칭, 형식, 키 체계), 시간 가변(이력 보존), 비휘발(커밋된 데이터는 덮어쓰지 않고 추가와 버전 관리).
</details>

**Q4.** 모델링 접근 방식을 고를 때 가장 먼저 감사 요건을 묻는 이유는?
<details><summary>정답</summary>

사후에 적용하기 가장 어려운 제약이기 때문이다. 실버에서 덮어쓴 이력은 골드 작업으로 복구할 수 없으므로, 감사가 필요하면 처음부터 Data Vault 같은 이력 보존 구조를 택해야 한다.
</details>

**Q5.** CDM, LDM, PDM의 차이와 각 소유자는?
<details><summary>정답</summary>

CDM은 엔티티와 관계만 있는 비즈니스 뷰(비즈니스 이해관계자), LDM은 속성, 키, 타입, 카디널리티, 정규화를 정한 플랫폼 독립 모델(데이터 아키텍트), PDM은 DDL, 제약, IDENTITY, 클러스터링을 포함한 구현(데이터 엔지니어).
</details>

**Q6.** CIF에서 ODS는 왜 EDW를 우회하는가?
<details><summary>정답</summary>

전체 ETL 주기를 기다릴 수 없는 준실시간 대시보드를 위해 소스에서 직접 데이터를 받기 때문이다.
</details>

**Q7.** 2NF와 3NF는 각각 어떤 종속성을 제거하는가? 예를 들어 설명하라.
<details><summary>정답</summary>

2NF는 부분 종속성을 제거한다. 복합 키 (OrderID, SKU)에서 Price가 SKU에만 종속되면 Product 테이블로 분리한다. 3NF는 이행 종속성을 제거한다. OrderID → CustID → CustomerName이면 고객 속성을 Customer 테이블로 분리한다.
</details>

**Q8.** Delta에서 실제로 쓰기 시점에 적용되는 제약과 정보성 제약은?
<details><summary>정답</summary>

NOT NULL과 CHECK는 적용되어 위반 행을 거부한다. PRIMARY KEY와 FOREIGN KEY는 정보성이라 중복 키나 고아 행도 삽입된다.
</details>

**Q9.** 강제되지 않는데도 PK/FK를 선언하는 이유 세 가지는?
<details><summary>정답</summary>

모델 의도 문서화, Unity Catalog ER 다이어그램 렌더링(및 AI/BI의 조인 경로 파악), RELY 선언 시 조인 제거 같은 옵티마이저 최적화.
</details>

**Q10.** RELY를 선언할 때의 위험은?
<details><summary>정답</summary>

옵티마이저가 무결성을 믿고 쿼리를 재작성하므로, 실제 데이터에 중복이나 고아 행이 있으면 쿼리 결과가 틀릴 수 있다. 파이프라인이 무결성을 실제로 보장할 때만 써야 한다.
</details>

**Q11.** 팩트 테이블의 그레인이란 무엇이며, 그레인을 섞으면 어떤 문제가 생기는가?
<details><summary>정답</summary>

팩트 한 행이 나타내는 것(예: 판매 라인 항목 하나당 한 행)이다. 라인 항목과 주문 헤더 같은 다른 그레인을 한 테이블에 섞으면 이중 집계가 발생한다.
</details>

**Q12.** 네 가지 팩트 테이블 유형과 UPDATE가 필요한 유형은?
<details><summary>정답</summary>

트랜잭션(이벤트당 한 행), 주기적 스냅샷(기간별 엔티티당 한 행), 누적 스냅샷(프로세스당 한 행, 마일스톤마다 갱신), 팩트리스(측정값 없이 발생 기록). UPDATE가 필요한 것은 누적 스냅샷이다.
</details>

**Q13.** 반가산 측정값의 예와 주의점은?
<details><summary>정답</summary>

계좌 잔액, 재고량. 고객 같은 차원으로는 합산할 수 있지만 시간축으로는 합산하면 안 된다(1월 잔액 + 2월 잔액은 무의미).
</details>

**Q14.** 차원 테이블의 PK로 비즈니스 키 대신 대리 키를 쓰는 이유는?
<details><summary>정답</summary>

비즈니스 키는 재번호 등으로 바뀔 수 있고, 소스 간에 겹칠 수 있으며, 같은 키의 여러 버전(SCD2)을 구분할 수 없고, BIGINT 대리 키가 조인 성능도 좋다.
</details>

**Q15.** `GENERATED ALWAYS AS IDENTITY`와 `GENERATED BY DEFAULT AS IDENTITY`의 차이는?
<details><summary>정답</summary>

ALWAYS는 항상 자동 생성하고 사용자가 값을 지정할 수 없다. BY DEFAULT는 값을 지정하지 않을 때만 자동 생성하며, 기존 웨어하우스의 대리 키 값을 그대로 가져올 때 쓴다.
</details>

**Q16.** SCD Type 0, 1, 2, 3을 각각 설명하고 예시 속성을 들어라.
<details><summary>정답</summary>

Type 0: 변경 무시(최초 가입일, 생년월일). Type 1: 덮어쓰기, 이력 없음(오타, 이메일 수정). Type 2: 기존 행을 닫고 새 대리 키로 새 행 추가, 전체 이력(주소, 세그먼트). Type 3: previous_ 컬럼으로 직전 값 하나만 보존.
</details>

**Q17.** SCD 유형은 테이블 단위로 정하는가?
<details><summary>정답</summary>

아니다. 속성 단위로 정한다. 한 차원에서 생년월일은 Type 0, 이메일은 Type 1, 주소는 Type 2를 함께 쓰는 것이 일반적이다.
</details>

**Q18.** SCD2를 하나의 MERGE로 처리하는 OLD + NEW 패턴을 설명하라.
<details><summary>정답</summary>

변경된 키마다 소스 행을 두 번 만든다. OLD 행은 대상의 현재 행과 매칭되어 WHEN MATCHED로 닫히고(is_current = false, end_date 설정), NEW 행은 ON 절의 `row_type = 'OLD'` 조건 때문에 매칭되지 않아 WHEN NOT MATCHED로 새 버전이 삽입된다. IDENTITY가 새 대리 키를 할당한다.
</details>

**Q19.** SCD2 MERGE를 멱등하게 만드는 두 가지 장치는?
<details><summary>정답</summary>

ON 절의 `t.address <> s.address`처럼 속성이 실제로 바뀐 경우에만 OLD가 매칭되게 하는 조건, 그리고 소스 단계에서 같은 내용의 현재 행이 이미 있으면 NEW 행을 만들지 않는 NOT EXISTS 필터. 그래서 같은 데이터로 다시 실행해도 변화가 없다.
</details>

**Q20.** MERGE 소스에 같은 키가 두 번 있으면? 해결책은?
<details><summary>정답</summary>

대상 행 하나에 여러 소스 행이 매칭되어 런타임 오류가 난다. 병합 전에 ROW_NUMBER 윈도우 함수나 GROUP BY로 소스를 중복 제거한다.
</details>

**Q21.** `WHEN NOT MATCHED BY SOURCE`는 SCD에서 어떻게 쓰이는가?
<details><summary>정답</summary>

소스에 없는 대상 행에 대해 UPDATE나 DELETE를 실행한다. 소스 추출에서 사라진 차원 레코드를 같은 MERGE 안에서 만료 처리할 때 쓴다.
</details>

**Q22.** 새 팩트를 로드할 때와 기존 팩트를 조회할 때 차원 조인 방식의 차이는?
<details><summary>정답</summary>

새 팩트는 `is_current = TRUE` 조건으로 조인해 로드 시점에 유효한 버전의 대리 키를 저장한다. 기존 팩트 조회는 저장된 대리 키로 직접 조인하므로 판매 당시의 속성이 보인다.
</details>

**Q23.** 역할 수행 차원과 정크 차원이란?
<details><summary>정답</summary>

역할 수행 차원은 하나의 물리 차원(DIM_DATE)을 주문일, 출고일, 배송일처럼 다른 별칭으로 여러 번 조인하는 것이다. 정크 차원은 어느 차원에도 속하지 않는 저카디널리티 플래그들을 한 테이블로 모아 팩트에는 FK 하나만 두는 것이다.
</details>

**Q24.** Delta에서 스노우플레이크보다 스타 스키마가 권장되는 이유와, 스노우플레이크가 정당화되는 경우는?
<details><summary>정답</summary>

열 기반 저장으로 넓은 차원이 I/O를 늘리지 않고, 딕셔너리 인코딩으로 반복 문자열 비용이 작으며, 평탄화된 차원은 브로드캐스트 조인이 가능해 조인이 적을수록 빠르다. 계층 엔티티가 독립적인 생명주기를 갖거나 수십억 행 규모의 차원일 때 스노우플레이크가 정당화된다.
</details>

**Q25.** Data Vault의 Hub, Link, Satellite를 각각 설명하라.
<details><summary>정답</summary>

Hub는 비즈니스 키를 담고 엔티티당 한 행이다. Link는 두 개 이상 Hub 간의 관계를 담고 부모 키 조합당 한 행이다. Satellite는 설명 속성과 이력을 담고, 속성이 바뀔 때마다 새 행을 추가하는 삽입 전용 테이블이다.
</details>

**Q26.** hash_key와 hash_diff의 차이는?
<details><summary>정답</summary>

hash_key는 비즈니스 키만 해시한 값으로 엔티티를 식별하며 변하지 않는다(Hub PK, Link, Satellite FK). hash_diff는 설명 속성 전체를 해시한 값으로 변경을 감지하며 Satellite에만 있다. 값이 바뀌면 새 Satellite 행을 넣는다.
</details>

**Q27.** 해시 키가 병렬 로드를 가능하게 하는 이유는?
<details><summary>정답</summary>

키가 시퀀스로 생성되는 것이 아니라 비즈니스 키에서 파생되므로, 어떤 작업이든 언제 계산해도 같은 값이 나온다. Hub가 먼저 로드되기를 기다릴 필요 없이 Hub, Link, Satellite를 동시에 로드할 수 있다.
</details>

**Q28.** hash_diff를 만들 때 구분자와 NULL 처리가 중요한 이유는?
<details><summary>정답</summary>

구분자가 없으면 '12'+'3'과 '1'+'23'이 같은 문자열이 되어 다른 속성 조합을 같다고 판단한다. NULL은 concat 결과 전체를 NULL로 만들거나(concat), 위치 정보를 잃게 하므로(concat_ws) COALESCE로 대체 값을 넣는다.
</details>

**Q29.** Satellite의 PK가 hash_key 단독이 아니라 (hash_key, load_ts)인 이유는?
<details><summary>정답</summary>

같은 엔티티의 여러 버전(이력)을 저장해야 하기 때문이다. Hub는 엔티티당 한 행이라 hash_key만으로 충분하다.
</details>

**Q30.** Raw Vault, Business Vault, Information Mart의 역할과, PIT 테이블의 용도는?
<details><summary>정답</summary>

Raw Vault(실버)는 비즈니스 규칙 없이 소스를 그대로 기록하는 감사 계층, Business Vault(골드)는 그 위에 규칙을 적용하는 계층(PIT, Bridge, 계산 Satellite), Information Mart(골드)는 BI용 스타 스키마나 평면 뷰다. PIT 테이블은 시점마다 각 Satellite의 유효 행을 미리 계산해 시점 기준 조인을 빠르게 한다.
</details>

**Q31.** 학습/서빙 스큐란 무엇이며 특성 저장소는 어떻게 해결하는가?
<details><summary>정답</summary>

같은 특성을 학습과 서빙에서 따로 구현해 생기는 미묘한 차이로 모델 성능이 조용히 떨어지는 현상이다. 특성 저장소는 특성을 한 번 정의하고 FeatureLookup으로 학습, 배치 추론, 실시간 서빙에서 같은 값을 쓰게 해 구조적으로 스큐를 막는다.
</details>

**Q32.** 메트릭 뷰의 네 가지 구성 요소와 해결하는 문제는?
<details><summary>정답</summary>

source(계산 대상 테이블), measures(이름 붙은 집계식), dimensions(유효한 분할 기준), filter(재정의할 수 없는 영구 행 필터). 팀마다 같은 지표를 다르게 계산하는 메트릭 드리프트를 막는다. 측정값은 `MEASURE()`로 조회한다.
</details>

**Q33.** OpenSharing이 CSV 내보내기보다 나은 점과, 공유할 골드 테이블을 설계할 때의 주의점은?
<details><summary>정답</summary>

복사 없이 실시간 데이터를 단기 사전 서명 URL로 읽게 하고, 제공자가 언제든 접근을 철회할 수 있으며, UC에 감사 추적이 남는다. Share는 테이블을 그대로 노출하므로 안정적인 스키마, 명확한 컬럼명, 일관된 단위, 적절한 집계 수준으로 설계해야 한다.
</details>

**Q34.** 모델링 결합의 세 가지 일반 패턴은?
<details><summary>정답</summary>

패턴 1: 3NF 실버 → Kimball 골드(안정 도메인, BI 중심). 패턴 2: Data Vault 실버 → Kimball 골드(규제, 다중 소스, 감사). 패턴 3: 실버에서 도메인별로 3NF와 Data Vault 혼용(도메인마다 요구가 다른 대규모 조직).
</details>

**Q35.** 같은 브론즈에서 3NF 경로와 Vault 경로로 만든 골드 결과가 같다면, 방식 선택은 무엇으로 결정하는가?
<details><summary>정답</summary>

결과의 정확성이 아니라 감사, 제공 속도, 소스 변화에 대한 유연성 같은 팀의 요구로 결정한다.
</details>

**Q36.** DATSIS 여섯 속성과 각각에 대응하는 Unity Catalog 기능은?
<details><summary>정답</summary>

Discoverable(태그, 검색, AI 설명), Addressable(catalog.schema.table), Trustworthy(리니지, expectation, 시간 여행, 감사 로그, 데이터 프로파일링), Self-describing(COMMENT ON TABLE/COLUMN), Interoperable(정보성 PK/FK, OpenSharing), Secure(GRANT, 행 필터, 컬럼 마스크, 단일 감사 로그).
</details>

**Q37.** Data Mesh를 도입하지 않아도 데이터 제품을 만들 수 있는가?
<details><summary>정답</summary>

그렇다. Data Mesh는 조직 모델이고 데이터 제품은 결과물이다. 어떤 팀이든 기존 테이블에 소유권, 태그, 주석, SLA, 권한을 더해 제품으로 만들 수 있으며, 도메인 하나, 테이블 하나부터 작게 시작하는 것이 권장된다.
</details>

---

## 23. 참고 링크

- 메달리온 아키텍처: https://docs.databricks.com/aws/en/lakehouse/medallion
- 데이터 웨어하우징 개념: https://docs.databricks.com/aws/en/sql/get-started/concepts
- 데이터 모델링 (Databricks): https://docs.databricks.com/aws/en/transform/data-modeling
- 제약 조건 (PK, FK, NOT NULL, CHECK): https://docs.databricks.com/aws/en/tables/constraints
- IDENTITY 컬럼: https://docs.databricks.com/aws/en/delta/generated-columns
- MERGE INTO: https://docs.databricks.com/aws/en/sql/language-manual/delta-merge-into
- Liquid 클러스터링: https://docs.databricks.com/aws/en/delta/clustering
- 데이터 스키핑과 Z-ORDER: https://docs.databricks.com/aws/en/delta/data-skipping
- md5 함수: https://docs.databricks.com/aws/en/sql/language-manual/functions/md5
- 특성 저장소 (UC 특성 엔지니어링): https://docs.databricks.com/aws/en/machine-learning/feature-store/
- 메트릭 뷰: https://docs.databricks.com/aws/en/metric-views/
- Delta Sharing (OpenSharing): https://docs.databricks.com/aws/en/delta-sharing/
- Share 생성과 관리: https://docs.databricks.com/aws/en/delta-sharing/create-share
- Unity Catalog 태그: https://docs.databricks.com/aws/en/database-objects/tags
- 객체 소유권: https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/ownership
- 데이터 리니지: https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-lineage
- 데이터 프로파일링 (품질 모니터링): https://docs.databricks.com/aws/en/lakehouse-monitoring/
- 행 필터와 컬럼 마스크: https://docs.databricks.com/aws/en/tables/row-and-column-filters
