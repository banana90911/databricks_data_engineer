# Databricks 데이터 프라이버시 — 핵심 개념 정리

---

## 목차

1. [프라이버시란: 보안, 거버넌스, 규정 준수와의 차이](#1-프라이버시란-보안-거버넌스-규정-준수와의-차이)
2. [식별, 보호, 관리, 검증 수명 주기와 의사결정 모델](#2-식별-보호-관리-검증-수명-주기와-의사결정-모델)
3. [예제 환경](#3-예제-환경)
4. [식별: Data Classification](#4-식별-data-classification)
5. [탐지 결과 검토와 오탐 처리](#5-탐지-결과-검토와-오탐-처리)
6. [탐지에서 거버넌스 태그로](#6-탐지에서-거버넌스-태그로)
7. [보호 기법 선택: 목적과 위험](#7-보호-기법-선택-목적과-위험)
8. [태그 기반 ABAC 열 마스크](#8-태그-기반-abac-열-마스크)
9. [수동 열 마스크와 행 필터](#9-수동-열-마스크와-행-필터)
10. [사례: 이름 기반 마스크를 ABAC로 교체](#10-사례-이름-기반-마스크를-abac로-교체)
11. [비식별화: 최소화, 가명화, 익명화](#11-비식별화-최소화-가명화-익명화)
12. [솔트 해시, 토큰화, 암호화, 일반화](#12-솔트-해시-토큰화-암호화-일반화)
13. [삭제 권리 1: 요청 추적과 대상 찾기](#13-삭제-권리-1-요청-추적과-대상-찾기)
14. [삭제 권리 2: 삭제, 전파, 물리적 제거](#14-삭제-권리-2-삭제-전파-물리적-제거)
15. [검증: 시스템 테이블로 증거 만들기](#15-검증-시스템-테이블로-증거-만들기)
16. [관련 기능과 범위 밖의 주제](#16-관련-기능과-범위-밖의-주제)
17. [핵심 정리 치트시트](#17-핵심-정리-치트시트)
18. [자가 점검 문제](#18-자가-점검-문제)
19. [참고 링크](#19-참고-링크)

---

## 1. 프라이버시란: 보안, 거버넌스, 규정 준수와의 차이

### 1-1. 네 도메인, 네 질문

| 도메인 | 하는 일 | 답하는 질문 |
|---|---|---|
| **보안** | 접근 제어, 암호화, 네트워크 제어로 무단 접근 차단 | 접근해서는 안 되는 사람으로부터 데이터가 보호되는가? |
| **거버넌스** | 소유, 카탈로그화, 정책 수립. 어떤 데이터가 어디에 있고 책임자가 누구인지 관리 | 우리가 무엇을 보유하고 있고, 누가 책임지는지 아는가? |
| **규정 준수** | 외부 법률, 규정, 표준을 지키고 이를 증명 | 의무를 충족하고, 충족했음을 증명할 수 있는가? |
| **프라이버시** | 개인의 권리가 존중되도록 개인 데이터를 처리. 찾고, 최소화하고, 보호하고, 삭제하고, 증명 | 개인 데이터를 책임 있게 처리하고 있으며, 이를 증명할 수 있는가? |

프라이버시는 독립된 영역이 아니라 나머지 셋이 겹치는 지점에 **개인 데이터와 개인의 권리**라는 초점을 더한 것이다. 보안으로 보호하고, 거버넌스로 위치를 파악하고, 규정 준수로 어떤 의무가 있는지 안다.

### 1-2. 구분이 중요한 이유

- **보안은 필요하지만 충분하지 않다.** 완벽히 보안된 테이블도 회사 안에서 보면 안 되는 사람에게 개인 데이터를 보여 줄 수 있고, 삭제했어야 할 데이터를 계속 들고 있을 수 있다. 프라이버시는 접근 제어 위에 **목적**과 **데이터 수명 주기**를 더한다.
- **거버넌스가 기반이다.** 카탈로그에 없거나 위치를 모르는 개인 데이터는 보호하거나 삭제할 수 없다.
- **규정 준수와 프라이버시는 다르다.** 규정은 "개인이 삭제를 요청할 수 있다"고 말할 뿐이다. 프라이버시는 데이터를 찾아 모든 곳에서 지우고 이를 증명하는 기술 작업이다.
- **함정**: 규정 준수 인증서를 프라이버시의 증거로 착각하는 것. 인증서는 플랫폼의 전반적 상태를 말할 뿐, 특정 테이블이 올바르게 처리되는지는 말해 주지 않는다.

### 1-3. 인증 보유 vs 준수 지원

| 구분 | 예 | 의미 |
|---|---|---|
| Databricks가 **보유한 인증** | ISO 27001, SOC 2 Type II, HITRUST | 제3자 감사로 검증된 플랫폼 자체의 인증 |
| Databricks가 **준수를 지원**하는 규정 | HIPAA (Compliance Security Profile + 계약), DORA (가이던스, 공동 책임 백서) | 플랫폼 기능과 가이던스로 충족을 돕지만 인증 자체는 아님 |

> 인증 목록과 적용 범위는 클라우드와 시점에 따라 바뀐다. 최신 정보는 Databricks Trust and Compliance Center가 기준이며, 법적 판단은 DPO나 법무팀에 맡긴다.

---

## 2. 식별, 보호, 관리, 검증 수명 주기와 의사결정 모델

### 2-1. 수명 주기

| 단계 | 하는 일 | 주요 기능 |
|---|---|---|
| **식별 (Identify)** | 데이터 자산 전체에서 개인 데이터를 찾는다 | Data Classification, 거버넌스 태그 |
| **보호 (Protect)** | 목적에 따라 데이터를 제한하거나 변환한다 | ABAC, 수동 마스크와 행 필터, 비식별화, 키 관리 |
| **관리 (Manage)** | 수명 주기 동안 수정, 삭제, 물리적 제거한다 | `DELETE`, `MERGE`, `REORG ... APPLY (PURGE)`, `VACUUM` |
| **검증 (Verify)** | 제어가 작동했음을 증명한다 | 시스템 테이블, 리니지, 테이블 이력 |

검증은 사후 고려가 아니라 핵심 단계다. 규제 기관, 감사자, 고객은 특정인의 데이터가 실제로 보호되거나 삭제되었음을 증명하라고 요구할 수 있다. **제어는 작동했음을 증명할 수 있을 때 비로소 완료된다.**

### 2-2. 의무를 기술적 질문으로 바꾸기

| 평문 의무 | 기술적 질문 | 단계 |
|---|---|---|
| 개인 데이터 찾기 | 전체 환경에서 이 사람의 데이터가 있는 모든 열과 행을 찾을 수 있는가? | 식별 |
| 제한 또는 변환 | 분석가는 유용한 데이터를 쓰되 원본은 못 보게 숨기거나 변환할 수 있는가? | 보호 |
| 수정 또는 삭제 | 원시 테이블부터 골드까지 퍼진 모든 곳에서 지우고, 이전 파일까지 제거할 수 있는가? | 관리 |
| 처리 내역 증명 | 태그, 마스킹 결과, 빈 삭제 쿼리, 감사 이벤트, 리니지, 테이블 이력을 제시할 수 있는가? | 검증 |

### 2-3. 프라이버시 의사결정 모델 (6가지 질문)

모든 기법에 같은 질문을 던진다. 새 기법이나 새로 태그된 열이 생길 때마다 다시 실행한다.

| # | 질문 | 단계 |
|---|---|---|
| 1 | 여기에 어떤 민감한 데이터가 있는가? | 식별 |
| 2 | 왜 필요하며, 어떤 목적으로 쓰는가? | 보호 |
| 3 | 원본 값을 볼 수 있는 사람은 누구인가? | 보호 |
| 4 | 연결(조인)이 필요한가, 끊어도 되는가? | 보호 |
| 5 | 다운스트림으로 어디까지 퍼졌는가? | 관리 |
| 6 | 삭제와 검증은 어떻게 하는가? | 검증 |

---

## 3. 예제 환경

다국적 소매업체 데이터를 기준으로 설명한다(모두 합성 데이터).

| 계층 | 테이블 | 내용 |
|---|---|---|
| 원시(브론즈) | `customer_profile` | 기본 PII: 이름, 이메일, 전화번호, 주소, 생년월일, `national_id`, IP 주소 |
| | `loyalty` | 포인트, 등급 (고객당 한 행) |
| | `clickstream` | 웹 이벤트, IP, 사용자 에이전트 |
| | `support_records` | 자유 텍스트 본문에 이메일, 전화번호가 섞여 있는 티켓 |
| 실버 | `customer_enriched` | `customer_profile` + `loyalty` 조인 (식별자 포함) |
| 골드 | `customer_metrics_gold` | 국가, 등급별 개수와 매출 (식별자 없음) |

리니지: `customer_profile`, `loyalty` → `customer_enriched` → `customer_metrics_gold`. 이 체인이 삭제와 증거의 추적 대상이다.

세 사람의 문제가 과정 전체를 이끈다.

| 인물 | 문제 | 관련 단계 |
|---|---|---|
| 데이터 엔지니어 | "어떤 열이 민감한가?" 접근 제어는 있지만 민감도 레이블이 없다 | 식별, 보호 |
| 분석가 | "식별자 없이도 쓸 만한 데이터가 있는가?" 조인은 해야 하지만 이메일은 보면 안 된다 | 비식별화 |
| 고객 | "내 데이터를 삭제해 달라." 데이터가 원시부터 골드까지 퍼져 있다 | 삭제, 검증 |

---

## 4. 식별: Data Classification

### 4-1. 분류되지 않은 데이터가 위험한 이유

| 접근 제어 | 분류 |
|---|---|
| "누가" 테이블을 열 수 있는지 결정 | "무엇이" 민감한지 (어떤 열에 개인 데이터가 있는지) 찾음 |
| 내부 내용과 무관하게 적용 | 데이터 자산 전체에서 열 수준으로 동작 |
| 접근 권한이 있으면 민감한 열도 모두 보임 | 이제 보호, 마스킹, 삭제할 대상을 앎 |

- **찾지 못한 것은 보호할 수 없다.** 열 마스크, 행 필터, 가명화, 삭제는 모두 대상 열을 지정해야 한다.
- 수작업 데이터 사전은 새 열이 추가되는 순간 낡는다. 검색은 먼저, 그리고 반복 가능하게 이루어져야 한다.
- 자유 텍스트 열(예: 티켓 본문)에 숨은 PII도 흔하다.

### 4-2. Data Classification 동작

Unity Catalog 기능으로, 테이블을 스캔해 열 수준에서 민감한 데이터를 감지하고 **시스템 관리 `class.*` 태그**를 붙인다. 서버리스로 실행된다.

1. **카탈로그에서 활성화**: 카탈로그에 대한 `MANAGE` 권한이 필요하다. 스캔할 스키마를 선택한다.
2. **비동기 스캔**: 활성화하면 초기 스캔이 자동으로 시작된다(소규모 환경은 약 15분). 이후 새로 추가되거나 바뀐 테이블과 열은 보통 약 24시간 안에 처리되며, 재스캔은 바뀐 부분만 증분으로 한다. 전체 재스캔은 카탈로그 UI에서 트리거하며, 현재 SQL이나 API로 스캔을 시작하는 방법은 없다.
3. **감지 및 태그 적용**: 모델이 열을 감지해 `class.email_address` 같은 태그를 붙인다.
4. **결과 기록**: 감지마다 `system.data_classification.results`에 한 행이 기록된다.

### 4-3. 감지 클래스

| 범주 | 예 | 지역 |
|---|---|---|
| 일반 식별자 | `class.name`, `class.email_address`, `class.phone_number`, `class.ip_address`, `class.location`, `class.credit_card`, `class.iban_code`, `class.passport`, `class.date_of_birth` | 모든 지역 |
| 특수 범주 | `class.ethnicity`, `class.religious_belief`, `class.political_opinion`, `class.genetic_data`, `class.biometric_data` 등 | 모든 지역 |
| 의료 | `class.medical_record_number`, `class.health_data` 등 | 모든 지역 |
| 미국 식별자 | `class.us_ssn`, `class.us_itin`, `class.us_driver_license` | **예외적으로 모든 지역** |
| 기타 국가 식별자 | `class.uk_nino`, `class.de_tax_id`, `class.in_aadhaar`, `class.br_cpf`, `class.jp_my_number` 등 | 워크스페이스가 해당 지역일 때만 |

그래서 다국적 데이터의 `national_id` 열은 워크스페이스 지역에서 지원하는 국가 형식에만 일부 매칭될 수 있다.

### 4-4. 결과 테이블과 품질 신호

`system.data_classification.results`의 각 행에는 열, 클래스, 신뢰도, 빈도, 그리고 **최대 5개의 실제 샘플 값**이 들어 있다.

| 신호 | 값 | 의미 |
|---|---|---|
| **confidence** | `HIGH` / `LOW` (이산값) | 감지를 신뢰할지 여부. `LOW`는 임계값이 아니라 "조치 전에 샘플을 검토하라"는 신호 |
| **frequency** | 0 ~ 1 | 열에서 해당 클래스와 일치한 행의 비율 |

| 조합 | 해석 | 예 |
|---|---|---|
| HIGH + frequency 1.0 근처 | 가장 강한 신호. 거의 모든 행에 있음 | `customer_profile.email` |
| HIGH + 부분 frequency | 실제 민감 데이터지만 일부 행에만 있음 (자유 텍스트에 흔함) | `support_records.body_text` (약 절반의 티켓에 이메일) |
| LOW | 사람의 판단이 필요한 후보, 오탐일 가능성 | `created_at`이 생년월일로 잡힘 |

> 주의
> - 결과 테이블의 샘플은 실제 값이라 민감하다. 읽으려면 어카운트 관리자가 `system.data_classification.results`에 대한 `SELECT`를 부여해야 한다.
> - **결과 테이블에 `ALTER TABLE`로 행 필터나 열 마스크를 걸면 안 된다.** Data Classification이 이 테이블의 쓰기를 소유하므로 스캔이 실패할 수 있다. ABAC 정책은 스캔에 영향을 주지 않는다.
> - 분석용으로는 샘플을 뺀 프로젝션(뷰나 사본)을 만들어 쓴다.

```sql
SELECT table_name, column_name, class_tag, confidence, frequency
FROM system.data_classification.results
WHERE catalog_name = 'main'
ORDER BY confidence DESC, table_name, column_name;
```

### 4-5. 사용자 정의 분류기 (Beta)

내부 직원 ID(예: `AGT-0000`)나 멤버십 번호처럼 조직 고유 식별자는 기본 클래스로 감지할 수 없다.

- 정규식을 쓸 필요 없이, 식별자를 **평문으로 설명**하고 그 값이 들어 있는 **샘플 열을 최대 10개** 지정하면 Databricks가 감지기를 만든다.
- 생성할 때 적용할 **거버넌스 태그**(예: `sensitivity_level`)를 고른다. 기본 감지는 `class.*`를 붙이고 사람이 매핑하지만, 사용자 정의 분류기는 **거버넌스 태그를 바로** 붙인다.
- 메타스토어 단위로 정의되어 Data Classification이 켜진 모든 카탈로그에 적용된다. 메타스토어당 최대 50개.
- 새로 추가하거나 편집한 분류기는 **다음 스캔부터** 적용된다. 생성 후에는 거버넌스 태그를 바꿀 수 없다(삭제 후 재생성).
- 권한: 메타스토어 관리자, 적용 태그에 대한 `ASSIGN`, 샘플 열 테이블에 대한 `SELECT`.

---

## 5. 탐지 결과 검토와 오탐 처리

### 5-1. 오탐(false positive) 예

| 항목 | 값 |
|---|---|
| 열 | `customer_profile.created_at` |
| 감지 클래스 | `class.date_of_birth` |
| 신뢰도 | LOW |
| 샘플 | 2021-06-02, 2022-01-17 |
| 판정 | 계정 생성 시각이지 생년월일이 아님 → 오탐 |

날짜, 타임스탬프 열은 생년월일 감지기를 잘못 트리거하기 쉽다. **신뢰도와 샘플을 함께 보는 것**이 분류(triage)의 핵심이다.

### 5-2. 처리 방법: 제외 (Beta)

검토 화면에서 감지를 **제외(exclude)** 하면 다음과 같이 처리된다.

- 해당 열의 분류 태그가 제거된다.
- 이후 스캔에서 다시 붙지 않는다.
- 분류 정확도를 높이는 피드백으로 쓰인다.

> 그냥 무시하면 안 된다. 제외하지 않은 오탐 태그가 남아 있으면, 그 태그를 기준으로 하는 제어가 무해한 열을 마스킹하거나 필터링하게 된다.

```sql
-- LOW 신뢰도 감지만 먼저 검토
SELECT table_name, column_name, class_tag, confidence, frequency
FROM system.data_classification.results
WHERE confidence = 'LOW';
```

---

## 6. 탐지에서 거버넌스 태그로

### 6-1. 클래스 태그는 사실, 거버넌스 태그는 결정

| 단계 | 예 | 성격 |
|---|---|---|
| 감지 | `class.email_address` | 열에 무엇이 있는지 (사실). 그 자체로는 아무것도 숨기지 않음 |
| 결정 | `sensitivity_level = restricted` | 어떻게 처리할지 (결정). 통제된 어휘의 거버넌스 태그 |
| 제어 | ABAC 열 마스크 | 정책이 거버넌스 태그와 매칭되어 보이는 값을 바꿈 |

중간에 거버넌스 태그를 두는 이유:

1. **사람의 검토 단계**가 생겨 `created_at` 같은 오탐이 정책까지 가지 않는다.
2. 서로 다른 여러 `class.*`(이메일, 주민번호, 여권)를 **하나의 레벨**로 묶어 정책 하나로 처리할 수 있다.
3. 같은 클래스라도 열마다 처리가 다를 수 있고, 통제된 어휘로 결정을 일관되게 유지한다.

> ABAC는 `class.*`에도 직접 매칭할 수 있다(`has_tag('class.email_address')`). `sensitivity_level`로 한 번 매핑하는 것은 플랫폼 제약이 아니라 설계상의 선택이다.

### 6-2. 작은 통제 어휘: sensitivity_level

| 값 | 의미 | 예 | 일반적인 제어 |
|---|---|---|---|
| `public` | 비개인 데이터, 공유 가능 | country, loyalty_tier | 불필요 |
| `internal` | 직원용 비즈니스 데이터 | points_balance, 주문 수 | 일반 권한 부여 |
| `confidential` | 대부분의 분석가에게 평문이 필요 없는 개인 데이터 | phone, street_address | 대부분 마스킹, 예외 그룹 허용 |
| `restricted` | 가장 엄격한 처리가 필요한 직접 식별자 | email, national_id | 기본 마스킹, 원본을 보는 사람은 소수 |

- 거버넌스 태그는 허용 값을 최대 수백 개까지 가질 수 있지만, 작은 집합이 일관되게 적용하기 쉽다.
- 서로 다른 클래스(email, national_id)가 같은 레벨(restricted)을 공유할 수 있고, 정책은 레벨에 작동한다.

### 6-3. 매핑과 태그 적용

```sql
-- 감지 결과 → 권장 민감도 (검토 계획)
SELECT table_name, column_name, class_tag, confidence,
  CASE
    WHEN class_tag = 'class.date_of_birth' AND confidence = 'LOW' THEN 'review (likely false positive)'
    WHEN class_tag IN ('class.email_address', 'class.us_ssn', 'class.passport',
                       'class.credit_card', 'class.iban_code') THEN 'restricted'
    WHEN class_tag IN ('class.phone_number', 'class.location', 'class.ip_address',
                       'class.date_of_birth') THEN 'confidential'
    ELSE 'internal'
  END AS recommended_sensitivity
FROM system.data_classification.results;

-- 거버넌스 태그 적용 (태그에 대한 ASSIGN, 테이블에 대한 APPLY TAG 필요)
ALTER TABLE customer_profile ALTER COLUMN email       SET TAGS ('sensitivity_level' = 'restricted');
ALTER TABLE customer_profile ALTER COLUMN national_id SET TAGS ('sensitivity_level' = 'restricted');
ALTER TABLE customer_profile ALTER COLUMN phone       SET TAGS ('sensitivity_level' = 'confidential');

-- 적용 결과 확인 (태그 증거)
SELECT table_name, column_name, tag_name, tag_value
FROM main.information_schema.column_tags
WHERE schema_name = 'priv_demo'
ORDER BY table_name, column_name, tag_name;
```

`class.*` 시스템 태그는 **존재 여부 기반**이라 `tag_value`가 비어 있다. 키 자체가 신호다.

### 6-4. 세 가지 태그 유형

| 유형 | 거버넌스 | 누가 정의/적용 | ABAC 매칭 |
|---|---|---|---|
| 시스템 `class.*` 태그 | 적용 | Data Classification이 자동 적용 | 가능 |
| 사용자 정의 거버넌스 태그 (예: `sensitivity_level`) | 적용 | 관리자가 키와 허용 값 정의, 사용자가 `ASSIGN` 권한으로 할당 | 가능 |
| 자유 형식 태그 | 미적용 | 누구나 자유롭게 | **불가** (검색, 정리용) |

> 주의
> - 거버넌스 태그를 만드는 것은 어카운트 수준 권한이 필요한 관리자 작업이다. 어휘 정의(관리자)와 적용(데이터 스튜어드)을 분리해 직무 분리를 지킨다.
> - **태그 키와 값에 PII나 비밀 값을 넣지 않는다.** 메타데이터와 시스템 테이블에 광범위하게 노출된다. `restricted` 같은 범주로만 태그한다.
> - ABAC를 쓰면 **태그 품질이 곧 보안 경계**다. 태그가 빠진 민감한 열은 보호되지 않고, 과하게 태그된 열은 불필요하게 마스킹된다.

### 6-5. 태그별로 어떤 제어를 걸 것인가

| 열 (클래스) | 민감도 | 제어 |
|---|---|---|
| email (`class.email_address`) | restricted | 기본 열 마스크, 예외 그룹은 원본 |
| national_id (`class.us_ssn`) | restricted | 마스크 + 조인 키가 필요하면 가명화 |
| phone (`class.phone_number`) | confidential | 대부분 마스크, 고객 지원 역할은 열람 |
| body_text (자유 텍스트 내 이메일) | confidential | 텍스트 내 연락처 제거 또는 열 제한 |
| created_at (`class.date_of_birth`, LOW) | 제외 | 오탐. 어떤 제어도 대상으로 삼지 않게 감지 제외 |

> **태그는 제어를 구동할 때만 유용하다.** 어떤 제어도 걸리지 않는 태그라면 필요 없을 가능성이 높다.

---

## 7. 보호 기법 선택: 목적과 위험

권한 부여(GRANT)는 누가 테이블을 열 수 있는지 정할 뿐, 안의 어떤 값을 볼 수 있는지는 정하지 않는다. 두 번째 결정이 보호 제어이며, **목적을 먼저, 그다음 위험을** 고려해 고른다.

원본 값을 어떻게 다루는지에 따라 스펙트럼이 있다: **유지 → 변환 → 제거**

| 처리 방식 | 기법 | 값에 하는 일 | 언제 쓰는가 |
|---|---|---|---|
| 값을 유지, 읽을 때 제어 | **마스크** | 권한 없는 사용자에게 쿼리 시점에 대체 값 표시 | 일부만 원본이 필요하고 대부분은 필요 없음 |
| | **행 필터** | 권한 없는 사용자에게 행 전체를 숨김 | 각자 자기 범위(지역, 테넌트)만 봐야 함 |
| 변환, 키로 복원 | **가명화** | 식별자를 일관된 토큰/해시로 대체 | 원시 식별자 없이 테이블 간 연결이 필요 |
| | **암호화** | 암호문으로 저장, 키가 있어야 복원 | 나중에 권한 있는 프로세스가 원본 복원 필요 |
| 보관하지 않음 | **억제 / 일반화** | 열 삭제, 또는 값을 거칠게 (나이 → 연령대) | 정확한 값이 아니라 형태나 범위만 필요 |
| | **최소화** | 아예 수집, 보관하지 않음 | 사용 사례에 그 값이 전혀 필요 없음 |
| | **삭제** | 현재 상태에서 지우고 스토리지에서도 제거 | 보관할 법적 근거가 없음 (삭제 요청) |

- 기법은 **겹쳐 쓴다.** `national_id`는 분석가에게는 마스킹, 조인 키로는 가명화, 삭제 요청 시에는 삭제될 수 있다.
- 마스크와 행 필터는 **저장된 값을 바꾸지 않으므로**, 테이블을 권한 있는 작업에 계속 써야 할 때 첫 번째 선택지다.

---

## 8. 태그 기반 ABAC 열 마스크

### 8-1. 정책 구조

```sql
CREATE OR REPLACE POLICY mask_restricted_columns
ON SCHEMA main.priv_secure
COLUMN MASK main.priv_secure.mask_restricted
TO `account users` EXCEPT `account privileged`
FOR TABLES
MATCH COLUMNS has_tag_value('sensitivity_level', 'restricted') AS c
ON COLUMN c;
```

| 절 | 의미 |
|---|---|
| `ON SCHEMA` (또는 `ON CATALOG`) | **범위**. 범위 안의 현재와 미래의 모든 테이블이 적용 대상 |
| `TO ... EXCEPT ...` | **대상 주체**. 마스크를 적용할 그룹과 제외할 그룹. 개인이 아닌 어카운트 그룹 사용 |
| `MATCH COLUMNS has_tag_value(...) AS c` | **매칭**. 열 이름이 아니라 거버넌스 태그로 열 선택. `has_tag('키')`도 가능. 최대 3개 표현식, 모두 만족해야 함 |
| `COLUMN MASK <UDF>` + `ON COLUMN c` | **동작**. 매칭된 열의 값을 UDF의 첫 번째 인자로 넘겨 재작성 |
| `WHEN ...` (선택) | 테이블, 스키마, 카탈로그 태그를 조건으로 추가 |

마스크 함수는 **무조건적**이다. 값만 변환하고 호출자를 확인하지 않는다. 누구에게 적용할지는 정책의 `TO`/`EXCEPT`가 정한다.

```sql
CREATE OR REPLACE FUNCTION main.priv_secure.mask_restricted(val STRING)
RETURNS STRING
RETURN CASE WHEN val IS NULL THEN NULL ELSE concat(left(val, 2), '****') END;
```

| 사용자 | email | national_id |
|---|---|---|
| `account users` (제한됨) | `ja****` | `12****` |
| `account privileged` (예외) | `james.smith@example.com` | `123-45-6789` |

두 사용자는 같은 쿼리를 실행한다. 누가 무엇을 보는지의 결정은 데이터나 쿼리가 아니라 **정책**에 있다.

권한과 요구 사항:

- 정책 생성: 범위(스키마/카탈로그)에 대한 `MANAGE`, 함수에 대한 `EXECUTE`.
- 열 마스크/행 필터 정책은 Databricks Runtime 16.4 이상(서버리스 충족)이 필요하다.
- `EXCEPT`에 지정한 그룹이 어카운트에 없으면 `PRINCIPAL_DOES_NOT_EXIST` 오류가 난다. 이 경우 `TO account users`만 두면 예외 없이 모두에게 마스크가 적용된다.

### 8-2. 검증

```sql
-- 내가 어떤 그룹에 속하는지
SELECT current_user(),
       is_account_group_member('account users')      AS in_account_users,
       is_account_group_member('account privileged') AS in_account_privileged;

-- 실제 결과 (내 신원 기준으로 마스킹되어 보임)
SELECT customer_id, email, national_id, phone FROM main.priv_secure.customer_profile LIMIT 8;

-- 신원과 무관하게 어떤 정책이 어떤 열에 걸려 있는지 확인
SHOW EFFECTIVE POLICIES ON TABLE main.priv_secure.customer_profile;

-- 정책 삭제
DROP POLICY mask_restricted_columns ON SCHEMA main.priv_secure;
```

한 세션은 한 신원으로만 실행되므로, 다른 신원이 무엇을 보는지는 `SHOW EFFECTIVE POLICIES`로 증명한다.

### 8-3. 새로 태그된 열이 자동으로 포함된다

정책이 열 목록이 아니라 태그로 매칭하므로, 나중에 `restricted`로 태그된 열은 **정책이나 테이블을 바꾸지 않아도** 자동으로 마스킹된다.

```sql
ALTER TABLE main.priv_secure.customer_profile
ALTER COLUMN last_name SET TAGS ('sensitivity_level' = 'restricted');
-- 이제 SHOW EFFECTIVE POLICIES에 last_name이 email, national_id와 함께 나타남
```

`ALTER POLICY`도, `ALTER TABLE ... SET MASK`도, 새 권한 부여도 필요 없다. 커지는 환경에서 정책이 편집 없이 새 열을 계속 덮는 방식이다.

### 8-4. 동적 뷰 vs 태그 기반 ABAC

| 동적 뷰 (`CASE WHEN is_account_group_member(...)`) | 태그 기반 ABAC |
|---|---|
| 로직이 뷰마다 존재 | 스키마/카탈로그에 정책 하나 |
| 대상 판단이 CASE 안에 | 대상이 `TO`, `EXCEPT`에 분리 |
| 뷰를 통해 읽는 사람만 보호 (기반 테이블 직접 권한이 있으면 우회) | **기반 테이블 자체**가 보호됨 |
| 환경이 바뀔 때마다 뷰 수정 | 새로 태그된 열과 새 테이블에 자동 적용 |

동적 뷰는 UDF 수명 주기 관리 없이 독립적인 읽기 전용 프로젝션이 필요할 때 여전히 간단한 선택이다.

### 8-5. 결정 근거를 테이블 속성으로 남기기

```sql
ALTER TABLE main.priv_secure.customer_profile SET TBLPROPERTIES (
  'privacy.control_justification' = 'Least-privilege masking of restricted PII.'
);
SHOW TBLPROPERTIES main.priv_secure.customer_profile;
```

사용자 정의 키에 한 줄짜리 근거(무엇을, 누구에게 마스킹하고, 누가 예외이며 그 이유)를 남기면 테이블과 함께 이동하는 문서가 된다(Databricks 기능이 아니라 관례).

프라이버시와 관련된 내장 `delta.*` 속성:

| 속성 | 제어 대상 | 기본값 |
|---|---|---|
| `delta.deletedFileRetentionDuration` | `VACUUM`이 삭제된 데이터 파일을 지우기 전까지 유지 기간 | 7일 (168시간) |
| `delta.logRetentionDuration` | 테이블 이력과 시간 여행 유지 기간 | 30일 |
| `delta.enableDeletionVectors` | 삭제 시 행을 표시(소프트)할지 파일을 재작성할지 | 워크스페이스 설정과 런타임에 따라 다름 |
| `delta.enableChangeDataFeed` | 행 수준 변경 레코드 유지 여부 | 꺼짐 |

---

## 9. 수동 열 마스크와 행 필터

### 9-1. 언제 수동 방식을 쓰는가

ABAC가 기본 제어 방식이지만, 거버넌스 태그에는 **어카운트당 상한(1,000개)** 이 있다. 수만 개 열을 관리하는 대규모 엔터프라이즈는 일부를 수동 `SET MASK`, `SET ROW FILTER`로 관리한다. ABAC를 먼저 쓰고, 태그 예산을 넘는 열이나 정책 범위 밖의 열에 수동 제어를 쓴다.

| 태그 기반 ABAC | 수동 SET MASK / SET ROW FILTER |
|---|---|
| 정책 하나로 태그된 모든 열 처리 | 테이블마다 직접 연결, 반복 필요 |
| 대상이 `TO`/`EXCEPT`에 | 대상 판단이 **UDF 안에** (`is_account_group_member`) |
| 거버넌스 태그 필요 | 태그 불필요 |
| 기반 테이블 보호 | 기반 테이블 보호 (동적 뷰와 달리) |

### 9-2. 구현

```sql
-- 수동 열 마스크: 호출자 판단이 함수 안에 있음
CREATE OR REPLACE FUNCTION main.priv_secure.mask_phone_manual(val STRING)
RETURNS STRING
RETURN CASE
  WHEN is_account_group_member('account privileged') THEN val
  WHEN val IS NULL THEN NULL
  ELSE concat(left(val, 3), '******')
END;

-- 열에는 마스크가 하나만 연결되므로, 재실행 시 기존 것을 먼저 제거
ALTER TABLE main.priv_secure.customer_profile ALTER COLUMN phone DROP MASK;
ALTER TABLE main.priv_secure.customer_profile
  ALTER COLUMN phone SET MASK main.priv_secure.mask_phone_manual;

-- 수동 행 필터: BOOLEAN 반환, TRUE인 행만 보임
CREATE OR REPLACE FUNCTION main.priv_secure.region_row_filter(country STRING)
RETURNS BOOLEAN
RETURN is_account_group_member('account privileged') OR country = 'United States';

ALTER TABLE main.priv_secure.customer_profile DROP ROW FILTER;
ALTER TABLE main.priv_secure.customer_profile
  SET ROW FILTER main.priv_secure.region_row_filter ON (country);

-- 연결된 마스크와 필터 확인
DESCRIBE EXTENDED main.priv_secure.customer_profile;
```

같은 테이블에 ABAC 정책(email 마스크)과 수동 마스크(phone), 수동 행 필터가 함께 적용될 수 있다.

> AI 어시스턴트로 마스크 UDF 초안을 만들었다면, 정책에 연결하기 전에 일반 값, 짧은 값, 형식이 맞지 않는 값, NULL 같은 경계 사례로 직접 실행해 결과를 확인한다.

---

## 10. 사례: 이름 기반 마스크를 ABAC로 교체

### 10-1. 상속받은 취약한 보호의 세 가지 결함

| 결함 | 상태 | 문제 |
|---|---|---|
| 잘못된 태그 | `national_id`가 restricted로 감지되었는데 `internal`로 태그됨 | 태그를 믿는 정책이 이 열을 보호하지 못함 |
| 이름에 고정된 마스크 | `email`, `national_id`에 열 이름 기준으로 마스크를 하나씩 연결 | 누군가 직접 이름을 지정한 열만 보호, 수동 유지 |
| 노출된 새 열 | 나중에 추가된 `passport_number`에 태그도 마스크도 없음 | 누구나 평문으로 읽음 |

세 결함의 근본 원인은 같다. **분류가 아니라 열 이름으로 보호**했기 때문이다.

### 10-2. 수정 순서

```sql
-- 1) 감지 결과와 태그를 대조해 잘못된 태그 수정
ALTER TABLE customer_profile ALTER COLUMN national_id SET TAGS ('sensitivity_level' = 'restricted');

-- 2) 이름 기반 마스크 제거 (열은 하나의 제어로만 관리)
ALTER TABLE customer_profile ALTER COLUMN email DROP MASK;
ALTER TABLE customer_profile ALTER COLUMN national_id DROP MASK;

-- 3) 무조건적 마스크 UDF + 스키마 범위 태그 기반 정책
CREATE OR REPLACE FUNCTION mask_restricted(val STRING)
RETURNS STRING
RETURN CASE WHEN val IS NULL THEN NULL ELSE concat(left(val, 2), '****') END;

CREATE OR REPLACE POLICY mask_lab_restricted
ON SCHEMA priv_lab
COLUMN MASK mask_restricted
TO `account users` EXCEPT `account privileged`
FOR TABLES
MATCH COLUMNS has_tag_value('sensitivity_level', 'restricted') AS c
ON COLUMN c;

-- 4) 새 열은 태그만 붙이면 정책 적용 대상이 됨
ALTER TABLE customer_profile ALTER COLUMN passport_number SET TAGS ('sensitivity_level' = 'restricted');

-- 5) 결정 근거 기록
ALTER TABLE customer_profile SET TBLPROPERTIES (
  'privacy.control_justification' = 'Least-privilege masking of restricted PII'
);
```

> 1단계를 먼저 하는 이유: 이름 기반 마스크를 지우고 태그 기반 정책으로 바꾸는 순간, `national_id`가 여전히 `internal`이면 그 열은 노출된다. 태그가 보안 경계가 되었기 때문이다.

결과: `phone`은 `confidential`이라 restricted 정책의 대상이 아니므로 계속 보이고, `email`, `national_id`, `passport_number`는 마스킹된다.

---

## 11. 비식별화: 최소화, 가명화, 익명화

마스킹은 쿼리 시점에 값을 숨긴다. 비식별화는 **저장된 값 자체를 바꿔** 분석가는 데이터를 쓰되 개인은 보호되게 한다.

예제 사용 사례: 분석가가 `customer_profile`과 `clickstream`을 고객별로 조인해 행동을 분석해야 하지만, 개인을 식별해서는 안 된다. 지금의 조인 키 `customer_id`는 개인을 직접 가리킨다.

### 11-1. 먼저 최소화

**보유하지 않은 데이터는 유출될 수 없다.** 사용 사례에 필요한 것만 남긴다.

| 분류 | 열 | 처리 |
|---|---|---|
| 필요 없음 | first_name, last_name, email, national_id | 삭제 |
| 유용하고 위험 낮음 | country, loyalty_tier | 유지 |
| 여전히 식별 가능 | customer_id (직접 식별자), date_of_birth, ip_address (준식별자) | 다음 단계에서 가명화/일반화 |

### 11-2. 가명화 vs 익명화

| 가명화 (Pseudonymization) | 익명화 (Anonymization) |
|---|---|
| 직접 식별자를 가명으로 대체 | 직접 식별자 제거 + 준식별자 일반화 |
| 테이블 간 연결 가능 | 개인으로 되돌아갈 합리적인 경로 없음 |
| 솔트, 키, 볼트로 원래 개인에게 되돌아갈 수 있음 | 재식별에 쓸 키가 없음 |
| **여전히 개인 데이터** (접근 제어, 감사, 삭제 대상) | 진정으로 익명이면 개인정보 규정 범위 밖일 수 있음 |

- 기준은 "이 테이블 하나로 식별 가능한가"가 아니라, **다른 데이터와의 결합을 포함해 누구든 합리적으로 재식별할 수 있는가**다.
- 마스킹된 쿼리 결과도 같은 이유로 익명화가 아니다.
- 증명할 수 있는 것(해시는 역산 불가, 식별자 열 제거)과 판단의 문제(데이터셋이 진정 익명인가)를 구분한다.

### 11-3. 재식별의 두 경로

| 경로 | 방식 | 차단 방법 |
|---|---|---|
| **경로 1: 키** | 솔트, 키, 볼트 매핑을 가진 사람이 가명을 원래 개인으로 되돌림 | 솔트와 키를 Secrets에, 볼트를 엄격히 관리하고 모든 역변환을 감사 |
| **경로 2: 보조 데이터 (모자이크 효과)** | 남은 준식별자(생년월일, 우편번호, 성별)를 외부 데이터와 결합해 특정 | 준식별자를 일반화 (연령대, 잘린 IP, 반올림) |

예: 우편번호, 생년월일, 성별만으로 미국 인구의 대다수가 고유하게 식별될 수 있다는 연구가 있다. 정확한 생년월일 옆의 가명은 안전하지 않다.

두 경로는 해결책이 달라 **기법을 겹쳐 쓴다.** 직접 식별자는 가명화하고, 준식별자는 일반화한다. 하나만 하면 경로가 남는다.

---

## 12. 솔트 해시, 토큰화, 암호화, 일반화

### 12-1. 비식별화 도구 비교

| 기법 | 값에 하는 일 | 복원 가능? | 연결(조인) 유지? |
|---|---|---|---|
| 억제 / 일반화 | 삭제하거나 그룹으로 축소 (연령대, /24, 반올림) | 아니오 | 아니오 |
| **솔트 해시** (`sha2`) | 단방향 결정론적 가명 | 아니오 | **예** |
| **토큰화** (볼트) | 불투명한 토큰, 매핑은 별도 볼트에 | 볼트에서만 | 예 |
| **암호화** (`aes_encrypt`) | 값 자리에 암호문 저장 | 키로만 | **아니오** |

| 필요한 것 | 기법 |
|---|---|
| 개인이 아닌 추세 | 억제/일반화 (보조 데이터 경로를 줄이는 유일한 방법) |
| 역추적 없는 연결/추적 | 솔트 `sha2` |
| 분석 환경 밖에서 권한 있는 복원 | 토큰화 |
| 제자리에서 권한 있는 복원 | `aes_encrypt` |

공통 원칙: **솔트와 키는 코드가 아니라 Databricks Secrets에 둔다.**

### 12-2. 솔트 해시와 Secrets

- `sha2(expr, bitLength)`: 224, 256, 384, 512 비트(0은 256). 16진수 문자열을 반환하며 256이면 64자다.
- 결정론적이라 같은 입력은 항상 같은 가명 → **조인 가능**.
- **솔트**는 해싱 전에 붙이는 비밀 문자열이다. 솔트가 없으면 주민번호, 이메일, 전화번호처럼 값의 범위가 작은 식별자는 모든 후보를 해시해 대조하는 **사전 공격**으로 쉽게 역추적된다.
- 결정론적 해시는 **값의 빈도를 보존**하므로 빈도 분석에 취약하다. 이것이 솔트 해시가 익명화가 아니라 가명화인 이유다.
- 두 테이블을 가명으로 조인하려면 **같은 솔트**로 해시해야 한다. 솔트를 바꾸면 모든 가명이 바뀐다(교체는 신중히 결정).

```sql
-- 나쁜 예: 솔트가 노트북 소스, 쿼리 이력, 버전 관리, 로그에 평문으로 남음
SELECT sha2(concat(cast(customer_id AS STRING), 'salt_2024_prod'), 256) AS pseudonym
FROM customer_profile;

-- 좋은 예: 솔트를 Secrets에서 쿼리 시점에만 읽음
SELECT sha2(concat(cast(customer_id AS STRING), secret('dbacademy_privacy', 'pii_salt')), 256) AS pseudonym
FROM customer_profile;
```

- `secret(scope, key)` 값은 결과나 로그에 기록되지 않고, 쿼리 이력과 로그에서 가려진다(최선의 노력 방식).
- 시크릿 범위와 값 생성(CLI):

```bash
databricks secrets create-scope dbacademy_privacy
databricks secrets put-secret dbacademy_privacy pii_salt --string-value "<랜덤 문자열>"
databricks secrets put-secret dbacademy_privacy pii_aes_key --string-value "$(openssl rand -base64 32)"
```

```sql
-- 같은 솔트로 두 분석 테이블을 만들면 가명으로 조인 가능
CREATE OR REPLACE TABLE priv_deid.customer_analytics AS
SELECT sha2(concat(cast(customer_id AS STRING), secret('dbacademy_privacy', 'pii_salt')), 256) AS customer_pseudonym,
       country, loyalty_tier, date_of_birth, ip_address
FROM priv_demo.customer_profile;

CREATE OR REPLACE TABLE priv_deid.clickstream_analytics AS
SELECT sha2(concat(cast(customer_id AS STRING), secret('dbacademy_privacy', 'pii_salt')), 256) AS customer_pseudonym,
       event_id, event_ts, page_url, device_type
FROM priv_demo.clickstream;

SELECT c.customer_pseudonym, c.country, count(k.event_id) AS events
FROM priv_deid.customer_analytics c
JOIN priv_deid.clickstream_analytics k USING (customer_pseudonym)
GROUP BY c.customer_pseudonym, c.country;
```

### 12-3. 토큰화와 볼트

해시는 되돌릴 수 없다. 고객에게 연락해야 하는 지원 담당자처럼 **권한 있는 워크플로가 원본을 다시 필요로 할 때** 토큰화를 쓴다.

| 위치 | 내용 | 접근 |
|---|---|---|
| 분석 환경 (`priv_deid.customer_contactable`) | `email_token = TKN-9F3A2C` 등 토큰만 | 분석가 |
| 볼트 (`priv_vault.email_token_vault`) | `TKN-9F3A2C → maria@example.com` 매핑 | 소수의 권한자만, 모든 읽기 감사 |

- 토큰은 값과 **수학적 관계가 없다.** 토큰에서 값을 계산할 수 없고, 유일한 연결 고리는 볼트의 행이다.
- 토큰은 할당되는 것이라 값의 빈도를 노출하지 않는다.
- 볼트가 침해되면 전체가 노출되므로 핵심 자산으로 엄격히 분리, 관리, 감사한다.
- Databricks에서는 단일 함수가 아니라 **패턴**이다: 별도 스키마/카탈로그의 조회 테이블 + 엄격한 권한 + 감사.

```sql
-- 분석 테이블에는 토큰만
CREATE OR REPLACE TABLE priv_deid.customer_contactable AS
SELECT v.email_token, p.country, p.loyalty_tier
FROM priv_demo.customer_profile p
JOIN priv_vault.email_token_vault v USING (email);

-- 역조회는 볼트 권한이 있는 사람만 가능
SELECT c.email_token, v.email AS email_recovered
FROM priv_deid.customer_contactable c
JOIN priv_vault.email_token_vault v USING (email_token);
```

### 12-4. 열 암호화 (aes_encrypt / aes_decrypt)

```sql
-- 암호화: 키는 Secrets의 base64 문자열 → unbase64로 원시 바이트로
CREATE OR REPLACE TABLE priv_deid.customer_secured AS
SELECT
  sha2(concat(cast(customer_id AS STRING), secret('dbacademy_privacy', 'pii_salt')), 256) AS customer_pseudonym,
  base64(aes_encrypt(national_id, unbase64(secret('dbacademy_privacy', 'pii_aes_key')), 'GCM')) AS national_id_ct
FROM priv_demo.customer_profile;

-- 복호화: 같은 키와 모드, 결과는 BINARY이므로 STRING으로 캐스팅
SELECT customer_pseudonym,
       cast(aes_decrypt(unbase64(national_id_ct),
                        unbase64(secret('dbacademy_privacy', 'pii_aes_key')), 'GCM') AS STRING) AS national_id_recovered
FROM priv_deid.customer_secured;
```

- `aes_encrypt(expr, key [, mode ...])`는 `BINARY`를 반환한다. 보통 `base64()`로 문자열 열에 저장한다.
- 키는 16, 24, 32바이트(32바이트 = AES-256).
- 기본 모드인 **GCM**은 데이터 인증도 하고, 호출마다 **새 무작위 IV**를 쓴다. 그래서 같은 값도 매번 다른 암호문이 된다 → **암호문으로는 조인할 수 없다.**
- 안정적인 조인 키가 필요하면 솔트 해시, 원본 복원이 필요하면 암호화.
- 키에 `secret()`을 호출하고 `aes_decrypt`를 실행할 수 있는 사람은 누구나 복원할 수 있으므로 시크릿 범위의 ACL과 복호화 경로를 통제한다.

### 12-5. 억제와 일반화

| 기법 | 전 (식별 가능) | 후 (그룹) |
|---|---|---|
| 날짜 구간화 | date_of_birth 1974-03-12 | 50대 |
| IP 잘라내기 | 203.0.113.42 | 203.0.113.0/24 |
| 숫자 반올림 | points 4,182 | 4,000 |

```sql
CREATE OR REPLACE TABLE priv_deid.customer_analytics_safe AS
SELECT
  customer_pseudonym, country, loyalty_tier,
  concat(cast(floor(datediff(current_date(), date_of_birth) / 365.25 / 10) * 10 AS INT), 's') AS age_band,
  concat(substring_index(ip_address, '.', 3), '.0/24') AS ip_prefix
FROM priv_deid.customer_analytics;
```

- **억제**: 필요 없는 열 제거, 너무 작은 그룹의 행 제거, 이상값 제외. 가장 강력하고 먼저 고려한다.
- **일반화**: 정밀도를 낮춰 값이 개인이 아니라 그룹을 나타내게 한다.
- 목표는 추세 분석 신호는 남기고 특정 개인을 지목하는 능력은 없애는 것이다.
- 가명화(직접 식별자) + 일반화(준식별자)를 함께 하면 키 경로와 보조 데이터 경로가 모두 줄어 가명 상태에서 익명에 가까운 상태로 간다.

---

## 13. 삭제 권리 1: 요청 추적과 대상 찾기

### 13-1. 완전한 삭제의 순서

추적 → 찾기 → 삭제 → 전파 → 물리적 제거(REORG PURGE) → 이전 파일 정리(VACUUM) → 증명

### 13-2. 요청 Control Plane

요청을 기록하고 완료까지 이끄는 관리 레이어다. 사용자가 직접 만드는 **설계 패턴**(예: `erasure_requests` 테이블)이며 Databricks 제품 기능이 아니다.

> 이름이 같지만 **Databricks 플랫폼 Control Plane**(웹 앱, 작업 및 쿼리 스케줄링을 담당하는 관리형 백엔드)과는 무관하다.

Control Plane은 무엇을 해야 하는지 결정하고 추적하며, 실제 데이터 변경은 `DELETE`, `MERGE`, `REORG`, `VACUUM` SQL이 수행한다.

| 필드 | 역할 |
|---|---|
| `subject_key` | 해당 인물의 **가명화된** 안정 ID. 등록부에 원시 식별자를 두지 않기 위함 |
| `request_id` | 요청당 고유 ID. **멱등성 키**이자 모든 증거를 요청에 묶는 조인 키 |
| `deadline_date` | 규정상 처리 기한 (GDPR, CCPA 기준 약 30일). 기한 위험 표시 |
| `status` | 작은 상태 머신 (received → in_progress → verified → closed). 재실행 시 완료 항목 건너뛰기 |
| `scope` | 대상 주체, 관련 데이터셋, 요청 유형 (삭제, 접근, 수정, 내보내기) |
| `affected_asset_count`, `completed_at`, `evidence_ref` | 증거 포인터 (테이블 이력, 영향 자산 수, 완료 시각) |

> 운영 필드에 **원시 PII를 넣지 않는다.** 삭제를 관리하는 테이블에 이메일이나 이름을 복사하면 목적에 어긋나고, 지워야 할 곳이 하나 더 생긴다. `subject_key`를 실제 `customer_id`로 바꾸는 것은 별도의 접근 통제된 조회 단계다.

```sql
CREATE TABLE IF NOT EXISTS priv_demo.erasure_requests (
  request_id STRING, subject_key STRING, request_type STRING, status STRING,
  received_date DATE, deadline_date DATE, scope STRING,
  affected_asset_count INT, completed_at TIMESTAMP, evidence_ref STRING
);

-- request_id 기준 멱등 등록: 다시 실행해도 행은 하나
MERGE INTO priv_demo.erasure_requests t
USING (SELECT 'DSR-2026-0412' AS request_id, 'SUBJ-7C4A9E' AS subject_key,
              'erasure' AS request_type, 'received' AS status,
              DATE'2026-08-20' AS received_date, DATE'2026-09-19' AS deadline_date,
              'customer estate: bronze (authoritative), silver (downstream)' AS scope) s
ON t.request_id = s.request_id
WHEN NOT MATCHED THEN INSERT (request_id, subject_key, request_type, status, received_date, deadline_date, scope)
VALUES (s.request_id, s.subject_key, s.request_type, s.status, s.received_date, s.deadline_date, s.scope);
```

운영 환경에서는 요청당 한 행 외에, 삭제 구문마다 **statement ID를 기록하는 작업 로그**를 함께 두어 나중에 쿼리 이력과 조인한다.

### 13-3. 분류와 리니지로 대상 찾기

| 도구 | 답하는 질문 |
|---|---|
| **분류** | 어떤 **열**이 개인 데이터인가? (삭제 대상과 사후 확인 항목) |
| **리니지** | 데이터가 어떤 **테이블**로 퍼졌는가? (복사본과 파생 테이블) |

```sql
-- 개인 데이터 열
SELECT table_name, column_name, class_tag, confidence
FROM system.data_classification.results
WHERE table_name = 'customer_profile' AND confidence = 'HIGH';

-- 테이블 체인
SELECT DISTINCT source_table_full_name, target_table_full_name
FROM system.access.table_lineage
WHERE source_table_full_name LIKE 'main.priv_demo.%';

-- 식별자 열이 도달하는 다운스트림
SELECT DISTINCT source_column_name, target_table_full_name, target_column_name
FROM system.access.column_lineage
WHERE source_table_full_name = 'main.priv_demo.customer_profile'
  AND source_column_name IN ('email', 'national_id', 'phone');
```

결과:

| 테이블 | 식별자 | 삭제 범위 |
|---|---|---|
| `customer_profile` (원본) | 있음 | 포함 |
| `customer_enriched` (실버 복사본) | 있음 (열 리니지로 확인) | 포함 |
| `customer_metrics_gold` (국가/등급 집계) | 없음 | 행 단위 삭제 불필요. 수치 정확성을 위해 재계산만 |

- 식별 불가능한 집계는 각 그룹이 충분히 커서 **어떤 행도 실질적으로 한 사람에 관한 것이 아닐 때만** 비식별 상태다.
- 시스템 테이블은 지연(분~시간)이 있으므로, 방금 만든 테이블의 리니지가 아직 없을 수 있다.

---

## 14. 삭제 권리 2: 삭제, 전파, 물리적 제거

### 14-1. 원본에서 삭제하고 다운스트림으로 전파

복사본보다 원본(가장 초기의 권위 있는 레이어)을 먼저 처리한다.

```sql
-- 원본에서 삭제
DELETE FROM priv_demo.erasure_bronze WHERE customer_id = 4212;

-- 다운스트림 전파: MERGE ... WHEN MATCHED THEN DELETE (재실행해도 안전)
MERGE INTO priv_demo.erasure_silver t
USING (SELECT 4212 AS customer_id) s
ON t.customer_id = s.customer_id
WHEN MATCHED THEN DELETE;

-- 확인: 두 테이블 모두 0행
SELECT 'bronze', count(*) FROM priv_demo.erasure_bronze WHERE customer_id = 4212
UNION ALL
SELECT 'silver', count(*) FROM priv_demo.erasure_silver WHERE customer_id = 4212;
```

- 집합 기반 삭제는 **멱등**이다. 이미 지운 대상을 다시 지우면 영향 행이 0개인 no-op이다.
- 골드 집계는 삭제된 실버로부터 다시 계산해 수치를 맞춘다.

```sql
CREATE OR REPLACE TABLE sales_by_country_gold AS
SELECT country, count(*) AS order_count, round(sum(amount), 2) AS total_amount,
       count(DISTINCT customer_id) AS distinct_customers
FROM orders_silver
GROUP BY country;
```

### 14-2. 전파 방식 선택

| 방식 | 특징 | 적합한 경우 |
|---|---|---|
| **배치 DELETE + MERGE** | 이해하고 검증하기 쉬움 | 대상과 테이블 수가 적을 때 (권장 기본) |
| **배치 Change Data Feed** | 원본의 커밋된 삭제를 한 번 읽어 여러 다운스트림에 적용 | 다운스트림 테이블이 많을 때 (팬아웃이 큼) |
| Structured Streaming + `skipChangeCommits` 등 | 삭제가 있어도 스트림이 멈추지 않게 처리 | 다운스트림이 스트리밍 파이프라인일 때. 소수 대상에는 과한 복잡도 |

```sql
-- CDF 사용: 테이블별로 활성화 후 배치로 변경 읽기
ALTER TABLE erasure_bronze SET TBLPROPERTIES ('delta.enableChangeDataFeed' = true);

SELECT customer_id, _change_type, _commit_version, _commit_timestamp
FROM table_changes('priv_demo.erasure_bronze', 5)
WHERE _change_type = 'delete';
```

`_change_type`은 `insert`, `update_preimage`, `update_postimage`, `delete` 중 하나다.

> 주의: CDF 레코드 자체도 보존 기간 동안 **이전 값을 보관**한다. 삭제된 값이 남는 또 하나의 장소다.

### 14-3. 논리적 삭제는 물리적 제거가 아니다

**Deletion Vectors(삭제 벡터)** 가 켜진 테이블에서 `DELETE`는 데이터 파일을 다시 쓰지 않는다. 어떤 행이 삭제되었는지만 작은 사이드 파일(메타데이터)에 표시하고, 읽을 때 걸러 낸다.

| 위치 | 상태 |
|---|---|
| Parquet 데이터 파일 | 대상 행(4212)이 **물리적으로 여전히 존재** |
| 삭제 벡터 | 4212를 삭제됨으로 표시 |
| 현재 테이블 상태 | 모든 쿼리에서 4212가 보이지 않음 |

- 삭제 벡터는 `DELETE`, `UPDATE`, `MERGE`를 빠르게 하려고 존재한다. 비용은 읽기 시점과 이후 파일 재작성으로 미뤄진다.
- 따라서 성공한 `DELETE`는 **현재 상태에서의 부재**만 증명할 뿐 물리적 제거가 아니다.
- 테이블에 삭제 벡터가 켜져 있는지는 가정하지 말고 테이블 속성을 직접 확인한다.

소프트 삭제 확인 방법:

| 확인 | 방법 | 소프트 삭제 신호 |
|---|---|---|
| 데이터 파일 | 전후로 `DESCRIBE DETAIL` | 파일 수와 크기가 동일 |
| 작업 로그 | `DESCRIBE HISTORY` | `numDeletedRows = 1`이지만 **`numRemovedFiles = 0`** (작은 DV 파일 때문에 `numAddedFiles`는 0이 아닐 수 있음) |

```sql
DESCRIBE DETAIL priv_demo.erasure_bronze;   -- numFiles, sizeInBytes

SELECT version, operation,
       operationMetrics['numDeletedRows']  AS numDeletedRows,
       operationMetrics['numRemovedFiles'] AS numRemovedFiles,
       operationMetrics['numAddedFiles']   AS numAddedFiles
FROM (DESCRIBE HISTORY priv_demo.erasure_bronze)
WHERE operation = 'DELETE';
```

> `DESCRIBE HISTORY`를 서브쿼리로 바로 쓸 수 없는 환경에서는 결과를 DataFrame으로 받아 필터링한다.

### 14-4. 삭제된 값이 남는 세 곳

| 위치 | 이유 | 지우는 방법 |
|---|---|---|
| 현재 데이터 파일 | 삭제 벡터로 표시만 됨 | `REORG TABLE ... APPLY (PURGE)` |
| 이전 테이블 버전 | 시간 여행으로 이전 파일을 읽을 수 있음 | 보존 기간 후 `VACUUM` |
| Change Data Feed | 변경 레코드에 이전 값 보관 | 같은 보존 기간에 만료 |

### 14-5. REORG ... APPLY (PURGE)

```sql
REORG TABLE priv_demo.erasure_bronze APPLY (PURGE);

-- 범위 지정 (대형 테이블에서 일부만 재작성)
REORG TABLE priv_demo.erasure_bronze WHERE country = 'US' APPLY (PURGE);
```

- 소프트 삭제된 데이터가 포함된 파일만 **재작성**해 삭제 벡터를 구체화한다. 현재 버전 파일에는 더 이상 삭제된 행이 없다.
- 새 테이블 버전이 생기며, `DESCRIBE HISTORY`에는 제거된 파일과 추가된 파일이 함께 표시된다.
- **이전 파일은 지우지 않는다.** 이전 파일에는 여전히 값이 있고, `VACUUM`이 지운다.
- 멱등이라 다시 실행해도 영향이 없다.
- `OPTIMIZE`나 자동 압축이 있는 쓰기도 파일을 재작성하지만, 데이터 삭제 목적에는 `REORG ... APPLY (PURGE)`를 명시적으로 쓴다.

### 14-6. 보존 기간을 고려한 VACUUM

`VACUUM`은 두 조건을 **모두** 만족하는 파일만 지운다.

1. 현재 버전에서 더 이상 참조되지 않음
2. 보존 기간(`delta.deletedFileRetentionDuration`, 기본 168시간 = 7일)보다 오래됨

```sql
VACUUM priv_demo.erasure_bronze DRY RUN;   -- 지울 파일을 미리 확인 (실제로 지우지 않음)
VACUUM priv_demo.erasure_bronze;           -- 보존 기간이 지난 뒤 실제 정리
```

- REORG 직후 `DRY RUN`이 0개 파일을 보여 주는 것은 오류가 아니다. 참조는 끊겼지만 아직 7일이 지나지 않았기 때문이다.
- **삭제 작업은 보존 기간이 지난 뒤 완료되도록 계획한다.** 시간 여행과 CDF 사본도 같은 기간이 지나면 함께 만료된다.

> 주의: 보존 기간을 0으로 낮추거나(`RETAIN 0 HOURS`, 안전 검사 비활성화) 즉시 VACUUM하는 것은 운영에서 쓰지 않는다. 보존 기간은 실행 중인 읽기, 쓰기를 보호하며, 너무 짧으면 진행 중인 작업이 아직 필요로 하는 파일을 지워 테이블이 손상될 수 있다. 명확한 이유가 있는 드문 예외로만 취급한다.

### 14-7. DESCRIBE HISTORY를 증거로 남기고 요청 종료

```sql
DESCRIBE HISTORY priv_demo.erasure_bronze;   -- DELETE, REORG(재작성), VACUUM 기록과 메트릭

MERGE INTO priv_demo.erasure_requests t
USING (SELECT 'DSR-2026-0412' AS request_id) s
ON t.request_id = s.request_id
WHEN MATCHED THEN UPDATE SET
  status = 'verified',
  affected_asset_count = 2,
  completed_at = current_timestamp(),
  evidence_ref = 'DESCRIBE HISTORY erasure_bronze, erasure_silver: DELETE, REORG PURGE; VACUUM after 168h';
```

테이블 이력은 작업이 실행되었다는 **보조 증거**이며, 완전한 프라이버시 감사는 아니다.

---

## 15. 검증: 시스템 테이블로 증거 만들기

### 15-1. 어떤 소스가 어떤 질문에 답하는가

| 소스 | 답하는 질문 | 단독으로 증명하지 못하는 것 |
|---|---|---|
| `system.data_classification.results` | 어떤 민감한 데이터가 있는가 | 데이터가 어디로 갔는지, 누가 접근했는지 |
| `system.access.column_lineage`, `table_lineage` | 데이터가 어떻게 흘렀는가 | 행이 실제로 삭제되었는지 (퍼졌을 수 있는 위치만) |
| `system.access.audit` | 누가 무엇을 언제 했는가 | 데이터가 물리적으로 지워졌는지 (작업이 기록되었다는 것만) |
| `system.query.history` | 어떤 SQL 구문이 실행되었는가 | SQL이 아닌 UI, API 작업 |
| `information_schema` | 현재 적용된 태그와 권한 | 이력 (현재 상태만) |
| `DESCRIBE HISTORY` | 한 테이블의 작업과 메트릭 | 다른 테이블, 외부 복사본, Delta가 아닌 시스템 |

- 리니지 두 테이블은 같은 질문에 세분화 수준이 다르다. 특정 식별자가 다운스트림에 도달했음을 증명하려면 열 리니지, 전체 체인은 테이블 리니지.
- 감사 로그와 쿼리 이력은 겹치지만 다르다. SQL `DELETE`는 둘 다에 나오지만, UI/API 작업은 감사 로그에만 나온다.
- 어떤 소스도 전체 감사를 혼자 담당하지 않는다. **하나의 요청에 대해 여러 소스를 나란히 정렬**해 증거를 조합한다.
- 시스템 테이블과 테이블 이력은 플랫폼에 기록된 것만 보여 준다. 플랫폼 밖으로 내보낸 파일, 스크린샷, 외부 다운스트림 시스템은 포함되지 않는다.

### 15-2. statement_text 가림 처리

| 읽는 사람 | `statement_text` |
|---|---|
| 일반 사용자 | `<REDACTED>` |
| 어카운트 관리자 또는 `databricks_pii_access` 그룹 구성원 | 실제 SQL 텍스트 |

- SQL 텍스트의 `WHERE` 절에 개인 데이터가 있을 수 있으므로 기본적으로 가려진다. `databricks_pii_access` 그룹은 관리자가 먼저 만들어야 한다.
- **`statement_id`는 가려지지 않으므로** statement ID 기준 조인은 권한과 무관하게 동작한다.
- 구문에 `/* DSR-2026-0412 */` 같은 요청 ID 주석을 붙이는 것은 유용한 레이블이지만, 텍스트가 가려질 수 있으므로 **등록부에 기록한 statement ID가 신뢰할 수 있는 연결 고리**다.

### 15-3. 소스를 연결하는 방법

모든 소스를 묶는 단일 키는 없다. 두 개의 연결로 이어 붙인다.

| 연결 | 방식 |
|---|---|
| 등록부 작업 로그 ↔ `system.query.history` | **statement_id 정확 매칭** |
| `system.query.history` ↔ `system.access.audit` | 감사의 테이블 쓰기 이벤트(`generateTemporaryTableCredential`, `operation = READ_WRITE`, `table_full_name`)에는 statement ID가 없으므로 **같은 테이블 + 같은 행위자 + 겹치는 시간**으로 상관 매칭 |

```sql
-- 요청의 구문들과 실행자
SELECT rs.statement_type, q.statement_id, q.executed_by, q.execution_status, q.start_time
FROM erasure_request_statements rs
JOIN system.query.history q ON q.statement_id = rs.statement_id
WHERE rs.request_id = 'DSR-2026-0412';

-- 감사 로그의 관리된 쓰기와 연결
SELECT rs.statement_type, rs.target_table, a.action_name,
       a.request_params['operation'] AS operation,
       a.user_identity.email AS actor, a.event_time
FROM erasure_request_statements rs
JOIN system.query.history q ON q.statement_id = rs.statement_id
JOIN system.access.audit a
  ON a.request_params['table_full_name'] = rs.target_table
 AND a.user_identity.email = q.executed_by
 AND a.event_time BETWEEN q.start_time - INTERVAL 2 MINUTES AND q.end_time + INTERVAL 2 MINUTES
WHERE rs.request_id = 'DSR-2026-0412';
```

### 15-4. 하나의 증거 행 조합

감사자의 두 질문("민감한 열이 관리되고 있는가", "삭제가 실제로 이루어졌는가")에 대한 답을 한 행으로 만든다.

| 열 | 출처 |
|---|---|
| request_id, subject_key, completion, completed_at | 요청 등록부 |
| scope_sensitive_data (예: `national_id (class.us_ssn), reaches customer_enriched`) | 분류 + 열 리니지 |
| actor | 쿼리 이력 |
| action (예: `DELETE on erasure_bronze, audited as generateTemporaryTableCredential (READ_WRITE)`) | 작업 로그 + 감사 로그 |
| subject_rows_after = 0 | 범위 내 테이블에 대한 검증 쿼리 |

### 15-5. 증거를 정직하게 읽기

| 구분 | 내용 |
|---|---|
| **증명됨** | national_id는 분류되어 현재 restricted로 태그됨. 지정된 행위자가 범위 내 테이블에 삭제를 실행하고 완료됨. 현재 대상은 범위 내 모든 테이블에서 0행 |
| **추론됨** | 리니지가 보여 준 범위가 데이터의 실제 모든 위치라는 점 (플랫폼에 기록된 흐름 기준) |
| **미결** | VACUUM 이후의 물리적 제거 완료, 플랫폼 밖 복사본 처리 |

증거가 보여 주는 것과 추론만 가능한 것을 명확히 구분한다. 감사 행 하나가 물리적 삭제를 증명한다는 식의 과장된 주장을 하지 않는다.

### 15-6. 단계별로 남는 증거

| 단계 | 증거 |
|---|---|
| 식별 | `class.*` 감지 결과, `sensitivity_level` 태그 |
| 보호 | 마스킹된 결과, `SHOW EFFECTIVE POLICIES`, 조인 가능한 가명, 테이블 속성의 근거 |
| 관리 | 빈 삭제 쿼리, `DESCRIBE HISTORY`의 DELETE/REORG/VACUUM |
| 검증 | 여러 소스를 조합한 증거 행 |

---

## 16. 관련 기능과 범위 밖의 주제

| 기능 | 내용 |
|---|---|
| 고객 관리형 키 (CMK) | 고객 키로 저장 데이터를 투명하게 암호화. 이 노트의 열 암호화는 쿼리 내 선택적 암호화 |
| 워크스페이스-카탈로그 바인딩 | 워크스페이스, 카탈로그, 클라우드 계정 격리로 환경을 구조적으로 분리 |
| Databricks Clean Rooms | 기반 행을 노출하지 않고 조직 간 승인된 분석 실행 |
| OpenSharing (Delta Sharing) | 데이터를 복사하지 않고 조직 간 라이브 데이터 공유 |
| `ai_mask` | AI Functions로 비정형 텍스트 안의 개인 데이터를 마스킹 |

---

## 17. 핵심 정리 치트시트

### 개념

| 키워드 | 핵심 |
|---|---|
| 프라이버시 | 보안, 거버넌스, 규정 준수의 교차점 + 개인 데이터와 권리 |
| 인증 vs 지원 | ISO 27001, SOC 2 Type II, HITRUST 보유 / HIPAA, DORA 지원 |
| 수명 주기 | 식별 → 보호 → 관리 → 검증. 제어는 증명할 수 있을 때 완료 |
| 의사결정 모델 | 무엇이 민감한가, 왜 필요한가, 누가 원본을 보는가, 연결이 필요한가, 어디까지 퍼졌나, 어떻게 삭제/검증하나 |

### 식별

| 키워드 | 핵심 |
|---|---|
| Data Classification | 카탈로그에서 활성화(`MANAGE`), 비동기 스캔(초기 약 15분, 이후 약 24시간), `class.*` 태그 |
| 결과 테이블 | `system.data_classification.results`, 샘플 최대 5개(민감), 필터/마스크 걸지 말 것 |
| confidence / frequency | HIGH/LOW 이산값 / 0~1 일치 비율 |
| 오탐 | LOW + 샘플 확인 → 제외(Beta). 무시하면 태그가 제어를 잘못 구동 |
| 지역 클래스 | 미국 태그는 모든 지역, 다른 국가 태그는 해당 지역에서만 |
| 사용자 정의 분류기 | 평문 설명 + 샘플 열 10개, 거버넌스 태그 직접 적용, 메타스토어당 50개, 다음 스캔부터 |

### 보호

| 키워드 | 핵심 |
|---|---|
| sensitivity_level | public / internal / confidential / restricted |
| 태그 유형 | `class.*`(시스템), 거버넌스 태그(ABAC 가능), 자유 태그(ABAC 불가) |
| ABAC 구문 | `CREATE POLICY ... ON SCHEMA ... COLUMN MASK fn TO ... EXCEPT ... FOR TABLES MATCH COLUMNS has_tag_value(k, v) AS c ON COLUMN c` |
| ABAC 장점 | 기반 테이블 보호, 대상이 정책에 분리, 새로 태그된 열 자동 포함 |
| 검증 | `SHOW EFFECTIVE POLICIES ON TABLE ...` |
| 수동 제어 | `ALTER COLUMN ... SET MASK fn`, `SET ROW FILTER fn ON (col)`, 대상 판단이 UDF 안, 태그 상한(어카운트당 1,000) 초과 시 |
| 근거 기록 | `TBLPROPERTIES ('privacy.control_justification' = ...)` |

### 비식별화

| 키워드 | 핵심 |
|---|---|
| 최소화 | 먼저. 보유하지 않은 데이터는 유출되지 않음 |
| 가명 vs 익명 | 가명은 여전히 개인 데이터 / 익명은 합리적 재식별 경로 없음 |
| 재식별 경로 | 키(Secrets, 볼트 관리로 차단) / 보조 데이터(일반화로 차단) |
| 솔트 해시 | `sha2(concat(id, secret(scope, key)), 256)`, 결정론적 → 조인 가능, 빈도 노출 |
| 토큰화 | 분석 쪽엔 토큰, 매핑은 분리된 볼트, 역조회는 감사 |
| 암호화 | `base64(aes_encrypt(v, unbase64(secret(...)), 'GCM'))`, 무작위 IV → 조인 불가, 키로 복원 |
| 일반화 | 연령대, IP /24, 반올림 |

### 관리와 검증

| 키워드 | 핵심 |
|---|---|
| 요청 Control Plane | 직접 만드는 등록부. request_id(멱등성 키), 가명 subject_key, 원시 PII 금지 |
| 찾기 | 분류(어떤 열) + 리니지(어떤 테이블). 식별 불가 집계는 재계산만 |
| 삭제/전파 | 원본 `DELETE` → 다운스트림 `MERGE ... WHEN MATCHED THEN DELETE` (멱등), 팬아웃이 크면 배치 CDF |
| 소프트 삭제 신호 | `numDeletedRows > 0`, `numRemovedFiles = 0` |
| 물리적 제거 | `REORG TABLE ... APPLY (PURGE)` (현재 파일 재작성) → 보존 기간 후 `VACUUM` (이전 파일 삭제) |
| 남는 세 곳 | 현재 파일(DV), 이전 버전(시간 여행), CDF |
| 보존 기간 | 기본 168시간. 0으로 낮추지 말고 기간 이후 완료되도록 계획 |
| 증거 소스 | 분류, 리니지, 감사, 쿼리 이력, information_schema, DESCRIBE HISTORY. 각각 단독으로는 한계 |
| 연결 | statement_id(정확) + 테이블/행위자/시간(상관) |
| statement_text | 관리자, `databricks_pii_access` 외에는 `<REDACTED>`. statement_id는 가려지지 않음 |

---

## 18. 자가 점검 문제

> 답을 가리고 먼저 생각해본 뒤 펼쳐보자.

**Q1.** 프라이버시는 보안, 거버넌스, 규정 준수와 어떻게 다른가?
<details><summary>정답</summary>

보안은 무단 접근 차단, 거버넌스는 무엇이 어디 있고 누가 책임지는지 파악, 규정 준수는 외부 의무 충족과 증명이다. 프라이버시는 이 셋이 겹치는 지점에서 개인 데이터와 개인의 권리에 초점을 맞춰, 개인 데이터를 찾고 최소화하고 보호하고 삭제하며 이를 증명하는 일이다.
</details>

**Q2.** 보안이 완벽해도 프라이버시가 충족되지 않을 수 있는 이유는?
<details><summary>정답</summary>

보안된 테이블이라도 회사 안에서 볼 필요가 없는 사람에게 개인 데이터를 보여 줄 수 있고, 삭제해야 할 데이터를 계속 보유할 수 있다. 프라이버시는 접근 제어 위에 목적과 데이터 수명 주기를 더한다.
</details>

**Q3.** Databricks가 "보유한 인증"과 "준수를 지원하는 규정"의 예를 들어라.
<details><summary>정답</summary>

보유: ISO 27001, SOC 2 Type II, HITRUST. 지원: HIPAA, DORA. 인증 목록과 범위는 바뀌므로 Trust and Compliance Center가 기준이다.
</details>

**Q4.** 식별, 보호, 관리, 검증 각 단계에서 쓰는 주요 기능은?
<details><summary>정답</summary>

식별: Data Classification, 거버넌스 태그. 보호: ABAC, 수동 마스크와 행 필터, 비식별화와 키 관리. 관리: DELETE, MERGE, REORG ... APPLY (PURGE), VACUUM. 검증: 시스템 테이블, 리니지, 테이블 이력.
</details>

**Q5.** 접근 제어가 이미 있는데 분류가 필요한 이유는?
<details><summary>정답</summary>

접근 제어는 누가 테이블을 열 수 있는지만 정하고, 어떤 열에 개인 데이터가 있는지는 알려 주지 않는다. 마스크, 필터, 가명화, 삭제는 모두 대상 열을 지정해야 하므로 찾지 못한 열은 보호할 수 없다.
</details>

**Q6.** Data Classification을 켜는 데 필요한 권한과 스캔 타이밍은?
<details><summary>정답</summary>

카탈로그에 대한 MANAGE 권한이 필요하다. 활성화하면 초기 스캔이 자동 시작되어 소규모 환경은 약 15분, 이후 새로 추가되거나 바뀐 테이블은 보통 약 24시간 안에 증분으로 처리된다. 스캔은 UI에서 트리거하며 SQL/API로 시작할 수 없다.
</details>

**Q7.** confidence와 frequency는 각각 무엇을 나타내며, HIGH + 낮은 frequency는 어떻게 해석하는가?
<details><summary>정답</summary>

confidence는 감지를 신뢰할지 나타내는 HIGH/LOW 이산값, frequency는 열에서 일치한 행의 비율(0~1)이다. HIGH + 낮은 frequency는 실제 민감 데이터지만 일부 행에만 있다는 뜻으로, 자유 텍스트 열에서 흔하다.
</details>

**Q8.** `system.data_classification.results`에 행 필터나 열 마스크를 걸면 안 되는 이유는?
<details><summary>정답</summary>

Data Classification이 이 테이블의 쓰기를 소유하므로, ALTER TABLE로 건 행 필터나 열 마스크가 스캔을 실패하게 할 수 있다(ABAC 정책은 영향 없음). 샘플 값이 민감하므로 분석에는 샘플을 뺀 사본이나 뷰를 쓴다.
</details>

**Q9.** `created_at`이 `class.date_of_birth`로 LOW 신뢰도로 감지되었다. 어떻게 처리하고, 그냥 두면 무슨 문제가 생기는가?
<details><summary>정답</summary>

샘플을 확인해 계정 생성 시각임을 확인하고 감지를 제외한다. 제외하면 태그가 제거되고 이후 스캔에서 다시 붙지 않으며 정확도 피드백이 된다. 그대로 두면 태그를 기준으로 하는 제어가 무해한 열을 마스킹하거나 필터링한다.
</details>

**Q10.** 미국 지역 워크스페이스에서 다국적 `national_id` 열의 frequency가 낮게 나오는 이유는?
<details><summary>정답</summary>

국가별 식별자 클래스는 워크스페이스가 해당 지역일 때만 감지되고(미국 태그만 예외적으로 모든 지역), 미국 형식 값만 일치하기 때문이다.
</details>

**Q11.** 사용자 정의 분류기는 무엇을 입력받고, 기본 감지와 무엇이 다른가?
<details><summary>정답</summary>

식별자의 평문 설명과 그 값이 들어 있는 샘플 열(최대 10개), 적용할 거버넌스 태그를 받는다. 기본 감지는 class.* 태그를 붙이고 사람이 매핑하지만, 사용자 정의 분류기는 선택한 거버넌스 태그를 바로 붙인다. 메타스토어 단위이며 다음 스캔부터 적용된다.
</details>

**Q12.** `class.*` 태그를 바로 정책에 쓰지 않고 `sensitivity_level`로 한 번 매핑하는 이유 두 가지는?
<details><summary>정답</summary>

자동 감지와 적용 사이에 사람의 검토 단계를 두어 오탐이 정책에 도달하지 않게 하고, 여러 class.* 클래스를 하나의 레벨로 묶어 정책 하나로 처리하기 위해서다. 플랫폼 제약이 아니라 설계 선택이다.
</details>

**Q13.** ABAC 정책에 사용할 수 있는 태그와 없는 태그는?
<details><summary>정답</summary>

거버넌스가 적용된 태그(시스템 class.* 태그, 사용자 정의 거버넌스 태그)는 has_tag, has_tag_value로 매칭할 수 있다. 자유 형식 태그는 통제된 키와 값이 없어 ABAC에 쓸 수 없다.
</details>

**Q14.** 태그 키나 값에 넣으면 안 되는 것은?
<details><summary>정답</summary>

PII나 비밀 값. 태그는 메타데이터와 시스템 테이블 전반에 노출되므로 `restricted` 같은 범주만 넣는다.
</details>

**Q15.** 보호 기법을 "값을 유지, 변환, 제거" 스펙트럼으로 분류하라.
<details><summary>정답</summary>

유지(읽을 때 제어): 마스크, 행 필터. 변환(키로 복원): 가명화, 암호화. 제거(보관하지 않음): 억제/일반화, 최소화, 삭제.
</details>

**Q16.** 태그 기반 ABAC 정책의 각 절(ON SCHEMA, TO/EXCEPT, MATCH COLUMNS, COLUMN MASK)의 역할은?
<details><summary>정답</summary>

ON SCHEMA는 범위(현재와 미래의 모든 테이블), TO/EXCEPT는 마스크를 적용할 그룹과 제외할 그룹, MATCH COLUMNS has_tag_value(...)는 열 이름이 아니라 태그로 대상 열을 선택, COLUMN MASK는 매칭된 열의 값을 UDF로 재작성한다.
</details>

**Q17.** ABAC 마스크 함수가 호출자를 확인하지 않는 "무조건적" 함수인 이유는?
<details><summary>정답</summary>

누구에게 적용할지는 정책의 TO와 EXCEPT가 결정하므로, 함수는 값 변환만 담당하면 된다. 대상 판단과 변환 로직이 분리된다.
</details>

**Q18.** 한 세션에서 다른 사용자가 무엇을 보는지 어떻게 증명하는가?
<details><summary>정답</summary>

세션은 한 신원으로만 실행되므로 내 결과는 직접 쿼리로 확인하고, 어떤 정책이 어떤 열에 걸려 있는지는 `SHOW EFFECTIVE POLICIES ON TABLE ...`로 신원과 무관하게 확인한다.
</details>

**Q19.** 새로 추가된 열을 ABAC 정책으로 보호하려면 무엇을 바꿔야 하는가?
<details><summary>정답</summary>

열에 정책이 매칭하는 태그(예: sensitivity_level = restricted)만 붙이면 된다. 정책이나 테이블, 권한은 바꿀 필요가 없다.
</details>

**Q20.** 동적 뷰 대비 태그 기반 ABAC의 장점은?
<details><summary>정답</summary>

동적 뷰는 뷰마다 로직과 대상 판단이 반복되고 뷰를 통해 읽는 사람만 보호하며 환경이 바뀔 때마다 수정해야 한다. ABAC는 정책 하나로 기반 테이블 자체를 보호하고, 대상은 TO/EXCEPT로 분리되며, 새로 태그된 열과 테이블에 자동 적용된다.
</details>

**Q21.** 수동 SET MASK / SET ROW FILTER는 언제 쓰며, ABAC와 어떤 차이가 있는가?
<details><summary>정답</summary>

거버넌스 태그 상한(어카운트당 1,000)을 넘는 대규모 환경이나 정책 범위 밖의 열에 쓴다. TO/EXCEPT가 없으므로 UDF 안에서 is_account_group_member로 호출자를 판단해야 하고, 테이블마다 반복해야 한다. 동적 뷰와 달리 기반 테이블을 보호한다.
</details>

**Q22.** 이름 기반 마스크를 ABAC로 바꾸기 전에 잘못된 태그부터 고쳐야 하는 이유는?
<details><summary>정답</summary>

태그 기반 정책으로 바꾸면 태그가 보안 경계가 된다. 기존 마스크를 지운 순간 잘못 태그된 열(예: internal로 된 national_id)은 정책에 매칭되지 않아 노출된다.
</details>

**Q23.** 가명화된 데이터가 여전히 개인 데이터인 이유는?
<details><summary>정답</summary>

솔트, 키, 볼트를 통해 원래 개인으로 되돌릴 수 있고, 남은 준식별자를 외부 데이터와 결합해 특정할 수 있기 때문이다. 따라서 접근 제어, 감사, 삭제 요청 대상에 여전히 포함된다.
</details>

**Q24.** 재식별의 두 경로와 각각의 차단 방법은?
<details><summary>정답</summary>

키 경로(솔트, 키, 볼트로 역추적)는 Secrets 보관, 볼트 관리, 역변환 감사로 차단한다. 보조 데이터 경로(준식별자를 외부 데이터와 결합하는 모자이크 효과)는 준식별자 일반화로 차단한다. 그래서 가명화와 일반화를 함께 쓴다.
</details>

**Q25.** 솔트를 하드코딩하면 안 되는 이유와 대안은?
<details><summary>정답</summary>

솔트 문자열이 노트북 소스, 쿼리 이력, 버전 관리, 로그에 남아 비밀이 아니게 되고, 공격자가 후보 ID를 같은 솔트로 해시해 매핑을 재구성할 수 있다. Databricks Secrets에 저장하고 `secret(scope, key)`로 쿼리 시점에만 읽는다.
</details>

**Q26.** 솔트 해시, 토큰화, 암호화를 복원 가능성과 조인 가능성으로 비교하라.
<details><summary>정답</summary>

솔트 해시: 복원 불가, 조인 가능(결정론적). 토큰화: 볼트로만 복원, 조인 가능. 암호화: 키로만 복원, 조인 불가(GCM이 무작위 IV를 써서 같은 값도 매번 다른 암호문).
</details>

**Q27.** `aes_encrypt`에 Secrets의 키를 넘길 때 `unbase64()`가 필요한 이유는?
<details><summary>정답</summary>

키가 Secrets에 base64 문자열로 저장되어 있고, aes_encrypt는 16/24/32바이트의 원시 키 바이트가 필요하기 때문이다. 결과도 BINARY이므로 보통 base64()로 감싸 문자열로 저장하고, 복호화 후 STRING으로 캐스팅한다.
</details>

**Q28.** 토큰화에서 볼트를 어떻게 관리해야 하는가?
<details><summary>정답</summary>

역변환 능력이 한곳에 모이므로 분석 환경과 분리된 별도 스키마나 카탈로그에 두고, 소수에게만 권한을 주며, 모든 읽기를 감사한다. 볼트가 침해되면 전체가 노출된다.
</details>

**Q29.** 요청 Control Plane은 무엇이며 Databricks 플랫폼 Control Plane과 어떻게 다른가?
<details><summary>정답</summary>

삭제 요청을 기록하고 완료까지 추적하는, 사용자가 직접 만드는 등록부(예: erasure_requests) 패턴이다. 플랫폼 Control Plane은 웹 앱과 스케줄링을 담당하는 Databricks 관리형 백엔드로, 이름만 같고 무관하다.
</details>

**Q30.** 요청 등록부에 원시 PII를 넣으면 안 되는 이유와, 멱등성을 보장하는 방법은?
<details><summary>정답</summary>

삭제를 관리하는 테이블이 또 하나의 PII 보관 장소가 되어 목적에 어긋나기 때문에 가명화된 subject_key만 저장한다. request_id를 키로 하는 MERGE로 쓰면 재실행해도 행이 중복되지 않는다.
</details>

**Q31.** 삭제 범위를 정할 때 분류와 리니지는 각각 무엇을 알려 주는가? 식별 불가능한 골드 집계는 어떻게 처리하는가?
<details><summary>정답</summary>

분류는 어떤 열이 개인 데이터인지, 리니지는 데이터가 어떤 테이블로 퍼졌는지 알려 준다. 골드 집계에 식별자가 도달하지 않았다면(열 리니지로 확인) 행 단위 삭제는 필요 없고, 수치 정확성을 위해 삭제된 실버로부터 재계산한다.
</details>

**Q32.** 다운스트림 전파에 `MERGE ... WHEN MATCHED THEN DELETE`를 쓰는 이유는? CDF는 언제 유리한가?
<details><summary>정답</summary>

집합 기반이고 멱등이라 대상이 이미 없으면 영향 행 0으로 안전하게 재실행할 수 있다. 다운스트림 테이블이 많아 하나의 커밋된 삭제를 여러 곳에 적용해야 할 때는 배치 Change Data Feed가 유리하다.
</details>

**Q33.** 삭제 벡터가 켜진 테이블에서 `DELETE`가 성공해도 물리적 제거가 아닌 이유와, 이를 확인하는 방법은?
<details><summary>정답</summary>

DELETE가 데이터 파일을 재작성하지 않고 삭제 벡터에 행을 표시만 하므로 값이 파일에 남는다. DESCRIBE DETAIL에서 파일 수와 크기가 그대로이고, DESCRIBE HISTORY에서 numDeletedRows > 0이지만 numRemovedFiles = 0이면 소프트 삭제다.
</details>

**Q34.** 삭제된 값이 남을 수 있는 세 곳과 각각을 지우는 방법은?
<details><summary>정답</summary>

현재 데이터 파일(삭제 벡터)은 REORG TABLE ... APPLY (PURGE)로, 이전 테이블 버전(시간 여행)은 보존 기간 후 VACUUM으로, Change Data Feed 레코드는 같은 보존 기간이 지나면 만료된다.
</details>

**Q35.** `REORG TABLE ... APPLY (PURGE)`가 하는 일과 하지 않는 일은?
<details><summary>정답</summary>

소프트 삭제된 데이터가 있는 파일만 재작성해 현재 버전에서 삭제된 행을 없앤다. 이전 파일은 지우지 않으므로 VACUUM이 필요하다. 멱등이며 WHERE로 범위를 제한할 수 있다.
</details>

**Q36.** REORG 직후 `VACUUM ... DRY RUN`이 0개 파일을 보여 주는 이유는? 보존 기간을 0으로 낮추면 안 되는 이유는?
<details><summary>정답</summary>

VACUUM은 참조되지 않으면서 보존 기간(기본 168시간)보다 오래된 파일만 지우는데, 이전 파일은 방금 참조가 끊겨 아직 오래되지 않았기 때문이다. 보존 기간은 실행 중인 읽기와 쓰기를 보호하므로 너무 짧으면 진행 중인 작업이 필요한 파일을 지워 테이블이 손상될 수 있다. 삭제는 기간이 지난 뒤 완료되도록 계획한다.
</details>

**Q37.** 각 증거 소스(분류, 리니지, 감사, 쿼리 이력, information_schema, DESCRIBE HISTORY)가 단독으로 증명하지 못하는 것은?
<details><summary>정답</summary>

분류는 이동 경로와 접근자를, 리니지는 실제 삭제 여부를, 감사는 물리적 삭제를, 쿼리 이력은 UI/API 작업을, information_schema는 이력을, DESCRIBE HISTORY는 다른 테이블과 외부 복사본을 증명하지 못한다. 그래서 여러 소스를 하나의 요청에 대해 함께 정렬한다.
</details>

**Q38.** 쿼리 이력과 감사 로그는 어떻게 연결하는가? statement_text가 `<REDACTED>`일 때도 동작하는 연결은?
<details><summary>정답</summary>

등록부 작업 로그와 쿼리 이력은 statement_id로 정확히 조인하고, 쿼리 이력과 감사의 테이블 쓰기 이벤트(generateTemporaryTableCredential)는 statement ID가 없으므로 같은 테이블, 같은 행위자, 겹치는 시간으로 매칭한다. statement_id는 가려지지 않으므로 권한과 무관하게 동작한다.
</details>

**Q39.** 증거 행이 "증명하는 것"과 "아직 미결인 것"을 구분하라.
<details><summary>정답</summary>

증명: 민감한 열이 분류되어 restricted로 태그됨, 지정된 행위자가 범위 내 테이블에 삭제를 실행함, 현재 대상이 범위 내 모든 테이블에서 0행. 미결: VACUUM 이후 물리적 제거 완료, 플랫폼 밖 복사본 처리. 증거 행은 강력하지만 전체 감사 그 자체는 아니다.
</details>

---

## 19. 참고 링크

- Data Classification: https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification
- 지원되는 분류 태그: https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification-tags
- 사용자 정의 분류기: https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification-custom-classifiers
- Data Classification 시스템 테이블: https://docs.databricks.com/aws/en/admin/system-tables/data-classification
- 거버넌스 태그: https://docs.databricks.com/aws/en/admin/governed-tags
- Unity Catalog 태그: https://docs.databricks.com/aws/en/database-objects/tags
- ABAC: https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/
- ABAC 모범 사례: https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/best-practices
- 행 필터와 열 마스크: https://docs.databricks.com/aws/en/tables/row-and-column-filters
- 시크릿 관리: https://docs.databricks.com/aws/en/security/secrets/
- secret 함수: https://docs.databricks.com/aws/en/sql/language-manual/functions/secret
- aes_encrypt: https://docs.databricks.com/aws/en/sql/language-manual/functions/aes_encrypt
- aes_decrypt: https://docs.databricks.com/aws/en/sql/language-manual/functions/aes_decrypt
- sha2: https://docs.databricks.com/aws/en/sql/language-manual/functions/sha2
- GDPR 준수를 위한 데이터 준비: https://docs.databricks.com/aws/en/ldp/gdpr
- 삭제 벡터: https://docs.databricks.com/aws/en/delta/deletion-vectors
- REORG TABLE: https://docs.databricks.com/aws/en/sql/language-manual/delta-reorg-table
- VACUUM: https://docs.databricks.com/aws/en/sql/language-manual/delta-vacuum
- DESCRIBE HISTORY: https://docs.databricks.com/aws/en/sql/language-manual/delta-describe-history
- Change Data Feed: https://docs.databricks.com/aws/en/delta/delta-change-data-feed
- 리니지 시스템 테이블: https://docs.databricks.com/aws/en/admin/system-tables/lineage
- 감사 로그 시스템 테이블: https://docs.databricks.com/aws/en/admin/system-tables/audit-logs
- 쿼리 이력 시스템 테이블: https://docs.databricks.com/aws/en/admin/system-tables/query-history
- 고객 관리형 키: https://docs.databricks.com/aws/en/security/keys/customer-managed-keys
- 워크스페이스-카탈로그 바인딩: https://docs.databricks.com/aws/en/catalogs/binding
- Clean Rooms: https://docs.databricks.com/aws/en/clean-rooms/
- ai_mask: https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_mask
- Security and Trust Center: https://www.databricks.com/trust
