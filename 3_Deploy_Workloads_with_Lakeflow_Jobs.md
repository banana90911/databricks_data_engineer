# Lakeflow Jobs로 워크로드 배포하기 — 핵심 개념 정리

---

## 목차

1. [Lakeflow Jobs 개요](#1-lakeflow-jobs-개요)
2. [작업(Job)과 태스크(Task)](#2-작업job과-태스크task)
3. [태스크 유형과 구성 옵션](#3-태스크-유형과-구성-옵션)
4. [Compute 선택](#4-compute-선택)
5. [DAG와 워크플로 패턴](#5-dag와-워크플로-패턴)
6. [파라미터, 태스크 값, 동적 값 참조](#6-파라미터-태스크-값-동적-값-참조)
7. [알림과 재시도 정책](#7-알림과-재시도-정책)
8. [트리거와 스케줄링](#8-트리거와-스케줄링)
9. [Run if 조건부 의존성](#9-run-if-조건부-의존성)
10. [If/Else 조건 태스크](#10-ifelse-조건-태스크)
11. [For Each 반복 태스크](#11-for-each-반복-태스크)
12. [종합 예시: 동적 소매 데이터 파이프라인](#12-종합-예시-동적-소매-데이터-파이프라인)
13. [실행 상태와 Repair Run](#13-실행-상태와-repair-run)
14. [성능 모니터링: 시스템 테이블과 Spark UI](#14-성능-모니터링-시스템-테이블과-spark-ui)
15. [운영 환경 모범 사례](#15-운영-환경-모범-사례)
16. [작업을 코드로 정의하기](#16-작업을-코드로-정의하기)
17. [자가 점검 문제](#17-자가-점검-문제)
18. [참고 링크](#18-참고-링크)

---

## 1. Lakeflow Jobs 개요

### 1-1. Databricks 데이터 엔지니어링 스택에서의 위치

| 계층 | 구성 요소 |
|---|---|
| 스토리지 | Delta Lake / Parquet / Iceberg |
| 거버넌스 | Unity Catalog (접근 제어, 감사, 리니지, 품질 모니터링, 데이터 탐색) |
| 처리 엔진 | Apache Spark + Structured Streaming (Photon) |
| 데이터 엔지니어링 | **Lakeflow**: Connect(수집) / Spark Declarative Pipelines(변환) / **Jobs(오케스트레이션)** |

**Lakeflow Jobs**는 Databricks의 워크플로 자동화 도구다. 데이터 처리 작업과 워크플로를 오케스트레이션하고, 복잡한 워크플로 안에서 여러 작업을 조율하며, 반복 가능한 프로세스의 스케줄링·최적화·관리를 지원한다. 이전 이름은 **Databricks Workflows**다.

- 노트북, SQL 쿼리, 대시보드, 파이프라인 등 **Databricks 안의 모든 유형의 워크로드**를 하나의 작업으로 오케스트레이션할 수 있다.

### 1-2. 외부 오케스트레이터의 문제

선택지: Apache Airflow, Prefect, Dagster, dbt 같은 오픈 소스 / 클라우드 네이티브 서비스(AWS, Azure, GCP) / 사내 커스텀 프레임워크

| 문제 | 내용 |
|---|---|
| 생산성 저하 | 많은 실무자가 사용하기 어렵다 |
| 다운스트림 가치 하락 | 불량 데이터가 흘러가도 근본 원인 파악이 어렵다 |
| 높은 소유 비용, 낮은 안정성 | 관리·유지가 복잡한 아키텍처 |
| **레이크하우스와 통합되지 않음** | 통합 문제, 데이터 사일로 발생 |

### 1-3. Lakeflow Jobs의 이점과 아키텍처

**Data Intelligence Platform에서 데이터·분석·AI를 위한 통합 오케스트레이션**

| 주요 이점 | 의미 |
|---|---|
| 간편한 작성 | UI, API, SDK, 번들 등 다양한 방식으로 손쉽게 작업 정의 |
| 실행 가능한 인사이트 | 실행 이력, 모니터링, 알림으로 문제를 빠르게 파악 |
| 검증된 신뢰성 | 플랫폼 내장. 수집·변환·Photon·Unity Catalog·Delta Lake·DW·ML과 원활히 통합 |

아키텍처 구성:
- 중심: 모든 것을 조율하는 **워크플로 엔진**
- **Compute 레이어**: ETL, ML/AI, 분석/BI 등 다양한 워크로드 지원
- **트리거**: Scheduled(시간 기반), Continuous(상시), File Arrival(이벤트 기반), Table Update
- **Observability**: 모니터링과 문제 해결
- **Control Flow**: 태스크 의존성과 실행 순서 관리

---

## 2. 작업(Job)과 태스크(Task)

| 개념 | 정의 |
|---|---|
| **작업 (Job)** | 데이터 처리, ETL, 분석, ML 워크로드를 **예약·조율·실행하는 기본 리소스**. 전체 워크플로를 담는 컨테이너 |
| **태스크 (Task)** | 작업 안에서 노트북, 스크립트, 쿼리 등 **특정 워크로드를 실행하는 단일 작업 단위** |

- 관계는 **계층적**이다: 하나의 작업은 **하나 이상의 태스크**로 구성된다.
- 작업 수준 설정: 이름, 작업 파라미터, 트리거/스케줄, 작업 수준 알림, 성능 모드(서버리스), 실행 주체(Run as), 최대 동시 실행 수 등
- 태스크 수준 설정: 태스크 유형, 소스/경로, compute, 의존성, Run if 조건, 태스크 파라미터, 라이브러리, 재시도, 타임아웃, 태스크 수준 알림 등

### 2-1. 하나의 작업이 오케스트레이션하는 것

| 요소 | 내용 |
|---|---|
| 태스크 | 하나 이상의 실행 단위 |
| 제어 흐름 | 순차, 병렬, If/Else 조건, Run Job(다른 작업 실행), For Each 반복 |
| 트리거 | 수동, 스케줄(cron), API, 파일 도착, 테이블 업데이트, 연속 |
| Compute | 위의 모든 패턴을 지원하는 실행 자원 |

---

## 3. 태스크 유형과 구성 옵션

### 3-1. 태스크 유형

| 유형 | 용도 |
|---|---|
| **Notebook** | Databricks 노트북 (Python, SQL, Scala, R) |
| **Python script** | `.py` 파일 |
| **Python wheel** | 패키징된 Python 코드 |
| **SQL** | SQL 쿼리, SQL 파일, 대시보드, 알림(Alert). SQL Warehouse에서 실행 |
| **Pipeline** | Spark Declarative Pipelines(구 DLT) 파이프라인 갱신 |
| **dbt** | dbt 프로젝트 실행 |
| **JAR** | Java/Scala JAR |
| **Spark Submit** | 레거시 Spark 애플리케이션 |
| **Dashboard** | AI/BI 대시보드 갱신 및 구독자에게 스냅샷 전송 |
| **Power BI** | Power BI 시맨틱 모델 갱신 연동 |
| **Run Job** | 다른 작업을 하위 작업으로 실행 (모듈식 설계) |
| **If/else condition** | 조건 분기 (제어 흐름 태스크) |
| **For each** | 입력 배열 반복 (제어 흐름 태스크) |

- 지원 언어: Python, SQL, Scala, R, Java(JAR)

### 3-2. 공통 구성 옵션

옵션은 **태스크 유형에 따라 달라진다.**

| 옵션 | 설명 |
|---|---|
| 경로 / 소스 | 실행할 코드 위치. Workspace 또는 **Git 공급자** |
| Compute | 서버리스, 작업 클러스터, All-purpose 클러스터, SQL Warehouse(SQL 태스크) |
| 라이브러리 | 태스크 실행에 필요한 패키지 |
| 파라미터 | 키-값 쌍 전달 (6장) |
| 의존성 (Depends on) | 먼저 끝나야 하는 상위 태스크 |
| Run if 조건 | 상위 태스크 결과에 따른 실행 조건 (9장) |
| 알림 | 시작/성공/실패 등 이벤트 알림 (7장) |
| 재시도 | 실패 시 재시도 횟수와 간격 (7장) |
| 타임아웃 / 기간 경고 | 실행 시간 제한, 임계값 초과 경고 |

| 노트북 태스크의 주요 설정 | SQL 태스크의 주요 설정 |
|---|---|
| 태스크 이름, 소스(Workspace/Git), 노트북 경로, compute, 파라미터 | 태스크 이름, SQL 유형(Query/File/Dashboard/Alert), 쿼리 선택, **SQL Warehouse** |

---

## 4. Compute 선택

### 4-1. Compute 옵션 비교

| Compute | 특징 | 적합한 용도 |
|---|---|---|
| **Interactive (All-purpose) 클러스터** | 여러 사용자가 공유. 작업이 없어도 켜져 있으면 비용 발생. 리소스 경합·가용성 문제 | 개발, 임시 분석, 탐색. **운영 작업에는 비권장** |
| **Job 클러스터** | 작업 시작 시 생성, **작업 종료 시 함께 종료** → All-purpose 대비 약 50% 저렴. 작업 전용 리소스로 예측 가능한 성능. 단, **클라우드 공급자의 VM 시작 시간**만큼 지연 | 운영 워크로드 |
| **서버리스** | 완전 관리형. VM 유형 선택 불필요, DevOps 인력 불필요. **Photon 기본 활성화**. VM이 Databricks 계정에서 실행되어 빠르게 시작. 자동 확장. 클라우드 장애로부터 보호 | 대부분의 운영 워크로드에 최적 |
| **SQL Warehouse** | SQL 쿼리·대시보드·BI 전용 (기본 서버리스). Intelligent Workload Management로 높은 동시성 + 자동 확장. 자동 시작/중지, 클러스터 크기·최대 클러스터 수 조정 | SQL 태스크, 대시보드 태스크 |

- 하나의 작업 안에서 **태스크마다 다른 compute**를 지정할 수 있다. (예: Task-1은 All-purpose, Task-2는 서버리스, Task-3은 Job 클러스터)
- 같은 작업의 여러 태스크가 **하나의 Job 클러스터를 공유(재사용)**하면 시작 시간과 비용을 줄일 수 있다.
- All-purpose 클러스터를 작업에 지정하면 All-purpose 요금이 청구된다. 운영 작업은 워크로드에 맞는 크기의 Job 클러스터나 서버리스로 실행한다.

### 4-2. 서버리스 작업의 성능 모드

작업 수준 설정(Performance optimized)으로 고른다. **서버리스 compute를 쓰는 태스크에만 적용**된다.

| 모드 | 특징 | 적합한 워크로드 |
|---|---|---|
| **Standard** (Performance optimized 끔) | 비용 효율 중심. 시작 시간이 길다 (보통 4~6분, 클래식 인프라와 비슷) | 긴급하지 않고 시간 여유가 있는 워크로드 |
| **Performance optimized** (켬) | 빠른 compute 시작과 실행 | 시간에 민감한 워크로드 |

### 4-3. 요금 구조: 클래식 vs 서버리스

| | 클래식 | 서버리스 |
|---|---|---|
| Databricks 비용 | DBU | DBU (**인프라·운영 비용 포함 단일 청구**) |
| 클라우드 공급자 비용 | 클러스터 VM, 네트워크(방화벽/NAT) 등 별도 지불 | 없음 (DBU에 포함) |
| 조직 내부 운영 비용 | 보안·활용도 모니터링, 인프라 배포·자동화·유지 관리, 비용·효율 관리 인력 시간 | 크게 감소 |
| 결론 | 비용 요소가 여러 곳에 흩어짐 | DBU 단가는 높을 수 있지만 **총 소유 비용(TCO)이 낮은 경우가 많음** |

---

## 5. DAG와 워크플로 패턴

### 5-1. DAG (Directed Acyclic Graph, 방향성 비순환 그래프)

데이터 처리 흐름을 포함한 일련의 활동을 개념적으로 표현한 것.

| 단어 | 의미 |
|---|---|
| **Directed** (방향성) | 각 간선(edge)에 명확한 방향이 있다 |
| **Acyclic** (비순환) | 순환(cycle)이 없다. 다시 자기 자신으로 돌아오지 않는다 |
| **Graph** (그래프) | 간선으로 연결된 정점(vertex)들의 모음 |

- 태스크 의존성(Depends on)을 설정하면 작업 안의 실행 순서가 정해지고, 그것이 곧 DAG가 된다.
- 태스크 오케스트레이션은 **UI, REST API, SDK, Declarative Automation Bundles** 중 어떤 방식으로도 정의할 수 있다.

예시:

| 태스크 | 의존 대상 | 실행 시점 |
|---|---|---|
| Task-1 | 없음 | 즉시 |
| Task-2 | Task-1 | Task-1 완료 후 |
| Task-3 | Task-1 | Task-1 완료 후 (Task-2와 **병렬**) |
| Task-4 | Task-2, Task-3 | 둘 다 완료 후 |

- 의존성이 없는 태스크들은 **동시에(병렬로)** 시작한다.

### 5-2. 일반적인 워크플로 패턴

| 패턴 | 구조 | 용도 |
|---|---|---|
| **Sequence (순차)** | A → B → C | 데이터 변환·처리·정제, Bronze → Silver → Gold |
| **Funnel (퍼널)** | 여러 태스크 → 하나의 태스크 | 여러 데이터 소스를 모아 통합 |
| **Fan-out (팬아웃, Star)** | 하나의 태스크 → 여러 태스크 | 단일 소스를 여러 다운스트림으로 분배 |

---

## 6. 파라미터, 태스크 값, 동적 값 참조

태스크 구성 옵션의 세 가지 핵심 범주는 **파라미터 & 동적 값 참조, 알림, 재시도**다.

### 6-1. 태스크 파라미터 vs 작업 파라미터

| | 태스크 파라미터 | 작업 파라미터 |
|---|---|---|
| 정의 위치 | 개별 태스크 | 작업 |
| 형태 | 키-값 쌍 (또는 JSON 배열) | 키-값 쌍 |
| 적용 범위 | 해당 태스크만 | **작업 내 모든 태스크에 자동 전달** |
| 같은 키가 있을 때 | — | **작업 파라미터가 태스크 파라미터보다 우선** |
| 런타임 재정의 | — | 실행 시 다른 값으로 재정의 가능 ("다른 설정으로 지금 실행", API 등) |
| 주 용도 | 태스크별 경로, 처리 옵션, 기본값 | 카탈로그·스키마 이름, 환경(dev/prod), 처리 날짜 등 공통 설정 |

- 우선순위 정리: 태스크 파라미터는 **태스크별 기본값**, 작업 파라미터는 **작업 전체에 일관되게 적용되는 중앙 집중식 재정의**다.
- 파라미터로 가능한 것: **조건부 실행**(어느 분기를 탈지), **반복 처리**(For Each 입력 배열), **컨텍스트 전달**(다운스트림 태스크가 읽을 값)

**노트북 태스크에서 파라미터 읽기**

```python
# 태스크 파라미터와 작업 파라미터 모두 위젯으로 전달된다 (같은 키면 작업 파라미터 값이 반환됨)
my_catalog = dbutils.widgets.get("catalog")
my_schema  = dbutils.widgets.get("schema")

my_volume_path = f"/Volumes/{my_catalog}/{my_schema}/trigger_storage_location/"
```

- 파라미터를 읽는 방법은 태스크 유형(노트북, SQL, Python wheel, JAR 등)마다 다르다.
- 하드코딩 대신 파라미터를 쓰면 같은 코드를 여러 환경·데이터에 재사용할 수 있다.

### 6-2. 태스크 값 (Task Values)

**작업 실행 중에 태스크가 만들어 다른 태스크와 공유하는 동적 키-값 쌍**이다.

```python
# 업스트림 태스크: 계산 결과를 저장
df = spark.sql("SELECT * FROM customers_sales_silver")
duplicate_exists = df.count() > df.dropDuplicates().count()
dbutils.jobs.taskValues.set(key="has_duplicates", value=duplicate_exists)

# 다운스트림 태스크: 특정 업스트림 태스크의 값을 읽기
has_dup = dbutils.jobs.taskValues.get(
    taskKey="customers_sales_summary",   # 값을 설정한 태스크 이름
    key="has_duplicates",
    default=False,                       # 값이 없을 때 기본값
    debugValue=False                     # 작업 밖(노트북 대화형 실행)에서 실행할 때 반환할 값
)
```

| 비교 | 파라미터 | 태스크 값 |
|---|---|---|
| 결정 시점 | 실행 **전** (설정값) | 실행 **중** (계산 결과) |
| 용도 | 구성, 환경 설정 | 레코드 수, 처리 상태, 생성된 파일 경로, 데이터 품질 판정 결과 |
| 의미 | 정적 입력 | 데이터 기반의 태스크 간 명시적 의존성 |

- 값은 JSON으로 직렬화 가능한 형태여야 한다 (문자열, 숫자, 불리언, 리스트, 딕셔너리).

### 6-3. 동적 값 참조 (Dynamic Value References)

`{{ }}` 표기법으로 **런타임 컨텍스트 값**을 파라미터나 조건에 넣는다.

| 참조 | 의미 |
|---|---|
| `{{job.id}}`, `{{job.name}}` | 작업 ID, 작업 이름 |
| `{{job.run_id}}` | 작업 실행 ID (추적·로깅) |
| `{{job.start_time.day}}`, `{{job.start_time.iso_date}}` | 실행 시작 시각의 일(day), ISO 날짜 |
| `{{job.trigger.type}}` | 실행을 시작한 트리거 유형 |
| `{{job.parameters.<이름>}}` | 작업 파라미터 값 |
| `{{task.name}}` | 현재 태스크 이름 |
| `{{task.retry_count}}` | 현재 태스크 재시도 횟수 |
| `{{tasks.<태스크명>.values.<키>}}` | 업스트림 태스크가 설정한 **태스크 값** |
| `{{input}}` | For Each 반복에서 현재 원소 |

```
# 예: 업스트림 태스크가 만든 출력 경로를 다음 태스크 파라미터로 전달
output_path = {{tasks.file-processor.values.output_path}}

# 예: 처리 날짜를 실행 시작일로
process_date = {{job.start_time.iso_date}}
```

---

## 7. 알림과 재시도 정책

### 7-1. 알림 (Notifications)

| 수준 | 동작 | 적합한 대상 |
|---|---|---|
| **작업 수준** | 작업 전체의 시작/성공/실패 등에 대해 알림 | 일별 보고서를 기다리는 비즈니스 사용자, 작업 결과에 의존하는 다운스트림 시스템 |
| **태스크 수준** | 태스크마다 다른 알림 구성 가능 | 태스크별 담당자가 다를 때 (수집 태스크 → 데이터 엔지니어링 팀, 보고 태스크 → 비즈니스 담당자) |

| 항목 | 내용 |
|---|---|
| 대상 채널 | **이메일, Microsoft Teams, PagerDuty, Slack, Webhook** |
| 이벤트 | **시작, 성공, 실패**, **기간 임계값 초과(duration warning)/타임아웃**, **스트리밍 백로그**(스트리밍 데이터 처리 지연) |
| Webhook | 서드파티 API와의 커스텀 통합 |

- 설계 원칙: 기술적 편의가 아니라 **운영 요구**에 맞춘다. 누가, 무엇을, 언제 알아야 하고, 알림을 받고 어떤 조치를 할 수 있는지를 기준으로 정한다.

### 7-2. 재시도 정책 (Retry Policy)

**실패한 실행을 언제, 몇 번 다시 시도할지** 정하는 정책. 작업·태스크 수준에서 설정하며, 일시적 오류에 대한 첫 번째 방어선이다.

| 설정 | 의미 |
|---|---|
| 재시도 횟수 | 실패 시 최대 몇 번 다시 시도할지 |
| 재시도 간 최소 간격 | 다음 시도까지 대기 시간 |
| 타임아웃 시 재시도 | 타임아웃으로 실패한 경우도 재시도할지 |

**고려 사항**

| 요소 | 판단 기준 |
|---|---|
| 오류 유형 | 일시적 네트워크 문제 → 즉시 재시도 적절 / 데이터 품질 문제 → 재시도해도 소용없음 |
| 리소스 영향 | 무거운 작업을 과도하게 재시도하면 리소스 경합 |
| 다운스트림 의존성 | 실패한 작업이 다른 워크플로에 영향을 주므로 재시도 타이밍이 중요 |
| 비즈니스 SLA | 엄격한 마감이 있으면 재시도할 수 있는 시간도 제한됨 |

- 서버리스 태스크에는 **자동 최적화(최대 3회 재시도 포함)** 옵션이 기본으로 켜져 있을 수 있다. 실패를 즉시 확인해야 하는 경우에는 이 옵션을 끈다.

---

## 8. 트리거와 스케줄링

**트리거**는 특정 조건이나 일정에 따라 작업 실행을 **자동으로 시작하는 규칙**이다. 사람의 개입 지점을 없애 지연, 오류, 실행 누락을 줄이고 24/7 일관된 운영을 가능하게 한다.

### 8-1. 트리거 유형 한눈에 보기

| 트리거 | 동작 | 적합한 상황 |
|---|---|---|
| **Manual (없음)** | 필요할 때 직접 실행 | 개발·테스트, 디버깅, 일회성 백필 |
| **Scheduled** | 정해진 시간·간격 (cron) | 매일 ETL, 주간 보고서 등 예측 가능한 반복 작업 |
| **File arrival** | 지정 위치에 **새 파일**이 생기면 실행 | 불규칙하게 도착하는 파일 (파트너 피드, IoT, 로그) |
| **Table update** | 지정 테이블이 **변경**되면 실행 | 상위 테이블 갱신 직후 처리 (cron 대체) |
| **Continuous** | 이전 실행이 끝나거나 실패하면 즉시 새 실행 | 스트리밍 워크로드 |
| **Model update** | Unity Catalog의 모델이 변경되면 실행 | ML 모델 배포·평가 자동화 |

### 8-2. Manual 트리거

- **Run now**: 현재 설정으로 즉시 실행
- **Run now with different settings**: 런타임에 **파라미터를 재정의**해서 실행
- 프로그래밍 방식: **REST API, CLI, SDK, Declarative Automation Bundles**로 외부 시스템에서 실행
- 운영 용도: 정상 일정 밖의 **백필**, 긴급 처리, 데이터 품질 문제 해결 후 **특정 기간 재처리**, 자동화 전 동작 검증
- 다른 트리거와 함께 쓸 수 있다 (트리거가 있어도 수동 실행 가능).

### 8-3. Scheduled 트리거

- **Simple**: 매시간·매일·매주 같은 주기 선택
- **Advanced**: **cron 문법**(Quartz cron)으로 정밀하게 지정
- **시간대(time zone)를 반드시 지정**한다. 여러 지역에서 운영하는 조직은 특히 중요하다.

Quartz cron 형식: `초 분 시 일 월 요일 [연도]`

| 표현식 | 의미 |
|---|---|
| `0 0 6 * * ?` | 매일 06:00 |
| `0 */15 9-18 ? * MON-FRI` | 평일 09~18시, 15분마다 |
| `0 0 7 ? * MON` | 매주 월요일 07:00 |
| `0 0 0 1 * ?` | 매월 1일 00:00 |

- `?`는 "지정 안 함"으로, 일(day-of-month)과 요일(day-of-week) 중 하나에 쓴다.

### 8-4. File arrival 트리거

- 지원 위치: **AWS S3, Azure Storage, Google Cloud Storage, Unity Catalog 볼륨** (Unity Catalog의 외부 위치 또는 볼륨이어야 함)
- 기본적으로 **약 1분마다** 위치를 확인한다.
- **새 파일만** 실행을 유발한다. **수정된 파일은 다시 트리거하지 않는다** (재트리거하려면 새 이름의 파일이어야 함).
- 고급 설정:
  - **트리거 간 최소 시간**: 연속 실행 사이 최소 간격
  - **마지막 변경 후 대기 시간**: 파일이 다 올라올 때까지 기다렸다가 실행 (여러 파일이 연달아 도착할 때)
- 파일 패턴을 맞춰 관련 파일만 처리하고 임시·불완전 업로드는 무시하도록 설계한다.
- 모니터링 위치에 들어 있는 파일 수에 제한이 있으므로 공식 문서의 제한 사항을 확인한다.
- 시간 기반에서 **이벤트 기반 처리**로의 전환: 다음 예약 시각을 기다리지 않고 데이터가 오면 바로 처리한다.

### 8-5. Table update 트리거

| 설정 | 내용 |
|---|---|
| 대상 테이블 | 트리거당 **최대 10개** 테이블 |
| 지원 테이블 | Unity Catalog 관리형 Delta·Iceberg 테이블, 외부 UC 테이블, Materialized View, Streaming Table |
| 감지하는 변경 | INSERT, UPDATE, DELETE, MERGE |
| 트리거 조건 | **Any table updated**: 하나라도 바뀌면 실행 / **All tables updated**: 선택한 테이블이 모두 갱신된 뒤 실행 |
| 트리거 간 최소 시간 | 자주 갱신되는 테이블 때문에 작업이 연달아 과도하게 실행되는 것 방지 |
| 마지막 변경 후 대기 | 데이터가 여러 배치로 들어올 때 마지막 갱신 후 일정 시간 기다렸다 실행 |

- cron 일정 대신 **새 데이터가 들어온 즉시** 실행되므로 데이터 최신성이 높아지고, 데이터가 없는데 도는 불필요한 compute 낭비가 줄어든다.

### 8-6. Continuous 트리거

- 이전 실행이 **완료되거나 실패하는 즉시** 새 실행을 시작해 작업이 계속 돌아간다.
- 재시도 로직이 **Databricks에 의해 자동 관리**된다.
- 용도: 실시간 분석(클릭스트림), 사기 탐지, IoT 처리, 데이터베이스 변경 스트림(CDC) 처리
- 끝나지 않고 계속 실행되는 작업이므로 완료 여부가 아니라 **지연·백로그 중심으로 모니터링**해야 한다.

### 8-7. 주의

- 스케줄·트리거를 테스트용으로 걸어두었다면 사용 후 **반드시 일시 중지하거나 삭제**한다. 그렇지 않으면 계속 실행되며 비용이 발생한다.

---

## 9. Run if 조건부 의존성

**업스트림 태스크의 결과에 따라** 이 태스크를 실행할지 결정하는 조건.

| 조건 | 실행되는 경우 | 활용 |
|---|---|---|
| **All succeeded** (기본) | 모든 상위 태스크 성공 | 일반적인 순차 처리 |
| **At least one succeeded** | 상위 중 하나 이상 성공 | 중복(이중화) 데이터 소스·처리 경로 중 하나만 성공해도 진행 |
| **None failed** | 실패한 상위가 없음 (**건너뛴 것은 허용**) | If/Else 분기 뒤 합류 지점 |
| **All done** | 성공·실패와 관계없이 모두 끝남 | 정리(cleanup), 로깅 |
| **At least one failed** | 하나 이상 실패 | 실패 알림, 보상(롤백) 처리 |
| **All failed** | 모두 실패 | 전체 실패 대응 |

예시: Task-4가 Task-2, Task-3에 의존하고 조건이 **At least one succeeded**라면, Task-3이 실패해도 Task-2가 성공하면 Task-4는 실행된다.

**효과**
- 한 부분의 실패가 전체를 멈추는 **연쇄 실패 방지**, 워크플로 복원력 강화
- 여러 성공 경로가 있는 실제 비즈니스 프로세스를 그대로 표현

**실행 흐름**
1. 의존성이 없는 태스크들이 **동시에** 시작된다.
2. 태스크가 끝날 때마다(성공이든 실패든) 시스템이 각 다운스트림 태스크의 조건을 **재평가**한다.
3. 조건을 만족한 태스크가 시작된다. 실행 경로가 미리 고정되지 않고 실제 결과에 따라 달라진다.

- 작업 DAG 그림에서 의존성 유형은 선 모양·색으로 구분되어, 장애 시 영향받은 태스크와 계속 실행 가능한 태스크를 바로 파악할 수 있다.

---

## 10. If/Else 조건 태스크

워크플로 안에 **불리언 조건 로직**을 넣어, 성공/실패를 넘어서는 **데이터 기반 분기**를 만든다.

| 항목 | 내용 |
|---|---|
| 비교 연산자 | `==`, `!=`, `>`, `>=`, `<`, `<=` |
| 비교 대상 | 태스크 값, 파라미터 값, 동적 값 참조, 상수 |
| 결과 | **True 분기**와 **False 분기**. 각 분기에는 여러 태스크를 연결할 수 있다 |
| 평가 조건 | 의존 태스크 중 **실패한 것이 없고 최소 하나가 실행된 경우**에만 평가 (의미 있는 업스트림 결과가 있을 때만) |

**활용 예**
- 데이터 품질 게이트: 레코드 수, null 비율, 검증 결과로 분기
- 처리 규모 결정: 대용량/소용량 데이터에 서로 다른 전략
- 환경별 로직: 환경 파라미터에 따라 다른 태스크
- 비즈니스 규칙: 예) 고위험 차입자가 기준치(100명)를 넘으면 별도 처리

**구성 예: 중복 데이터 처리**

| 태스크 | 유형 | 의존 대상 | 조건 / 설정 |
|---|---|---|---|
| `customers_sales_summary` | Notebook | 수집 태스크들 | 끝에서 `taskValues.set("has_duplicates", ...)` |
| `checking_for_duplicates` | If/else | `customers_sales_summary` | `{{tasks.customers_sales_summary.values.has_duplicates}} == true` |
| `dropping_duplicate_records` | Notebook | `checking_for_duplicates` (**true**) | 중복 제거 |
| `transforming_customers_sales_table` | Notebook | `checking_for_duplicates` (**false**) + `dropping_duplicate_records` | Run if: **None failed** |

**마지막 태스크에 None failed를 쓰는 이유**

| 상황 | 결과 |
|---|---|
| 중복 없음 | False 분기 실행, `dropping_duplicate_records`는 **건너뜀(skipped)** → 실패가 없으므로 변환 태스크 즉시 실행 |
| 중복 있음 | True 분기로 중복 제거 실행, False 분기는 건너뜀 → 중복 제거가 성공하면 변환 태스크 실행 |

- 만약 **All succeeded**로 두었다면, 어느 쪽이든 한 분기는 건너뛰어지므로 변환 태스크가 실행되지 않는다. 분기 후 합류 지점에는 **None failed**가 핵심이다.

---

## 11. For Each 반복 태스크

**입력 배열을 순회하며 각 원소마다 같은 중첩 태스크를 실행**한다. 반복 작업을 효율적으로 처리하면서 시각적 워크플로 관리의 이점을 유지한다.

### 11-1. 구성 요소

| 구성 요소 | 역할 |
|---|---|
| **For Each 태스크 (컨테이너)** | 반복 로직 관리: 입력 배열, 동시성(concurrency), 리소스 할당 |
| **중첩 태스크 (Nested task)** | 각 반복에서 실제로 실행되는 태스크. 노트북, SQL, Python 스크립트 등 모든 유형 가능. 현재 원소를 **`{{input}}`**으로 받음 |

### 11-2. 동작 규칙

- **입력(Inputs)**: JSON 배열. 예: `["CA", "NY", "VA"]` (작업 파라미터나 태스크 값을 동적 참조로 넣을 수도 있다)
- 중첩 태스크의 파라미터에 `{{input}}`을 연결한다. 예: 키 `state`, 값 `{{input}}`
- **동시성(Concurrency)**: 동시에 실행할 반복 수. 비워두면 순차 실행. 독립적인 처리라면 병렬로 크게 빨라진다.
- **다운스트림 의존성은 중첩 태스크가 아니라 For Each 컨테이너에 연결**한다. 모든 반복이 끝나야 다음 태스크가 시작된다.
- 반복이 몇 번이든 워크플로에서는 **하나의 논리 단위**로 보이므로 DAG가 깔끔하게 유지된다.
- 처리 로직(중첩 태스크)과 반복 동작(컨테이너)이 분리되어 있어 유지 관리가 쉽다.

```python
# 중첩 태스크 노트북: 반복마다 다른 state 값을 받아 주별 테이블 생성
state = dbutils.widgets.get("state")          # {{input}}으로 전달된 값 (예: "CA")
(spark.table("customers_orders_silver")
      .filter(f"state = '{state}'")
      .write.mode("overwrite")
      .saveAsTable(f"customers_orders_{state.lower()}_silver"))
```

**활용 예**: 지역(주)별 병렬 처리, 날짜 범위별 처리, 고객 세그먼트별 동일 분석, 여러 파일에 같은 로직 적용

---

## 12. 종합 예시: 동적 소매 데이터 파이프라인

지금까지의 개념을 모두 사용한 하나의 작업 구성 예.

**목표 흐름**: 클라우드 스토리지 → 수집 → 조인 → If/Else(중복 검사) → For Each(주별 보고서) → 변환 → 대시보드 갱신

| # | 태스크 | 유형 | 의존 대상 (Run if) | 내용 |
|---|---|---|---|---|
| 1 | `ingesting_orders` | Notebook | — | 주문 Bronze 테이블 생성 |
| 2 | `ingesting_sales` | SQL | — | 판매 Bronze 테이블 생성 |
| 3 | `ingesting_customers` | Notebook | — | 고객 Bronze 테이블 생성. 파라미터 `catalog`, `schema` 사용. **File arrival 트리거**로 새 파일 도착 시 실행 |
| 4 | `customers_sales_summary` | Notebook | 2, 3 (All succeeded) | 고객+판매 조인 → Silver. 태스크 값 `has_duplicates` 설정 |
| 5 | `customers_orders_report` | Notebook | 1, 3 (All succeeded) | 고객+주문 조인 → Silver |
| 6 | `checking_for_duplicates` | If/else | 4 | `has_duplicates == true` |
| 7 | `dropping_duplicate_records` | Notebook | 6 (true) | 중복 제거 |
| 8 | `transforming_customers_sales_table` | Notebook | 6 (false), 7 (**None failed**) | 정제·변환 → Gold |
| 9 | `customers_orders_state_wise_report_iterator` | For each | 5 (All succeeded) | 입력 `["CA","NY","VA"]` |
| 9-1 | `customers_orders_state_wise_report` | Notebook (중첩) | — | 파라미터 `state = {{input}}` → 주별 Silver 테이블 |
| 10 | `transforming_customers_orders_data` | Notebook | 9 (All succeeded) | 주별 테이블 변환 → Gold |
| 11 | `refreshing_retail_dashboard` | Dashboard | 8, 10 (All succeeded) | 대시보드 갱신 후 구독자에게 스냅샷 이메일 |

- 1·2·3은 의존성이 없어 **병렬**로 시작한다 (팬아웃 후 퍼널 구조).
- 대시보드 태스크로 쓰려면 대시보드가 **게시(Publish)**되어 있어야 하고, 실행할 **SQL Warehouse**와 **구독자**를 지정한다.

---

## 13. 실행 상태와 Repair Run

### 13-1. 실행 이력과 상태

- 작업의 실행(run)마다 **run_id, 시작 시각, 기간, 상태**가 기록되고, 태스크별 소요 시간을 막대 차트와 표로 확인할 수 있다.
- 실행을 열면 태스크별 출력, 사용된 노트북 코드, 오류 메시지를 볼 수 있어 오류 진단에 쓴다.

| 상태 | 의미 |
|---|---|
| Pending / Queued | 시작 대기 |
| Running | 실행 중 |
| Succeeded | 성공 |
| Failed | 실패 |
| Retrying (Waiting for retry) | 재시도 대기 |
| Skipped | 조건 때문에 실행하지 않음 (If/Else 반대 분기, Run if 불충족) |
| Upstream failed | 상위 태스크가 실패해서 실행되지 않음 |
| Canceled / Timed out | 취소 / 시간 초과 |

### 13-2. Repair Run (복구 실행)

실패한 실행에서 **실패한 태스크와 그 하위(의존) 태스크만** 다시 실행하는 기능.

| 기능 | 설명 |
|---|---|
| 선택적 재실행 | 성공한 태스크는 다시 돌리지 않는다. 재실행할 태스크를 직접 선택·해제할 수도 있다 |
| 파라미터 재정의 | 복구 실행 시 파라미터 값을 바꿔서 실행 |
| 코드 수정 후 재실행 | 노트북 코드를 고친 뒤 실패한 부분만 다시 실행 |
| 복구 이력 | 같은 실행 안에 원래 시도와 복구 시도가 모두 남아 **무엇이, 언제, 누구에 의해** 복구되었는지 감사 가능 |

**복구 시나리오**
- 구성 수정: 실패를 유발한 파라미터 값 수정
- 리소스 조정: 메모리·compute 부족으로 실패한 태스크에 자원 증설
- 코드 수정: 로직 오류를 고치고 영향받은 태스크만 재실행
- 데이터 품질 문제 대응: 처리 로직 조정

**이점**: 복잡한 파이프라인에서 복구 시간과 compute 비용 대폭 절감, 복구 중 연쇄 장애 위험 감소, SLA 준수

- 주의: Repair run에서 파라미터를 바꿔 성공했더라도 **작업 정의 자체는 바뀌지 않는다.** 다음 정상 실행에도 반영하려면 작업 설정을 직접 수정해야 한다.

---

## 14. 성능 모니터링: 시스템 테이블과 Spark UI

### 14-1. 시스템 테이블: `system.lakeflow`

- **같은 리전의 모든 워크스페이스**의 작업 활동을 기록하는 내장 **읽기 전용** 스키마
- 시스템 카탈로그(`system`)에는 그 밖에 청구(billing), 접근(access), compute 등의 스키마가 있다.

| 테이블 | 내용 |
|---|---|
| `jobs` | 작업 메타데이터·구성 |
| `job_tasks` | 태스크 정의 |
| `job_run_timeline` | 작업 실행 이력 |
| `job_task_run_timeline` | 태스크 실행 이력 |
| `pipelines` | 선언적 파이프라인 정보 |

- **타임라인 테이블**은 `period_start_time`, `period_end_time`으로 오래 걸리는 실행을 **시간 단위로 나눠** 기록한다. 그래서 장시간 실행에서도 기간·동시성·SLA 분석이 정확하다.
- 활용: 비용 분석, 성능 추세, SLA 준수 추적, 리소스 활용 최적화

```sql
SHOW SCHEMAS IN system;
SHOW TABLES IN system.lakeflow;

-- 특정 작업의 태스크별 실행 이력
SELECT
  jobs.workspace_id,
  jobs.name AS job_name,
  jobs.job_id,
  timeline.run_id,
  timeline.period_start_time,
  timeline.period_end_time,
  timeline.task_key,
  timeline.result_state
FROM system.lakeflow.jobs AS jobs
JOIN system.lakeflow.job_task_run_timeline AS timeline
  ON jobs.job_id = timeline.job_id
WHERE lower(jobs.name) LIKE 'retail_job_%'
ORDER BY timeline.period_start_time;
```

### 14-2. Spark UI / 쿼리 상세

| 확인 항목 | 내용 |
|---|---|
| 실행 타임라인 | 태스크 시작/종료, 기간, 겹침 → 병목 즉시 파악 |
| 태스크 상세 | 상태, 타임스탬프, 기간, 클러스터/런타임, 로그, I/O |
| 쿼리 상세 | 전체 쿼리 텍스트, 실행 ID, 시간 분해(계획·최적화·프루닝 vs 실행), 읽기/쓰기 파일, 파티션, **스필(spill)** |

| 증상 | 의미하는 개선 방향 |
|---|---|
| **계획(planning) 시간이 길다** | 프루닝·파티셔닝 개선, 메타데이터 최적화 |
| **실행 시간이 길다** | 조인·집계 최적화 (브로드캐스트 조인, 스큐 처리) |
| 메모리·CPU·I/O 병목 | 클러스터 구성 조정 |

---

## 15. 운영 환경 모범 사례

### 15-1. Compute와 비용

- 운영 작업은 **Job 클러스터 또는 서버리스**를 사용한다.
- 운영 워크로드에 **Interactive(All-purpose) 클러스터는 쓰지 않는다** (비용 초과, 리소스 충돌).
- **Photon**을 활성화해 더 빠르고 저렴하게 실행한다.
- 태스크 간 **클러스터를 재사용**해 시작 시간과 비용을 줄인다.

### 15-2. 오케스트레이션과 모듈성

- 복잡한 파이프라인은 **Run Job 태스크**로 모듈화한다.
- **멀티 태스크 작업**으로 병렬·확장 가능한 실행을 구성한다.
- **Run if, If/Else, For Each**로 조건·반복 로직을 구현한다.
- 작업 하나의 태스크 수를 관리 가능한 수준으로 유지한다 (**작업당 최대 1,000개 태스크**).

**모듈식 설계 (부모-자식 작업)**
- 큰 DAG를 기술 구성 요소가 아니라 **논리적 비즈니스 단위**로 분해한다.
- **부모 작업**이 Run Job 태스크로 **자식 작업**을 실행해 전체를 조율한다.

| 이점 | 내용 |
|---|---|
| 유지 관리성 | 작은 작업은 이해·수정·문제 해결이 쉽다 |
| 재사용성 | 하나의 자식 작업을 여러 부모 작업에서 재사용 |
| 팀 협업 | 팀별로 자기 모듈을 담당 |
| 테스트 | 모듈을 독립적으로 테스트 → 품질 향상, 배포 위험 감소 |

### 15-3. 모니터링과 거버넌스

- 작업 소유권과 데이터 접근에 개인 계정 대신 **서비스 주체(Service Principal)**를 사용한다 (작업의 **Run as**). 담당자가 퇴사하거나 권한이 바뀌어도 작업이 계속 동작하고, 접근 제어가 명확해진다.
- **실패, 지연, 완료**에 대한 알림을 구성한다.
- **Repair run**으로 재처리 비용을 줄인다.
- 태스크를 **파라미터화**해 재사용성과 유연성을 높인다.

### 15-4. Git 연동

작업의 태스크 소스를 **Git 공급자**로 지정할 수 있다.

| 이점 | 내용 |
|---|---|
| 의도치 않은 변경 방지 | 누군가 운영용 폴더에서 직접 편집하거나 브랜치를 바꿔 운영 작업이 깨지는 일을 막는다 |
| 단일 진실 공급원 | 항상 특정 브랜치·태그의 **커밋된 코드**를 실행 → 운영에서 어떤 코드가 도는지 명확 |
| CI/CD | 변경을 자동 테스트·배포 파이프라인으로 검증 후 반영 |
| 협업 | 브랜치, Pull Request, 코드 리뷰 등 표준 Git 워크플로 |
| 지원 플랫폼 | GitHub, GitLab, AWS CodeCommit, Azure DevOps, Bitbucket 등 |

**구성 단계**
1. Git 공급자를 소스로 하는 태스크 생성: **저장소 URL, 브랜치/태그/커밋, 인증 자격 증명** 지정
2. **저장소 루트 기준**으로 노트북 경로 지정

- 운영에서는 계속 바뀌는 main 대신 **특정 릴리스 브랜치나 태그**를 쓰고, 운영 브랜치로 병합하기 전에 코드 리뷰를 거친다.

---

## 16. 작업을 코드로 정의하기

작업은 UI 외에도 **REST API, Databricks SDK, CLI, Declarative Automation Bundles**(이전 명칭: Databricks Asset Bundles)로 정의할 수 있다. UI로 만든 작업도 **코드로 보기(View as code)**로 YAML, Python(SDK·번들), JSON 형식을 확인하고 **YAML로 편집**할 수 있다.

**Declarative Automation Bundles**: 작업과 파이프라인을 버전 관리되는 코드로 정의하고, dev/staging/prod 같은 **환경별 설정**을 둬서 CI/CD로 배포하는 도구.

아래는 이 문서의 개념을 번들 YAML 형식으로 표현한 예다.

```yaml
resources:
  jobs:
    retail_job:
      name: retail_job

      # 작업 파라미터: 모든 태스크에 전달, 같은 키면 태스크 파라미터보다 우선
      parameters:
        - name: catalog
          default: dev_catalog
        - name: schema
          default: jobs

      # Scheduled 트리거 (Quartz cron + 시간대)
      schedule:
        quartz_cron_expression: "0 0 6 * * ?"
        timezone_id: Asia/Seoul

      # 작업 수준 알림
      email_notifications:
        on_failure:
          - data-team@example.com

      tasks:
        - task_key: ingesting_customers
          notebook_task:
            notebook_path: ../src/ingesting_customers.py
          max_retries: 2                          # 재시도 정책
          min_retry_interval_millis: 60000

        - task_key: customers_sales_summary
          depends_on:
            - task_key: ingesting_customers
          run_if: ALL_SUCCESS                     # Run if 조건
          notebook_task:
            notebook_path: ../src/customers_sales_summary.py

        # If/Else 조건 태스크
        - task_key: checking_for_duplicates
          depends_on:
            - task_key: customers_sales_summary
          condition_task:
            op: EQUAL_TO
            left: "{{tasks.customers_sales_summary.values.has_duplicates}}"
            right: "true"

        - task_key: dropping_duplicate_records
          depends_on:
            - task_key: checking_for_duplicates
              outcome: "true"                     # True 분기
          notebook_task:
            notebook_path: ../src/dropping_duplicates.py

        - task_key: transforming_customers_sales_table
          depends_on:
            - task_key: checking_for_duplicates
              outcome: "false"                    # False 분기
            - task_key: dropping_duplicate_records
          run_if: NONE_FAILED                     # 어느 분기든 합류
          notebook_task:
            notebook_path: ../src/transforming.py

        # For Each 반복 태스크
        - task_key: state_wise_report_iterator
          depends_on:
            - task_key: customers_sales_summary
          for_each_task:
            inputs: '["CA", "NY", "VA"]'
            concurrency: 3
            task:
              task_key: state_wise_report
              notebook_task:
                notebook_path: ../src/state_report.py
                base_parameters:
                  state: "{{input}}"
```

| Run if 값 (API) | UI 표시 |
|---|---|
| `ALL_SUCCESS` | All succeeded |
| `AT_LEAST_ONE_SUCCESS` | At least one succeeded |
| `NONE_FAILED` | None failed |
| `ALL_DONE` | All done |
| `AT_LEAST_ONE_FAILED` | At least one failed |
| `ALL_FAILED` | All failed |

---

## 17. 자가 점검 문제

> 답을 가리고 먼저 생각해본 뒤 펼쳐보자.

**Q1.** 작업(Job)과 태스크(Task)의 정의와 관계는?
<details><summary>정답</summary>

작업은 워크로드를 예약·조율·실행하는 기본 리소스(컨테이너)이고, 태스크는 작업 안에서 노트북·스크립트·쿼리 등을 실행하는 단일 작업 단위다. 하나의 작업은 하나 이상의 태스크로 구성된다.
</details>

**Q2.** 외부 오케스트레이터 대신 Lakeflow Jobs를 쓰는 이유는?
<details><summary>정답</summary>

외부 도구는 레이크하우스와 통합되어 있지 않아 생산성 저하, 근본 원인 파악의 어려움, 높은 소유 비용과 낮은 안정성, 데이터 사일로를 만든다. Lakeflow Jobs는 플랫폼 내장으로 Unity Catalog, Delta Lake, Photon, 파이프라인, ML과 원활히 통합된다.
</details>

**Q3.** All-purpose 클러스터, Job 클러스터, 서버리스를 운영 작업 관점에서 비교하라.
<details><summary>정답</summary>

All-purpose: 공유·상시 실행이라 비싸고 경합 발생, 운영에 비권장. Job 클러스터: 작업 종료 시 함께 종료되어 약 50% 저렴, 전용 리소스지만 VM 시작 지연이 있음. 서버리스: 완전 관리형, 빠른 시작, Photon 기본, 자동 확장, 대부분의 운영 워크로드에 최적.
</details>

**Q4.** 서버리스 작업의 Standard 모드와 Performance optimized 모드의 차이는?
<details><summary>정답</summary>

Standard는 비용 효율 중심으로 시작이 느리다(보통 4~6분). Performance optimized는 빠른 시작과 실행으로 시간에 민감한 워크로드에 적합하다. 서버리스 compute를 쓰는 태스크에만 적용된다.
</details>

**Q5.** 서버리스 요금이 클래식보다 TCO가 낮을 수 있는 이유는?
<details><summary>정답</summary>

클래식은 DBU 외에 VM·네트워크 같은 클라우드 비용과 인프라 배포·모니터링·유지 관리 인력 비용이 따로 든다. 서버리스는 인프라·운영 비용이 단일 DBU 가격에 포함되고 운영 부담이 줄어든다.
</details>

**Q6.** DAG의 세 단어는 각각 무엇을 뜻하는가? Task-2, Task-3이 모두 Task-1에만 의존하면 둘은 어떻게 실행되는가?
<details><summary>정답</summary>

Directed(간선에 방향), Acyclic(순환 없음), Graph(간선으로 연결된 정점 모음). Task-2와 Task-3은 Task-1이 끝난 뒤 병렬로 실행된다.
</details>

**Q7.** Sequence, Funnel, Fan-out 패턴을 설명하라.
<details><summary>정답</summary>

Sequence: 순차 처리(Bronze → Silver → Gold). Funnel: 여러 소스를 하나로 모음. Fan-out: 하나의 소스를 여러 다운스트림으로 분배.
</details>

**Q8.** 작업 파라미터와 태스크 파라미터에 같은 키가 있으면 어느 값이 쓰이는가? 노트북에서 파라미터를 읽는 방법은?
<details><summary>정답</summary>

작업 파라미터가 우선한다. 노트북에서는 `dbutils.widgets.get("키")`로 읽는다.
</details>

**Q9.** 파라미터와 태스크 값의 차이는? 태스크 값을 설정·조회하는 코드는?
<details><summary>정답</summary>

파라미터는 실행 전에 정해지는 정적 입력, 태스크 값은 실행 중 계산되어 태스크 간에 공유되는 값이다. 설정 `dbutils.jobs.taskValues.set(key=..., value=...)`, 조회 `dbutils.jobs.taskValues.get(taskKey=..., key=..., default=..., debugValue=...)`.
</details>

**Q10.** If/Else 조건에서 업스트림 태스크 `summary`가 설정한 태스크 값 `has_dup`를 참조하는 문법은?
<details><summary>정답</summary>

`{{tasks.summary.values.has_dup}}` (동적 값 참조).
</details>

**Q11.** 알림을 받을 수 있는 채널과 이벤트 종류는?
<details><summary>정답</summary>

채널: 이메일, Teams, PagerDuty, Slack, Webhook. 이벤트: 시작, 성공, 실패, 기간 임계값 초과/타임아웃, 스트리밍 백로그.
</details>

**Q12.** 재시도 정책을 정할 때 고려할 네 가지는?
<details><summary>정답</summary>

오류 유형(일시적 vs 데이터 문제), 리소스 영향, 다운스트림 의존성, 비즈니스 SLA.
</details>

**Q13.** 파트너가 하루 중 불규칙하게 파일을 올린다. 적합한 트리거와 그 특징은?
<details><summary>정답</summary>

File arrival 트리거. S3/ADLS/GCS/UC 볼륨 위치를 약 1분마다 확인해 새 파일이 생기면 실행한다. 수정된 파일은 다시 트리거하지 않으며, 트리거 간 최소 시간과 마지막 변경 후 대기 시간을 설정할 수 있다.
</details>

**Q14.** 상위 팀의 테이블 세 개가 모두 갱신된 뒤에만 집계 작업을 돌리고 싶다. 어떻게 설정하는가?
<details><summary>정답</summary>

Table update 트리거에 세 테이블을 등록하고(트리거당 최대 10개) 조건을 "All tables updated"로 설정한다. 데이터가 여러 배치로 들어온다면 "마지막 변경 후 대기"도 설정한다.
</details>

**Q15.** Continuous 트리거의 동작과 용도는?
<details><summary>정답</summary>

이전 실행이 끝나거나 실패하면 즉시 새 실행을 시작해 계속 돌아간다. 재시도는 Databricks가 자동 관리한다. 스트리밍 워크로드에 쓴다.
</details>

**Q16.** Quartz cron `0 0 6 * * ?`의 의미는? 스케줄에서 꼭 지정해야 하는 것은?
<details><summary>정답</summary>

매일 06:00. 시간대(time zone)를 반드시 지정해야 한다.
</details>

**Q17.** Run if 조건 여섯 가지를 쓰고, 실패 알림 태스크와 정리 태스크에는 각각 무엇을 쓰는가?
<details><summary>정답</summary>

All succeeded, At least one succeeded, None failed, All done, At least one failed, All failed. 실패 알림: At least one failed. 정리: All done.
</details>

**Q18.** If/Else의 True 분기 태스크와 False 분기 태스크가 합류하는 다음 태스크에 None failed를 써야 하는 이유는?
<details><summary>정답</summary>

항상 한쪽 분기는 건너뛰어(skipped)지므로 All succeeded로는 실행되지 않는다. None failed는 건너뛴 것을 허용하므로 어느 분기로 가든 합류 태스크가 실행된다.
</details>

**Q19.** For Each 태스크에서 현재 원소를 중첩 태스크에 전달하는 방법과, 다운스트림 의존성을 연결하는 대상은?
<details><summary>정답</summary>

중첩 태스크의 파라미터 값으로 `{{input}}`을 쓴다. 다운스트림 의존성은 중첩 태스크가 아니라 For Each 컨테이너 태스크에 연결한다. 모든 반복이 끝나야 다음 태스크가 시작된다.
</details>

**Q20.** 10개 태스크 중 7번째가 실패했다. 처음부터 다시 돌리지 않고 복구하는 방법과, 주의할 점은?
<details><summary>정답</summary>

Repair run으로 실패한 태스크와 그 하위 태스크만 재실행한다(필요하면 파라미터 재정의, 코드 수정 후 실행). Repair run에서 바꾼 파라미터는 작업 정의에 반영되지 않으므로 작업 설정도 직접 고쳐야 한다.
</details>

**Q21.** `system.lakeflow`의 주요 테이블과, 타임라인 테이블이 시간 단위로 기록하는 이유는?
<details><summary>정답</summary>

jobs, job_tasks, job_run_timeline, job_task_run_timeline, pipelines. 오래 걸리는 실행을 period_start_time/period_end_time 기준 시간 단위로 나눠 기록해야 기간·동시성·SLA 분석이 정확하기 때문이다.
</details>

**Q22.** Spark UI에서 계획 시간이 긴 경우와 실행 시간이 긴 경우 각각 어떤 개선을 검토하는가?
<details><summary>정답</summary>

계획 시간이 길면 프루닝·파티셔닝(메타데이터) 개선, 실행 시간이 길면 조인·집계 최적화(브로드캐스트, 스큐 처리).
</details>

**Q23.** 운영 작업을 서비스 주체로 실행해야 하는 이유는?
<details><summary>정답</summary>

개인 계정으로 실행하면 퇴사·권한 변경 시 작업이 깨진다. 서비스 주체를 쓰면 작업이 계속 동작하고 소유권·데이터 접근 제어가 명확해진다.
</details>

**Q24.** 작업 태스크를 Git 소스에서 실행하면 얻는 이점과, 운영에서 권장되는 브랜치 사용 방식은?
<details><summary>정답</summary>

운영 코드의 의도치 않은 변경 방지, 단일 진실 공급원, CI/CD, 협업. 운영에서는 계속 바뀌는 main 대신 특정 릴리스 브랜치나 태그를 사용하고 병합 전 코드 리뷰를 거친다.
</details>

**Q25.** 큰 작업을 모듈화할 때 쓰는 태스크 유형과 이점은? 작업당 태스크 수 제한은?
<details><summary>정답</summary>

Run Job 태스크로 부모 작업이 자식 작업을 실행한다. 유지 관리성, 재사용성, 팀 협업, 독립 테스트가 가능하다. 작업당 최대 1,000개 태스크.
</details>

---

## 18. 참고 링크

- Lakeflow Jobs: https://docs.databricks.com/aws/en/jobs/
- 작업 파라미터화: https://docs.databricks.com/aws/en/jobs/parameters
- 동적 값 참조: https://docs.databricks.com/aws/en/jobs/dynamic-value-references
- 태스크 값: https://docs.databricks.com/aws/en/jobs/task-values
- 트리거와 스케줄: https://docs.databricks.com/aws/en/jobs/triggers
- 파일 도착 트리거: https://docs.databricks.com/aws/en/jobs/file-arrival-triggers
- 조건부 태스크 (Run if, If/Else): https://docs.databricks.com/aws/en/jobs/conditional-tasks
- For Each 태스크: https://docs.databricks.com/aws/en/jobs/for-each
- 실패한 작업 복구 (Repair run): https://docs.databricks.com/aws/en/jobs/repair-job-failures
- Jobs 시스템 테이블: https://docs.databricks.com/aws/en/admin/system-tables/jobs
- 시스템 테이블 개요: https://docs.databricks.com/aws/en/admin/system-tables
- 번들 (Declarative Automation Bundles): https://docs.databricks.com/aws/en/dev-tools/bundles/
