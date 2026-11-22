# 선언형 자동화 번들(DAB)을 통한 자동 배포 — 핵심 개념 정리

---

## 목차

1. [DevOps, DataOps, MLOps](#1-devops-dataops-mlops)
2. [CI/CD와 테스트 피라미드](#2-cicd와-테스트-피라미드)
3. [환경 격리와 환경별 데이터](#3-환경-격리와-환경별-데이터)
4. [Databricks 프로젝트의 구성 요소와 배포 여정](#4-databricks-프로젝트의-구성-요소와-배포-여정)
5. [배포 도구 비교](#5-배포-도구-비교)
6. [Databricks CLI 연결과 인증](#6-databricks-cli-연결과-인증)
7. [선언형 자동화 번들(DAB) 개요](#7-선언형-자동화-번들dab-개요)
8. [databricks.yml과 최상위 매핑](#8-databricksyml과-최상위-매핑)
9. [resources: 작업 정의하기](#9-resources-작업-정의하기)
10. [targets와 배포 모드](#10-targets와-배포-모드)
11. [번들 수명 주기 CLI 명령](#11-번들-수명-주기-cli-명령)
12. [변수와 치환](#12-변수와-치환)
13. [include로 모듈화하고 여러 환경에 배포하기](#13-include로-모듈화하고-여러-환경에-배포하기)
14. [번들 템플릿](#14-번들-템플릿)
15. [CI/CD 프로젝트 구성 예](#15-cicd-프로젝트-구성-예)
16. [기존 워크플로에 태스크 추가하기](#16-기존-워크플로에-태스크-추가하기)
17. [로컬 개발: CLI, Databricks Connect, VS Code](#17-로컬-개발-cli-databricks-connect-vs-code)
18. [CI/CD 모범 사례와 GitHub Actions](#18-cicd-모범-사례와-github-actions)
19. [DAB 생태계의 최근 변화](#19-dab-생태계의-최근-변화)
20. [핵심 정리 치트시트](#20-핵심-정리-치트시트)
21. [자가 점검 문제](#21-자가-점검-문제)
22. [참고 링크](#22-참고-링크)

---

## 1. DevOps, DataOps, MLOps

### 1-1. DevOps

DevOps는 **소프트웨어 엔지니어링 모범 사례**(버전 관리, 자동화 테스트, 반복 가능한 빌드)와 **IT 운영**(배포, 모니터링, 인프라 관리)이 겹치는 영역이다. 두 조직이 작업을 떠넘기지 않고 전체 수명 주기를 함께 소유하게 하는 문화이자 실천 방식이다.

| 이점 | 내용 |
|---|---|
| 빠른 배포 | 자동화로 느린 수동 인계를 없애 변경이 빨리 사용자에게 전달된다 |
| 향상된 협업 | 개발과 운영이 문제 발생 시 서로 탓하지 않고 소유권을 공유한다 |
| 강화된 안정성 | 자동화 테스트와 일관된 배포로 운영 환경의 예상치 못한 문제가 준다 |
| 더 나은 확장성 | 반복 가능한 자동화 프로세스는 수동 프로세스보다 훨씬 잘 확장된다 |

DevOps 수명 주기: 계획 → 코드 → 빌드 → 테스트 → 릴리스 → 배포 → 운영 → 모니터링 → (다시 계획)

### 1-2. 데이터와 ML로의 확장

| 이름 | 대상 | 초점 |
|---|---|---|
| **DataOps** | 데이터 엔지니어링 | 데이터 파이프라인의 흐름, 품질, 관리 자동화. 수집부터 처리까지 안정적인 데이터 흐름 |
| **MLOps** | 머신 러닝 | 모델의 개발 → 운영 이동과 지속적인 성능 모니터링 |

둘 다 자동화, 협업, 지속적 개선이라는 DevOps의 같은 원칙을 각 영역에 적용한 것이다.

---

## 2. CI/CD와 테스트 피라미드

### 2-1. CI와 CD

CI/CD는 코드 통합, 테스트, 배포 자동화에 초점을 맞춘 DevOps의 핵심 하위 집합이다.

| 구분 | 포함 단계 | 내용 |
|---|---|---|
| **CI (지속적 통합)** | 개발, 빌드, 테스트, 버전 관리 | 여러 기여자의 변경을 중앙 리포지토리에 자주 병합하고, 병합 때마다 자동화 테스트를 실행. 테스트에 실패한 코드는 반영되지 않는다 |
| **CD (지속적 전달/배포)** | 스테이지 배포, 운영 배포 | 릴리스 프로세스, 배포, 운영, 모니터링 자동화 |

CI의 네 가지 이점: 문제 조기 발견, 빠른 개발 주기, 더 깔끔하고 모듈화된 코드와 협업, 자동화된 테스트와 검증으로 안정적인 코드.

테스트를 언제 실행하고 커밋을 어떻게 반영할지는 조직의 **브랜치 전략**에 따라 달라진다.

### 2-2. Continuous Delivery vs Continuous Deployment

두 방식 모두 스테이징까지는 같은 자동화 파이프라인을 쓴다. 차이는 마지막 운영 배포다.

| | 빌드 | 테스트 | 스테이지 배포 | 운영 배포 |
|---|---|---|---|---|
| **Continuous Delivery** | 자동 | 자동 | 자동 | **수동** (언제 내보낼지 사람이 결정) |
| **Continuous Deployment** | 자동 | 자동 | 자동 | **자동** (테스트를 통과하면 즉시 운영으로) |

Continuous Deployment는 배포 여부를 자신 있게 맡길 수 있을 만큼 잘 설계된 테스트가 전제다.

### 2-3. 테스트 피라미드

| 층 | 대상 | 예 | 특징 |
|---|---|---|---|
| **시스템 테스트** (위) | 전체 애플리케이션을 운영과 유사한 환경에서 엔드투엔드로 | 워크플로 안의 엔드투엔드 데이터 파이프라인 | 가장 느리고 비싸며 적게 실행 |
| **통합 테스트** (중간) | 구성 요소 간 상호작용 | 노트북, Declarative Pipelines, Jobs | 중간 |
| **단위 테스트** (아래) | 개별 함수나 메서드를 독립적으로 | 커스텀 PySpark 함수 | 빠르고 싸며 커버리지가 넓고 자동화됨 |

---

## 3. 환경 격리와 환경별 데이터

### 3-1. 격리 방법

코드는 운영에 적용되기 전에 개발과 스테이징에서 개발, 테스트된다. Databricks에서는 다음 중 하나 또는 조합으로 환경을 격리한다.

| 방법 | 내용 |
|---|---|
| **여러 워크스페이스** | 환경마다 워크스페이스 하나 (DEV, STAGE, PROD) |
| **여러 카탈로그** | 하나의 워크스페이스 안에서 환경마다 카탈로그 하나 |
| 둘 다 | 워크스페이스와 카탈로그를 함께 분리 |

최소 구성은 두 환경(개발+스테이지, 운영)이지만 조직 요구에 따라 달라진다.

### 3-2. 환경별 데이터

| 환경 | 데이터 | 목적 |
|---|---|---|
| **Dev** | 운영 데이터의 작은 정적 부분 집합, 익명화 또는 합성 데이터 | 빠른 개발과 테스트, 개인정보와 운영 데이터 무결성 보호 |
| **Stage** | 운영의 구조와 볼륨을 반영, 보통 정적, 민감 정보는 익명화하거나 제거 | 현실적인 테스트와 검증 |
| **Prod** | 라이브, 실제 사용자 데이터, 지속적으로 갱신 | 높은 보안, 개인정보 보호, 규정 준수 필요 |

각 환경은 현실성, 보안, 규정 준수의 균형에 맞는 데이터를 쓴다.

---

## 4. Databricks 프로젝트의 구성 요소와 배포 여정

### 4-1. 프로젝트의 세 가지 구성 요소

| 구성 요소 | 의미 | 예 |
|---|---|---|
| **코드** | 데이터 제품을 만드는 로직 | 노트북, Python 파일, `.whl`, JAR, dbt, SQL/Python/R |
| **리소스** | 코드가 실행되는 Databricks 객체 | Lakeflow Jobs, Spark Declarative Pipelines(SDP), MLflow Tracking/Registry |
| **실행 환경** | 프로젝트가 어디서 어떻게 실행되는지 | 워크스페이스, Unity Catalog, compute 구성 |

프로젝트가 만드는 결과물(테이블, 파이프라인, 작업, 모델, 대시보드, 외부 서비스 호출 등)이 필요한 구성 요소를 결정한다. 간단한 보고서는 단일 노드 compute의 노트북 하나로 충분하고, 완전한 MLOps 파이프라인은 MLflow, 특성 저장소, Model Serving이 필요하다.

### 4-2. 개발 → 스테이징 → 운영 여정

| 환경 | 브랜치 (예) | 승격 방법 | 구성 예 |
|---|---|---|---|
| 개발 | `dev` | 개발자가 직접 배포 | 단일 노드, **사용자로 실행**, 개발 데이터 |
| 스테이징 | `stage` | `dev` → `stage` pull request | 서버리스, **서비스 주체로 실행**, 스테이징 데이터. 운영과 최대한 같게 |
| 운영 | `main` | `stage` → `main` 릴리스 | 서버리스, **서비스 주체로 실행**, 주간 일정 |

스테이징과 운영에서 서비스 주체(service principal)로 실행하면 자동화 도구가 필요한 리소스에만 접근하게 되어, 사용자나 그룹 권한으로 실행하는 것보다 안전하다.

---

## 5. 배포 도구 비교

### 5-1. DAB 이전의 세 가지 오케스트레이션 방법

| 방법 | 장점 | 단점 |
|---|---|---|
| **수동 (UI)** | 배우기 쉽고 접근성이 높다 | 시간 소모, 사람의 오류, CI/CD에 부적합 |
| **프로그래밍 (REST API / SDK)** | 낮은 수준의 세밀한 제어 | 배울 API와 클래스가 많고, 전체 CI/CD 자동화 코드를 짜는 데 시간이 많이 든다 |
| **Terraform (Databricks 공급자)** | 매우 강력하고 표현력이 높다. 관리자의 구성 관리 도구로 선호 | 데이터 엔지니어와 과학자에게 학습 부담, 관리할 도구가 하나 더 늘어남 |

### 5-2. Databricks의 배포 자동화 도구

| 도구 | 특징 |
|---|---|
| **REST API** | HTTP 요청으로 기능에 직접 접근. 요청 구성과 응답 처리를 직접 해야 한다 |
| **Databricks SDK** | 모든 공개 REST API 작업 지원. Python, Java, Go, R |
| **Databricks CLI** | 터미널, 셸 스크립트에서 쓰기 쉬운 인터페이스. CLI의 `bundle` 명령으로 **DAB를 통해 인프라를 코드로 작성** |

Databricks는 작업과 기타 리소스를 소스 코드로 만들고, 개발, 배포, 테스트할 때 DAB 사용을 권장한다.

---

## 6. Databricks CLI 연결과 인증

### 6-1. CLI를 실행하는 위치

| 위치 | 특징 |
|---|---|
| **웹 터미널** | 워크스페이스 안에서 셸 명령 실행, 기본으로 최신 CLI, 현재 사용자로 인증, 활성화 필요 |
| **로컬 (VS Code 등)** | CLI를 로컬에 설치하고 워크스페이스에 인증. VS Code 확장으로 추가 기능 |
| **노트북 `%sh`** | 노트북 안에서 CLI 설치 후 실행, 토큰으로 인증. 학습용으로 간편 |

> 운영 환경에서는 노트북에서 `databricks bundle deploy`를 실행하지 않는다. 표준 패턴은 **개발자의 로컬 머신**(CLI가 워크스페이스에 구성된 상태) 또는 **서비스 주체로 OAuth M2M 인증된 CI/CD 러너**(GitHub Actions, Azure DevOps, GitLab CI, Jenkins)에서 배포하는 것이다.

- 노트북의 `%sh` 셀은 셀마다 새 셸을 시작한다. 한 셀에서의 `cd`는 다음 셀로 이어지지 않으므로, 디렉터리 이동과 CLI 명령은 같은 셀에서 실행한다.
- CLI는 현재 작업 디렉터리를 기준으로 `databricks.yml`을 찾는다. 번들 명령이 아니더라도 현재 디렉터리의 `databricks.yml`에 구문 오류가 있으면 CLI 명령이 실패할 수 있다(호스트나 프로필 같은 인증 정보가 그 파일에 있을 수 있기 때문).

### 6-2. 인증 방식

CLI는 환경 변수 `DATABRICKS_HOST`(워크스페이스 URL)와 인증 정보를 읽어 인증할 수 있다.

| 방식 | 용도 |
|---|---|
| **개인 액세스 토큰 (PAT)** | 개인 사용자 자동화. `DATABRICKS_TOKEN` |
| **OAuth M2M (서비스 주체)** | 작업과 CI/CD 파이프라인. `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET` |
| **워크로드 ID 페더레이션** | 외부 CI 시스템의 ID 토큰으로 비밀 없이 인증. 장기 실행 자동화 |
| **노트북 세션의 단기 토큰** | 학습 편의용. 운영 패턴이 아니다 |

- 토큰은 사용자 이름과 비밀번호처럼 다룬다. 노출되면 즉시 폐기하고, 공유하지 않는다.
- 발급한 토큰은 Databricks Secrets 같은 비밀 저장소에 보관한다.
- PAT는 범위(scope)를 필요한 API로 좁혀 발급한다. "모든 API" 범위는 권장되지 않는다.
- PAT 값은 생성 직후에만 볼 수 있다. 분실하면 폐기하고 새로 만든다.

```bash
databricks -v                 # 버전 확인
databricks --help             # 명령 목록
databricks catalogs --help    # 하위 명령 도움말
databricks catalogs list      # 인증이 되었는지 확인하는 간단한 방법
```

---

## 7. 선언형 자동화 번들(DAB) 개요

### 7-1. 정의

DAB(Declarative Automation Bundles, 이전 이름 Databricks Asset Bundles)는 데이터와 AI 프로젝트에 소스 제어, 코드 리뷰, 테스트, CI/CD 같은 소프트웨어 엔지니어링 모범 사례를 쉽게 도입하게 해 주는 도구다.

| 질문 | 답 |
|---|---|
| 무엇인가 | 프로젝트의 아티팩트, 리소스, 구성을 **YAML 파일**로 기술한다. "코드를 한 번 작성하고 어디서나 배포" |
| 어떻게 동작하는가 | Databricks CLI의 `bundle` 명령이 YAML을 읽어 검증, 배포, 실행한다 |
| 어디에 쓰는가 | 개발과 CI/CD 중에 자산을 대상 환경에 배포하면서 환경별 구성을 바꾸는 일 |

DAB가 해결하는 요구:

- 모든 구성과 코드를 **YAML** 같은 읽기 쉬운 형식으로 **함께 버전 관리**
- 리소스를 **기존 REST API 파라미터**로 정의 (다른 도구와 파라미터 일관성 유지)
- 배포 중 **사용자 격리** (개발자끼리 서로의 배포를 덮어쓰지 않음)
- **환경별 재정의와 변수**

### 7-2. DAB를 쓴 개발과 CI/CD 흐름

1. 로컬 또는 워크스페이스에서 프로젝트 번들을 작성한다.
2. 개발 워크스페이스(또는 카탈로그)에 직접 배포해 운영과 분리된 상태로 테스트한다.
3. 변경을 버전 관리에 커밋하고 푸시한다.
4. 커밋이 CI/CD 파이프라인을 트리거한다. 파이프라인은 서비스 주체로 스테이징에 배포하고 코드와 통합 테스트를 실행한다.
5. 테스트 통과와 코드 리뷰, 승인 후 운영에 배포한다.

### 7-3. 표준 프로젝트 구조

| 경로 | 내용 |
|---|---|
| `databricks.yml` | **필수** 번들 구성 파일. 번들 루트에 정확히 하나 |
| `resources/` | 추가 YAML 구성 파일 (작업, 파이프라인 등 리소스별) |
| `src/` | 소스 파일 (노트북, Python 파일 등) |
| `tests/` | 단위 테스트와 통합 테스트 |

`databricks.yml`의 조건:

- YAML 형식이어야 한다.
- 최소한 최상위 `bundle` 매핑을 포함해야 한다.
- 번들 루트에 `databricks.yml`이라는 이름의 구성 파일이 하나만 있어야 한다. 다른 YAML 파일은 `include`로 참조한다.

---

## 8. databricks.yml과 최상위 매핑

최상위 매핑은 왼쪽 정렬된 키이고, 하위 매핑은 들여쓴다.

| 매핑 | 역할 |
|---|---|
| `bundle` | 번들 식별 정보. **필수** `name`. 선택적으로 `cluster_id`, `compute_id`, `git` 등 |
| `resources` | 번들이 관리하는 Databricks 객체 (jobs, pipelines, models, experiments, dashboards 등). REST API 파라미터로 정의 |
| `targets` | 환경과 환경별 재정의 (dev, stage, prod 등) |
| `variables` | 사용자 정의 변수 |
| `workspace` | 호스트, 배포 루트 경로 등 워크스페이스 설정 |
| `include` | 함께 병합할 다른 YAML 파일 경로 |
| `permissions` | 번들 리소스에 적용할 권한 |
| `artifacts` | 배포 시 빌드할 아티팩트 (예: Python wheel) |
| `sync` | 워크스페이스에 동기화할 파일의 포함/제외 규칙 |
| `presets` | 리소스 전반에 일괄 적용할 설정 (이름 접두사, 태그 등) |
| `run_as` | 번들 리소스를 실행할 ID (사용자 또는 서비스 주체) |

```yaml
bundle:
  name: demo01_bundle

resources:
  jobs:
    l1_simple_dab:                          # 리소스 키 (작업 키)
      name: my_job_name_l1_simple_dab       # 워크스페이스에 생성될 작업 이름
      tasks:
        - task_key: create_bronze_table
          notebook_task:
            notebook_path: ./src/create_bronze_table.py
            source: WORKSPACE

targets:
  development:
    mode: development
    default: true
    workspace:
      host: https://dev.cloud.databricks.com/
  production:
    mode: production
    workspace:
      host: https://prod.cloud.databricks.com/
```

---

## 9. resources: 작업 정의하기

### 9-1. 리소스 키와 이름

| 항목 | 위치 | 용도 |
|---|---|---|
| **리소스 키 (작업 키)** | `resources.jobs.<키>` | 번들 안에서 고유해야 한다. `databricks bundle run <키>`와 `${resources.jobs.<키>.id}`에서 쓴다 |
| **작업 이름** | `resources.jobs.<키>.name` | 워크스페이스에 표시되는 실제 이름 |

`bundle run`에 작업 이름을 넘기면 `resource with key "..." not found` 오류가 난다. 항상 **키**를 넘긴다.

### 9-2. 기존 작업에서 YAML 얻기

개발 단계에서는 UI로 작업을 한 번 만들어 테스트한 뒤, **작업을 코드로 보기(View as code)** 기능으로 구성을 내보내는 것이 가장 빠르다.

| 형식 | 용도 |
|---|---|
| YAML | 번들 `resources`에 바로 붙여 넣기. YAML로 작업 구성을 편집할 수도 있다 |
| Python | Databricks SDK 코드 또는 Python 기반 번들 정의 |
| JSON | CLI, SDK, REST API로 작업 생성, 수정, 조회 |

내보낸 YAML을 번들에 붙인 뒤 할 일:

1. 리소스 키를 사용자 이름이 붙지 않은 간단한 이름으로 바꾼다.
2. 절대 경로(`/Workspace/Users/...`)인 `notebook_path`를 번들 기준 **상대 경로**로 바꾼다. `resources/` 아래 YAML이라면 `../src/...`처럼 그 파일 위치 기준이다.
3. **노트북 확장자**를 정확히 붙인다(`.ipynb`, `.py`, `.sql`). 2024년 12월 이후 새 노트북의 기본 형식은 `.ipynb`다. 확장자가 틀리면 `Error: notebook src/xxx.ipynb not found`가 난다.
4. 작업 파라미터의 하드코딩 값(카탈로그 이름 등)을 변수로 바꾼다.

```yaml
resources:
  jobs:
    demo05_simple_dab:
      name: demo05_simple_dab_${workspace.current_user.short_name}
      tasks:
        - task_key: create_bronze_table
          notebook_task:
            notebook_path: ./src/create_bronze_table.ipynb
            source: WORKSPACE
        - task_key: create_silver_table
          depends_on:
            - task_key: create_bronze_table
          notebook_task:
            notebook_path: ./src/create_silver_table.ipynb
            source: WORKSPACE
      parameters:
        - name: display_target
          default: ${bundle.target}
        - name: catalog_name
          default: ${var.target_catalog}
```

- 태스크의 실행 순서는 나열 순서가 아니라 **`depends_on`** 으로 정해진다.
- compute를 지정하지 않은 태스크는 서버리스로 실행된다. 기존 클러스터는 `existing_cluster_id`로 지정한다(작업에 범용 클러스터를 쓰는 것은 권장 방식이 아니다).
- 노트북에는 작업 수준 `parameters` 외에 태스크 수준 `notebook_task.base_parameters`로 값을 넘길 수 있다.

---

## 10. targets와 배포 모드

### 10-1. targets

`targets`는 하나 이상의 대상 환경으로 구성되며, 각 대상은 고유한 이름을 가진다. 각 대상은 아티팩트, 워크스페이스 설정, 작업/파이프라인 세부 정보의 고유한 조합이다.

| 키 | 의미 |
|---|---|
| `mode` | `development` 또는 `production` |
| `default: true` | `-t`를 생략했을 때 쓰는 기본 대상. 개발 대상에 두어야 실수로 운영에 배포하는 일을 막는다 |
| `workspace.host` | 배포할 워크스페이스. 생략하면 현재 인증된 워크스페이스 |
| `workspace.root_path` | 배포 파일이 놓일 경로 |
| `variables` | 이 대상에서 쓸 변수 값 재정의 |
| `resources` | 이 대상에서 리소스 설정 재정의 (예: 태스크 compute) |
| `run_as` | 이 대상에서 리소스를 실행할 ID |

`-t`를 생략해도 기본 대상으로 배포되지만, **항상 명시하는 것이 모범 사례**다.

### 10-2. 배포 모드

| 동작 | `mode: development` | `mode: production` |
|---|---|---|
| 리소스 이름 | `[dev ${workspace.current_user.short_name}]` 접두사 | 접두사 없음 |
| 태그 | 작업과 파이프라인에 `dev` 태그 | 없음 |
| 일정/트리거 | 일시 중지 | 정의대로 동작 |
| 목적 | 개발자별 격리된 배포, 빠른 반복 | 운영 배포의 안전장치 |

- 개발 모드는 이 밖에도 동시 실행 허용, 파이프라인을 개발 모드로 실행하는 등의 동작을 한다. 운영 모드는 파이프라인이 개발 모드가 아닌지, 지정된 Git 브랜치와 현재 브랜치가 맞는지 같은 검증을 하며, 공유 경로와 서비스 주체 `run_as` 사용을 권장한다. 세부 동작은 배포 모드 문서를 기준으로 확인한다.
- 개발 모드의 접두사 덕분에 여러 개발자가 같은 번들을 배포해도 서로의 작업을 덮어쓰지 않는다.

### 10-3. 배포 위치와 소스 연결 배포

- 기본 배포 경로는 `/Workspace/Users/<사용자>/.bundle/<번들 이름>/<대상>`이며, 그 아래에 `artifacts`, `files`, `state` 폴더가 생긴다.
- 흔히 다음처럼 변수 치환으로 명시한다.

```yaml
workspace:
  root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
```

- **워크스페이스 안에서** 개발 대상으로 배포하면 **소스 연결 배포(source-linked deployment)** 가 쓰인다. 소스 파일을 배포 폴더로 복사하지 않고 기존 워크스페이스 파일을 직접 참조하므로 `files` 폴더가 비어 있다. 운영 모드 배포는 파일이 `files` 폴더에 복사된다.
- 배포된 작업에는 번들에 연결된 리소스라는 표시가 붙는다. 번들로 관리하는 리소스는 UI에서 직접 고치지 말고 번들 소스를 고쳐 재배포한다(직접 고친 내용은 다음 배포에서 덮어쓰인다).

### 10-4. 서비스 주체로 실행

```yaml
targets:
  production:
    mode: production
    run_as:
      service_principal_name: "<서비스 주체 애플리케이션 ID>"
```

운영에서는 작업을 사람이 아닌 서비스 주체로 실행해, 퇴사나 권한 변경에 영향받지 않고 최소 권한을 적용한다.

---

## 11. 번들 수명 주기 CLI 명령

| 명령 | 역할 |
|---|---|
| `databricks bundle init <템플릿>` | 템플릿으로 새 번들 생성 |
| `databricks bundle validate [-t <대상>]` | 구성 검증. 알 수 없는 속성은 경고, 구문/참조 오류는 실패 |
| `databricks bundle validate --output json` | 변수와 치환이 모두 해석된 최종 구성을 JSON으로 출력 |
| `databricks bundle summary [-t <대상>]` | 번들에 정의된 리소스와 배포 후 생성될 이름 확인 |
| `databricks bundle deploy -t <대상>` | 대상 환경에 배포 (파일 업로드, 리소스 생성/갱신) |
| `databricks bundle run -t <대상> <리소스 키>` | 배포된 작업이나 파이프라인 실행 |
| `databricks bundle destroy -t <대상> [--auto-approve]` | 배포된 작업, 파이프라인, 아티팩트를 **영구 삭제** |

```bash
databricks bundle validate
databricks bundle deploy -t development
databricks bundle run -t development demo05_simple_dab
# databricks.yml 수정 후
databricks bundle validate
databricks bundle deploy -t development
# 정리
databricks bundle destroy -t development --auto-approve
```

- 구성 파일을 고치면 **반드시 재배포**해야 워크스페이스에 반영된다.
- `destroy`는 기본적으로 확인을 묻는다. `--auto-approve`는 확인 없이 삭제하므로 자동화에서만 신중히 쓴다. 대상마다 따로 실행해야 한다.
- 템플릿 이름 없이 `bundle init`을 실행하면 대화형으로 선택을 묻는다. 대화형 입력이 안 되는 환경(노트북 `%sh`, CI)에서는 템플릿 이름을 지정한다.
- 대상만 바꿔 같은 번들을 승격한다: `-t development` → `-t stage` → `-t production`.

---

## 12. 변수와 치환

모두 `${...}`로 참조하며 배포/실행 시점에 값이 정해진다.

### 12-1. 기본 치환

| 치환 | 값 |
|---|---|
| `${bundle.name}` | 번들 이름 |
| `${bundle.target}` | 현재 대상 이름 (구 `${bundle.environment}` 대신 사용) |
| `${workspace.host}` | 워크스페이스 URL |
| `${workspace.current_user.short_name}` | 현재 사용자 짧은 이름 |
| `${workspace.current_user.userName}` | 현재 사용자 이름 (보통 이메일) |
| `${workspace.root_path}` | 배포 루트 경로 |
| `${workspace.file_path}` | 배포된 파일 경로 |
| `${resources.jobs.<키>.id}` | 배포된 작업 ID |
| `${resources.pipelines.<키>.id}` / `.name` | 배포된 파이프라인 ID / 이름 |
| `${resources.models.<키>.name}` | 모델 이름 |

### 12-2. 사용자 정의 변수

최상위 `variables` 매핑은 하나만 둘 수 있고, `${var.<이름>}`으로 참조한다.

```yaml
variables:
  # 단순 변수 (기본 string)
  my_lab_user_name:
    description: Your user name
    default: ${workspace.current_user.short_name}

  # 다른 변수를 이용해 구성
  catalog_dev:
    default: ${var.my_lab_user_name}_1_dev
  catalog_prod:
    default: ${var.my_lab_user_name}_3_prod
  target_catalog:
    default: ${var.catalog_dev}
  raw_data_path:
    default: /Volumes/${var.target_catalog}/default/health

  # 복합 변수: 구조화된 값
  my_cluster:
    description: My cluster
    type: complex
    default:
      spark_version: "15.4.x-scala2.12"
      node_type_id: "Standard_DS3_v2"
      num_workers: 2

  # 조회 변수: 이름으로 객체 ID를 찾음
  cluster_id:
    description: Look up the cluster ID by name
    lookup:
      cluster: my-cluster-name
```

| 종류 | 특징 |
|---|---|
| 단순 변수 | 기본 타입은 string. 다른 변수와 리터럴을 조합 가능 |
| 복합 변수 | `type: complex`. 클러스터 정의처럼 구조화된 값을 하나의 변수에 |
| 조회 변수 | `lookup`으로 이름을 ID로 변환. 환경마다 ID가 달라도 같은 이름으로 참조 가능 |

조회 가능한 객체 유형: `alert`, `cluster_policy`, `cluster`, `dashboard`, `instance_pool`, `job`, `metastore`, `notification_destination`, `pipeline`, `query`, `service_principal`, `warehouse`.

### 12-3. 대상별 재정의

```yaml
targets:
  development:
    mode: development
    default: true
  production:
    mode: production
    variables:
      target_catalog: ${var.catalog_prod}
```

- 대상에서 재정의하지 않은 변수는 `variables`의 기본값을 쓴다.
- **재정의하려면 최상위 `variables`에 그 변수가 선언(기본값 정의)되어 있어야 한다.**

### 12-4. 변수 값을 넣는 다른 방법

| 방법 | 예 |
|---|---|
| CLI 옵션 | `databricks bundle deploy -t stage --var="target_catalog=myuser_2_stage"` |
| 환경 변수 | `BUNDLE_VAR_target_catalog=myuser_2_stage` |
| 대상 재정의 / 기본값 | `targets.<대상>.variables`, `variables.<이름>.default` |

CLI 옵션이 가장 우선하고, 그다음 환경 변수, 대상 재정의, 기본값 순으로 적용된다(세부 우선순위는 변수 문서로 확인).

### 12-5. 변수를 쓰는 이유

| 이점 | 내용 |
|---|---|
| 환경별 맞춤 | 카탈로그, 파일 경로, 연결 정보를 환경마다 쉽게 변경 |
| 재사용성 | 변수 값만 바꿔 같은 번들을 여러 팀이나 워크스페이스에서 사용 |
| 유지 관리 | 한 곳만 고쳐 전체를 일관되게 갱신, 오류 감소 |

---

## 13. include로 모듈화하고 여러 환경에 배포하기

### 13-1. include

프로젝트가 커지면 리소스를 리소스별 YAML 파일로 나누는 것이 모범 사례다. `include`에 나열한 파일은 배포 시 `databricks.yml`과 병합된다.

```yaml
include:
  - resources/*.yml          # 글롭 패턴 가능
  - resources/variables.yml
```

리소스 파일 이름은 관례적으로 `<이름>.job.yml`, `<이름>.pipeline.yml`처럼 리소스 유형을 드러낸다. 변수도 `variables.yml`로 분리할 수 있다.

### 13-2. 하나의 번들로 개발/운영에 다르게 배포하는 예

요구 사항:

| | 개발 | 운영 |
|---|---|---|
| 카탈로그 | `<user>_1_dev` | `<user>_3_prod` |
| compute | 작은 기존 클러스터 (데이터가 작고 정적) | 서버리스 (데이터가 계속 증가, 자동 확장) |
| 모드 | development, 기본 대상 | production |

`resources/demo.job.yml`:

```yaml
resources:
  jobs:
    demo_job:
      name: ${bundle.target}_demo_dab_${workspace.current_user.userName}
      tasks:
        - task_key: create_bronze_table
          notebook_task:
            notebook_path: ../src/create_bronze_table.ipynb
        - task_key: create_silver_table
          depends_on:
            - task_key: create_bronze_table
          notebook_task:
            notebook_path: ../src/create_silver_table.ipynb
      parameters:
        - name: display_target
          default: ${bundle.target}
        - name: catalog_name
          default: ${var.target_catalog}
```

`databricks.yml`:

```yaml
bundle:
  name: demo_bundle

include:
  - resources/demo.job.yml

variables:
  user_name:
    default: ${workspace.current_user.short_name}
  catalog_dev:
    default: ${var.user_name}_1_dev
  catalog_prod:
    default: ${var.user_name}_3_prod
  target_catalog:
    default: ${var.catalog_dev}
  cluster_id:
    lookup:
      cluster: small-dev-cluster

targets:
  development:
    mode: development
    default: true
    workspace:
      root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
    resources:
      jobs:
        demo_job:
          tasks:                                  # task_key로 병합되어 compute만 재정의
            - task_key: create_bronze_table
              existing_cluster_id: ${var.cluster_id}
            - task_key: create_silver_table
              existing_cluster_id: ${var.cluster_id}

  production:
    mode: production
    workspace:
      root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
    variables:
      target_catalog: ${var.catalog_prod}         # compute 재정의 없음 → 서버리스
```

결과:

| | 개발 배포 | 운영 배포 |
|---|---|---|
| 작업 이름 | `[dev <user>] development_demo_dab_<user>` | `production_demo_dab_<user>` |
| `catalog_name` | `<user>_1_dev` | `<user>_3_prod` |
| `display_target` | `development` | `production` |
| compute | 기존 작은 클러스터 | 서버리스 |

작업 정의를 복제하지 않고 대상 수준 재정의만으로 환경 차이를 표현한다. 작업 파라미터 자체를 대상별로 재정의할 수도 있다.

```yaml
targets:
  dev:
    resources:
      jobs:
        demo_job:
          parameters:
            - name: catalog_name
              default: ${var.catalog_dev}
```

> 운영 모드에서 사용자 홈 아래 `root_path`를 쓰는 것은 학습용 단순화다. 실제 운영은 공유 경로와 서비스 주체 `run_as`를 쓴다.

---

## 14. 번들 템플릿

### 14-1. 기본 템플릿

번들을 처음부터 쓰지 않고 `databricks bundle init <템플릿>`으로 동작하는 프로젝트를 생성한다.

| 템플릿 | 생성 내용 |
|---|---|
| `default-python` | Python 작업과 Spark Declarative Pipeline이 포함된 번들. 대부분의 ETL 프로젝트 출발점 |
| `default-sql` | SQL 웨어하우스에서 SQL 쿼리를 실행하는 작업 |
| `dbt-sql` | 로컬 개발에 dbt-core를 쓰고 dbt 태스크로 배포하는 번들 |
| `mlops-stacks` | 모델 학습, 서빙, 모니터링을 포함한 MLOps 풀스택 |

`default-python`이 생성하는 구조 예: `resources/`(`<project>.job.yml`, `<project>.pipeline.yml`), `src/`(운영 코드), `scratch/`(탐색용 노트북), `tests/`, `databricks.yml`(보통 `dev`, `prod` 두 대상), `pytest.ini`, `README.md`.

```bash
databricks bundle init --help
databricks bundle init default-python
cd my_project && databricks bundle validate     # 번들은 하위 폴더에 생기므로 이동 후 실행
```

템플릿 목록과 생성 내용은 시간이 지나며 바뀌므로 템플릿 문서를 확인한다.

### 14-2. 커스텀 템플릿

- 최소 구성: `databricks_template_schema.json`(사용자 프롬프트 정의) + `databricks.yml.tmpl`.
- 사용자 프롬프트, 폴더 구조, 설정을 조직에 맞게 정의한다.
- 로컬 경로나 원격 URL(예: Git 리포지토리)을 넘겨 초기화한다.

```bash
databricks bundle init /projects/templates/test-template
databricks bundle init https://github.com/<org>/<template-repo>
```

커스텀 템플릿은 조직 전체가 같은 폴더 구조, 태스크, IaC 패턴으로 번들을 만들게 한다.

---

## 15. CI/CD 프로젝트 구성 예

### 15-1. 요구 사항과 아키텍처

- 결과물: 건강 데이터 시각화
- 작업: 일별 증분 CSV를 브론즈로 수집 → 정제된 실버 → 소비자용 골드
- 자산: 노트북, SDP, 워크플로, compute

| 카탈로그 | 데이터 |
|---|---|
| dev | 운영의 작은 부분 집합 (익명화, 약 7,500행) |
| stage | 운영의 더 큰 부분 집합 (약 35,000행) |
| prod | 매일 도착하는 CSV 전체 |

워크플로 하나 안에서 **단위 테스트 → 기대치를 쓰는 SDP(ETL + 통합 테스트) → 시각화** 순으로 실행한다.

### 15-2. 테스트 전략

| 테스트 | 구현 |
|---|---|
| 단위 테스트 | pytest로 커스텀 PySpark 함수 검증. `test_`로 시작하는 함수, `assert`, 자동 탐색, 플러그인(커버리지, 병렬) |
| 통합 테스트 | **같은 SDP 코드**가 모든 환경에서 실행되고, 입력 소스는 파이프라인 구성 변수로 바뀐다. dev와 stage에서는 테스트 테이블의 **기대치(expectations)** 로 브론즈/실버 행 수, 집계 뷰의 고유값 등을 검증 |
| 시스템 테스트 | 워크플로 전체의 엔드투엔드 실행 |

### 15-3. 폴더 구조

| 경로 | 내용 |
|---|---|
| `databricks.yml` | 번들 이름, `include`, 세 대상(`development`, `stage`, `production`) |
| `resources/variables.yml` | 사용자 이름, 이메일, 카탈로그, 원시 데이터 경로, 클러스터 조회 등 변수 |
| `resources/job/dabs_workflow.job.yml` | 작업과 태스크, 의존성 |
| `resources/pipeline/health_etl_pipeline.pipeline.yml` | SDP 파이프라인 정의 |
| `src/dlt_pipelines/` | 브론즈/실버, 골드 파이프라인 노트북 |
| `src/helpers/` | 변환용 커스텀 Python 함수 |
| `src/Final Visualization` | 시각화 노트북 |
| `tests/unit_tests/`, `tests/integration_test/` | 단위 테스트와 통합 테스트 |

DAB로 옮기기 전에 노트북은 각각 따로 테스트해 둔다.

### 15-4. 작업에서 파이프라인 참조

```yaml
resources:
  jobs:
    health_etl_workflow:                              # bundle run에 쓰는 키
      name: health_etl_workflow_${bundle.target}
      email_notifications:
        on_failure:
          - ${var.my_email}
      tasks:
        - task_key: Unit_Tests
          notebook_task:
            notebook_path: ../../tests/unit_tests/run_unit_tests.ipynb
        - task_key: Health_ETL
          depends_on:
            - task_key: Unit_Tests
          pipeline_task:
            pipeline_id: ${resources.pipelines.health_etl_pipeline.id}   # 같은 번들의 파이프라인 ID
        - task_key: Visualization
          depends_on:
            - task_key: Health_ETL
          notebook_task:
            notebook_path: ../../src/Final Visualization.ipynb
            base_parameters:
              target_catalog: ${var.target_catalog}
```

`${resources.pipelines.<키>.id}`는 배포 시 실제 파이프라인 ID로 해석되므로, 환경마다 다른 ID를 하드코딩할 필요가 없다.

### 15-5. 세 대상

| 대상 | 모드 | 재정의 | compute |
|---|---|---|---|
| `development` | development, 기본 대상 | 기본값 사용 (dev 카탈로그) | 일부 태스크는 기존 클러스터, 나머지 서버리스 |
| `stage` | development (예시) | `target_catalog: ${var.catalog_stage}`, `raw_data_path: /Volumes/${var.catalog_stage}/default/health` | 일부 태스크는 기존 클러스터 |
| `production` | production | `target_catalog`, `raw_data_path`를 prod로 | 재정의 없음 → 전부 서버리스 |

```bash
databricks bundle validate -t stage
databricks bundle deploy -t stage
databricks bundle run health_etl_workflow -t stage
```

CI 단계에서 개발에 배포하고, CD 단계에서 stage와 production에 배포한다. 코드는 그대로 두고 `-t`만 바뀐다.

---

## 16. 기존 워크플로에 태스크 추가하기

등록된 Unity Catalog 모델로 추론하는 태스크를 기존 엔지니어링 번들에 붙이는 경우다. 모델 자체를 알 필요는 없고, 자산을 번들에 연결하는 방법이 핵심이다.

1. `variables.yml`에 새 변수 추가: 모델 이름, 입력 테이블 이름, 클러스터 조회.
2. 작업 YAML에 새 태스크 추가: 앞 태스크에 의존, 변수로 파라미터 전달.
3. `bundle summary`로 생성될 리소스 확인 → `validate` → `deploy` → `run`.
4. 같은 번들을 `-t stage`로 승격.

```yaml
# resources/variables.yml
variables:
  base_model_name:
    description: Registered model name in Unity Catalog
    default: ${var.username}_1_dev.default.health_model
  silver_table_name:
    description: Input table for inference
    default: ${var.username}_1_dev.default.silver_sample_ml
  cluster_id:
    lookup:
      cluster: ${var.username}
```

```yaml
# resources/job/dabs_workflow_with_ml.job.yml 의 tasks 에 추가
        - task_key: ML_test
          depends_on:
            - task_key: Health_ETL
          existing_cluster_id: ${var.cluster_id}
          notebook_task:
            notebook_path: ../../src/Inference.ipynb
            base_parameters:
              base_model_name: ${var.base_model_name}
              silver_table_name: ${var.silver_table_name}
              target_catalog: ${var.target_catalog}
            source: WORKSPACE
```

워크플로를 태스크 하나씩 늘리고 변경마다 검증하면 문제를 쉽게 격리할 수 있다.

---

## 17. 로컬 개발: CLI, Databricks Connect, VS Code

| 도구 | 적합한 작업 |
|---|---|
| **Databricks CLI** | 셸 스크립팅, 가벼운 명령줄 작업, 개발과 CI/CD. 통합 인증(OAuth, PAT 등) |
| **Databricks Connect** | 로컬 IDE에서 작성한 Spark 코드를 원격 Databricks compute에서 실행. 대화형 디버깅. `pip install "databricks-connect==<런타임에 맞는 버전>"` |
| **VS Code용 Databricks 확장** | VS Code에 Databricks 통합. 리소스 탐색, 클러스터에서 Python 파일 실행과 디버깅, DAB 기능 |

VS Code 확장으로 할 수 있는 것:

- `databricks.yml`을 스키마 검증과 자동 완성으로 편집
- 편집기에서 `validate`, `deploy`, `run`, `destroy` 실행
- 로컬 코드를 워크스페이스와 동기화해 빠르게 반복

인증은 워크스페이스 URL과 인증 정보(PAT 또는 OAuth)로 한다. 실제 팀은 대부분 로컬 IDE에서 번들을 작성하고, 배포는 CI/CD에 맡긴다.

---

## 18. CI/CD 모범 사례와 GitHub Actions

### 18-1. 다섯 가지 실천 사항

| 실천 | 내용 |
|---|---|
| 명확한 테스트 전략 | 단위, 통합, 엔드투엔드 테스트를 정의하고 자동화 |
| 버전 관리 전략 | 코드, 노트북, 구성 파일을 Git으로 관리하고 브랜치 전략 정의 |
| 코드 리뷰 프로세스 | 병합 전 필수 피어 리뷰, PR마다 CI 빌드 트리거 |
| DAB로 배포 자동화 | 일관되고 반복 가능한 배포, 사람의 오류 최소화, 올바른 코드가 올바른 환경으로 |
| 파이프라인 모니터링과 최적화 | 피드백 수집, 병목 파악, 자동화 효율 개선 |

### 18-2. GitFlow + DAB + GitHub Actions 패턴

| 브랜치 | 역할 | 트리거되는 동작 |
|---|---|---|
| `feature/*` | `dev`에서 분기해 기능을 격리 개발 | 개발자가 CLI로 DEV 워크스페이스에 직접 배포 |
| `dev` | 리뷰를 거친 기능이 병합되는 곳 | 병합 시 Actions가 QA(스테이지)에 배포, 테스트, 코드 커버리지, 보안 스캔 실행, release 브랜치 생성 후 `main`으로 PR |
| `release/*` | 운영 출시 후보 | release PR이 병합되면 Actions가 운영에 배포, 릴리스 태그 생성, `main` → `dev` PR |
| `main` | 운영 코드 | |
| `hotfix/*` | `main`에서 분기한 긴급 수정 | 수정 후 `main`과 `dev` 모두에 병합 |

### 18-3. GitHub Actions 워크플로 예

```yaml
# .github/workflows/deploy-stage.yml
name: deploy-stage
on:
  push:
    branches: [dev]

jobs:
  deploy:
    runs-on: ubuntu-latest
    env:
      DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
      DATABRICKS_CLIENT_ID: ${{ secrets.DATABRICKS_CLIENT_ID }}          # 서비스 주체
      DATABRICKS_CLIENT_SECRET: ${{ secrets.DATABRICKS_CLIENT_SECRET }}
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - run: databricks bundle validate -t stage
      - run: databricks bundle deploy -t stage
      - run: databricks bundle run -t stage health_etl_workflow
```

- 비밀 값은 CI 시스템의 secret 저장소에 두고, 서비스 주체 OAuth M2M(또는 워크로드 ID 페더레이션)으로 인증한다.
- 운영 배포 워크플로는 별도로 두고 `main` 병합이나 릴리스 태그로 트리거하며, 필요하면 승인 게이트를 둔다(Continuous Delivery).

### 18-4. Databricks와 Git

- Git은 소스 코드 변경을 추적하는 오픈 소스 버전 관리 시스템이다. 롤백과 협업, 브랜치와 병합을 통한 병렬 작업, 개발자마다 전체 로컬 리포지토리를 갖는 분산 워크플로를 지원한다.
- GitHub, GitHub Actions, GitLab, Azure DevOps, Bitbucket 등과 통합할 수 있다.
- Databricks는 Git 폴더로 노트북 개발 중 원격 리포지토리를 쓰게 하고, Repos REST API로 CI/CD에서 Git 작업을 자동화할 수 있다.

---

## 19. DAB 생태계의 최근 변화

| 변화 | 내용 |
|---|---|
| 이름 변경 | Databricks Asset Bundles → Declarative Automation Bundles. CLI 명령(`databricks bundle`)과 개념은 같다 |
| 워크스페이스 UI 지원 | 워크스페이스 파일 브라우저에서 번들을 보고, 편집하고, 배포. 어떤 작업과 파이프라인이 번들에 속하는지 확인하고 소스 보기와 렌더링된 리소스 보기를 전환 |
| Python으로 번들 작성 | YAML 대신 Python으로 리소스 정의. 반복문, 조건문, 공유 헬퍼로 비슷한 리소스가 많은 프로젝트에 유용 |
| 대시보드 등 리소스 확대 | AI/BI 대시보드 같은 리소스도 같은 워크플로로 안전하게 배포 |
| AI 어시스턴트 | YAML 스캐폴딩, 대상 재정의 초안, 검증 오류 진단을 도울 수 있다. 원리를 알아야 결과를 판단할 수 있다 |

DAB는 Databricks CLI에 포함되어 배포되므로, 새 기능은 CLI 릴리스 노트에서 확인한다.

---

## 20. 핵심 정리 치트시트

### 개념

| 키워드 | 핵심 |
|---|---|
| DataOps / MLOps | DevOps를 데이터 엔지니어링 / ML에 적용 |
| CI | 개발, 빌드, 테스트, 버전 관리. 자주 병합하고 자동 테스트 |
| Delivery vs Deployment | 운영 배포가 수동 / 자동 |
| 테스트 피라미드 | 단위(많고 빠름) → 통합 → 시스템(적고 느림) |
| 환경 격리 | 워크스페이스, 카탈로그, 또는 둘 다 |
| 환경별 데이터 | dev 작은 익명/합성, stage 운영 유사, prod 실제 |
| 프로젝트 구성 요소 | 코드, 리소스, 실행 환경 |
| 실행 ID | dev는 사용자, stage/prod는 서비스 주체 |
| 배포 도구 | UI, REST API, SDK, Terraform, CLI + DAB |

### 번들 구성

| 키워드 | 핵심 |
|---|---|
| 구조 | `databricks.yml`(루트에 하나, 필수 `bundle.name`), `resources/`, `src/`, `tests/` |
| 최상위 매핑 | bundle, resources, targets, variables, workspace, include, permissions, artifacts, sync, presets, run_as |
| 리소스 키 vs 이름 | 키는 `bundle run`과 참조용, 이름은 UI 표시용 |
| 코드로 보기 | UI로 만든 작업의 YAML 추출 → 상대 경로, 확장자, 변수로 수정 |
| `depends_on` | 태스크 순서를 결정 (나열 순서 무관) |
| compute 미지정 | 서버리스 |
| development 모드 | `[dev 사용자]` 접두사, `dev` 태그, 일정 일시 중지 |
| production 모드 | 접두사 없음, 운영 검증. 서비스 주체 `run_as` 권장 |
| `default: true` | 개발 대상에. `-t`는 항상 명시 |
| 소스 연결 배포 | 워크스페이스에서 dev 배포 시 파일 복사 없이 원본 참조 |

### CLI와 변수

| 키워드 | 핵심 |
|---|---|
| 수명 주기 | init → validate → summary → deploy → run → destroy |
| `--output json` | 치환이 해석된 최종 구성 확인 |
| `%sh` | 셀마다 새 셸. `cd`와 명령을 같은 셀에 |
| 기본 치환 | `${bundle.name}`, `${bundle.target}`, `${workspace.current_user.userName}`, `${resources.jobs.<키>.id}` |
| 사용자 변수 | `${var.x}`. 단순(string), 복합(`type: complex`), 조회(`lookup`) |
| 대상 재정의 | `targets.<t>.variables`. 최상위에 선언이 있어야 함 |
| CLI 재정의 | `--var="name=value"`, 환경 변수 `BUNDLE_VAR_name` |
| include | 리소스별 YAML 분리, `variables.yml` 분리 |
| 템플릿 | default-python, default-sql, dbt-sql, mlops-stacks. 커스텀은 schema JSON + `databricks.yml.tmpl` |

### CI/CD

| 키워드 | 핵심 |
|---|---|
| 승격 | 같은 번들, `-t development` → `-t stage` → `-t production` |
| 파이프라인 참조 | `pipeline_task.pipeline_id: ${resources.pipelines.<키>.id}` |
| 인증 | 사람은 PAT/OAuth U2M, 자동화는 서비스 주체 OAuth M2M 또는 워크로드 ID 페더레이션 |
| 운영 배포 위치 | 로컬 머신 또는 CI/CD 러너, 노트북 아님 |
| 모범 사례 5 | 테스트 전략, 버전 관리, 코드 리뷰, DAB 자동 배포, 모니터링 |
| GitFlow | feature → dev(QA 배포) → release → main(운영 배포), hotfix는 main과 dev에 병합 |

---

## 21. 자가 점검 문제

> 답을 가리고 먼저 생각해본 뒤 펼쳐보자.

**Q1.** DevOps, DataOps, MLOps의 관계는?
<details><summary>정답</summary>

DevOps는 소프트웨어 엔지니어링 모범 사례와 IT 운영을 결합해 코드를 지속적으로 통합, 테스트, 배포하는 문화와 실천이다. DataOps는 이를 데이터 파이프라인에, MLOps는 머신 러닝 모델의 배포와 모니터링에 적용한 것이다.
</details>

**Q2.** Continuous Delivery와 Continuous Deployment의 차이는?
<details><summary>정답</summary>

둘 다 스테이징 배포까지 자동화한다. Continuous Delivery는 운영 배포를 사람이 결정하는 수동 게이트로 남기고, Continuous Deployment는 테스트를 통과하면 운영까지 자동으로 배포한다. 후자는 잘 설계된 테스트가 전제다.
</details>

**Q3.** 테스트 피라미드의 세 층과 각각의 예는?
<details><summary>정답</summary>

단위 테스트(개별 함수, 예: 커스텀 PySpark 함수)가 가장 많고 빠르며, 통합 테스트(구성 요소 간 상호작용, 예: 노트북, 파이프라인, 작업)가 중간, 시스템 테스트(전체 엔드투엔드, 예: 워크플로 안의 데이터 파이프라인)가 가장 적고 느리다.
</details>

**Q4.** Databricks에서 환경을 격리하는 방법과 환경별로 쓰는 데이터는?
<details><summary>정답</summary>

환경마다 워크스페이스를 나누거나, 카탈로그를 나누거나, 둘 다 쓴다. 개발은 운영의 작은 익명화/합성 부분 집합, 스테이지는 운영의 구조와 볼륨을 반영하되 민감 정보를 제거한 데이터, 운영은 실제 라이브 데이터를 쓴다.
</details>

**Q5.** Databricks 프로젝트를 이루는 세 가지 구성 요소는?
<details><summary>정답</summary>

데이터 제품을 만드는 로직인 코드(노트북, Python 파일, wheel, JAR, dbt 등), 코드가 실행되는 Databricks 객체인 리소스(작업, 파이프라인, MLflow 등), 어디서 어떻게 실행되는지인 실행 환경(워크스페이스, Unity Catalog, compute 구성)이다.
</details>

**Q6.** 스테이징과 운영에서 사용자 대신 서비스 주체로 작업을 실행하는 이유는?
<details><summary>정답</summary>

자동화가 필요한 리소스에만 접근하는 최소 권한을 적용할 수 있고, 개인의 퇴사나 권한 변경에 작업이 영향받지 않으며, 사용자나 그룹 권한으로 실행하는 것보다 안전하기 때문이다.
</details>

**Q7.** DAB 이전의 배포 방법 세 가지(UI, REST API/SDK, Terraform)의 단점은?
<details><summary>정답</summary>

UI는 시간이 많이 들고 사람의 오류가 생기며 CI/CD에 맞지 않는다. REST API/SDK는 배울 API와 클래스가 많고 전체 자동화 코드를 짜는 데 오래 걸린다. Terraform은 강력하지만 데이터 엔지니어에게 학습 부담이 크고 관리할 도구가 하나 더 늘어난다.
</details>

**Q8.** DAB가 충족하는 네 가지 요구는?
<details><summary>정답</summary>

모든 구성과 코드를 YAML로 함께 버전 관리, 기존 REST API 파라미터로 리소스 정의, 배포 중 사용자 격리, 환경 기반 재정의와 변수 지정이다.
</details>

**Q9.** 운영 환경에서 번들은 어디서 배포하는가? 노트북에서 배포하지 않는 이유는?
<details><summary>정답</summary>

CLI가 구성된 개발자의 로컬 머신이나, 서비스 주체로 OAuth M2M 인증된 CI/CD 러너(GitHub Actions, Azure DevOps 등)에서 배포한다. 노트북 배포는 학습 편의용이며, 노트북 세션 토큰은 단기이고 환경 변수에 노출되며 재현 가능한 자동화 파이프라인이 아니기 때문이다.
</details>

**Q10.** CI/CD 파이프라인에서 Databricks CLI를 인증할 때 권장되는 방식은?
<details><summary>정답</summary>

서비스 주체를 쓰는 OAuth M2M(`DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`) 또는 비밀 없이 CI 시스템의 ID 토큰을 쓰는 워크로드 ID 페더레이션이다. 비밀 값은 CI의 secret 저장소에 둔다. PAT는 개인 사용자 자동화용이다.
</details>

**Q11.** 번들 프로젝트의 표준 구조와 `databricks.yml`이 만족해야 할 조건은?
<details><summary>정답</summary>

루트의 `databricks.yml`, 추가 YAML을 담는 `resources/`, 소스 코드의 `src/`, 테스트의 `tests/`로 구성한다. `databricks.yml`은 YAML 형식이어야 하고, 최상위 `bundle` 매핑(필수 `name`)을 포함해야 하며, 번들 루트에 하나만 있어야 한다.
</details>

**Q12.** `databricks.yml`의 최상위 매핑 중 bundle, resources, targets의 역할은?
<details><summary>정답</summary>

bundle은 번들 이름 같은 식별 정보, resources는 번들이 관리하는 작업, 파이프라인 등 Databricks 객체를 REST API 파라미터로 정의, targets는 dev, prod 같은 대상 환경과 환경별 재정의를 정의한다.
</details>

**Q13.** 리소스 키와 작업 이름의 차이와, `bundle run`에 넘겨야 하는 것은?
<details><summary>정답</summary>

리소스 키는 `resources.jobs.<키>`로 번들 안에서 고유한 식별자이고, 작업 이름은 `name` 값으로 워크스페이스에 표시된다. `bundle run`에는 리소스 키를 넘기며, 이름을 넘기면 resource with key not found 오류가 난다.
</details>

**Q14.** 작업을 코드로 보기로 얻은 YAML을 번들에 붙인 뒤 수정해야 할 것은?
<details><summary>정답</summary>

리소스 키를 간단한 이름으로 바꾸고, 절대 경로 notebook_path를 번들 기준 상대 경로로 바꾸고, 올바른 노트북 확장자(.ipynb, .py, .sql)를 붙이고, 카탈로그 같은 하드코딩 값을 변수로 바꾼다.
</details>

**Q15.** `notebook src/xxx.ipynb not found` 오류의 흔한 원인은?
<details><summary>정답</summary>

노트북의 실제 형식과 경로에 쓴 확장자가 다르거나 상대 경로가 잘못된 경우다. 새 노트북의 기본 형식은 .ipynb지만 .py나 .sql로 저장된 노트북도 있으므로 실제 형식을 확인한다. resources 아래 파일에서는 그 파일 위치 기준 상대 경로(../src/...)를 쓴다.
</details>

**Q16.** 태스크 실행 순서는 무엇으로 정해지며, compute를 지정하지 않은 태스크는 어디서 실행되는가?
<details><summary>정답</summary>

YAML에 나열된 순서가 아니라 depends_on으로 정해진다. compute를 지정하지 않은 태스크는 서버리스로 실행된다.
</details>

**Q17.** development 모드와 production 모드의 차이는?
<details><summary>정답</summary>

development 모드는 리소스 이름에 [dev 사용자] 접두사와 dev 태그를 붙이고 일정과 트리거를 일시 중지해 개발자별로 격리된 배포를 만든다. production 모드는 접두사 없이 정의대로 배포하며, 파이프라인 개발 모드 여부나 Git 브랜치 같은 운영 검증을 하고 서비스 주체 run_as 사용을 권장한다.
</details>

**Q18.** `default: true`를 개발 대상에 두는 이유와, 그래도 `-t`를 명시하는 이유는?
<details><summary>정답</summary>

-t를 생략했을 때 실수로 운영에 배포되지 않도록 기본 대상을 개발로 두는 것이다. 그래도 의도한 대상을 명확히 하고 실수를 막기 위해 항상 -t를 명시하는 것이 모범 사례다.
</details>

**Q19.** 소스 연결 배포란 무엇이며 언제 쓰이는가?
<details><summary>정답</summary>

워크스페이스 안에서 개발 대상으로 배포할 때 쓰이는 방식으로, 소스 파일을 배포 폴더로 복사하지 않고 기존 워크스페이스 파일을 직접 참조한다. 그래서 .bundle/.../files 폴더가 비어 있다.
</details>

**Q20.** 번들 수명 주기 명령을 순서대로 나열하고 각각의 역할을 설명하라.
<details><summary>정답</summary>

init(템플릿으로 생성), validate(구성 검증), summary(생성될 리소스 확인), deploy(대상에 배포), run(리소스 키로 작업이나 파이프라인 실행), destroy(배포된 리소스 영구 삭제)다.
</details>

**Q21.** `databricks bundle validate --output json`은 언제 유용한가?
<details><summary>정답</summary>

변수와 치환이 모두 해석된 최종 번들 구성을 JSON으로 보여 주므로, 대상별로 변수가 예상대로 해석되었는지와 실제로 무엇이 배포될지 확인할 때 쓴다.
</details>

**Q22.** `databricks.yml`을 수정한 뒤 해야 할 일과 `destroy` 사용 시 주의점은?
<details><summary>정답</summary>

수정 내용은 재배포해야 워크스페이스에 반영되므로 validate 후 deploy를 다시 실행한다. destroy는 배포된 작업, 파이프라인, 아티팩트를 되돌릴 수 없게 삭제하고, --auto-approve는 확인 없이 삭제하므로 신중히 쓰며 대상마다 따로 실행해야 한다.
</details>

**Q23.** 노트북 `%sh` 셀에서 하위 폴더의 번들을 검증하려면 어떻게 해야 하는가?
<details><summary>정답</summary>

CLI는 현재 작업 디렉터리 기준으로 databricks.yml을 찾고, %sh 셀은 셀마다 새 셸을 시작해 cd가 다음 셀로 이어지지 않으므로, 같은 셀에서 cd my_project 후 databricks bundle validate를 실행한다.
</details>

**Q24.** 자주 쓰는 기본 치환 다섯 가지를 들어라.
<details><summary>정답</summary>

${bundle.name}, ${bundle.target}, ${workspace.current_user.short_name}, ${workspace.current_user.userName}, ${workspace.root_path}, ${workspace.file_path}, ${resources.jobs.<키>.id}, ${resources.pipelines.<키>.id} 등이다. ${bundle.environment}는 더 이상 쓰지 않고 ${bundle.target}을 쓴다.
</details>

**Q25.** 단순 변수, 복합 변수, 조회 변수의 차이는?
<details><summary>정답</summary>

단순 변수는 기본 string 타입이고, 복합 변수는 type: complex로 클러스터 정의처럼 구조화된 값을 담으며, 조회 변수는 lookup으로 클러스터, 웨어하우스, 서비스 주체 등의 이름을 해당 객체 ID로 변환한다.
</details>

**Q26.** 조회 변수를 쓰면 좋은 이유는?
<details><summary>정답</summary>

클러스터나 웨어하우스 ID는 환경마다 다르고 재생성되면 바뀌지만 이름은 안정적이다. 이름으로 조회하면 ID를 하드코딩하지 않고도 배포 시 올바른 ID가 들어간다.
</details>

**Q27.** 대상별로 변수 값을 재정의하는 방법과 전제 조건은?
<details><summary>정답</summary>

targets.<대상>.variables 아래에 새 값을 지정한다. 재정의하려면 최상위 variables 매핑에 그 변수가 선언되어 기본값이 정의되어 있어야 하며, 재정의하지 않은 대상은 기본값을 쓴다.
</details>

**Q28.** YAML을 고치지 않고 CLI에서 변수 값을 바꾸는 방법은?
<details><summary>정답</summary>

`--var="name=value"` 옵션을 주거나(예: `databricks bundle deploy -t stage --var="target_catalog=x_2_stage"`) `BUNDLE_VAR_name` 환경 변수를 설정한다. CLI 옵션이 가장 우선한다.
</details>

**Q29.** `include` 매핑을 쓰는 이유와 일반적인 파일 구성은?
<details><summary>정답</summary>

프로젝트가 커질수록 리소스를 리소스별 YAML로 나누면 관리가 쉬워진다. include에 나열한 파일은 배포 시 databricks.yml과 병합된다. 보통 resources/<이름>.job.yml, <이름>.pipeline.yml, variables.yml로 나누고 글롭 패턴으로 포함한다.
</details>

**Q30.** 하나의 번들로 개발은 작은 클러스터, 운영은 서버리스로 실행하려면 어떻게 구성하는가?
<details><summary>정답</summary>

리소스 파일에는 compute를 지정하지 않고(기본 서버리스), development 대상의 resources 아래에서 task_key별로 existing_cluster_id: ${var.cluster_id}를 재정의한다. production 대상은 compute를 재정의하지 않고 카탈로그 변수만 바꾼다.
</details>

**Q31.** Databricks가 제공하는 기본 번들 템플릿 네 가지는?
<details><summary>정답</summary>

Python 작업과 SDP가 포함된 default-python, SQL 웨어하우스에서 SQL을 실행하는 default-sql, dbt를 쓰는 dbt-sql, MLOps 풀스택인 mlops-stacks다. `databricks bundle init <템플릿>`으로 생성한다.
</details>

**Q32.** 커스텀 번들 템플릿의 최소 구성과 사용 방법은?
<details><summary>정답</summary>

최소한 databricks_template_schema.json과 databricks.yml.tmpl이 필요하다. 사용자 프롬프트와 폴더 구조, 설정을 정의할 수 있고, 로컬 경로나 원격 URL을 databricks bundle init에 넘겨 초기화한다.
</details>

**Q33.** 노트북 `%sh`나 CI에서 `bundle init`을 실행할 때 템플릿 이름을 지정해야 하는 이유는?
<details><summary>정답</summary>

템플릿 이름이 없으면 대화형 모드로 선택을 묻는데, 노트북 %sh나 CI에서는 대화형 입력을 할 수 없기 때문이다.
</details>

**Q34.** 같은 SDP 코드로 dev, stage, prod에서 통합 테스트를 하는 방법은?
<details><summary>정답</summary>

같은 파이프라인 노트북을 모든 환경에서 쓰고, 입력 소스와 카탈로그는 파이프라인 구성 변수(번들 변수로 주입)로 바꾼다. dev와 stage에서는 테스트 테이블의 기대치로 브론즈/실버 행 수나 집계 뷰의 고유값을 검증한다.
</details>

**Q35.** 같은 번들의 파이프라인을 작업 태스크에서 실행하려면 파이프라인 ID를 어떻게 지정하는가?
<details><summary>정답</summary>

pipeline_task.pipeline_id에 ${resources.pipelines.<파이프라인 키>.id}를 쓴다. 배포 시 실제 ID로 해석되므로 환경마다 다른 ID를 하드코딩하지 않아도 된다.
</details>

**Q36.** CI/CD에서 번들을 dev → stage → prod로 승격할 때 무엇이 바뀌고 무엇이 그대로인가?
<details><summary>정답</summary>

번들과 코드는 그대로이고 배포 대상(-t)만 바뀐다. 대상별 재정의(카탈로그, 원시 데이터 경로, compute, 모드, 실행 ID)가 환경 차이를 처리한다. 개발 배포는 CI 단계, stage와 prod 배포는 CD 단계에서 이루어진다.
</details>

**Q37.** 기존 워크플로에 모델 추론 태스크를 추가할 때 필요한 번들 변경은?
<details><summary>정답</summary>

variables.yml에 모델 이름, 입력 테이블 이름, 클러스터 조회 같은 변수를 추가하고, 작업 YAML에 앞 태스크에 depends_on으로 의존하며 notebook_task.base_parameters로 변수를 넘기는 새 태스크를 추가한다. 그다음 summary, validate, deploy, run으로 확인하고 -t stage로 승격한다.
</details>

**Q38.** Databricks CLI, Databricks Connect, VS Code 확장은 각각 언제 쓰는가?
<details><summary>정답</summary>

CLI는 셸 스크립팅과 CI/CD, 번들 명령에, Databricks Connect는 로컬 IDE의 Spark 코드를 원격 compute에서 실행하며 대화형 디버깅할 때, VS Code 확장은 리소스 탐색, 클러스터에서 파일 실행과 디버깅, databricks.yml 자동 완성과 번들 배포까지 IDE 안에서 처리할 때 쓴다.
</details>

**Q39.** 데이터 엔지니어링 CI/CD의 다섯 가지 모범 사례는?
<details><summary>정답</summary>

명확한 테스트 전략(단위, 통합, 엔드투엔드 자동화), 버전 관리와 브랜치 전략, 필수 코드 리뷰와 PR 빌드, DAB를 통한 배포 자동화, CI/CD 파이프라인의 지속적 모니터링과 최적화다.
</details>

**Q40.** GitFlow와 DAB, GitHub Actions를 결합한 배포 패턴을 설명하라.
<details><summary>정답</summary>

개발자는 dev에서 feature 브랜치를 만들어 DEV 워크스페이스에 직접 배포하며 개발하고, PR 리뷰 후 dev에 병합하면 Actions가 QA에 번들을 배포하고 테스트, 커버리지, 보안 스캔을 실행한 뒤 release 브랜치와 main으로의 PR을 만든다. release PR이 병합되면 Actions가 운영에 배포하고 릴리스 태그를 만든다. 긴급 수정은 main에서 hotfix 브랜치를 만들어 main과 dev 모두에 병합한다.
</details>

**Q41.** 번들로 배포한 작업을 UI에서 직접 고치면 안 되는 이유는?
<details><summary>정답</summary>

번들 소스가 진실 공급원이므로 다음 배포에서 UI 변경 내용이 번들 정의로 덮어쓰인다. 변경은 번들 YAML을 고쳐 버전 관리하고 재배포한다.
</details>

**Q42.** 최근 DAB 생태계의 변화 세 가지는?
<details><summary>정답</summary>

워크스페이스 파일 브라우저에서 번들을 보고, 편집하고, 배포하는 UI 지원, YAML 대신 Python으로 번들을 작성하는 기능, 그리고 Databricks Asset Bundles에서 Declarative Automation Bundles로의 이름 변경(명령과 개념은 동일)이다.
</details>

---

## 22. 참고 링크

- 번들 개요: https://docs.databricks.com/aws/en/dev-tools/bundles/
- 번들 구성 참조: https://docs.databricks.com/aws/en/dev-tools/bundles/reference
- 번들 설정 (최상위 매핑): https://docs.databricks.com/aws/en/dev-tools/bundles/settings
- 변수와 치환: https://docs.databricks.com/aws/en/dev-tools/bundles/variables
- 배포 모드: https://docs.databricks.com/aws/en/dev-tools/bundles/deployment-modes
- 번들 실행 ID (run_as): https://docs.databricks.com/aws/en/dev-tools/bundles/run-as
- 클러스터 설정 재정의: https://docs.databricks.com/aws/en/dev-tools/bundles/cluster-override
- 번들 템플릿: https://docs.databricks.com/aws/en/dev-tools/bundles/templates
- 번들 작업 튜토리얼: https://docs.databricks.com/aws/en/dev-tools/bundles/work-tasks
- `databricks bundle` 명령: https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands
- Databricks CLI 설치: https://docs.databricks.com/aws/en/dev-tools/cli/install
- Databricks CLI 명령: https://docs.databricks.com/aws/en/dev-tools/cli/commands
- 작업을 코드로 보기: https://docs.databricks.com/aws/en/jobs/automate#view-jobs-as-code
- 작업 생성과 관리: https://docs.databricks.com/aws/en/jobs/create-run-jobs
- 인증 개요: https://docs.databricks.com/aws/en/dev-tools/auth/
- 개인 액세스 토큰: https://docs.databricks.com/aws/en/dev-tools/auth/pat
- OAuth M2M: https://docs.databricks.com/aws/en/dev-tools/auth/oauth-m2m
- 시크릿 관리: https://docs.databricks.com/aws/en/security/secrets/
- Databricks의 CI/CD: https://docs.databricks.com/aws/en/dev-tools/index-ci-cd
- VS Code용 Databricks 확장: https://docs.databricks.com/aws/en/dev-tools/vscode-ext
- VS Code 확장의 번들 기능: https://docs.databricks.com/aws/en/dev-tools/vscode-ext/bundles
- Databricks Connect: https://docs.databricks.com/aws/en/dev-tools/databricks-connect/
- MLOps Stacks: https://docs.databricks.com/aws/en/machine-learning/mlops/mlops-stacks
- Databricks CLI 릴리스: https://github.com/databricks/cli/releases
