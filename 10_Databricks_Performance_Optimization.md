# Databricks 성능 최적화 — 핵심 개념 정리

---

## 목차

1. [성능의 기준: 지연 시간, 처리량, 실행당 DBU](#1-성능의-기준-지연-시간-처리량-실행당-dbu)
2. [Spark가 쿼리를 실행하는 방식](#2-spark가-쿼리를-실행하는-방식)
3. [분산 쿼리가 느린 이유와 두 가지 레버](#3-분산-쿼리가-느린-이유와-두-가지-레버)
4. [플랫폼 우선 워크플로](#4-플랫폼-우선-워크플로)
5. [먼저 측정하기: Query Profile](#5-먼저-측정하기-query-profile)
6. [Performance Insights와 query.history로 문제 쿼리 찾기](#6-performance-insights와-queryhistory로-문제-쿼리-찾기)
7. [정직한 벤치마크](#7-정직한-벤치마크)
8. [소형 파일 문제와 관리형 테이블의 자동 튜닝](#8-소형-파일-문제와-관리형-테이블의-자동-튜닝)
9. [Predictive Optimization](#9-predictive-optimization)
10. [파티셔닝은 예외적인 선택](#10-파티셔닝은-예외적인-선택)
11. [Liquid 클러스터링](#11-liquid-클러스터링)
12. [캐시 계층](#12-캐시-계층)
13. [셔플과 조인 전략](#13-셔플과-조인-전략)
14. [스큐](#14-스큐)
15. [스필](#15-스필)
16. [행 폭발과 고차 함수](#16-행-폭발과-고차-함수)
17. [드라이버 함정과 실행 계획 비대화](#17-드라이버-함정과-실행-계획-비대화)
18. [UDF 선택과 Photon 이탈](#18-udf-선택과-photon-이탈)
19. [서버리스 우선](#19-서버리스-우선)
20. [Photon](#20-photon)
21. [비용 환산과 비용 귀속](#21-비용-환산과-비용-귀속)
22. [종합 시나리오](#22-종합-시나리오)
23. [핵심 정리 치트시트](#23-핵심-정리-치트시트)
24. [자가 점검 문제](#24-자가-점검-문제)
25. [참고 링크](#25-참고-링크)

---

## 1. 성능의 기준: 지연 시간, 처리량, 실행당 DBU

모든 최적화는 "빠름"과 "저렴함" 두 축으로 평가한다.

| 지표 | 의미 | 주로 중요한 곳 |
|---|---|---|
| **지연 시간 (Latency)** | 한 번 실행에 걸리는 실제 경과 시간. 사용자가 기다리는 시간 | 대화형 쿼리, 대시보드 |
| **처리량 (Throughput)** | 단위 시간당 완료되는 작업량 | 파이프라인, 동시성 |
| **비용 (Cost)** | 실행 한 번에 소비되는 플랫폼 리소스. **실행당 DBU**로 측정하고 추세를 본다 | 모든 워크로드 |

- **비용은 1급 지표다.** compute를 늘리면 거의 모든 작업이 빨라지지만 비용은 조용히 증가한다. 10% 빨라지고 비용이 2배가 된 쿼리는 개선이 아니다.
- 비용은 월말에 확인하는 것이 아니라 작업, 웨어하우스, 사용자 단위로 귀속시키고 추세로 추적한다.
- 서버리스 지출은 태그가 없으면 귀속할 수 없으므로, 태그 지정도 성능 작업의 일부다.

---

## 2. Spark가 쿼리를 실행하는 방식

### 2-1. 기본 용어

| 용어 | 의미 |
|---|---|
| **파티션** | 데이터의 한 청크. 런타임 개념이다. 디스크 레이아웃을 뜻하는 "파티셔닝"과는 별개다 |
| **태스크** | 코어 하나가 파티션 하나를 처리하는 작업 단위 |
| **병렬성** | 동시에 실행할 수 있는 태스크 수 = 전체 코어 수. 파티션이 코어보다 많으면 여러 웨이브로 실행되고, 적으면 코어가 논다 |
| **지연 평가** | `filter`, `join`, `groupBy` 같은 변환은 계획만 만든다. `count`, `show`, `collect`, `write` 같은 **액션**이 실행을 트리거한다 |

실행 계층 구조는 다음과 같다.

- **드라이버**: 계획을 받아 Catalyst로 최적화하고 태스크를 실행자에게 분배한다. 서버리스(Spark Connect)에서는 드라이버가 원격에 있고 노트북은 경량 클라이언트로 통신한다.
- **실행자**: 실제 데이터 처리를 한다. 각 실행자는 코어와 메모리를 가지며, Databricks에서는 Photon(C++ 엔진)이 스캔, 조인, 집계, 쓰기를 처리한다.
- **코어**: 한 번에 태스크 하나를 실행한다.
- 한 스테이지의 소요 시간은 **가장 느린 태스크**가 결정한다.

### 2-2. 좁은 변환과 넓은 변환

| 구분 | 연산 | 특징 |
|---|---|---|
| **좁은 변환 (narrow)** | `filter`, `select`, `withColumn` | 각 파티션을 독립적으로 처리. 네트워크 이동 없음 |
| **넓은 변환 (wide)** | `groupBy`, `join`, `distinct`, `orderBy`, 윈도우 함수 | 같은 키가 한곳에 모이도록 행을 네트워크로 재분배. 이것이 **셔플** |

셔플은 대부분의 쿼리에서 가장 비용이 큰 단일 작업이다.

### 2-3. 코드에서 태스크까지

1. **변환 작성**: 지연 실행. 미확인 논리 계획이 만들어진다.
2. **액션**: 실행 트리거.
3. **Catalyst 최적화**: 참조 확인, 조건자 푸시다운, 컬럼 프루닝, 조인 재정렬, 물리 연산자 선택(브로드캐스트 vs 셔플 조인, 해시 vs 정렬 집계).
4. **작업 → 스테이지**: 셔플 경계(Exchange 연산자)마다 스테이지가 나뉜다. 스테이지 안에서는 네트워크 I/O가 없다.
5. **스테이지 → 태스크**: 스테이지의 태스크 수 = 해당 스테이지의 파티션 수.

3과 5 사이에서 런타임 재최적화가 개입한다.

| 기능 | 하는 일 |
|---|---|
| **AQE (Adaptive Query Execution)** | 완료된 스테이지의 실제 통계로 계획을 재조정. 작은 셔플 파티션 병합, 스큐 파티션 분할, 작은 쪽이 판명되면 브로드캐스트 조인으로 전환, 빈 릴레이션 단락 처리. 기본으로 켜져 있다 |
| **PQE (Predictive Query Execution)** | Databricks 확장. 스필 압력, 중간 결과 크기 같은 실행 엔진의 실시간 신호로 실행 중에 재계획 |

---

## 3. 분산 쿼리가 느린 이유와 두 가지 레버

### 3-1. 두 가지 물리적 원인과 세 가지 악화 요인

| 원인 | 비용 | 대응 |
|---|---|---|
| 데이터를 너무 많이 읽음 | I/O | 필터가 파일을 건너뛸 수 있는 레이아웃, 소형 파일 방지 |
| 데이터를 너무 많이 이동 | 네트워크 (보통 가장 큼) | 이동하는 행 수 줄이기, 적절한 조인 전략 |

| 악화 요인 | 현상 |
|---|---|
| **스큐 (Skew)** | 일부 키에 행이 몰려 소수 태스크가 대부분의 일을 하고 나머지는 대기 |
| **스필 (Spill)** | 태스크 메모리가 부족해 중간 데이터를 디스크에 기록 |
| **조인 폭발 (Exploding join)** | 집계 전에 조인으로 행이 크게 늘어나 이후 단계가 입력보다 훨씬 많은 데이터를 처리 |

### 3-2. 사용자가 제어하는 두 레버

| 레버 | 목표 | 수단 |
|---|---|---|
| **Read Less** | 스캔에서 읽는 바이트와 여는 파일 수 감소 | 데이터 레이아웃과 클러스터링, 컬럼/파일 통계를 이용한 데이터 스키핑, 소형 파일 방지 |
| **Shuffle Less** | 네트워크로 이동하는 행 수 감소 | 더 나은 조인과 집계, 불필요한 셔플 제거, 런타임 스큐/스필 처리 |

느린 워크로드를 만나면 먼저 둘 중 어느 쪽 문제인지 파악한다.

### 3-3. 플랫폼이 자동화하는 것

| 기능 | 자동화하는 대상 | 연결되는 레버 |
|---|---|---|
| **Predictive Optimization** | UC 관리형 테이블의 `OPTIMIZE`, `VACUUM`, `ANALYZE` | Read Less |
| **Liquid 클러스터링** | 필터 기준 데이터 배치 (`CLUSTER BY AUTO`는 키 선택까지) | Read Less |
| **AQE + PQE** | 런타임 셔플 파티션 크기, 스큐, 조인 전략 | Shuffle Less |
| **Photon** | 스캔, 조인, 집계, 쓰기 가속 | 둘 다 |
| **서버리스** | 노드 유형, 클러스터 크기, 스케일링 | 둘 다 |

> 자동화는 무조건적이지 않다. 대부분이 **Unity Catalog 관리형 테이블**을 전제로 하고, 일부는 Predictive Optimization에 의존한다. 기능이 실제로 작동했는지 항상 확인한 뒤에 성과로 인정한다.

---

## 4. 플랫폼 우선 워크플로

순서는 **플랫폼 우선 → 확인 → 튜닝**이다.

1. 관리형 테이블과 서버리스에서 시작해 기본값이 작동하게 둔다.
2. Predictive Optimization, AQE, Photon이 일하게 둔다.
3. Query Profile에서 프루닝, 캐시 적중, Photon 적용, 스큐 처리가 실제로 일어났는지 확인한다.
4. 차이가 남은 곳만 수동으로 튜닝한다.

플랫폼이 일할 기회를 갖기 전에 튜닝하면 기본값과 충돌하거나 이미 처리된 문제를 다시 고치게 된다.

### 4-1. 더 이상 맞지 않는 오래된 조언

| 오래된 조언 | 지금 맞지 않는 이유 |
|---|---|
| `spark.sql.shuffle.partitions=200` 같은 고정값 설정 | AQE가 실제 데이터로 런타임에 셔플 파티션 크기를 정한다. 하드코딩은 대개 해가 된다 |
| 실행자 크기, GC 플래그 튜닝 | 서버리스와 Photon이 리소스와 메모리 모델을 관리한다 |
| `.cache()` / `.persist()`를 곳곳에 추가 | 디스크 캐시와 결과 캐시가 자동으로 처리한다. 서버리스에서는 `.cache()`가 아무 동작도 하지 않고 `CACHE TABLE`은 오류가 난다 |
| 예약된 `OPTIMIZE` / `VACUUM` cron | 관리형 테이블은 Predictive Optimization이 실행한다. cron은 이중 비용이다 |
| 습관적인 날짜 파티셔닝 | 소형 파일만 만든다. 레이아웃 키가 필요하면 Liquid 클러스터링을 쓴다 |

### 4-2. 최적화 루프

| 단계 | 내용 |
|---|---|
| 1. 측정 | Query Profile, `system.query.history`로 현재 상태와 대상 쿼리 파악 |
| 2. 병목 명명 | Performance Insights 레이블(DATA_SKEW, DATA_SPILL, EXPLODING_JOIN 등)로 병목에 이름 붙이기 |
| 3. 수정 하나 적용 | 반복당 변경은 하나. 두 개를 동시에 바꾸면 무엇이 효과였는지 알 수 없다 |
| 4. 벤치마크 | 결과 캐시 없이 중앙값 기간과 실행당 DBU 비교 |

---

## 5. 먼저 측정하기: Query Profile

엔지니어의 직관은 병목을 자주 잘못 짚는다. 잘못된 추측은 시간을 낭비하고 성능을 더 나쁘게 만들 수 있다. **변경하기 전에 프로파일을 연다.** 프로파일은 액션이 실행된 뒤에만 존재한다.

### 5-1. 두 개의 시계

| 시계 | 의미 |
|---|---|
| **Wall-clock 기간** | 사용자가 체감한 시간. 스케줄링 → 최적화 + 파일 프루닝 → 실행의 세 단계로 구성 |
| **집계된 태스크 시간** | 모든 노드, 모든 태스크의 코어-초 합계. 클러스터가 실제로 한 일의 양 |

- 집계된 태스크 시간은 항상 wall-clock 이상이다.
- **wall-clock보다 훨씬 크면** 병렬 처리가 잘 된 것이다(예: 70~150초의 일을 6초에 끝냄).
- **wall-clock에 가까우면** 일이 소수 태스크나 드라이버에 몰려 코어 대부분이 놀았다는 뜻이다.

Wall-clock 세 단계의 해석:

| 단계 | 길면 의심할 것 |
|---|---|
| 스케줄링 | 쿼리가 아니라 웨어하우스 크기나 동시성 문제 |
| 쿼리 최적화와 파일 프루닝 | 메타데이터나 통계 문제 |
| 실행 | 실제 작업. DAG가 보여 주는 부분 |

### 5-2. DAG 읽는 법

- **아래에서 위로** 읽는다. 스캔이 리프, 결과가 루트다.
- 색상 기준 세 가지:

| 색상 기준 | 드러나는 것 |
|---|---|
| **Time spent** | 핫 패스. 여기서 시작한다 |
| **Memory peak** | 셔플 메모리와 스필 위험 |
| **Rows** | 팬아웃(조인에서 행 증가)과 축소(집계에서 행 감소) |

- 비용 상위 연산자 목록을 보면 DAG를 다 훑지 않고 병목으로 바로 갈 수 있다.
- 스캔 연산자에서 가장 결정적인 지표는 **읽은 파일 수 대 프루닝된 파일 수**다.

### 5-3. 캐시 히트에는 프로파일이 없다

- 결과 캐시에서 반환된 쿼리는 실행되지 않았으므로 "Query profile is not available"이 표시된다.
- SQL 웨어하우스에서는 `SET use_cached_result = false`, 그 외에는 쿼리 텍스트를 사소하게 바꿔 다시 실행시킨다.
- `system.query.history`의 `from_result_cache = true`로 캐시 히트를 확인한다.
- 디스크 캐시(`read_io_cache_percent`)는 프로파일 생성을 막지 않는다.

### 5-4. 프로파일 읽기 예: 의도적으로 느린 쿼리

```sql
SELECT c.customer_id, c.tier, p.category,
       SUM(oi.quantity * p.list_price) AS revenue, COUNT(*) AS line_items
FROM orders o
JOIN order_items oi ON o.order_idx = oi.order_idx
JOIN products p     ON oi.product_idx = p.product_idx
JOIN customers c    ON o.customer_idx = c.customer_idx
WHERE date_format(o.order_date, 'yyyy-MM') BETWEEN '2024-01' AND '2024-12'
GROUP BY c.customer_id, c.tier, p.category
ORDER BY revenue DESC;
```

| 증상 | 프로파일에서 보이는 것 | 원인 |
|---|---|---|
| **프루닝 없음** | orders 스캔: 파일 96개 읽음, 0개 프루닝 | 컬럼을 `date_format()`으로 감싸 파일별 min/max 통계를 쓸 수 없음 |
| **과도한 셔플** | 메모리 피크가 높은 Shuffle 연산자 다수, 큰 `shuffle_read_bytes` | 차원 테이블(500만 행, 80~126MB)이 브로드캐스트 임계값(10MB)을 넘어 모든 조인이 양쪽을 셔플. 조인 3개 = Exchange 6개 + GROUP BY 1개 |
| **스큐** | 나머지가 끝난 뒤에도 오래 도는 태스크 하나 | 1%의 대형 고객이 주문의 80%를 보유 |

스큐 수동 확인: 가장 뜨거운 셔플 노드의 태스크 메트릭에서 **최대 소요 시간 / 중앙값 소요 시간이 5배 이상**이면 스큐로 본다.

> 서버리스에서는 디스크 캐시가 거의 즉시 데워져 셔플 연산자가 시간을 지배하는 경우가 많다. 고정 메모리의 클래식 compute에서는 콜드 스캔이 지배하거나 실제 스필(`spilled_local_bytes > 0`)이 발생할 수 있다.

---

## 6. Performance Insights와 query.history로 문제 쿼리 찾기

### 6-1. Performance Insights

쿼리 실행 후 Databricks가 가장 비용이 큰 패턴을 자동으로 감지하고 순위를 매겨 설명한다. 실행 가능한 인사이트가 있으면 AI 어시스턴트에게 재작성 제안을 받을 수 있다. 이 기능이 없어도 Query Profile 연산자와 `system.query.history`에서 같은 신호를 얻을 수 있다.

| 인사이트 | 의미 |
|---|---|
| **DATA_SKEW** | 데이터가 compute 리소스에 고르지 않게 분산됨 |
| **DATA_SPILL** | 메모리에 맞지 않아 디스크로 스필됨 |
| 통계 미사용 | 필터가 데이터 유형/컬럼을 변환해 통계를 쓸 수 없음 |
| **EXPLODING_JOIN** | 조인 출력이 입력보다 훨씬 큼 |
| **SELECTIVE_JOIN** | 조인이 읽은 행보다 훨씬 적은 행을 생성 |
| **COVERAGE_PHOTON** | 쿼리 일부가 Photon을 벗어나 JVM으로 폴백됨 |

> AI 어시스턴트의 재작성 제안은 원리를 알아야 옳고 그름을 판단할 수 있다. 제안을 받으면 결과가 같은지, 프로파일 지표가 실제로 개선됐는지 직접 확인한다.

### 6-2. sargeable 조건자

엔진은 **함수로 감싸지 않은 컬럼과 리터럴의 비교**만 파일별 min/max 통계와 대조할 수 있다.

```sql
-- 나쁨: 모든 행에서 함수를 평가, 파일 프루닝 불가
WHERE date_format(o.order_date, 'yyyy-MM') BETWEEN '2024-01' AND '2024-12'

-- 좋음 (sargeable): max(order_date) < 2024-01-01 이거나 min >= 2025-01-01인 파일을 건너뜀
WHERE o.order_date >= DATE'2024-01-01' AND o.order_date < DATE'2025-01-01'
```

조건자를 고쳐도 파일마다 전체 날짜 범위가 섞여 있으면 여전히 0개가 프루닝된다. **조건자 수정 + 레이아웃 수정(해당 컬럼으로 클러스터링)** 둘 다 필요하다.

```sql
ALTER TABLE orders CLUSTER BY (order_date);
OPTIMIZE orders FULL;
```

### 6-3. system.query.history로 전체 환경 트리아지

프로파일은 쿼리 하나, `system.query.history`는 전체 환경(리전 내 메타스토어의 모든 워크스페이스)이다. 어떤 쿼리에 시간을 쓸지 정하는 데 쓴다.

| 컬럼 | 의미 |
|---|---|
| `total_duration_ms` | 컴파일 + 실행 (결과 가져오기 제외). "가장 느린 쿼리" 순위 기준 |
| `execution_duration_ms` | 실행만. total과의 차이가 컴파일 시간 |
| `total_task_duration_ms` | 모든 태스크의 집계 compute 시간. **서버리스에서 DBU의 대리 지표** |
| `read_bytes`, `read_files`, `pruned_files` | 스캔 양과 프루닝 비율 |
| `shuffle_read_bytes` | 셔플로 네트워크 전송된 바이트. 0이면 브로드캐스트 조인을 쓴 것 |
| `spilled_local_bytes` | 디스크로 스필된 데이터 |
| `from_result_cache` | true면 결과 캐시 응답 (실제 작업 없음) |
| `read_io_cache_percent` | `read_bytes` 중 로컬 디스크 캐시에서 읽은 비율 |

```sql
SELECT start_time, statement_id, total_duration_ms, execution_duration_ms,
       total_task_duration_ms, read_bytes, read_files, pruned_files,
       shuffle_read_bytes, spilled_local_bytes, read_io_cache_percent
FROM system.query.history
WHERE statement_type = 'SELECT'
  AND execution_status = 'FINISHED'
  AND from_result_cache = false
ORDER BY execution_duration_ms DESC
LIMIT 10;
```

- 증상별로 정렬 기준을 바꾼다: 느림 → `total_duration_ms`, 스필 → `spilled_local_bytes`, 셔플 → `shuffle_read_bytes`, 과다 스캔 → `read_bytes` 또는 `read_files` 대비 `pruned_files`.
- 느린 작업을 찾을 때는 항상 `from_result_cache = false`로 거른다. 캐시된 결과는 잘못된 이유로 빠르다.
- 수집 지연이 5~10분 있다.
- `statement_text`는 기본적으로 `<REDACTED>`다. 어카운트 관리자와 `databricks_pii_access` 그룹 구성원만 원문을 본다. 그래서 `LIKE`로 쿼리를 식별할 수 없고, **항상 보이는 `statement_id`나 시간 범위**로 실행을 구분한다.
- 접근 권한은 기본적으로 관리자에게 제한되며, 관리자가 사용자별로 필터링된 동적 뷰를 만들어 공유할 수 있다.

---

## 7. 정직한 벤치마크

서버리스는 실행마다 compute 할당이 달라 같은 쿼리도 시간이 매번 다르다. 측정 한 번으로는 아무것도 증명하지 못한다.

| 단계 | 방법 | 이유 |
|---|---|---|
| 1. 결과 캐시 우회 | SQL 웨어하우스: `SET use_cached_result = false`. 서버리스 노트북(Spark Connect)에서는 이 설정을 쓸 수 없으므로 실행마다 고유 주석(`-- run_1_1690000000`)을 붙여 캐시 키를 바꾼다 | 캐시는 정확한 구문 텍스트를 키로 쓴다. 캐시 응답은 쿼리를 측정하지 않는다 |
| 2. 워밍업 후 중앙값 | 첫 실행은 버리고 여러 번 실행해 **중앙값** 보고 | 첫 실행은 콜드 디스크 캐시와 컴파일 비용을 포함. 중앙값은 간헐적 이상값에 강하다 |
| 3. 실행당 DBU 추적 | 즉시: `total_task_duration_ms`. 정산: `system.billing.usage` (1~2시간 지연, 10~60분 단위) | 빠르지만 비싼 쿼리는 이긴 것이 아니다 |
| 4. 공정한 A/B | 변수 하나만 변경, 같은 웨어하우스, 경쟁 작업 없음, 둘 다 `from_result_cache = false` 확인 | 다른 요인이 섞이면 원인을 알 수 없다 |

- 결과 캐시만 끄고 **디스크 캐시는 그대로 둔다.** 운영 환경의 동작을 반영하기 위해서다.
- 비교 전에 두 테이블 모두 `ANALYZE TABLE ... COMPUTE STATISTICS FOR ALL COLUMNS`를 실행해 통계를 맞춘다. 통계가 다르면 한쪽은 셔플 해시, 다른 쪽은 브로드캐스트를 선택해 비교가 불공정해진다. 단계 간 `shuffle_read_bytes`가 크게 다르면(예: 1.2GB vs 0) 조인 전략이 달라진 것이다.

### 7-1. Wall-clock이 아니라 태스크 시간을 믿는다

| 지표 | 성격 |
|---|---|
| `execution_duration_ms` (wall-clock) | 디스크 캐시 워밍으로 부풀려진다. A가 먼저 콜드로 돌고 B가 데워진 캐시를 재사용하면 B가 실제보다 훨씬 빨라 보인다 |
| `total_task_duration_ms` | 모든 태스크의 실제 compute 합계. 캐시 영향이 적은 정직한 비용 지표 |
| `read_bytes` 감소, `pruned_files > 0` | 개선이 **구조적**(읽은 데이터 감소)임을 확인 |

- A와 B의 태스크 시간이 같은데 wall-clock만 다르면 그 "개선"은 캐시 아티팩트다.
- 태스크 시간이 같고 wall-clock이 짧으면 병렬도가 높아진 것이지 작업량이나 비용이 준 것이 아니다.
- wall-clock 비율은 거의 항상 태스크 시간 비율보다 크다. 비용 비교에는 **태스크 시간 비율**을 보고한다.

예: 조건자만 고친 1단계에서는 wall-clock이 빨라 보여도 바이트, 파일, 태스크 시간이 거의 같다(캐시 아티팩트). 클러스터링까지 적용한 2단계에서야 `pruned_files > 0`, `read_bytes` 감소, 태스크 시간 감소가 함께 나타난다.

---

## 8. 소형 파일 문제와 관리형 테이블의 자동 튜닝

### 8-1. 소형 파일이 생기는 원인

- 최적화된 쓰기 없이 태스크마다 파일을 하나씩 쓰는 고병렬 쓰기
- 잦은 소량 추가, 스트리밍 마이크로배치
- 과도한 재파티셔닝, 카디널리티가 높은 컬럼으로 파티셔닝
- 압축되지 않은 비관리형 테이블에 누적된 소량 `MERGE`, `UPDATE`, `DELETE`

### 8-2. 왜 느려지는가

- **읽기**: 엔진은 파일 바이트를 태스크당 최대 약 128MB(`spark.sql.files.maxPartitionBytes`)까지 묶고, 파일마다 약 4MB의 열기 비용(`spark.sql.files.openCostInBytes`)을 매긴다. 열기 비용보다 작은 파일이 수천 개면 패킹이 무력화되어 수천 개의 작은 태스크가 생기고, 시간이 데이터 처리가 아니라 스케줄링과 파일 열기에 쓰인다. (서버리스에서는 이 설정을 플랫폼이 관리하며 조정할 수 없다.)
- **셔플과 DML**: 파일이 많을수록 추적할 메타데이터, 목록 조회, 파일당 조정 비용이 늘어난다.
- **반대 문제**: 지나치게 큰 파일은 통계 집합이 하나뿐이라 필터와 상관없이 항상 열어야 하고, 스캔 병렬도가 낮아진다. `.repartition(1).write`로 만든 5GB 파일 하나는 250MB 파일 20개보다 읽기와 압축 모두에서 불리하다.

### 8-3. 확인: DESCRIBE DETAIL

```sql
DESCRIBE DETAIL main.sales.orders;    -- numFiles, sizeInBytes, partitionColumns, clusteringColumns
DESCRIBE HISTORY main.sales.orders;   -- 커밋별 operationMetrics (OPTIMIZE는 numAddedFiles / numRemovedFiles)
```

`sizeInBytes / numFiles`로 평균 파일 크기를 계산한다. 예를 들어 1,000만 행이 파일 300개(각 약 0.55MB)라면 명백한 소형 파일 문제다.

### 8-4. 관리형 테이블이 자동으로 하는 네 가지

| 기능 | 하는 일 |
|---|---|
| **최적화된 쓰기** | 쓰기 직전에 데이터를 셔플해 출력 파일이 목표 크기에 가깝게 생성되도록 함 |
| **자동 압축** | 커밋 후 남은 작은 파일을 결합. 백그라운드 자동 압축이 테이블 수명 내내 동작 |
| **파일 크기 자동 조정** | 테이블 크기에 따라 목표 크기 결정: 2.56TB 미만 256MB, 2.56~10TB 구간 선형 증가, 10TB 초과 1GB |
| **수집 시간 클러스터링** | 파티션이 없는 추가 전용 테이블도 보통 순서대로 들어오는 컬럼(주로 타임스탬프)에서 효과적으로 프루닝 |

같은 행을 UC 관리형 테이블에 쓰기만 해도, 설정이나 `OPTIMIZE` 없이 훨씬 적고 큰 파일이 생긴다. 직접 조정하는 대신 `DESCRIBE DETAIL`로 결과를 확인한다.

> 주의
> - 관리형 테이블에 `delta.targetFileSize`를 습관적으로 설정하지 않는다. 자동 조정보다 의도적으로 파일을 더 많이 만들어야 하는 경우(예: 작은 테이블에서 프루닝 효과를 확인하려고 `16mb`로 낮추는 경우)에만 쓴다.
> - `delta.tuneFileSizesForRewrites`와 "MERGE가 많은 테이블은 16~64MB 파일" 규칙은 현재 문서에서 빠진 레거시 지침이다.
> - 최적화된 쓰기가 켜져 있으면 쓰기 직전에 `coalesce(n)`이나 `repartition(n)`을 실행하지 않는다.

---

## 9. Predictive Optimization

UC 관리형 테이블의 **유지 관리 계층**이다. 데이터를 쓰면 유지 관리가 효과적인 시점을 판단해 서버리스에서 실행한다.

| 항목 | 내용 |
|---|---|
| 실행하는 작업 | `OPTIMIZE`(압축 + 증분 클러스터링), `VACUUM`, `ANALYZE`. **ZORDER는 실행하지 않는다** (Z-ordered 파일은 그대로 둔다) |
| 실행 방식 | 비동기. 쓰기 직후가 아니라 테이블에 도움이 될 때 작업을 큐에 넣는다 |
| 과금 | 서버리스 jobs SKU. 일정 설정이나 전용 유지 관리 클러스터 불필요 |
| 기본 활성화 | 2024년 11월 이후 생성된 계정은 기본 활성화. 기존 계정은 단계적으로 확대 중이므로 실제 상태를 확인한다 |
| 적용 제외 | 외부 테이블, Delta Sharing 수신 테이블. 이들은 여전히 `OPTIMIZE`, `VACUUM`, `ANALYZE`를 직접 실행해야 한다 |

```sql
-- 실행 이력 (최대 약 2시간 지연)
SELECT start_time, catalog_name, schema_name, table_name,
       operation_type,     -- COMPACTION / CLUSTERING / VACUUM / ANALYZE
       operation_status,
       usage_unit,         -- ESTIMATED_DBU
       usage_quantity
FROM system.storage.predictive_optimization_operations_history
WHERE catalog_name = 'main' AND schema_name = 'sales'
ORDER BY start_time DESC
LIMIT 20;

-- 활성화 여부: 'Predictive Optimization' 항목 확인
DESCRIBE TABLE EXTENDED main.sales.orders;
```

`operation_type`에는 `OPTIMIZE`라는 단어가 나오지 않는다. OPTIMIZE는 `COMPACTION`과 `CLUSTERING`으로 기록된다.

> 관리형 테이블에 걸어 둔 야간 `OPTIMIZE` / `VACUUM` cron은 삭제한다. Predictive Optimization이 이미 실행하므로 순수한 이중 비용이다.

---

## 10. 파티셔닝은 예외적인 선택

| 테이블 크기 | 권장 |
|---|---|
| 1TB 미만 | 파티셔닝하지 않는다. 관리형 테이블이 스스로 튜닝하게 두고, 레이아웃 키가 필요할 때만 Liquid 클러스터링 |
| 1TB ~ 100TB | Liquid 클러스터링. 클러스터 키가 파티셔닝을 대체 |
| 100TB 이상 | 파티셔닝이 도움될 수 있다. 단, Liquid 클러스터링을 먼저 검증하고 측정한 뒤에 고정 파티션 경계를 확정 |

- 파티셔닝을 꼭 해야 한다면 **파티션 값당 최소 1GB**를 유지한다. 그보다 작으면 소형 파일 문제로 돌아간다.
- 카디널리티가 높은 컬럼으로 파티셔닝하면 값마다 작은 폴더가 생겨 소형 파일 문제의 직접 원인이 된다.
- 파티션 단위 덮어쓰기/삭제를 위한 격리라는 전통적 이유도 이제는 다른 Delta 기능(선택적 덮어쓰기 등)으로 대부분 해결된다.

---

## 11. Liquid 클러스터링

### 11-1. 세 가지 레이아웃 비교

| | 파티셔닝 | ZORDER | Liquid 클러스터링 |
|---|---|---|---|
| 방식 | 값당 물리 폴더 | `OPTIMIZE` 안에서 여러 컬럼 기준 정렬 | 목표 파일 크기를 유지하며 키 기준 배치 |
| 키 | 저카디널리티 컬럼 1~몇 개, 생성 시 고정 | 여러 개(많을수록 효과 감소) | 최대 4개, 고카디널리티 가능 |
| 증분 | 해당 없음 | 아니오 (실행마다 재작성) | 예 (새 데이터만 클러스터링) |
| 키 변경 | 테이블 재작성 필요 | 다시 실행 | 재작성 없이 `ALTER TABLE ... CLUSTER BY` |
| 프루닝 | 파티션 컬럼 필터에만 | 정렬 컬럼 필터 | 클러스터 키 필터 |
| 현재 위치 | 초대형 테이블의 예외 | 아직 마이그레이션 안 된 테이블만 | 새 기본값 |

Liquid 클러스터링은 파티셔닝, ZORDER와 **함께 쓸 수 없다**. 테이블은 하나만 선택한다.

### 11-2. 파티셔닝의 전형적 함정

보고서가 주로 일 단위라서 `event_date`로 파티셔닝했지만, 비용이 큰 쿼리는 `customer_idx`로 필터링하는 경우다.

- 파티션 프루닝은 파티션 컬럼을 필터링할 때만 동작한다.
- 한 고객이 매일 활동하면 모든 날짜 폴더에 그 고객의 행이 있다.
- 각 파일의 `customer_idx` min/max 범위가 모두 필터 값을 포함하므로 데이터 스키핑도 불가능하다.
- 결과: 약 12행을 찾으려고 730개 파일(약 190MB) 전체를 읽는다.

### 11-3. 클러스터링 적용

```sql
-- 새 테이블 (기존 테이블과 나란히 비교하려면 CTAS)
CREATE TABLE events_clustered
CLUSTER BY (customer_idx, event_date)
AS SELECT * FROM events_partitioned;

-- 기존 데이터 전체를 처음 클러스터링
OPTIMIZE events_clustered FULL;

-- 확인: clusteringColumns에 키, partitionColumns는 비어 있어야 함
DESCRIBE DETAIL events_clustered;

-- 파티션 테이블을 제자리 변환하는 방법도 있다
ALTER TABLE events_partitioned REPLACE PARTITIONED BY WITH CLUSTER BY (customer_idx, event_date);
```

같은 필터의 결과 예:

| 지표 | 파티셔닝 | 클러스터링 |
|---|---|---|
| `read_files` | 730 | 2 |
| `read_bytes` | 약 191MB | 약 20MB |
| `total_task_duration_ms` | 기준 | 약 12배 감소 |
| `execution_duration_ms` | 기준 | 약 1.7배 감소 (작은 규모에서는 컴파일, 스케줄링 같은 고정 오버헤드가 지배) |

### 11-4. 파일 안에서 일어나는 스키핑

두 경우 모두 `pruned_files`는 0일 수 있다. 클러스터링 테이블은 파일이 2개뿐이라 파일 단위로 뺄 것이 없기 때문이다. 그런데도 `read_bytes`가 크게 주는 이유는 **파일 내부 행 그룹 단위 스키핑**이다.

| 파일 | 행 그룹별 `customer_idx` 범위 | 필터 = 42 |
|---|---|---|
| 정렬 안 됨 (파티셔닝) | 1~90, 5~96, 2~88, 3~99 | 모든 그룹이 42를 포함 → 전부 읽음 |
| 정렬됨 (클러스터링) | 1~19, 40~44, 45~78, 79~99 | 40~44 그룹만 읽고 나머지 3개는 건너뜀 |

파일 수만으로는 효과를 다 설명할 수 없다. 클러스터링은 행 그룹의 min/max 범위를 좁혀 열린 파일 안에서도 대부분을 건너뛰게 한다.

### 11-5. 파일 경계와 증분 동작

- 실질적인 제약은 **목표 파일 크기**뿐이다. 고정된 날짜나 고객 경계를 따르지 않는다.
  - 소규모 고객 한 명의 여러 날짜가 파일 하나에 들어갈 수 있다.
  - 한 날짜의 여러 일반 고객이 파일 하나를 공유할 수 있다.
  - 한 고객의 한 날짜가 파일 하나에 비해 너무 크면 시간 단위 같은 더 세밀한 값으로 나뉜다.
- 증분 진행 순서:
  1. **먼저 수집**: 새 파일 하나에 여러 고객이 섞여 저장된다(빠른 쓰기 경로).
  2. **과부하 영역 분할**: 데이터가 쌓이면 가장 큰 고객부터 자체 파일로 분리된다. 스킵 효과가 가장 큰 곳이 먼저다.
  3. **정리**: 유지 관리가 남은 작은 파일을 병합한다. UC 관리형 테이블에서는 Predictive Optimization이 실행한다.

### 11-6. 키 선택과 변경

- 쿼리 필터에 가장 많이 쓰는 컬럼을 고른다. 조인 키와 MERGE 키도 스캔 시 필터 역할을 하므로 좋은 후보다.
- 최대 4개지만 **적을수록 좋다.** 작은 테이블에서는 키 4개가 키 2개보다 단일 컬럼 필터 성능을 떨어뜨릴 수 있다. 상관관계가 높은 두 컬럼은 하나만 남긴다.
- 클러스터링 키는 통계가 수집되는 컬럼이어야 한다(기본은 처음 32개 컬럼).

```sql
ALTER TABLE events_clustered CLUSTER BY (account_key);   -- 키 변경: 기존 데이터는 재작성되지 않음
OPTIMIZE events_clustered FULL;                          -- 기존 데이터까지 새 키로 재클러스터링
ALTER TABLE events_clustered CLUSTER BY NONE;            -- 클러스터링 해제
```

| 명령 | 대상 |
|---|---|
| `OPTIMIZE` | 증분. 새로 추가된, 아직 클러스터링되지 않은 데이터만 |
| `OPTIMIZE ... FULL` | 모든 데이터 재클러스터링. 기존 데이터가 있는 테이블에 클러스터링을 처음 켜거나 키를 바꾼 뒤에 필요 |

### 11-7. CLUSTER BY AUTO

```sql
CREATE TABLE events_auto CLUSTER BY AUTO AS SELECT * FROM events_base;
-- 또는 ALTER TABLE events_auto CLUSTER BY AUTO;

DESCRIBE DETAIL events_auto;                              -- clusteringColumns (처음에는 비어 있을 수 있음)
SHOW TBLPROPERTIES events_auto ('clusterByAuto');         -- true면 AUTO 활성
```

- Predictive Optimization이 테이블의 과거 쿼리 워크로드를 분석해 키를 고르고, 쿼리 패턴이 바뀌면 키를 진화시킨다.
- **비용을 고려한다.** 예상 스키핑 절감이 재클러스터링 비용보다 클 때만 키를 바꾼다.
- 전제 조건: **UC 관리형, 파티션 없는 Delta 테이블 + Predictive Optimization 활성화**. PO가 꺼져 있으면 AUTO는 아무것도 하지 않는다. 파티션 테이블에는 AUTO를 붙일 수 없으므로 클러스터링 테이블로 재생성한 뒤 켠다.
- 비동기라서 켠 직후 레이아웃이 바뀌지 않는다.

### 11-8. 파티션 덮어쓰기 대체

클러스터링 테이블에는 덮어쓸 파티션이 없다.

```sql
-- 임의 조건자와 일치하는 행을 원자적으로 교체
INSERT INTO events_clustered REPLACE WHERE event_date = '2024-02-08'
SELECT * FROM staging_events WHERE event_date = '2024-02-08';
```

컬럼 동등 비교로 동적으로 교체하는 `REPLACE USING (cols)` 변형도 있다.

---

## 12. 캐시 계층

### 12-1. DataFrame은 데이터가 아니라 계획이다

DataFrame은 연산을 설명하는 논리 계획이다. 액션이 실행될 때까지 데이터는 스토리지에 그대로 있고, 캐시가 가로채지 않는 한 **액션마다 소스부터 다시 계산**한다. 같은 셀을 두 번 돌려도 두 번 다 느릴 수 있는 이유다.

- 캐싱이 도움이 되는 경우: 같은 데이터를 여러 번 스캔하는 반복적 ML 학습, 같은 DataFrame을 여러 각도로 탐색하는 대화형 분석.
- 도움이 안 되는 경우: 읽기 → 변환 → 쓰기의 단일 직선형 파이프라인. 데이터가 한 번만 흐르므로 캐싱은 직렬화와 저장 오버헤드만 더한다.

### 12-2. 세 가지 캐시

| 캐시 | 캐시 대상 | 특징 |
|---|---|---|
| **디스크 캐시** (이전 명칭 Delta/DBIO 캐시) | 데이터 파일 | 첫 읽기 때 원격 Parquet 파일을 로컬 SSD에 복사. SSD 노드, 서버리스 SQL 웨어하우스, 서버리스 compute에서 기본 활성화. 파일이 바뀌면 자동 무효화. `read_io_cache_percent`로 측정. 쿼리 텍스트와 무관하게 같은 파일 재스캔에 적용 |
| **결과 캐시** (DBSQL) | 쿼리 결과 전체 | 로컬: 웨어하우스 메모리, 재시작 시 삭제. 원격: 서버리스 전용, 워크스페이스 공유, 재시작 후에도 유지, 약 24시간. **정확한 구문 텍스트**가 키. `from_result_cache`로 확인 |
| **Spark 캐시** | 메모리 블록 (`.cache()`, `.persist()`) | 수동. 지연 동작, 실행자 메모리 경쟁, 데이터 변경 시 자동 갱신 안 됨, 오토스케일링으로 워커가 빠지면 손실 |

서버리스에서 Spark 캐시의 동작:

| API | 결과 |
|---|---|
| DataFrame API (`df.cache()`, `df.persist()`) | 호출은 받지만 아무 동작 없음. `is_cached`는 `False` |
| SQL API (`CACHE TABLE`, `spark.catalog.cacheTable()`) | `AnalysisException [NOT_SUPPORTED_WITH_SERVERLESS]` |

> Spark Connect 지원과 서버리스 지원은 같지 않다. DataFrame 캐시 API는 Databricks Connect로 클래식 클러스터에 붙으면 동작하지만 서버리스에서는 아무 동작도 하지 않는다.

### 12-3. 어떤 캐시가 응답했는지 확인

빠른 두 번째 실행을 쿼리 개선으로 인정하기 전에, 어느 계층이 응답했는지 확인한다.

| 컬럼 | 가리키는 계층 |
|---|---|
| `from_result_cache = true` | 결과 캐시. 쿼리가 재실행되지 않음. 프로파일도 없음 |
| `read_io_cache_percent` | 디스크 캐시. 전혀 다른 계층이므로 혼동하지 않는다 |

### 12-4. 버려야 할 캐시 습관

| 안티패턴 | 대체 |
|---|---|
| `CACHE SELECT`로 사전 워밍 | SQL 웨어하우스와 DBR 14.2 이상에서는 무시된다. 향상된 디스크 캐싱이 무엇을 유지할지 결정한다 |
| 수동 `.cache()` / `.persist()` | 디스크 캐시가 재스캔을 처리한다 |
| 데워진 캐시 기준 벤치마크 | 결과 재사용을 끄고 측정한다 |
| Spark 캐시를 위해 큰 메모리 클러스터 구매 | 디스크 캐시는 메모리가 아니라 로컬 SSD를 쓴다 |

두 자동 캐시가 수동 캐싱의 두 시나리오를 대체한다. **같은 쿼리 재실행은 결과 캐시, 같은 파일 재스캔은 디스크 캐시**다.

---

## 13. 셔플과 조인 전략

### 13-1. 셔플의 두 단계

| 단계 | 하는 일 |
|---|---|
| 쓰기 (맵 측) | 각 태스크가 `hash(key) mod N`으로 행을 나눠 다운스트림 파티션별 블록을 로컬 셔플 파일에 쓴다 (서버리스 Photon에서 소규모는 메모리 내 처리) |
| 읽기 (리듀스 측) | 각 다운스트림 태스크가 모든 업스트림 태스크에서 자기 블록을 네트워크로 가져온다. 비용이 큰 부분. `shuffle_read_bytes` |

- 프로파일에서는 스테이지 사이의 **Exchange** 연산자로 보인다. Memory peak 색상으로 가장 많이 버퍼링하는 Exchange를 찾는다.
- 셔플 파티션 수: 서버리스에서는 AQE가 실제 데이터로 결정하고 사용자가 조정할 설정이 없다. 클래식은 `spark.sql.shuffle.partitions`(기본 200)에서 시작해 AQE가 병합한다.
- **파티션 수를 바꾸거나 compute를 늘려도 이동하는 총 바이트는 그대로다.** 효과가 있는 방법은 네트워크로 보내는 양을 줄이는 것뿐이다: 작은 쪽 브로드캐스트, 조인 전 사전 집계, 필터 푸시다운.

### 13-2. 네 가지 조인 전략

| 전략 | 셔플 | 선택 조건 |
|---|---|---|
| **브로드캐스트 해시 조인** | 없음 | 작은 쪽을 모든 태스크에 복사해 로컬 조인. 작은 쪽 추정 크기가 `spark.sql.autoBroadcastJoinThreshold`(기본 10MB) 이하 |
| **Sort-merge 조인** | 양쪽 | 양쪽을 키로 재분배, 정렬, 병합. 대용량 대 대용량의 기본값. 모든 규모에서 동작 |
| **셔플 해시 조인** | 양쪽 | sort-merge와 같은 셔플 후 정렬 대신 해시 테이블 + 프로브. Photon 비용 모델이 유리하다고 판단할 때 계획 시점에 전환(힌트로 강제 불가). 프로파일에 "Photon Shuffled Hash Join"으로 표시, Sort 노드 없음 |
| **브로드캐스트 중첩 루프** | 없음, 대신 O(N x M) 비교 | 동등 조건이 없는 조인(범위, 부등식, 크로스)에서 유일한 선택지 |

### 13-3. 브로드캐스트 결정 세 구간

| 구간 | 결정 주체 | 내용 |
|---|---|---|
| 10MB 미만 (추정) | 플래너 | 처음부터 브로드캐스트. 셔플이 아예 계획되지 않는다. 정확한 통계가 필요하다 |
| 30MB 이하 (실측) | AQE | 셔플 단계가 끝난 뒤 실제 출력 크기를 `spark.databricks.adaptive.autoBroadcastJoinThreshold`(기본 30MB)와 비교해 sort-merge를 브로드캐스트로 전환. **셔플 네트워크 비용은 이미 지불됨**, sort-merge compute만 절감 |
| 그 이상 | 개발자 | 조인 힌트로 강제 |

```sql
SELECT /*+ BROADCAST(dim) */ ... FROM fact JOIN dim ON fact.k = dim.k;
```

```python
from pyspark.sql.functions import broadcast
fact.join(broadcast(dim), "k")
```

- 서버리스에서는 `spark.sql.autoBroadcastJoinThreshold`를 설정할 수 없다(`CONFIG_NOT_AVAILABLE`). 힌트가 유일한 재정의 수단이다.
- **힌트보다 통계 수정이 먼저다.** 관리형 테이블은 Predictive Optimization이 `ANALYZE`를 실행해 플래너가 정확한 크기를 안다. 힌트는 복잡한 서브쿼리, 임시 뷰, 통계가 없는 테이블처럼 플래너가 크기를 알 수 없을 때 쓴다.
- 컬럼 통계가 있으면 비용 기반 옵티마이저가 inner 조인을 재정렬해 중간 결과를 작게 유지한다.
- 범위 조인(`BETWEEN`, 구간 겹침)은 `/*+ RANGE_JOIN(table, bin_size) */` 힌트로 구간을 버킷화해 전체 쌍 비교를 피한다.

### 13-4. 셔플 해시 vs 브로드캐스트 비교 예

| | 대형 차원 3개 조인 | 4행 조회 테이블 조인 |
|---|---|---|
| 조인 알고리즘 | Photon Shuffled Hash Join | Broadcast Hash Join |
| 조인 양쪽 Exchange | 있음 | 없음 |
| `shuffle_read_bytes` | 약 779MB | 0 (GROUP BY는 부분 집계 28행만 이동) |
| 태스크 수 | 약 140 | 9 |
| 시간 지배 연산자 | Exchange, 집계 | 스캔 |

---

## 14. 스큐

일부 핫 키가 대부분의 행을 가져 그 파티션을 맡은 태스크가 나머지가 끝난 뒤에도 오래 실행된다(straggler). 스테이지 시간 = 가장 느린 태스크 시간이다.

- 프로파일: Time 색상에서 셔플이나 조인의 태스크 하나가 유독 길다. 인사이트는 **DATA_SKEW**.
- 예: 주문의 80%가 1%의 고객에게 몰려 있으면 `customer_idx` 기준 집계/조인에서 소수 태스크가 대부분을 처리한다.

### 14-1. 수정 순서 (비용이 작은 것부터)

| 단계 | 방법 | 설명 |
|---|---|---|
| 1 | **AQE가 처리하게 둔다** | 스큐 조인 처리가 기본 활성화. 셔플 파티션이 256MB보다 크고 중앙값의 5배를 넘으면 하위 파티션으로 나눠 병렬 실행. 먼저 AQE가 이미 해결했는지 확인 |
| 2 | **null과 불필요한 키 정리** | 가장 흔한 인위적 스큐 원인. 조인 전에 필터링하거나 사전 집계. 한 줄 변경 |
| 3 | **솔팅 (최후의 수단)** | 핫 키에 무작위 접미사를 붙여 N개 버킷으로 분산 |

### 14-2. null 키 스큐

모든 null은 같은 값으로 해시되므로 셔플 후 한 파티션에 몰린다. 그런데 `NULL`은 어떤 값과도 같지 않아 조인에서 절대 매칭되지 않는다. 시간만 낭비한다.

```sql
SELECT o.channel, COUNT(*) AS lines, SUM(d.quantity) AS units
FROM order_items_dirty d
LEFT JOIN orders o ON d.order_idx = o.order_idx
WHERE d.order_idx IS NOT NULL      -- 핫 파티션 제거
GROUP BY o.channel;
```

> 주의: LEFT JOIN에서 null 키 행을 걸러내면 결과에서 "매칭되지 않은 행" 그룹이 사라진다. 그 행 수 자체가 필요한 보고서라면 따로 집계한다.

### 14-3. 솔팅

```sql
-- 핫 쪽: 0~N-1 무작위 솔트
WITH salted_fact AS (
  SELECT *, CAST(rand() * 8 AS INT) AS salt FROM orders
),
-- 반대쪽: 모든 솔트 값으로 복제
salted_dim AS (
  SELECT d.*, s.salt
  FROM customers d
  CROSS JOIN (SELECT explode(sequence(0, 7)) AS salt) s
)
SELECT f.customer_idx, COUNT(*) AS n
FROM salted_fact f
JOIN salted_dim d ON f.customer_idx = d.customer_idx AND f.salt = d.salt
GROUP BY f.customer_idx;
```

비용: 반대쪽을 N배 복제하므로 셔플이 늘고, 쿼리가 복잡해져 유지 보수가 어렵다. AQE와 키 정리가 실패했을 때만 쓴다.

> 스큐를 감당하려고 더 큰 클러스터를 사지 않는다. 태스크 하나가 대부분의 행을 가진 상황에서 코어를 늘려도 straggler를 기다리는 유휴 코어만 늘어난다.

---

## 15. 스필

태스크의 작업 집합이 메모리를 넘으면 초과분을 로컬 디스크에 쓴다. 메모리는 GB/s 단위 처리량이고 로컬 디스크는 10~100배 느리다. 스필된 바이트는 직렬화 → 쓰기 → 읽기 → 역직렬화를 거치며 태스크 시간이 늘어난다.

| 확인 위치 | 신호 |
|---|---|
| 프로파일 | Spill (memory): 오버플로의 메모리 내 크기, Spill (disk): 디스크에 쓴 압축 바이트 |
| `system.query.history` | `spilled_local_bytes > 0` |
| Performance Insights | **DATA_SPILL** |

### 15-1. 해결 방법

| 방법 | 내용 |
|---|---|
| 1. 더 작은 파티션 | 셔플 파티션 `auto`(기본). AQE가 런타임에 과대 파티션을 나눈다 |
| 2. 코어당 메모리 증가 | 더 큰 인스턴스 유형 (클래식 전용) |
| 3. 근본 원인 해결 | 지속적인 스필은 증상인 경우가 많다. 스큐(한 태스크에 데이터 집중)나 행 폭발(팬아웃)을 고치면 사라진다 |

- **워커를 추가해도 스필은 해결되지 않는다.** 스케일 아웃은 코어 수를 늘릴 뿐 코어당 메모리는 그대로다.
- `spark.sql.shuffle.partitions = 200`을 수동으로 고정하던 방식은 파티션을 크게 만들어 스필을 악화시켰다.

### 15-2. 서버리스에서는 전조를 본다

서버리스의 Photon은 처리하는 데이터 양에 비례해 태스크 메모리를 할당하므로 일반적인 규모에서는 `spilled_local_bytes = 0`이다. 대신 Memory peak 색상에서 **peak shuffle memory**(최고 수위)를 본다. 고정 메모리 클래식 compute나 운영 볼륨에서 스필이 일어날 위치를 알려 주는 선행 지표다.

---

## 16. 행 폭발과 고차 함수

출력 행이 입력 행을 크게 초과하는 현상이다. 프로파일을 Rows 색상으로 보면 조인 연산자에서 행 수가 급증했다가(10M → 60M) 집계에서 다시 줄어든다. 인사이트는 **EXPLODING_JOIN**이다. 집계에서 행이 줄어도 이미 60M 행 전체가 셔플된 뒤다(셔플 바이트, 메모리, 집계 시간이 모두 6배).

| 원인 | 해결 |
|---|---|
| 정상적인 팬아웃 (일대다: 주문-항목, 사용자-이벤트) | 조인 전에 다(多) 쪽을 사전 집계 |
| 중복 키 조인 (데이터 품질 버그) | 조인 전에 중복 제거 |

### 16-1. 사전 집계

```sql
-- 조인 후 집계: 약 60M 행이 조인을 통과
SELECT o.customer_idx, SUM(oi.quantity) AS total_units
FROM orders o JOIN order_items oi ON o.order_idx = oi.order_idx
GROUP BY o.customer_idx;

-- 사전 집계 후 조인: 약 10M 행만 조인을 통과, 결과는 동일
WITH per_order AS (
  SELECT order_idx, SUM(quantity) AS order_units
  FROM order_items
  GROUP BY order_idx
)
SELECT o.customer_idx, SUM(po.order_units) AS total_units
FROM orders o JOIN per_order po ON o.order_idx = po.order_idx
GROUP BY o.customer_idx;
```

### 16-2. 고차 함수

배열에서 행별 지표만 필요하면 explode했다가 다시 모을 필요가 없다. `transform`, `filter`, `aggregate`, `size` 같은 고차 함수는 배열을 제자리에서 처리하며 Photon 안에서 실행된다.

```sql
-- explode 방식: 배열 요소 수만큼 중간 행 생성 후 GROUP BY
SELECT order_idx, COUNT(*) AS item_count, SUM(q) AS total_units, COUNT_IF(q >= 3) AS bulk_lines
FROM order_baskets, LATERAL explode(quantities) AS t(q)
GROUP BY order_idx;

-- 고차 함수 방식: 행 확장 없음, 결과 동일
SELECT order_idx,
       size(product_idxs)                             AS item_count,
       aggregate(quantities, 0, (acc, x) -> acc + x)  AS total_units,
       size(filter(quantities, q -> q >= 3))          AS bulk_lines
FROM order_baskets;
```

---

## 17. 드라이버 함정과 실행 계획 비대화

셔플, 스큐, 스필, 폭발은 분산 작업이 부하를 받는 증상이라 프로파일에 뚜렷하게 보인다. 다섯 번째 패턴은 반대다. **작업이 드라이버를 벗어나지 못하거나 Photon에 들어가지 못해** 코어는 놀고 시간만 흐른다. 프로파일은 조용하다.

### 17-1. 드라이버와 실행자

| | 역할 |
|---|---|
| 드라이버 | 단일 프로세스. 계획 수립, 태스크 스케줄링, 최종 결과 수집. 대량 데이터 처리 장소가 아니다 |
| 실행자 | 여러 노드, 여러 코어에서 병렬 작업. `select`, `filter`, 조인, 집계가 Photon으로 실행 |

서버리스 오토스케일링은 분산할 작업이 있을 때 실행자를 추가한다. `collect()`는 분산할 것이 없으므로 추가된 실행자도 논다.

### 17-2. 드라이버 함정

| 함정 | 증상 | 대체 |
|---|---|---|
| `collect()` / `toPandas()` | 긴 정지, 바쁜 스레드 하나, 유휴 실행자. 대규모에서는 드라이버 메모리 고갈 | 집계를 분산 상태로 유지하고 작은 최종 결과만 수집. 요약 수백 행은 괜찮고 팩트 테이블은 안 된다 |
| 드라이버의 단일 스레드 pandas / scikit-learn | 코어 하나만 바쁨, 실행자 메트릭 변화 없음 | `pyspark.pandas`, pandas UDF, `pyspark.ml` |
| 운영 코드의 `display()` / 반복 `count()` | 같은 스캔과 셔플이 여러 번 반복 | 각 `count()`는 그 시점까지의 계획을 재실행한다. 필요한 것을 한 번 계산해 저장하고 다음 단계는 그 결과를 읽는다 |

드라이버 크기를 키우는 것은 해결책이 아니다. 작업이 분산되도록 코드를 고친다.

### 17-3. 실행 계획 비대화

```python
# 나쁨: withColumn마다 프로젝션 노드가 하나씩 추가되어 계획이 길어지고 컴파일이 느려짐
for c in cols:
    df = df.withColumn(c, F.expr(c))

# 좋음: 프로젝션 한 번
df = df.withColumns({c: F.expr(c) for c in cols})   # 또는 하나의 select(...)
```

```python
df.dropDuplicates()               # 나쁨: 모든 컬럼이 키, 전체 행 너비 셔플
df.dropDuplicates(["order_id"])   # 좋음: 중복을 정의하는 키만
```

`df.explain()`으로 계획을 확인한다. 프로젝션이 길게 쌓였거나 예상치 못한 넓은 셔플이 있으면 신호다.

---

## 18. UDF 선택과 Photon 이탈

### 18-1. UDF 옵션 순위 (빠른 순)

| 순위 | 옵션 | 특징 | Photon |
|---|---|---|---|
| 1 | **내장 함수** (Spark SQL 함수, 고차 함수) | Python 왕복 없음. 문자열, 날짜, 수학, 배열, 맵, JSON 함수 제공 | 완전히 안에서 실행 |
| 2 | **Arrow 최적화 Python UDF** (`useArrow=True`) | JVM과 Python 사이를 Arrow 배치로 전달. 직렬화 비용 크게 감소 | 벗어남 |
| 3 | **pandas UDF** | 배치마다 `pandas.Series` 단위로 동작(Arrow 기반). 벡터화된 pandas/NumPy 연산이 필요할 때 | 벗어남 |
| 4 | **pickle 방식 Python UDF** | 한 번에 한 행씩 pickle 직렬화/역직렬화 | 벗어남 |

```python
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import StringType
import pandas as pd

# 1) 내장 함수: Photon에서 완전 실행
df.withColumn("tier", F.concat(
    F.upper(F.substring("category", 1, 3)), F.lit("-"),
    F.when(F.col("list_price") > 500, "A").when(F.col("list_price") > 100, "B").otherwise("C")))

# 2) Arrow 최적화 Python UDF
def price_band(category, list_price):
    band = "A" if list_price > 500 else "B" if list_price > 100 else "C"
    return f"{category[:3].upper()}-{band}"
tier_udf = udf(price_band, StringType(), useArrow=True)

# 3) pandas UDF
@pandas_udf("string")
def price_band_pandas(category: pd.Series, list_price: pd.Series) -> pd.Series:
    band = pd.cut(list_price, bins=[0, 100, 500, float("inf")], labels=["C", "B", "A"])
    return category.str[:3].str.upper() + "-" + band.astype(str)
```

런타임별 Arrow 기본값:

| 런타임 | `spark.sql.execution.pythonUDF.arrow.enabled` | 일반 `udf(fn, type)`의 동작 |
|---|---|---|
| 서버리스 환경 v5 | `true`로 고정 (변경 시 `CONFIG_NOT_AVAILABLE`) | Arrow 배치 |
| 클래식 DBR 17.3 | 기본 `false` | `useArrow=True`가 없으면 행 단위 pickle |

**Arrow는 직렬화 비용만 줄일 뿐 Photon 적용을 되돌리지 않는다.** 어떤 Python UDF든 해당 연산자는 Photon이 아니라 JVM에서 실행된다. 먼저 내장 함수를 시도하고, Python이 꼭 필요할 때만 Arrow UDF를 쓴다.

### 18-2. 같은 변환의 비용 차이 예

약 6천만 행에 티어 레이블을 계산해 GROUP BY하는 경우:

| | Python UDF (Arrow) | 내장 표현식 |
|---|---|---|
| 지배 연산자 | **Arrow Eval Python** (실행 시간 대부분) | 스캔, Inner Join (모두 Photon 네이티브) |
| `total_task_duration_ms` | 약 460K | 약 50K (약 10배 감소) |
| Wall-clock | 약 60초 | 약 4초 |

### 18-3. Photon 이탈을 프로파일에서 찾기

- 프로파일은 연산자마다 실행 엔진(Photon 또는 JVM)을 표시한다. Python UDF 연산자만 Photon 밖에 있는 형태가 전형적이다.
- 실행 세부 정보에서 **Photon에서 소요된 태스크 시간 비율**을 확인한다.
- **COVERAGE_PHOTON** 인사이트가 이탈 지점을 알려 준다.
- 연산자 속성에 `pythonUDF0` 같은 이름이 보이면 해당 컬럼이 Python으로 직렬화되고 있다는 뜻이다.
- 이탈 원인: Python UDF(pickle, Arrow 모두), Scala/Java UDF, RDD 경로, Photon이 아직 지원하지 않는 일부 연산자.

AI 어시스턴트로 드라이버 바인딩 코드를 재작성했다면 다음을 확인한다: `collect()`, `toPandas()`가 사라졌는가, 로직이 Spark 표현식으로 유지되는가, 작업이 여러 태스크에 분산되는가, `total_task_duration_ms`가 원본보다 낮은가.

---

## 19. 서버리스 우선

### 19-1. 네 가지 서버리스 서비스

| 서비스 | 내용 |
|---|---|
| **Jobs** | 예약/트리거된 Lakeflow Jobs가 서버리스 작업 compute에서 실행 |
| **Notebooks** | 대화형 노트북이 몇 초 만에 연결 |
| **DBSQL 웨어하우스** | SQL 편집기, 대시보드, BI 도구. 빠른 시작과 탄력적 확장 |
| **SDP** | Lakeflow Spark Declarative Pipelines. 향상된 오토스케일링 내장 |

사라지는 것: 노드 유형 선택, 워커 크기 조정, 자동 종료 설정, 시작/종료 중 유휴 용량 비용. 시작은 분이 아니라 초 단위다.

서버리스가 대체하는 레거시: `spark.dynamicAllocation.*`, 실행자/GC 크기 계산("실행자당 코어 5개"), Instance Pools(VM 부팅 시간 단축용), 수동 JVM 내부 설정(Photon과 Arrow로 대체).

### 19-2. 세 가지 필수 전제 조건

| 조건 | 내용 |
|---|---|
| **Standard 액세스 모드** | 이전 명칭 "Shared". Lakeguard가 사용자 코드를 격리해 행 필터, 열 마스크, 동적 뷰를 안전하게 적용. ("Single-user"는 이제 Dedicated) |
| **Unity Catalog 전용** | 읽기/쓰기는 UC를 통해서만. 마운트, `dbfs:/` 경로, `hive_metastore` 불가. `catalog.schema.table`로 참조 |
| **Spark Connect 호환 코드** | DataFrame과 SQL API만. `sparkContext`, RDD, `_jvm` 같은 Py4J JVM 접근 불가 |

- 하나라도 통과하지 못하면 서버리스에서 실행되지 않는다. 이전 전에 세 가지를 모두 점검한다.
- 대부분의 최신 DataFrame/SQL 코드는 이미 세 번째 조건을 만족한다. RDD로 내려가거나 `_jvm`에 접근하는 코드만 다시 쓴다. `sc.broadcast` 같은 RDD 수준 튜닝은 어차피 레거시다.
- Dedicated compute도 세분화된 접근 제어를 지원할 수 있지만, 워크스페이스에 서버리스가 활성화되어 있고 DBR 15.4 LTS 이상일 때만 가능하다(필터링을 서버리스에 위임하기 때문).
- ANSI SQL 모드가 서버리스(와 DBR 17.3)에서 기본 활성화되어 있다. 허용적인 클래식 작업을 옮기면 오버플로나 잘못된 캐스트로 새 오류가 날 수 있다.

### 19-3. 클래식이 여전히 맞는 경우 (유한한 예외 목록)

| 범주 | 예 |
|---|---|
| 언어와 런타임 | Scala, R 워크로드, 서버리스가 지원하지 않는 특정 런타임이 필요한 라이브러리 |
| 노드와 네트워크 | GPU, 엄격한 이그레스 제어의 프라이빗 서브넷, 특정 설정이 결과에 영향을 준다고 **증명할 수 있는** 튜닝 |
| 환경 설정 | 커스텀 init 스크립트, OS 수준 패키지 |

- 서버리스에서 Python 의존성용 init 스크립트는 레거시다. **Workspace Base Environments(WBE)** 가 대체한다. 환경 버전과 Python 패키지 목록을 선언적 YAML로 정의하고, 관리자가 빌드와 캐시를 해 두면 세션이 패키지가 준비된 상태로 빠르게 시작된다.
- WBE는 OS/커널 수준 변경(apt 패키지, 커널 조정)을 다루지 않는다. 그런 init 스크립트가 필요하다는 것 자체가 클래식에 남을 이유다.

### 19-4. 비용 모델과 성능 모드

- 서버리스는 실제 사용한 compute를 초 단위로 측정해 DBU로 과금한다. 별도 VM 비용이나 유휴 비용이 없다. 클래식은 VM 비용 + DBU이고 유휴 상태에서도 실행 중이면 과금된다.

| 모드 | 시작 | 스케일링 | 적합한 곳 |
|---|---|---|---|
| **성능 최적화** | 초 단위 (리소스를 웜 상태로 유지) | 적극적 | 대화형, 지연에 민감한 실행. 누군가 지금 결과를 기다릴 때 |
| **Standard** | 더 느림 | 보수적 | 예약 배치 작업과 파이프라인. 시작이 느려도 비용 절감이 의미 있을 때 |

- Standard 모드는 Jobs와 Pipelines에서만 고를 수 있다. 대화형 노트북은 항상 성능 최적화 모드다.
- 문서상 적합한 워크로드에서 성능 최적화 대비 최대 약 70% 절감이 가능하다고 하나, 실제 수치는 워크로드에 따라 다르므로 직접 확인한다.
- 실제 실행된 모드는 Jobs API `runs/get` 응답의 `performance_target`, 또는 `system.billing.usage`의 `product_features.performance_target`으로 확인하고, 모드별 실행당 DBU를 비교한다.

---

## 20. Photon

Spark 위에서 동작하는 **벡터화된 컬럼 기반 C++ 엔진**이다. API와 옵티마이저는 그대로 두고 실행 계층만 바꾼다.

| JVM 엔진 | Photon |
|---|---|
| 한 번에 한 행씩 처리, 행당 오버헤드 | 수천 행 단위 컬럼 배치에 SIMD 명령으로 한 번에 연산 |
| GC 일시 정지, JIT 워밍업 | 네이티브 코드라 GC 정지, JIT 워밍업, JVM 메모리 오버헤드 없음 |

| 항목 | 내용 |
|---|---|
| 활성화 | 서버리스 compute와 SQL 웨어하우스: 항상 켜짐, 끌 수 없음. 클래식 범용/작업 compute: 클러스터별 선택 |
| 가속 대상 | 조인, 집계, 스캔, 쓰기(Delta DML 포함). Photon 벡터화 셔플이 기존 Tungsten 셔플을 대체해 메모리 압박을 줄이므로 수동 셔플 튜닝이 대부분 불필요. 실행자 측 브로드캐스트로 브로드캐스트 빌드의 드라이버 병목 제거 |
| 폴백 대상 | Python UDF, Scala/Java UDF, RDD 코드 경로, 일부 상태 저장 스트리밍 등. 해당 연산자만 JVM으로 투명하게 넘어간다 |
| 확인 방법 | Query Profile의 Photon 태스크 시간 비율, 연산자별 엔진 표시, COVERAGE_PHOTON 인사이트. 클래식 범용 compute에서는 Spark UI로 확인 |

> 클래식에서 Photon을 켜면 DBU 단가가 올라간다. 워크로드가 Photon 연산자 위주인지(태스크 시간 비율) 확인하고 실행당 비용으로 판단한다.

---

## 21. 비용 환산과 비용 귀속

### 21-1. DBU를 달러로

`system.billing.usage`는 소비량을 DBU로 기록하고, `system.billing.list_prices`는 SKU와 클라우드별 DBU당 가격을 가진다. 두 테이블을 조인하면 비용이 나온다.

```sql
SELECT
  COALESCE(u.usage_metadata.job_name, u.usage_metadata.run_name,
           u.usage_metadata.notebook_path, u.billing_origin_product) AS workload,
  u.identity_metadata.run_as                                         AS run_as,
  u.custom_tags['team']                                              AS team,
  ROUND(SUM(u.usage_quantity), 2)                                    AS total_dbus,
  ROUND(SUM(u.usage_quantity * p.pricing.default), 2)                AS est_cost_usd
FROM system.billing.usage u
JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name
 AND u.cloud = p.cloud
 AND p.price_end_time IS NULL           -- 현재 유효한 가격만
WHERE u.usage_date >= current_date() - INTERVAL 30 DAYS
GROUP BY 1, 2, 3
ORDER BY est_cost_usd DESC;
```

| 컬럼 | 의미 |
|---|---|
| `usage_quantity` | 소비된 DBU |
| `sku_name`, `cloud` | 조인 키 |
| `pricing.default` | DBU당 가격 (`list_prices`) |
| `price_end_time` | 가격은 유효 기간 행으로 관리된다. 현재 가격은 `NULL` |
| `identity_metadata.run_as` | 실행한 사용자 또는 서비스 주체. 사용자별 귀속 기준 |
| `usage_metadata` | `job_id`, `job_name`, `warehouse_id`, `notebook_path`, `dlt_pipeline_id` 등 관련 항목만 채워짐 |
| `billing_origin_product` | JOBS, DLT, SQL, ALL_PURPOSE, INTERACTIVE 등 |
| `custom_tags` | 태그 MAP. 비용 귀속의 핵심 |
| `product_features.is_serverless` | 서버리스 여부 |

- 비용 관리의 나머지는 모두 `GROUP BY` 기준을 무엇으로 하느냐의 문제다(작업, 웨어하우스, 사용자, 팀).
- 순위 상위의 워크로드가 튜닝 시간 대비 효과가 가장 크다. 실행당 빠른 작업도 자주 돌거나 필요 이상 일하면 상위에 올 수 있다.
- 시스템 테이블은 SQL로 조회, 조인, 알림이 가능한 단일 진실 공급원이다. 월말 스프레드시트 조정을 대체한다.
- `list_prices`는 정가다. 계약 할인은 반영되지 않는다.

### 21-2. 지연과 즉시 대리 지표

| 소스 | 지연 | 용도 |
|---|---|---|
| `system.query.history.total_task_duration_ms` | 5~10분 | 즉시 A/B 비용 판단. 서버리스에서 DBU는 집계 태스크 시간에 비례 |
| `system.billing.usage` | 1~2시간, 10~60분 단위 레코드 | 실제 금액 정산 |

`execution_duration_ms`(wall-clock)는 비용을 예측하지 못한다. 6초에 끝난 쿼리도 24개 코어에서 집계 태스크 시간 150초를 쓸 수 있다. 1분짜리 벤치마크는 빌링 레코드와 정확히 맞지 않으므로, 겹치는 레코드의 DBU를 활성 시간 비율로 안분해야 한다.

### 21-3. 서버리스 비용 귀속: 사용 정책

서버리스에는 사용자가 소유하는 클러스터도, 클러스터 태그도 없다. **태그가 없는 서버리스 지출은 설계상 귀속할 수 없다.** 합계에는 잡히지만 어느 팀에도 청구할 수 없다.

- **서버리스 사용 정책(usage policy)** 은 `custom_tags` 집합을 가진 관리자 객체다. 정책이 할당된 사용자가 서버리스 워크로드를 실행하면 그 태그가 사용량 레코드에 붙는다.
- `custom_tags`는 사용 정책 태그 + 작업 수준 태그 + 상속된 워크스페이스 태그의 합집합이며, 키가 충돌하면 정책 태그가 우선한다. `usage_policy_id`에 적용된 정책이 기록된다.
- 쿼리별이 아니라 조직 차원의 해결책이다. 관리자가 한 번 정의하고 할당하면 이후 모든 서버리스 사용량이 귀속 가능해진다.

| 권장 | 주의 |
|---|---|
| 청구에 쓰는 키(팀, 프로젝트, 비용 센터)로 정책 정의 | 워크로드가 스스로 태그를 붙인다고 가정하지 않는다 |
| 사용자와 그룹에 할당하고 `custom_tags`로 다시 읽어 확인 | 정책은 **이후 사용량에만** 적용된다. 과거 사용량은 소급되지 않는다 |
| 값 집합을 작고 정규화된 상태로 유지 | 서버리스 정책은 클래식 compute에 태그를 붙이지 않는다 |
| | 수동 입력 자유 형식 태그에 의존하지 않는다 (값이 일관되지 않아 집계 불가) |

```sql
-- 귀속 격차: 서버리스 지출 중 팀 태그가 없는 비율
SELECT
  CASE WHEN u.custom_tags['team'] IS NULL THEN 'untagged (귀속 불가)' ELSE 'tagged (귀속 가능)' END AS status,
  ROUND(SUM(u.usage_quantity * p.pricing.default), 2) AS est_cost_usd
FROM system.billing.usage u
JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name AND u.cloud = p.cloud AND p.price_end_time IS NULL
WHERE u.product_features.is_serverless = true
GROUP BY 1;
```

---

## 22. 종합 시나리오

### 22-1. 2년 된 느린 아침 보고서

상황: 특정 계정의 사용 이력을 집계하는 보고서가 결과는 맞지만 점점 느려졌다. 테이블은 2년간 일별 이벤트로 수백만 행이 되었고, 생성 이후 레이아웃을 손댄 사람이 없다.

```sql
SELECT feature, event_type, COUNT(*) AS events,
       ROUND(SUM(usage_units), 2) AS total_units, COUNT(DISTINCT event_date) AS active_days
FROM usage_events
WHERE account_key = 7
GROUP BY feature, event_type
ORDER BY total_units DESC;
```

| 단계 | 내용 |
|---|---|
| 1. 측정 | 결과 캐시를 우회하고 워밍업을 뺀 중앙값 기록. 프로파일 스캔에서 읽은 파일, 프루닝된 파일, 읽은 바이트 기록. 계정 하나를 찾는데 대량 병렬 작업이 일어나면 과다 스캔 신호 |
| 2. 진단 | `DESCRIBE DETAIL`, `SHOW PARTITIONS`. `event_date`로 하루 하나씩 수백 개 파티션(파티션당 1GB에 크게 못 미침) → 과도한 파티셔닝. 필터는 `account_key`라 파티션 프루닝 무효. 계정 행이 모든 날짜 파일에 흩어져 min/max도 무력 → `pruned_files = 0` |
| 3. 수정 | 필터 컬럼으로 재클러스터링 |
| 4. 증명 | 같은 방식으로 재벤치마크. `pruned_files > 0`, `read_bytes`와 `total_task_duration_ms` 감소, `shuffle_read_bytes`는 비슷(스캔만 바꿨으므로) |

```sql
CREATE TABLE usage_events_clustered
CLUSTER BY (account_key)
AS SELECT * FROM usage_events;

-- 작은 테이블에서 프루닝이 보이도록 파일을 의도적으로 잘게 (운영 규모에서는 자동 조정에 맡김)
ALTER TABLE usage_events_clustered SET TBLPROPERTIES ('delta.targetFileSize' = '16mb');
OPTIMIZE usage_events_clustered FULL;

-- 레이아웃만 변수가 되도록 통계를 맞춤
ANALYZE TABLE usage_events_clustered COMPUTE STATISTICS FOR ALL COLUMNS;

DESCRIBE DETAIL usage_events_clustered;   -- clusteringColumns = [account_key], partitionColumns 비어 있음
```

필터 컬럼이 하나이므로 클러스터 키도 하나면 충분하다. 실행당 태스크 시간 감소에 일일 실행 횟수를 곱하면 절감액이 된다.

### 22-2. 원인 모를 비용 급증

상황: 새 워크로드도 새 고객도 없는데 월 청구액이 올랐다. 배포 이후 한 번도 프로파일링되지 않은 작업 하나가 증가분 대부분을 차지한다.

| 단계 | 내용 |
|---|---|
| 1. 비용 순위 | usage와 list_prices 조인 후 워크로드, `run_as`, 팀별 순위. 귀속할 수 없는 비용은 고칠 수 없다(어떤 작업을 볼지 모르므로) |
| 2. 진단 | 상위 작업을 실행하고 프로파일 확인. 태스크 시간 대부분이 Python UDF 연산자. 스캔은 빠름. Photon 태스크 시간 비율이 낮음 |
| 3. 수정 | UDF를 내장 표현식으로 재작성. 결과(티어, 개수, 합계)가 완전히 같은지 비교 |
| 4. 증명 | 즉시: `total_task_duration_ms` 급감. 나중에: 빌링 데이터에서 해당 작업의 비용 차이 |

```sql
-- 문제: Python UDF로 레이블 생성 → 해당 연산자가 Photon을 벗어나 행마다 Python으로 직렬화
SELECT settlement_tier_bad(region, amount) AS settlement_tier,
       COUNT(*) AS invoice_lines, ROUND(SUM(amount), 2) AS total_amount
FROM invoice_lines GROUP BY 1;

-- 수정: 내장 표현식 → 처음부터 끝까지 Photon
SELECT CONCAT(UPPER(SUBSTRING(region, 1, 2)), '-',
              CASE WHEN amount >= 500 THEN 'A' WHEN amount >= 100 THEN 'B' ELSE 'C' END) AS settlement_tier,
       COUNT(*) AS invoice_lines, ROUND(SUM(amount), 2) AS total_amount
FROM invoice_lines GROUP BY 1;
```

> 주의: 원래 UDF가 `region`이나 `amount`가 NULL이면 NULL을 반환했다면, 내장 버전도 같은 결과를 내는지 확인한다. 위 `CASE`는 `amount`가 NULL이면 `'C'`로 떨어지므로 `WHEN region IS NULL OR amount IS NULL THEN NULL`을 먼저 둬야 동일하다. **결과를 바꾸는 비용 수정은 버그다.**

UDF는 매일 밤 올바른 결과를 냈다. 이것은 정확성 문제가 아니라 비용 문제다. 행 단위 직렬화로 태스크 시간이 늘고, 실행당 DBU가 늘고, 그것이 매일 반복되어 청구서를 지배했다.

---

## 23. 핵심 정리 치트시트

### 기본

| 키워드 | 핵심 |
|---|---|
| 평가 기준 | 지연 시간 + 처리량 + 실행당 DBU. 빠르지만 비싸면 개선 아님 |
| 두 레버 | Read Less (I/O), Shuffle Less (네트워크) |
| 악화 요인 | 스큐, 스필, 조인 폭발 |
| 워크플로 | 플랫폼 우선 → 프로파일로 확인 → 남은 부분만 튜닝. 반복당 변경 하나 |
| AQE | 셔플 파티션 병합, 스큐 분할, 런타임 브로드캐스트 전환. 기본 활성 |

### 측정

| 키워드 | 핵심 |
|---|---|
| 두 시계 | wall-clock vs 집계 태스크 시간. 차이가 크면 병렬 처리 정상 |
| DAG 색상 | Time(핫 패스), Memory peak(스필 위험), Rows(팬아웃) |
| 캐시 히트 | 프로파일 없음, `from_result_cache = true` |
| sargeable | 컬럼을 함수로 감싸지 않아야 min/max 프루닝 가능 |
| 트리아지 | `system.query.history`: 느림/스필/셔플/과다 스캔별 정렬, `from_result_cache = false` |
| `statement_text` | 기본 `<REDACTED>`. `statement_id`와 시간 범위로 구분 |
| 정직한 벤치마크 | 결과 캐시 우회(디스크 캐시는 유지), 워밍업 제외, 중앙값, 태스크 시간, `ANALYZE`로 통계 통일 |
| 비용 지표 | `total_task_duration_ms` (서버리스 DBU 대리 지표, 캐시 영향 적음) |

### 레이아웃

| 키워드 | 핵심 |
|---|---|
| 소형 파일 | 128MB 패킹, 4MB 열기 비용. `DESCRIBE DETAIL`의 `numFiles`, `sizeInBytes` |
| 관리형 자동 튜닝 | 최적화된 쓰기, 자동 압축, 크기 자동 조정(256MB~1GB), 수집 시간 클러스터링 |
| Predictive Optimization | OPTIMIZE(COMPACTION/CLUSTERING), VACUUM, ANALYZE. ZORDER 안 함. 외부 테이블 제외 |
| 파티셔닝 | 1TB 미만 금지, 1~100TB는 LC, 100TB 이상만 고려, 파티션당 1GB 이상 |
| Liquid 클러스터링 | 최대 4키(적을수록 좋음), 증분, 재작성 없이 키 변경, 파티셔닝/ZORDER와 병행 불가 |
| OPTIMIZE / FULL | 증분 / 전체 재클러스터링(처음 활성화, 키 변경 후) |
| CLUSTER BY AUTO | UC 관리형 + 비파티션 + PO 필수. `clusterByAuto` 속성 |
| 파티션 덮어쓰기 대체 | `REPLACE WHERE`, `REPLACE USING` |

### 캐시와 워크로드 패턴

| 키워드 | 핵심 |
|---|---|
| 디스크 캐시 | 파일을 로컬 SSD에, 자동 무효화, `read_io_cache_percent` |
| 결과 캐시 | 원격은 서버리스 전용, 워크스페이스 공유, 약 24시간, 정확한 텍스트가 키 |
| Spark 캐시 (서버리스) | `.cache()` no-op, `CACHE TABLE` 오류 |
| 브로드캐스트 | 추정 10MB 이하(계획), 실측 30MB 이하(AQE, 셔플 비용은 이미 지불), 힌트 |
| 스큐 수정 순서 | AQE(256MB 초과 + 중앙값 5배) → null/불필요 키 필터 → 솔팅 |
| 스필 | 워커 추가로 해결 안 됨. 파티션 축소, 코어당 메모리, 스큐/폭발 해결. 서버리스는 peak shuffle memory를 본다 |
| 폭발 | 조인 전 사전 집계, 중복 제거, 고차 함수 |
| 드라이버 함정 | `collect()`, `toPandas()`, 드라이버 pandas, 운영 코드의 `display()`/`count()` |
| 계획 비대화 | 루프 `withColumn` 대신 `withColumns`/`select`, `dropDuplicates`는 키 지정 |
| UDF 순위 | 내장 > Arrow UDF > pandas UDF > pickle UDF. 내장만 Photon 안 |

### compute와 비용

| 키워드 | 핵심 |
|---|---|
| 서버리스 서비스 | Jobs, Notebooks, DBSQL 웨어하우스, SDP |
| 전제 조건 | Standard 액세스 모드, UC 전용, Spark Connect 호환 |
| 클래식 예외 | Scala, R, GPU, 프라이빗 서브넷, OS 수준 init 스크립트. Python 의존성은 WBE |
| 성능 모드 | 성능 최적화(대화형) vs Standard(배치, 저렴, Jobs/Pipelines만) |
| Photon | 서버리스/SQL 웨어하우스에서 항상 켜짐. 확인: Photon 태스크 시간 비율, COVERAGE_PHOTON |
| 비용 조인 | `usage` x `list_prices` ON `sku_name`, `cloud`, `price_end_time IS NULL` |
| 귀속 | 서버리스는 사용 정책 태그 → `custom_tags`. 소급 적용 안 됨 |

---

## 24. 자가 점검 문제

> 답을 가리고 먼저 생각해본 뒤 펼쳐보자.

**Q1.** 이 노트에서 최적화를 평가하는 세 가지 지표는 무엇이며, 비용을 1급 지표로 다뤄야 하는 이유는?
<details><summary>정답</summary>

지연 시간, 처리량, 실행당 DBU로 측정한 비용이다. compute를 늘리면 거의 모든 작업이 빨라지지만 비용이 조용히 증가하므로, 속도만 보면 성능 저하를 놓친다. 10% 빠르지만 2배 비싼 쿼리는 개선이 아니다.
</details>

**Q2.** 좁은 변환과 넓은 변환의 차이와, 넓은 변환이 비싼 이유는?
<details><summary>정답</summary>

좁은 변환(filter, select, withColumn)은 각 파티션을 독립적으로 처리해 네트워크 이동이 없다. 넓은 변환(groupBy, join, distinct, orderBy, 윈도우)은 같은 키가 한곳에 모이도록 행을 네트워크로 재분배(셔플)해야 하며, 셔플은 대부분의 쿼리에서 가장 비싼 단일 작업이다.
</details>

**Q3.** 사용자가 제어하는 두 가지 레버와 각각이 줄이는 비용은?
<details><summary>정답</summary>

Read Less는 스캔에서 읽는 바이트와 파일 수를 줄여 I/O 비용을 낮추고, Shuffle Less는 네트워크로 이동하는 행 수를 줄여 네트워크 비용을 낮춘다.
</details>

**Q4.** "플랫폼 우선 → 확인 → 튜닝" 순서를 지켜야 하는 이유는?
<details><summary>정답</summary>

플랫폼(Predictive Optimization, AQE, Photon 등)이 일할 기회를 갖기 전에 튜닝하면 기본값과 충돌하거나 이미 처리된 문제를 다시 고치게 된다. 프로파일로 무엇이 작동했는지 확인한 뒤 남은 차이만 튜닝한다.
</details>

**Q5.** `spark.sql.shuffle.partitions=200` 고정 설정이 더 이상 권장되지 않는 이유는?
<details><summary>정답</summary>

AQE가 실제 데이터를 기반으로 런타임에 셔플 파티션 크기를 결정하므로 하드코딩된 값은 대개 해가 된다. 서버리스에서는 사용자가 조정할 수도 없다. 고정 200은 파티션을 크게 만들어 스필을 악화시키기도 했다.
</details>

**Q6.** Query Profile의 두 시계는 무엇이며, 두 값이 가까울 때 무엇을 의심하는가?
<details><summary>정답</summary>

사용자가 체감한 wall-clock 기간과 모든 태스크의 코어-초 합계인 집계 태스크 시간이다. 두 값이 가까우면 병렬 처리가 거의 일어나지 않았다는 뜻으로, 작업이 소수 태스크나 드라이버에 몰려 코어 대부분이 놀았거나 compute 슬롯 대기가 있었을 수 있다.
</details>

**Q7.** 결과 캐시에서 응답한 쿼리의 프로파일을 볼 수 없는 이유와, 다시 측정하는 방법은?
<details><summary>정답</summary>

캐시에서 결과만 반환되고 실행이 일어나지 않았기 때문이다. SQL 웨어하우스에서는 `SET use_cached_result = false`를, 서버리스 노트북에서는 쿼리 텍스트에 고유 주석을 붙이는 등 사소하게 바꿔 다시 실행시킨다. `from_result_cache = true`로 캐시 히트를 확인할 수 있다.
</details>

**Q8.** `date_format(order_date, 'yyyy-MM') BETWEEN ...` 필터가 파일을 하나도 프루닝하지 못하는 이유와 해결 방법은?
<details><summary>정답</summary>

컬럼을 함수로 감싸면 엔진이 파일별 min/max 통계와 비교할 수 없어 모든 행에서 함수를 평가해야 한다. `order_date >= DATE'2024-01-01' AND order_date < DATE'2025-01-01'` 같은 sargeable 조건자로 바꾸고, 파일마다 날짜 범위가 좁아지도록 해당 컬럼으로 클러스터링(OPTIMIZE FULL)까지 해야 프루닝이 일어난다.
</details>

**Q9.** `system.query.history`에서 문제 쿼리를 찾을 때 증상별로 어떤 컬럼으로 정렬하며, 항상 넣어야 하는 필터는?
<details><summary>정답</summary>

느림은 total_duration_ms, 스필은 spilled_local_bytes, 셔플은 shuffle_read_bytes, 과다 스캔은 read_bytes 또는 read_files 대비 pruned_files다. 캐시된 결과는 잘못된 이유로 빠르므로 항상 `from_result_cache = false`로 거른다.
</details>

**Q10.** `statement_text`가 `<REDACTED>`로 나올 때 특정 실행을 어떻게 식별하는가?
<details><summary>정답</summary>

statement_text는 PII가 있을 수 있어 어카운트 관리자와 databricks_pii_access 그룹 외에는 가려진다. 항상 보이는 statement_id를 쓰거나, 실행 전후 시간 경계로 실행을 구분한다.
</details>

**Q11.** 정직한 벤치마크의 네 단계는?
<details><summary>정답</summary>

결과 캐시 우회(디스크 캐시는 유지), 첫 실행을 버린 뒤 여러 번 실행해 중앙값 보고, 시간과 함께 실행당 DBU(total_task_duration_ms) 추적, 변수 하나만 바꾸고 같은 compute에서 둘 다 캐시 히트가 아님을 확인하는 공정한 A/B다.
</details>

**Q12.** A/B 비교에서 wall-clock보다 `total_task_duration_ms`를 믿어야 하는 이유는?
<details><summary>정답</summary>

wall-clock은 먼저 실행된 쿼리가 데운 디스크 캐시 덕분에 뒤 쿼리가 빨라 보이는 식으로 부풀려진다. 태스크 시간은 모든 태스크의 실제 compute 합계라 캐시 영향이 적고, 서버리스에서 DBU에 비례하는 비용 지표다. 태스크 시간이 같고 wall-clock만 다르면 그 차이는 캐시 아티팩트다.
</details>

**Q13.** 벤치마크 전에 두 테이블에 `ANALYZE TABLE ... FOR ALL COLUMNS`를 실행하는 이유는?
<details><summary>정답</summary>

통계가 다르면 옵티마이저가 한쪽에서는 셔플 해시, 다른 쪽에서는 브로드캐스트처럼 다른 조인 전략을 선택해 비교가 불공정해진다. 통계를 맞추면 레이아웃 같은 의도한 변수만 남는다.
</details>

**Q14.** 소형 파일이 스캔을 느리게 하는 원리와, 반대로 지나치게 큰 파일의 문제는?
<details><summary>정답</summary>

엔진은 태스크당 약 128MB까지 파일을 묶고 파일마다 약 4MB의 열기 비용을 매기는데, 작은 파일이 수천 개면 패킹이 무력화되어 수천 개의 작은 태스크가 생기고 시간이 스케줄링과 파일 열기에 쓰인다. 지나치게 큰 파일은 통계 집합이 하나라 필터와 무관하게 항상 열어야 하고 스캔 병렬도가 떨어진다.
</details>

**Q15.** UC 관리형 테이블이 자동으로 수행하는 네 가지 레이아웃 기능은?
<details><summary>정답</summary>

쓰기 직전 셔플로 파일을 목표 크기에 맞추는 최적화된 쓰기, 커밋 후 작은 파일을 합치는 자동 압축, 테이블 크기에 따라 256MB~1GB로 목표를 정하는 파일 크기 자동 조정, 순서대로 들어오는 컬럼에서 프루닝을 가능하게 하는 수집 시간 클러스터링이다.
</details>

**Q16.** Predictive Optimization이 실행하는 작업, 실행하지 않는 작업, 적용되지 않는 테이블은?
<details><summary>정답</summary>

OPTIMIZE(압축 + 증분 클러스터링), VACUUM, ANALYZE를 서버리스에서 비동기로 실행한다. ZORDER는 실행하지 않는다. 외부 테이블과 Delta Sharing 수신 테이블에는 적용되지 않으므로 직접 유지 관리해야 한다.
</details>

**Q17.** `predictive_optimization_operations_history`에서 OPTIMIZE 실행을 찾으려면 `operation_type`에서 어떤 값을 봐야 하는가?
<details><summary>정답</summary>

OPTIMIZE라는 값은 없고 COMPACTION과 CLUSTERING으로 기록된다. 그 외에 VACUUM, ANALYZE가 있으며 비용은 usage_unit = ESTIMATED_DBU로 표시된다.
</details>

**Q18.** 테이블 크기별 파티셔닝 권장 사항과 파티셔닝 시 지켜야 할 최소 크기는?
<details><summary>정답</summary>

1TB 미만은 파티셔닝하지 않고, 1~100TB는 Liquid 클러스터링을 쓰며, 100TB 이상에서만 Liquid 클러스터링을 먼저 검증한 뒤 파티셔닝을 고려한다. 파티셔닝한다면 파티션 값당 최소 1GB를 유지한다.
</details>

**Q19.** 날짜로 파티셔닝된 테이블에 `WHERE customer_idx = 42`를 실행하면 왜 모든 파일을 읽는가?
<details><summary>정답</summary>

파티션 프루닝은 파티션 컬럼을 필터링할 때만 동작한다. 고객이 매일 활동하면 모든 날짜 폴더에 행이 있고, 각 파일의 customer_idx min/max 범위가 필터 값을 포함하므로 데이터 스키핑으로도 파일을 뺄 수 없다.
</details>

**Q20.** Liquid 클러스터링이 파티셔닝과 ZORDER보다 나은 점과 제약은?
<details><summary>정답</summary>

최대 4개 키에 고카디널리티 컬럼도 쓸 수 있고, 새 데이터만 증분으로 클러스터링하며, 테이블 재작성 없이 키를 바꿀 수 있다. 파티셔닝이나 ZORDER와 함께 쓸 수 없고, 키는 통계가 수집되는 컬럼이어야 하며, 키가 많을수록 단일 컬럼 필터 효과가 희석된다.
</details>

**Q21.** `OPTIMIZE`와 `OPTIMIZE FULL`의 차이와, FULL이 필요한 시점은?
<details><summary>정답</summary>

OPTIMIZE는 아직 클러스터링되지 않은 새 데이터만 증분 처리하고, OPTIMIZE FULL은 모든 데이터를 재클러스터링한다. 기존 데이터가 있는 테이블에 클러스터링을 처음 켰을 때와 클러스터 키를 바꾼 뒤에 필요하다.
</details>

**Q22.** 클러스터링 후 `pruned_files`가 0인데도 `read_bytes`가 크게 줄어드는 이유는?
<details><summary>정답</summary>

파일이 몇 개로 통합되어 파일 단위로는 뺄 것이 없지만, 파일 안에서 키 기준으로 정렬되어 행 그룹의 min/max 범위가 좁아졌기 때문이다. 파일을 열더라도 대상 키를 포함하지 않는 행 그룹 대부분을 건너뛴다.
</details>

**Q23.** `CLUSTER BY AUTO`의 동작 방식과 전제 조건은?
<details><summary>정답</summary>

Predictive Optimization이 과거 쿼리 워크로드를 분석해 키를 고르고 패턴 변화에 따라 진화시키며, 예상 스키핑 절감이 재클러스터링 비용보다 클 때만 키를 바꾼다. UC 관리형이고 파티션이 없는 Delta 테이블이어야 하며 Predictive Optimization이 활성화되어 있어야 한다. 비동기로 동작한다.
</details>

**Q24.** 서버리스에서 `df.cache()`와 `CACHE TABLE`은 각각 어떻게 동작하며, 무엇이 그 역할을 대신하는가?
<details><summary>정답</summary>

df.cache()는 호출을 받지만 아무 동작도 하지 않고(is_cached = False), CACHE TABLE은 NOT_SUPPORTED_WITH_SERVERLESS 예외를 낸다. 같은 파일 재스캔은 디스크 캐시가, 같은 쿼리 재실행은 결과 캐시가 자동으로 처리한다.
</details>

**Q25.** `from_result_cache`와 `read_io_cache_percent`는 각각 어떤 캐시를 가리키는가?
<details><summary>정답</summary>

from_result_cache는 결과 캐시로, true면 쿼리가 재실행되지 않고 결과가 반환된 것이다. read_io_cache_percent는 디스크 캐시로, 읽은 바이트 중 로컬 SSD에서 제공된 비율이다. 서로 다른 계층이므로 혼동하지 않는다.
</details>

**Q26.** 네 가지 조인 전략과 각각 셔플이 일어나는지 설명하라.
<details><summary>정답</summary>

브로드캐스트 해시 조인은 작은 쪽을 모든 태스크에 복사해 셔플이 없다. sort-merge 조인과 셔플 해시 조인은 양쪽을 셔플하며, 후자는 정렬 대신 해시 테이블을 쓴다. 브로드캐스트 중첩 루프는 동등 조건이 없을 때의 유일한 선택지로 셔플은 없지만 모든 행 쌍을 비교하는 O(N x M) 비용이 든다.
</details>

**Q27.** 정적 브로드캐스트(10MB)와 AQE 런타임 전환(30MB)의 차이는?
<details><summary>정답</summary>

정적 브로드캐스트는 추정 크기가 10MB 이하일 때 플래너가 처음부터 브로드캐스트를 계획해 셔플이 아예 없다. AQE 전환은 셔플이 끝난 뒤 실측 크기가 30MB 이하면 sort-merge를 브로드캐스트로 바꾸는 것으로, 셔플 네트워크 비용은 이미 지불했고 sort-merge compute만 절감된다. 그래서 정확한 통계가 중요하다.
</details>

**Q28.** 서버리스에서 브로드캐스트를 강제하는 방법과, 그보다 먼저 시도할 것은?
<details><summary>정답</summary>

서버리스에서는 autoBroadcastJoinThreshold를 설정할 수 없으므로 `/*+ BROADCAST(t) */` 힌트나 `broadcast(df)`를 쓴다. 그보다 먼저 통계를 정확히 하는 것이 권장되며, 관리형 테이블은 Predictive Optimization이 ANALYZE를 실행한다. 힌트는 서브쿼리, 임시 뷰, 통계 없는 테이블처럼 크기를 알 수 없을 때 쓴다.
</details>

**Q29.** 스큐 수정 순서와 각 단계의 내용은?
<details><summary>정답</summary>

먼저 AQE 스큐 조인 처리(256MB 초과이면서 중앙값 5배를 넘는 파티션을 분할)가 이미 해결했는지 확인한다. 다음으로 null이나 불필요한 키를 조인 전에 필터링하거나 사전 집계한다. 마지막으로 핫 키에 무작위 솔트를 붙여 분산하는 솔팅을 쓴다.
</details>

**Q30.** null 조인 키가 스큐를 일으키는 이유와, 필터링해도 inner 조인 결과가 바뀌지 않는 이유는?
<details><summary>정답</summary>

모든 null이 같은 값으로 해시되어 셔플 후 한 파티션에 몰린다. NULL은 어떤 값과도 같지 않아 조인에서 절대 매칭되지 않으므로 걸러내도 매칭 결과는 같다. 단, LEFT JOIN이면 매칭되지 않은 행 그룹이 결과에서 사라진다.
</details>

**Q31.** 솔팅이 최후의 수단인 이유는?
<details><summary>정답</summary>

반대쪽 데이터를 솔트 버킷 수만큼 복제해야 하므로 셔플이 늘고, 실제 코드 변경이 필요하며 쿼리가 복잡해져 유지 보수가 어렵다. 대부분의 스큐는 AQE와 키 정리로 해결된다.
</details>

**Q32.** 워커를 추가해도 스필이 해결되지 않는 이유와 실제 해결 방법은?
<details><summary>정답</summary>

스케일 아웃은 코어 수를 늘릴 뿐 코어당 메모리는 그대로라, 너무 큰 태스크는 여전히 슬롯에 들어가지 않는다. 파티션을 작게 하거나(AQE 자동), 코어당 메모리를 늘리거나(클래식에서 더 큰 인스턴스), 근본 원인인 스큐나 행 폭발을 고친다.
</details>

**Q33.** 서버리스에서 스필 위험은 무엇으로 판단하는가?
<details><summary>정답</summary>

서버리스 Photon은 데이터 양에 비례해 태스크 메모리를 할당해 일반 규모에서는 spilled_local_bytes가 0이다. 대신 프로파일을 Memory peak로 색칠해 peak shuffle memory를 본다. 고정 메모리 환경이나 운영 볼륨에서 스필이 날 위치를 알려 주는 선행 지표다.
</details>

**Q34.** 행 폭발을 프로파일에서 어떻게 찾고, 어떻게 고치는가?
<details><summary>정답</summary>

Rows 색상에서 조인 연산자의 출력 행이 입력보다 크게 늘었다가 집계에서 줄어드는 모양으로 보이며 EXPLODING_JOIN 인사이트가 뜬다. 정상적인 일대다 팬아웃은 조인 전에 다 쪽을 사전 집계하고, 중복 키 버그는 조인 전에 중복을 제거한다. 배열은 explode 대신 고차 함수로 제자리에서 처리한다.
</details>

**Q35.** 드라이버 함정 세 가지와 공통 해결 원칙은?
<details><summary>정답</summary>

collect()/toPandas()로 모든 행을 드라이버에 가져오기, 드라이버에서 단일 스레드 pandas/scikit-learn 실행, 운영 코드에 남긴 display()와 반복 count()다. 드라이버를 키우는 대신 작업을 DataFrame/SQL 연산으로 표현해 실행자에 분산된 상태로 유지하고 작은 최종 결과만 수집한다.
</details>

**Q36.** 루프 안의 `withColumn`과 인수 없는 `dropDuplicates()`가 문제인 이유는?
<details><summary>정답</summary>

withColumn은 호출마다 프로젝션 노드를 추가해 계획이 비대해지고 컴파일이 느려지므로 withColumns나 하나의 select로 바꾼다. 인수 없는 dropDuplicates는 모든 컬럼을 키로 써서 전체 행 너비를 셔플하므로 중복을 정의하는 키 컬럼만 지정한다.
</details>

**Q37.** UDF 옵션을 빠른 순으로 나열하고, Photon 안에서 실행되는 것은 무엇인가?
<details><summary>정답</summary>

내장 함수, Arrow 최적화 Python UDF, pandas UDF, pickle 방식 Python UDF 순이다. Photon 안에서 완전히 실행되는 것은 내장 함수뿐이고 나머지는 해당 연산자를 JVM으로 넘긴다.
</details>

**Q38.** Arrow가 켜져 있으면 Python UDF가 Photon에서 실행되는가?
<details><summary>정답</summary>

아니다. Arrow는 JVM과 Python 사이 직렬화를 배치로 처리해 비용을 줄일 뿐, Photon 적용을 되돌리지 않는다. 어떤 Python UDF든 해당 연산자는 JVM과 Python 워커에서 실행된다.
</details>

**Q39.** 쿼리가 Photon을 벗어났는지 확인하는 방법은?
<details><summary>정답</summary>

Query Profile에서 Photon 태스크 시간 비율과 연산자별 실행 엔진을 확인하고, COVERAGE_PHOTON 인사이트를 본다. Python UDF가 있으면 Arrow Eval Python 같은 연산자가 태스크 시간을 지배한다. 클래식 범용 compute에서는 Spark UI로 확인한다.
</details>

**Q40.** 서버리스 실행의 세 가지 전제 조건은?
<details><summary>정답</summary>

Standard 액세스 모드(이전 Shared, Lakeguard가 격리), Unity Catalog 전용 읽기/쓰기(마운트, dbfs:/, hive_metastore 불가), Spark Connect 호환 코드(DataFrame과 SQL API만, sparkContext, RDD, _jvm 접근 불가)다.
</details>

**Q41.** 클래식 compute가 여전히 맞는 경우와, Python 의존성용 init 스크립트를 대체하는 기능은?
<details><summary>정답</summary>

Scala와 R 워크로드, 지원되지 않는 런타임, GPU, 엄격한 프라이빗 네트워킹, 효과가 증명된 설정 의존 튜닝, OS 수준 init 스크립트가 필요한 경우다. Python 의존성은 환경 버전과 패키지 목록을 선언적 YAML로 정의하는 Workspace Base Environments가 대체한다.
</details>

**Q42.** 서버리스 성능 최적화 모드와 Standard 모드의 차이와 사용처는?
<details><summary>정답</summary>

성능 최적화 모드는 리소스를 웜 상태로 유지해 초 단위로 시작하고 적극적으로 확장하며 대화형과 지연 민감 작업에 맞다. Standard 모드는 시작이 느리고 확장이 보수적인 대신 비용이 낮아 예약 배치 작업과 파이프라인에 맞다. Standard는 Jobs와 Pipelines에서만 선택할 수 있고 노트북은 항상 성능 최적화 모드다.
</details>

**Q43.** DBU를 달러로 환산하는 조인의 조인 키와 필수 필터는?
<details><summary>정답</summary>

system.billing.usage와 system.billing.list_prices를 sku_name과 cloud로 조인하고, 현재 유효한 가격만 남기도록 price_end_time IS NULL로 거른다. 비용은 SUM(usage_quantity * pricing.default)이며 나머지는 GROUP BY 기준의 문제다.
</details>

**Q44.** 빌링 데이터를 기다릴 수 없을 때 쓸 대리 지표와, wall-clock이 비용 지표가 될 수 없는 이유는?
<details><summary>정답</summary>

system.query.history의 total_task_duration_ms를 쓴다. 5~10분 안에 확인할 수 있고 서버리스 DBU에 비례한다. wall-clock은 병렬도에 따라 달라져 6초에 끝난 쿼리도 여러 코어에서 150초의 태스크 시간을 쓸 수 있으므로 비용을 예측하지 못한다.
</details>

**Q45.** 태그 없는 서버리스 지출이 귀속 불가능한 이유와 해결책, 해결책의 한계는?
<details><summary>정답</summary>

서버리스에는 사용자가 소유하고 태그를 붙일 클러스터가 없어 custom_tags가 비어 있다. 관리자가 custom_tags를 가진 서버리스 사용 정책을 정의해 사용자와 그룹에 할당하면 이후 사용량에 태그가 붙는다. 정책은 과거 사용량에 소급되지 않고 클래식 compute에는 태그를 붙이지 않는다.
</details>

**Q46.** Python UDF를 내장 표현식으로 바꾸는 비용 수정에서 절감을 주장하기 전에 반드시 확인할 것은?
<details><summary>정답</summary>

결과(티어, 개수, 합계)가 원래 버전과 완전히 같은지 확인한다. NULL 처리 같은 경계 사례도 동일해야 한다. 결과를 바꾸는 비용 수정은 버그다. 그 다음 total_task_duration_ms 감소와 이후 빌링 데이터의 비용 차이로 절감을 증명한다.
</details>

---

## 25. 참고 링크

- 성능 효율성 모범 사례: https://docs.databricks.com/aws/en/lakehouse-architecture/performance-efficiency/best-practices
- 최적화 권장 사항: https://docs.databricks.com/aws/en/optimizations/
- Query Profile: https://docs.databricks.com/aws/en/sql/user/queries/query-profile
- 쿼리 성능 인사이트: https://docs.databricks.com/aws/en/sql/user/queries/performance-insights
- 쿼리 이력 시스템 테이블: https://docs.databricks.com/aws/en/admin/system-tables/query-history
- Predictive Optimization: https://docs.databricks.com/aws/en/optimizations/predictive-optimization
- 파일 크기 제어: https://docs.databricks.com/aws/en/delta/tune-file-size
- 파티셔닝 사용 시점: https://docs.databricks.com/aws/en/tables/partitions
- Liquid 클러스터링: https://docs.databricks.com/aws/en/delta/clustering
- 데이터 스키핑: https://docs.databricks.com/aws/en/delta/data-skipping
- 선택적 덮어쓰기: https://docs.databricks.com/aws/en/delta/selective-overwrite
- 디스크 캐시: https://docs.databricks.com/aws/en/optimizations/disk-cache
- 쿼리 캐싱: https://docs.databricks.com/aws/en/sql/user/queries/query-caching
- Adaptive Query Execution: https://docs.databricks.com/aws/en/optimizations/aqe
- 조인 힌트: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-qry-select-hints
- 범위 조인 최적화: https://docs.databricks.com/aws/en/optimizations/range-join
- 고차 함수: https://docs.databricks.com/aws/en/semi-structured/higher-order-functions
- Python UDF: https://docs.databricks.com/aws/en/udf/python
- pandas UDF: https://docs.databricks.com/aws/en/udf/pandas
- Photon: https://docs.databricks.com/aws/en/compute/photon
- 서버리스 compute: https://docs.databricks.com/aws/en/compute/serverless/
- 서버리스 제한 사항: https://docs.databricks.com/aws/en/compute/serverless/limitations
- 서버리스 작업 성능 모드: https://docs.databricks.com/aws/en/jobs/run-serverless-jobs
- 액세스 모드: https://docs.databricks.com/aws/en/compute/access-mode-limitations
- 빌링 사용량 시스템 테이블: https://docs.databricks.com/aws/en/admin/system-tables/billing
- 가격 시스템 테이블: https://docs.databricks.com/aws/en/admin/system-tables/pricing
- 서버리스 사용 정책: https://docs.databricks.com/aws/en/admin/usage/budget-policies
- Predictive Optimization 시스템 테이블: https://docs.databricks.com/aws/en/admin/system-tables/predictive-optimization
