# Apache Spark 소개 — 핵심 개념 정리

---

## 목차

1. [Apache Spark란](#1-apache-spark란)
2. [Spark와 Databricks의 역사](#2-spark와-databricks의-역사)
3. [Spark 컴포넌트 스택](#3-spark-컴포넌트-스택)
4. [Spark 4의 주요 변화](#4-spark-4의-주요-변화)
5. [런타임 아키텍처: Driver, Cluster Manager, Worker, Executor](#5-런타임-아키텍처-driver-cluster-manager-worker-executor)
6. [SparkSession과 SparkContext](#6-sparksession과-sparkcontext)
7. [Job, Stage, Task와 DAG](#7-job-stage-task와-dag)
8. [Spark UI와 Databricks compute](#8-spark-ui와-databricks-compute)
9. [DataFrame이란](#9-dataframe이란)
10. [변환, 액션, 지연 평가](#10-변환-액션-지연-평가)
11. [쿼리 계획과 최적화: Catalyst, AQE, Photon](#11-쿼리-계획과-최적화-catalyst-aqe-photon)
12. [컬럼형 저장](#12-컬럼형-저장)
13. [데이터 읽기와 쓰기](#13-데이터-읽기와-쓰기)
14. [스키마와 데이터 타입](#14-스키마와-데이터-타입)
15. [Spark SQL과 메타스토어](#15-spark-sql과-메타스토어)
16. [분산 처리 기초: 공유 없음, 파티셔닝, 셔플](#16-분산-처리-기초-공유-없음-파티셔닝-셔플)
17. [MapReduce로 보는 DataFrame 파이프라인](#17-mapreduce로-보는-dataframe-파이프라인)
18. [DataFrame 변환 메서드와 컬럼 참조](#18-dataframe-변환-메서드와-컬럼-참조)
19. [내장 함수와 결측값 처리](#19-내장-함수와-결측값-처리)
20. [ANSI 모드와 try_cast](#20-ansi-모드와-try_cast)
21. [UDF와 Pandas UDF](#21-udf와-pandas-udf)
22. [종합 예제: 항공편 데이터 ETL](#22-종합-예제-항공편-데이터-etl)
23. [종합 예제: 거래 데이터 분석](#23-종합-예제-거래-데이터-분석)
24. [핵심 정리 치트시트](#24-핵심-정리-치트시트)
25. [자가 점검 문제](#25-자가-점검-문제)
26. [참고 링크](#26-참고-링크)

---

## 1. Apache Spark란

Apache Spark는 대규모 데이터 처리를 위한 **오픈 소스 분산 통합 분석 엔진**이다.

| 특징 | 내용 |
|---|---|
| **통합** | SQL, 스트리밍, 머신 러닝, 그래프 처리를 하나의 일관된 API로 지원. 같은 DataFrame 코드로 배치 ETL, 스트리밍 파이프라인, ML 특성 준비를 처리. SQL, Python, Scala, Java, R 인터페이스가 모두 같은 실행 엔진으로 컴파일되어 같은 쿼리 계획을 만든다 |
| **분산** | 데이터를 파티션으로 나눠 여러 머신에서 병렬 처리. 노드를 추가해 수평 확장. 노드 장애 시 쿼리 계획에 기록된 **리니지**로 잃어버린 파티션만 다시 계산 |
| **빠름** | 중간 결과를 가능한 한 메모리에 유지해, 단계마다 디스크에 쓰던 Hadoop MapReduce보다 훨씬 빠르다. Databricks에서는 Photon이 벡터화 배치 처리로 더 가속 |
| **개방성** | S3, ADLS, GCS 같은 클라우드 객체 스토리지, 로컬/분산 파일 시스템, JDBC 데이터베이스에 내장 커넥터. Parquet, JSON, CSV, Delta Lake, Iceberg 같은 개방형 형식 사용 |

---

## 2. Spark와 Databricks의 역사

| 연도 | 사건 |
|---|---|
| 2009 | UC Berkeley AMPLab에서 시작. Hadoop MapReduce의 성능 한계, 특히 ML 같은 반복 작업 문제를 해결하려고 |
| 2010 | 최초 공개 릴리스 |
| 2013 | Apache Software Foundation에 기증, 원래 창시자들이 Databricks 설립 |
| 2014 | Apache 최상위 프로젝트 |
| 2015 | DataFrame API 출시 |
| 2016 | Spark 2.0: 통합 진입점 SparkSession, Structured Streaming |
| 2018 | Delta Lake 발표 |
| 2020 | Spark 3.0: Adaptive Query Execution |
| 2021 | Unity Catalog 발표 |
| 2023 | Spark 3.4: Spark Connect |
| 2025 | Spark 4.0 출시 (Databricks Runtime 17.x의 기반) |

DataFrame API와 SparkSession이 도입되면서 Spark 개발은 저수준 RDD 코드에서 **선언적이고 옵티마이저 친화적인** 방식으로 옮겨 갔다.

---

## 3. Spark 컴포넌트 스택

| 계층 | 구성 | 설명 |
|---|---|---|
| 라이브러리 | **Structured Streaming** | 배치와 같은 DataFrame 연산으로 무한 데이터 처리. 스트리밍 파이프라인이 배치와 거의 같은 모양 |
| | **MLlib** | 분산 ML 알고리즘과 특성 변환기 |
| | **Spark SQL** | 표준 SQL 인터페이스. DataFrame API와 옵티마이저, 실행 엔진을 공유 |
| | **pandas API on Spark** | pandas 호환 인터페이스로 클러스터 규모 실행 |
| | **GraphX** | 그래프 알고리즘. Python에서는 DataFrame 기반 GraphFrames를 주로 사용 |
| 고수준 API | **DataFrame API** | 현대 Spark의 주된 프로그래밍 인터페이스. 원하는 결과를 선언하면 Catalyst가 계산 방법을 정함 |
| 기반 | **Spark Core (RDD API)** | 스케줄링, 메모리 관리, 장애 복구, RDD(Resilient Distributed Dataset) 추상화. 직접 프로그래밍할 일은 드물지만 모든 실행은 여기서 일어남 |

- RDD API는 Spark Connect에서 사용할 수 없다.
- SparkR은 Spark 4.0부터 사용 중단(deprecated)되었고 sparklyr 사용이 권장된다.

---

## 4. Spark 4의 주요 변화

| 기능 | 의미 |
|---|---|
| **ANSI SQL 모드 기본 활성** | 잘못된 형변환과 산술 오버플로가 `NULL`을 조용히 반환하는 대신 **오류**. 허용적 변환은 `try_cast()` |
| **Spark Connect** | gRPC 기반 클라이언트-서버 구조. 애플리케이션은 얇은 클라이언트, Driver는 원격 실행 |
| **`VARIANT` 타입** | JSON 같은 반정형 데이터를 파싱된 이진 형태로 저장. 문자열로 저장하고 매번 파싱하는 것보다 빠르고 유연 |
| **네이티브 플로팅** | pandas 변환 없이 PySpark DataFrame에서 `df.plot` (Plotly 기반) |
| **SQL 스크립팅** | `BEGIN ... END` 블록, 변수, 조건문, 반복문을 갖춘 절차적 SQL |
| **Arrow 최적화 Python UDF** | Python UDF 직렬화에 Apache Arrow를 써서 Python-JVM 오버헤드 감소 |

---

## 5. 런타임 아키텍처: Driver, Cluster Manager, Worker, Executor

Spark 애플리케이션은 서로 협력하는 프로세스의 집합이다.

| 역할 | 하는 일 |
|---|---|
| **Driver** | 애플리케이션의 중심. 실행 계획 수립과 조율 |
| **Cluster Manager (Master)** | 클러스터 리소스를 관리하고 Driver에 할당 |
| **Worker** | Executor를 호스팅하는 클러스터 노드 |
| **Executor** | Worker 위의 프로세스. Driver가 준 **태스크**(I/O와 데이터 처리)를 실행 |

흐름: 클라이언트(노트북, 작업, 앱)가 Driver에 작업 제출 → Driver가 Cluster Manager와 협상해 Executor 확보 → Driver가 Executor에 **직접** 태스크를 스케줄링하고 결과 수집.

### Driver의 역할

- 모든 Spark 애플리케이션의 진입점인 `SparkSession` 생성
- 애플리케이션을 분석해 DAG 구성
- 태스크를 스케줄링해 Executor에 배분
- 진행 상황 모니터링과 장애 처리
- 결과를 클라이언트에 반환

### Executor

- 보통 Worker 노드당 Executor 하나.
- 각자 독립적으로 **RAM 캐시, 디스크 캐시, CPU 코어**를 가진다.
- 캐시에는 명시적으로 유지(persist)한 파티션을 보관해 여러 작업에서 재계산 없이 재사용한다.
- 태스크 조정과 데이터 전송을 위해 Driver와 통신한다.

### Databricks에서

- Cluster Manager 역할은 플랫폼이 맡는다. 클러스터를 만들면 Driver 노드와 Worker 노드를 프로비저닝하고 연결한다.
- **Spark Connect**: 노트북이나 IDE가 얇은 클라이언트가 되고, Driver는 원격에서 gRPC로 쿼리 계획을 받는다. Databricks 서버리스 compute와 Databricks Connect가 이 방식이다. 작성하는 DataFrame 코드는 같다.

---

## 6. SparkSession과 SparkContext

`SparkSession`은 Spark 2.0에서 도입된 **모든 Spark 기능의 통합 진입점**이다. 예전의 SparkContext, SQLContext, HiveContext, StreamingContext를 대체한다.

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("MySparkApplication")
         .config("spark.some.config.option", "some-value")
         .getOrCreate())
```

- Databricks 노트북에서는 `spark` 변수로 **이미 생성**되어 있다.
- `getOrCreate()`는 기존 세션이 있으면 그것을 돌려준다(관리되는 싱글톤).

```python
print(spark.version)                     # 4.x
print(spark.conf.get("spark.app.id"))    # Spark Connect에서도 안전
print(SparkSession.builder.getOrCreate() is spark)   # True
```

| | SparkSession (`spark`) | SparkContext (`sc`) |
|---|---|---|
| 위치 | 현대적 진입점 | SparkSession 아래의 레거시 객체 |
| 전용(Dedicated) 액세스 클래식 클러스터 | 사용 가능 | `spark.sparkContext`, `sc` 사용 가능 |
| 표준(Standard, 구 Shared) 액세스, 서버리스 | 사용 가능 | **사용 불가** (Spark Connect가 JVM 직접 접근 차단). RDD API도 불가 |

현대 Spark 코드는 SparkSession과 DataFrame으로 작성한다. `spark.sparkContext.applicationId` 대신 `spark.conf.get("spark.app.id")`처럼 Spark Connect에서도 동작하는 방식을 쓴다.

---

## 7. Job, Stage, Task와 DAG

### 7-1. 실행 계층

| 단위 | 의미 |
|---|---|
| **Application** | SparkSession 하나에 해당하는 전체 프로그램 |
| **Job** | **액션 하나**가 실행을 트리거할 때 생기는 작업 |
| **Stage** | Job을 셔플 경계에서 나눈 단위. 병렬로 실행 가능한 태스크 묶음 |
| **Task** | 가장 작은 실행 단위. **파티션 하나를 처리** |

스테이지 안의 태스크들은 병렬로 실행되며 서로 통신하지 않는다(**shared nothing**).

### 7-2. DAG (Directed Acyclic Graph)

| 단어 | 의미 |
|---|---|
| Directed | 계산이 스테이지를 따라 한 방향으로 흐른다 |
| Acyclic | 되돌아가지 않으므로 작업은 반드시 끝난다 |
| Graph | 스테이지가 의존성 그래프로 구성된다 |

예: 소스 스캔 → 행 필터 → 컬럼 프로젝션 (스테이지 1) → **셔플(교환)** → 키별 집계 → 결과 반환 (스테이지 2)

- 필터, 프로젝션처럼 파티션마다 독립적으로 할 수 있는 연산은 하나의 스테이지로 **파이프라인화**된다.
- 데이터를 클러스터 전체에 재분배해야 하는 **셔플** 지점에서 스테이지 경계가 생긴다. 키별 집계를 하려면 같은 키의 행이 한 Executor에 모여야 하기 때문이다.

---

## 8. Spark UI와 Databricks compute

### 8-1. Spark UI

| UI | 범위 | 보여 주는 것 |
|---|---|---|
| 애플리케이션 UI | SparkSession당 하나 | 진행 상황과 태스크 실행, DAG 시각화와 스테이지 세부, 리소스 사용과 성능 지표 |
| 마스터 UI | 클러스터당 하나 | Worker 상태, 클러스터 전체 리소스 할당, 실행 중인 애플리케이션 |

Spark UI의 주요 화면:

| 화면 | 내용 |
|---|---|
| Jobs | 잡 목록과 DAG. 셔플이 있으면 여러 스테이지로 보인다. 스테이지에서 태스크 수준 세부로 이동 |
| Executors | Executor별 코어, 메모리, 리소스 사용 |
| Storage | 캐시된 DataFrame과 Executor 메모리 사용 |

- Databricks에서는 클러스터 상세 화면이나 노트북 셀 아래의 Spark Jobs 링크로 연다.
- **서버리스 compute에서는 Spark UI가 노출되지 않는다.** Spark UI로 내부를 보려면 클래식 compute를 쓴다.

```python
from pyspark.sql.functions import col, sum

df = spark.range(0, 1000000).withColumn("square", col("id") * col("id"))
result = df.groupBy(col("id") % 100).agg(sum("square").alias("sum_squares"))   # 셔플 → 2 스테이지
result.cache()        # 캐시 표시 (액션 시 채워짐)
result.collect()      # 액션 → Job
print(result.count()) # 또 다른 Job
result.unpersist()    # 캐시 해제
```

### 8-2. Databricks compute 유형

| 유형 | 설명 |
|---|---|
| 범용(All-purpose) 클러스터 | 노트북, 작업, 대시보드를 지원하는 대화형 클러스터. 자동 종료 설정 |
| 작업(Job) 클러스터 | 작업 실행 시 시작해 끝나면 종료되는 임시 클러스터. 비대화형 워크로드 |
| SQL 웨어하우스 | SQL 쿼리에 최적화. 빠른 시작과 자동 확장 |
| 서버리스 | Databricks가 운영하는 완전 관리형. 노트북이 Spark Connect로 얇은 클라이언트처럼 연결 |

### 8-3. 노트북 매직 명령

| 명령 | 용도 |
|---|---|
| `%python`, `%sql`, `%scala`, `%r` | 노트북 기본 언어와 다른 언어로 셀 실행 |
| `%run` | 다른 노트북을 현재 노트북 안에서 실행 (설정 코드 재사용) |
| `%sh`, `%fs`, `%md` | 셸 명령, 파일 시스템, 마크다운 |

---

## 9. DataFrame이란

DataFrame은 **같은 스키마를 가진 레코드의 분산 컬렉션**이다.

- 사용자에게는 하나의 테이블처럼 보이지만, 실제 행은 클러스터의 Executor들에 **파티션**으로 나뉘어 있다.
- 스키마를 추적하고, SQL 함수와 관계형 연산자(`JOIN`, `GROUP BY` 등)를 지원한다.
- **지연 평가**되어 DAG로 실행되며, 내결함성을 주는 리니지 정보를 가진다.
- **불변(immutable)** 이다. 변환은 기존 DataFrame을 바꾸지 않고 새 DataFrame을 반환한다.

DataFrame을 만드는 소스:

```python
df = spark.read.csv("/Volumes/my_catalog/my_schema/files/people.csv")   # 파일 (JSON, CSV, Parquet, ORC, 텍스트 등)
df = spark.read.table("my_catalog.my_schema.people")                    # 카탈로그의 테이블이나 뷰
df = spark.range(1000)                                                   # 숫자 범위
# 그 밖에 Delta 등 테이블 형식 디렉터리, 외부 데이터베이스, 기존 RDD(클래식 compute 전용)
```

---

## 10. 변환, 액션, 지연 평가

| 구분 | 의미 | 예 |
|---|---|---|
| **변환 (Transformation)** | 기존 DataFrame에서 새 DataFrame을 만든다. **무엇을** 계산할지 기술할 뿐 실행하지 않는다 | `select`, `filter`/`where`, `withColumn`, `groupBy`, `agg`, `join`, `orderBy` |
| **액션 (Action)** | 실제 계산을 트리거하고 결과를 만든다 | `count`, `show`, `take(n)`, `first`, `collect`, `write`, `display` |

변환을 여러 개 연결해도 액션이 호출될 때까지 아무것도 실행되지 않는다. 이것이 **지연 평가(lazy evaluation)** 다.

지연 평가가 중요한 이유:

- 실행 전에 전체 변환 체인을 보므로, 옵티마이저가 연산 순서를 바꾸고 합치고 불필요한 작업을 없앨 수 있다(예: 필터를 파일 스캔 단계로 내려 읽는 데이터 자체를 줄임).
- 기록된 변환 체인(리니지)으로 잃어버린 파티션을 다시 계산할 수 있다.
- 변환 10개를 정의하는 것은 밀리초 단위다. 비용은 액션이 누적된 계획을 실행할 때 생긴다.

> 주의: `display()`, `count()` 같은 확인용 액션을 운영 코드에 반복해서 남기면, 액션마다 그 시점까지의 계획이 다시 실행된다.

---

## 11. 쿼리 계획과 최적화: Catalyst, AQE, Photon

### 11-1. 쿼리 계획 단계

DataFrame이 평가되면 Driver는 논리 계획을 효율적인 물리 실행 계획으로 바꾼다.

| 단계 | 하는 일 |
|---|---|
| 미해결 논리 계획 | 작성한 코드 그대로 |
| **분석 (Analysis)** | 카탈로그로 이름 해석. 테이블이 무엇인지, 컬럼이 존재하는지, 타입이 무엇인지 확인 → 분석된 논리 계획 |
| **논리 최적화** | 규칙 기반 재작성: **조건자 푸시다운**(필터를 최대한 일찍), **컬럼 가지치기**(필요한 컬럼만 읽기), 상수 폴딩 → 최적화된 논리 계획 |
| **물리 계획** | 구체적 실행 전략 선택. 예: 한쪽이 작으면 셔플 조인 대신 브로드캐스트 조인 |
| **코드 생성** | JVM 바이트코드로 컴파일하거나, Databricks에서는 Photon에 넘겨 벡터화 네이티브 실행 |

```python
df.explain()           # 물리 계획 출력
df.explain(True)       # 논리 계획부터 물리 계획까지 전체 단계
```

### 11-2. 최적화 기능

| 기능 | 내용 |
|---|---|
| **Catalyst Optimizer** | DataFrame 연산을 최적화된 실행 계획으로 바꾸는 쿼리 최적화 엔진. 규칙 기반과 비용 기반 최적화 |
| **Adaptive Query Execution (AQE)** | 실행 중 실제 데이터 통계로 계획을 재조정. 작은 셔플 파티션 병합, 더 나은 조인 전략 선택, 치우친(skew) 파티션 분할. Spark 3에서 도입, Spark 4에서 기본 활성 |
| **기본 내장 통계** | Parquet, Delta 같은 형식으로 저장할 때 통계를 수집해 더 나은 계획에 활용 |
| **인메모리 컬럼형 형식** | DataFrame 내부의 컬럼형 메모리 표현으로 분석 성능 향상, 메모리 절감 |
| **Photon** | Databricks 네이티브 벡터화 쿼리 엔진. 행이 아닌 배치 단위 처리. SQL 웨어하우스와 서버리스에서 기본, 범용/작업 클러스터에서는 선택 활성화 |

SQL로 쓴 쿼리와 같은 의미의 DataFrame 코드는 **같은 물리 계획**으로 컴파일된다. 둘은 같은 Catalyst와 실행 엔진 위의 두 가지 입구이므로 더 명확한 쪽을 쓰면 된다.

---

## 12. 컬럼형 저장

| 행 지향 저장 | 컬럼형 저장 |
|---|---|
| 한 레코드의 모든 값을 함께 저장 | 한 컬럼의 모든 값을 함께 저장 |
| 컬럼 하나만 필요해도 모든 행 전체를 읽음 | 필요한 컬럼 블록만 읽고 나머지는 건너뜀 (`SUM(amt)`는 amt 블록만) |
| 트랜잭션 처리에 유리 | 분석(스캔, 집계)에 유리 |

같은 컬럼의 값은 타입이 같고 비슷한 경우가 많아 **압축 효율**도 높다. DataFrame 내부 메모리 형식과 Parquet, ORC 같은 파일 형식이 컬럼형이다.

---

## 13. 데이터 읽기와 쓰기

### 13-1. DataFrameReader (`spark.read`)

```python
# 일반 형식
df = (spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/Volumes/catalog/schema/files/customers"))

# 형식별 단축 메서드
df = spark.read.csv("/Volumes/catalog/schema/files/csv-dir")
df = spark.read.parquet("s3://mybucket/parquet-dir")
df = spark.read.json("/Volumes/catalog/schema/files/json-dir")
df = spark.read.table("catalog.schema.table")
```

- CSV에 헤더가 있으면 컬럼 이름을 헤더에서 가져오고, 없으면 `_c0`, `_c1`처럼 붙는다.
- 디렉터리 경로를 주면 그 안의 파일을 모두 읽는다.

### 13-2. DataFrameWriter (`df.write`)

```python
# 파일로 쓰기
(df.write
   .format("parquet")
   .mode("overwrite")
   .save("/Volumes/catalog/schema/output/customers_parquet"))

# 테이블로 쓰기 (Unity Catalog)
df.write.mode("overwrite").saveAsTable("catalog.schema.customers")

# DataFrameWriterV2
df.writeTo("catalog.schema.customers").createOrReplace()
```

| 저장 모드 | 동작 |
|---|---|
| `error` / `errorifexists` (기본) | 대상이 있으면 오류 |
| `overwrite` | 덮어쓰기 |
| `append` | 추가 |
| `ignore` | 대상이 있으면 아무것도 하지 않음 |

- 파일 출력은 **단일 파일이 아니라 파트 파일들(`part-00000...`)로 구성된 디렉터리**다. 각 Executor가 파티션별로 병렬로 쓰기 때문이다.
- `write` 자체가 액션이다.
- DataFrameWriterV2(`writeTo`)는 `create`, `createOrReplace`, `append`, `overwrite` 같은 테이블 중심 메서드와 파티셔닝 옵션을 제공한다.

---

## 14. 스키마와 데이터 타입

### 14-1. 스키마 지정 방법

| 방법 | 특징 |
|---|---|
| **추론** (`inferSchema`) | 편하지만 데이터를 **한 번 더 읽어야** 하고, 잘못 추론할 수 있다(예: 우편번호를 정수로) |
| **`StructType`** | 프로그래밍 방식으로 명시 |
| **DDL 문자열** | 같은 내용을 SQL 형태로 읽기 쉽게 명시 |

운영 파이프라인에서는 **스키마를 명시**한다. Parquet, Delta 같은 **자기 기술(self-describing)** 형식은 스키마가 파일에 들어 있어 추론이 필요 없다.

```python
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

customer_schema = StructType([
    StructField("customer_id", LongType(), False),     # False = NOT NULL
    StructField("name", StringType(), True),
    StructField("account_balance", DoubleType(), True),
])

ddl_schema = "customer_id BIGINT NOT NULL, name STRING, account_balance DOUBLE"

df1 = spark.read.format("csv").option("header", "true").schema(customer_schema).load(path)
df2 = spark.read.csv(path, header=True, schema=ddl_schema)

df1.printSchema()     # 트리 형태
print(df1.schema)     # StructType 표현
```

### 14-2. 기본 데이터 타입

| pyspark.sql.types | Spark SQL | Python |
|---|---|---|
| `ByteType` | `TINYINT` | int |
| `ShortType` | `SMALLINT` | int |
| `IntegerType` | `INT` | int |
| `LongType` | `BIGINT` | int |
| `FloatType` | `FLOAT` | float |
| `DoubleType` | `DOUBLE` | float |
| `BooleanType` | `BOOLEAN` | bool |
| `StringType` | `STRING` | str |
| `BinaryType` | `BINARY` | bytearray |
| `TimestampType` | `TIMESTAMP` | datetime.datetime |
| `DateType` | `DATE` | datetime.date |

`TIMESTAMP_NTZ`는 시간대 정보가 없는 타임스탬프다.

### 14-3. 복합 타입과 VARIANT

| 타입 | SQL | Python | 비고 |
|---|---|---|---|
| `ArrayType` | `ARRAY` | list | 같은 타입 요소의 순서 있는 목록 |
| `MapType` | `MAP` | dict | 키와 값의 타입이 다를 수 있음 |
| `StructType` | `STRUCT` | tuple, dict | 이름과 타입이 있는 필드 |
| `VariantType` | `VARIANT` | - | Spark 4 신규. 반정형 값(JSON 등)을 최적화된 이진 형식으로 저장 |

`VARIANT`는 JSON을 문자열로 두고 매 쿼리마다 파싱하던 방식을 대체한다. `data:order.items[0].sku` 같은 경로 표현식을 더 편하고 빠르게 쓸 수 있다.

---

## 15. Spark SQL과 메타스토어

### 15-1. Spark SQL

Spark SQL은 DataFrame을 실행하는 **같은 엔진에 대한 SQL 인터페이스**다.

```python
df.createOrReplaceTempView("flights_temp")      # 세션 범위 임시 뷰
# df.createGlobalTempView("flights_g")          # global_temp 스키마의 전역 임시 뷰

result_df = spark.sql("""
  SELECT carrier, COUNT(*) AS flights
  FROM flights_temp
  GROUP BY carrier
""")                                             # 결과도 DataFrame
```

- 임시 뷰로 등록하면 DataFrame을 SQL로 조회할 수 있다.
- `spark.sql()`의 결과는 DataFrame이라 SQL과 DataFrame 코드를 자유롭게 섞을 수 있다.
- `selectExpr()`은 SQL 표현식 문자열로 컬럼을 고른다.

### 15-2. 메타스토어

메타스토어는 Spark SQL의 메타데이터를 관리한다.

- 테이블 스키마와 위치
- 테이블 파티션
- 테이블, 뷰, 함수의 속성과 정보

Databricks에서는 **Unity Catalog**가 메타스토어 역할을 하는 중앙 메타데이터 서비스다. 계정 수준에 있고 워크스페이스에 할당되며, `catalog.schema.table` 3단계 네임스페이스와 세분화된 보안, 거버넌스를 제공한다.

---

## 16. 분산 처리 기초: 공유 없음, 파티셔닝, 셔플

### 16-1. 공유 없는 아키텍처 (Shared Nothing)

노드들이 메모리나 디스크를 공유하지 않고 협력한다.

| 특성 | 내용 |
|---|---|
| 독립성 | 각 노드가 자기 CPU, 메모리, 디스크를 관리 |
| 확장성 | 리소스 경합 없이 노드를 추가해 성능 향상 |
| 내결함성 | 장애가 개별 노드에 격리 |
| 리소스 파티셔닝 | 데이터와 작업을 노드별로 나눠 경합 제거 |

### 16-2. 파티셔닝

- 데이터는 서로 겹치지 않는 메모리 파티션으로 나뉜다.
- 기본 파티셔닝은 입력(파일 등)에 따라 정해지며 조정할 수 있다.
- **파티션 1개 = 태스크 1개.** 파티션의 수와 크기가 병렬 처리 수준을 결정한다.
- 파티션은 독립적으로, 여러 개가 병렬로 처리된다.

> 여기서의 파티션은 실행 시점의 메모리 단위다. 테이블을 특정 컬럼 값별 폴더로 저장하는 디스크 파티셔닝(`partitionBy`)과는 다른 개념이다.

### 16-3. 셔플

셔플은 **파티션 사이에 데이터를 재분배**하는 것이다.

| 항목 | 내용 |
|---|---|
| 필요한 연산 | `groupBy`, `join`, `distinct`, 전역 `orderBy`, `repartition` 같은 키 기반 연산 |
| 비용 | Spark에서 가장 비싼 연산. 데이터를 직렬화해 맵 쪽 디스크에 쓰고, 네트워크로 보내고, 리듀스 쪽에서 역직렬화 |
| 구조 | 모든 맵 쪽 파티션이 모든 키를 가질 수 있으므로, 리듀스 쪽 파티션마다 모든 맵 쪽 파티션에서 데이터를 받아 오는 **전체 대 전체** 교환 |
| 결과 | 셔플마다 DAG에 스테이지 경계 |

| 좁은 변환 (narrow) | 넓은 변환 (wide) |
|---|---|
| 각 출력 파티션이 입력 파티션 하나에만 의존 | 출력 파티션이 여러 입력 파티션에 의존 |
| `select`, `filter`, `withColumn` | `groupBy`, `join`, `distinct`, `orderBy` |
| 셔플 없음, 한 스테이지로 파이프라인화 | 셔플 발생, 스테이지 경계 생성 |

**성능 습관**: 가능한 한 **일찍 필터링하고 필요한 컬럼만 골라** 셔플되는 데이터를 줄인다. AQE가 런타임에 작은 셔플 파티션 병합, 조인 전략 변경, 치우친 파티션 재조정을 자동으로 돕는다.

---

## 17. MapReduce로 보는 DataFrame 파이프라인

DataFrame 파이프라인은 컴파일되면 **Map → Shuffle → Reduce** 구조가 된다.

```python
input_df   = spark.read.csv("sales_data.csv", header=True)                  # READ
filtered   = input_df.select("id", "amt").filter("amt > 50")                # MAP: 좁은 변환, 이동 없음
grouped    = filtered.groupBy("id")                                         # SHUFFLE: 같은 id를 같은 파티션으로
aggregated = grouped.agg({"amt": "sum"})                                    # REDUCE: id별 합계
aggregated.write.csv("/Volumes/catalog/schema/output/sales_by_id")          # WRITE: 파티션별 파일
```

| 단계 | 일어나는 일 |
|---|---|
| Map | 파일마다 파티션이 되고 프로젝션과 필터가 파티션별로 독립 적용. `amt > 50`을 통과하지 못한 행은 여기서 사라져 이후 셔플까지 작아진다 |
| Shuffle | `groupBy('id')`가 키를 해싱해 같은 id 행을 같은 리듀스 파티션으로 재분배 |
| Reduce | 키별로 합계를 계산하고 각 리듀스 파티션이 자기 출력 파일(`part-00000.csv`, `part-00001.csv`)을 씀 |

| 연산 | Map | Shuffle | Reduce |
|---|---|---|---|
| `groupBy` | 키 추출 | 키 기준 재분배 | 집계 |
| `join` | 키 준비 | 일치하는 키를 같은 위치로 | 행 결합 |
| `filter` | 조건 평가 | 불필요 | 불필요 |

---

## 18. DataFrame 변환 메서드와 컬럼 참조

### 18-1. 변환 메서드와 SQL 대응

DataFrame 변환은 모든 파티션에 자동으로 분산되고, 새 DataFrame을 반환하며, 액션 전까지 논리 계획에 쌓인다.

| DataFrame 메서드 | SQL | 설명 |
|---|---|---|
| `select()` | `SELECT` | 컬럼 선택 |
| `filter()`, `where()` | `WHERE` | 조건으로 행 필터 |
| `groupBy()` + `agg()` | `GROUP BY` | 그룹화와 집계 |
| `orderBy()`, `sort()` | `ORDER BY` | 정렬 |
| `join()` | `JOIN` | 키로 조인 |
| `withColumn()` | `SELECT ..., expr AS c` | 컬럼 추가 또는 교체 |
| `drop()` | | 컬럼 제거 |
| `distinct()`, `dropDuplicates()` | `DISTINCT` | 중복 제거 |
| `limit()` | `LIMIT` | 행 수 제한 |

### 18-2. 컬럼을 참조하는 방법

| 방법 | 예 | 특징 |
|---|---|---|
| 문자열 | `df.select("name")` | 가장 간단, 단순 선택 |
| 속성 | `df.select(df.first_name)` | 컬럼 이름이 유효한 Python 식별자일 때만 |
| 인덱스 표현식 | `df.select(df["first-name"])` | 모든 이름에 가능, 조인 시 어느 DataFrame의 컬럼인지 명확히 |
| **Column 객체** | `df.select(col("name").alias("customer_name"))` | 복잡한 표현식, `alias`, `cast`, `asc`, `desc` 같은 컬럼 메서드에 필요 |
| SQL 표현식 | `df.selectExpr("ElapsedTimeDiff > 0 AS LongerThanScheduled")` | SQL 문법 그대로 |

### 18-3. 주요 Column 메서드

| 메서드 | 설명 | 예 |
|---|---|---|
| `alias()` | 이름 변경 | `col("name").alias("customer_name")` |
| `cast()` / `astype()` | 타입 변환 | `col("age").cast("int")` |
| `try_cast()` | 실패 시 NULL로 변환 | `col("ArrDelay").try_cast("integer")` |
| `isNull()` / `isNotNull()` | NULL 확인 | `col("name").isNotNull()` |
| `contains()` | 문자열 포함 | `col("title").contains("Manager")` |
| `asc()` / `desc()` | 정렬 방향 | `df.sort(col("c1").desc(), col("c2").asc())` |

조건 결합은 Python의 `and`, `or`가 아니라 `&`, `|`, `~`를 쓰고 각 조건을 괄호로 감싼다.

```python
df.filter((col("amt") > 50) & (col("region") == "US"))
```

---

## 19. 내장 함수와 결측값 처리

### 19-1. 내장 함수

`pyspark.sql.functions`에는 수학, 날짜/시간, 문자열, 컬렉션, 집계, 윈도우 함수가 있고, 대부분 같은 이름의 SQL 함수가 있다.

| 함수 | SQL | 설명 |
|---|---|---|
| `round(col, scale)` | `ROUND` | 반올림 |
| `concat(col1, col2)` | `CONCAT` | 문자열 연결 |
| `date_format(col, fmt)` | `DATE_FORMAT` | 날짜를 문자열로 |
| `regexp_replace(col, pattern, repl)` | `REGEXP_REPLACE` | 정규식 치환 |
| `coalesce(col1, col2)` | `COALESCE` | 첫 번째 NULL이 아닌 값 |
| `when(...).otherwise(...)` | `CASE WHEN` | 조건 분기 |
| `make_timestamp_ntz(y, m, d, h, mi, s)` | `MAKE_TIMESTAMP_NTZ` | 구성 요소로 타임스탬프 생성 |
| `lpad(col, n, pad)`, `substr(col, pos, len)` | `LPAD`, `SUBSTR` | 문자열 채우기, 자르기 |
| `count_if(cond)` | `COUNT_IF` | 조건을 만족하는 행 수 |

### 19-2. 결측값

| 함수 | 용도 |
|---|---|
| `isNull()` / `isNotNull()` | NULL 여부 확인 |
| `count(col)` | 해당 컬럼의 **NULL이 아닌** 값 개수 (`count("*")`는 전체 행) |
| `df.fillna()` / `df.na.fill()` | NULL을 특정 값으로 대체 |
| `df.dropna()` / `df.na.drop()` | NULL이 있는 행 제거 |

```python
df.na.drop(how="any", subset=["CRSElapsedTime"])   # 해당 컬럼이 NULL인 행 제거
df.dropna()                                        # 어느 컬럼이든 NULL이면 제거
df.fillna("unknown")                               # 문자열 컬럼의 NULL 대체
df.fillna({"amount": 0, "city": "unknown"})        # 컬럼별 대체값
```

---

## 20. ANSI 모드와 try_cast

Spark 4는 **ANSI SQL 모드가 기본**이다.

| 표현식 | Spark 3 (ANSI 끔) | Spark 4 (ANSI 켬) |
|---|---|---|
| `CAST('NA' AS INT)` | 조용히 `NULL` | **오류** |
| `try_cast('NA' AS INT)` | `NULL` | `NULL` |
| 정수 오버플로 | 조용히 값이 돌아감(wrap) | **오류** |

품질을 모르는 데이터를 정제할 때는 `try_cast()`로 잘못된 값을 NULL로 바꾼 뒤 찾아서 처리한다. 작업 전체가 실패하는 것을 막는다.

```sql
SELECT
  COUNT_IF(TRY_CAST(DepTime AS INT) IS NULL)  AS invalid_deptime,
  COUNT_IF(TRY_CAST(ArrDelay AS INT) IS NULL) AS invalid_arrdelay
FROM flights;
```

```python
from pyspark.sql.functions import col
valid_df = df.filter(
    col("ArrDelay").try_cast("integer").isNotNull() &
    col("DepTime").try_cast("integer").isNotNull()
)
```

---

## 21. UDF와 Pandas UDF

### 21-1. Python UDF

```python
from pyspark.sql.functions import udf

@udf("string")
def make_greeting(name):
    return f"Hello, {name}!"

df.select(make_greeting("name"))
```

- 재사용 가능한 사용자 정의 로직을 만들 수 있다.
- 단점: Catalyst가 내부를 **최적화할 수 없고**, JVM과 Python 워커 사이에 **행 단위 직렬화** 오버헤드가 있다.

### 21-2. Pandas UDF (벡터화 UDF)

```python
from pyspark.sql.functions import pandas_udf

@pandas_udf("integer")
def age_plus_one(age_series):        # pandas Series 배치를 받는다
    return age_series + 1

df.select(age_plus_one("age"))
```

- 행 하나가 아니라 **행 배치(pandas Series)** 단위로 동작하고, **Apache Arrow**로 Python-JVM 데이터를 효율적으로 주고받는다.
- 일반 Python UDF보다 빠르지만 여전히 Catalyst에는 불투명하다.
- Spark 4에서는 일반 UDF도 Arrow 직렬화를 쓸 수 있다: `@udf("string", useArrow=True)`.

| 선택 순서 | 이유 |
|---|---|
| 1. 내장 함수 | 옵티마이저가 이해하므로 항상 가장 빠르다 |
| 2. Pandas UDF / Arrow UDF | 사용자 정의 로직이 꼭 필요할 때 |
| 3. 일반 Python UDF | 행 단위 직렬화로 가장 느리다 |

> 주의: Pandas UDF가 받는 Series는 **전체 컬럼이 아니라 한 배치**다. 배치 안에서 평균과 표준편차를 구해 z-점수를 만들면 배치마다 기준이 달라진다. 전체 기준 정규화는 내장 집계(`avg`, `stddev`)나 윈도우 함수로 전체 통계를 먼저 구해 계산한다.

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()   # 전체를 하나의 윈도우로 (데이터가 크면 집계 후 조인이 낫다)
df = df.withColumn("diff_z",
        (F.col("ElapsedTimeDiff") - F.avg("ElapsedTimeDiff").over(w)) / F.stddev("ElapsedTimeDiff").over(w))
```

---

## 22. 종합 예제: 항공편 데이터 ETL

### 22-1. 요구 사항

소스 `flights` 테이블은 숫자여야 할 컬럼(`DepTime`, `ActualElapsedTime`, `ArrDelay`)이 문자열이고 `NA`나 NULL 같은 잘못된 값이 섞여 있다. 이를 정제, 보강해 다음 테이블을 만든다.

| 컬럼 | 타입 | 파생 방법 |
|---|---|---|
| `FlightDateTime` | `TIMESTAMP_NTZ` | `Year`, `Month`, `DayofMonth`, `DepTime`(HHMM)으로 생성 |
| `FlightNum` | `INT` | 그대로 |
| `ElapsedTimeDiff` | `INT` | `ActualElapsedTime - CRSElapsedTime` |
| `ArrDelayCategory` | `STRING` | `ArrDelay`로 On Time / Slight / Moderate / Severe Delay |

### 22-2. 단계별 처리

| 단계 | 내용 |
|---|---|
| 1. 로드와 검사 | `spark.read.table`, `printSchema()`로 문자열 컬럼 확인. **필요한 컬럼만 먼저 선택**(일찍, 자주 줄이기) |
| 2. 잘못된 값 분석 | `TRY_CAST`한 임시 뷰를 만들고 `COUNT_IF(col IS NULL)`로 컬럼별 잘못된 값 수 집계. 같은 집계를 DataFrame(`sum(when(isNull, 1))`)으로 써도 `explain()`의 물리 계획이 같다 |
| 3. 정제 | `na.drop(subset=...)`, `try_cast(...).isNotNull()` 필터, `try_cast`로 타입 변환 |
| 4. 보강 | 파생 컬럼 생성 후 구성 컬럼 `drop` |
| 5. 시각화 | 집계 후 `df.plot.bar(...)` (Spark 4 네이티브 플로팅) |
| 6. 저장 | 하나의 체인으로 읽기부터 `saveAsTable`까지 |

### 22-3. 전체 파이프라인

```python
from pyspark.sql.functions import col, make_timestamp_ntz, lpad, substr, lit, when

(spark.read.table("flights")
    # 필요한 컬럼만, 문자열은 try_cast로 정수 변환 (잘못된 값은 NULL)
    .selectExpr(
        "Year", "Month", "DayofMonth",
        "TRY_CAST(DepTime AS INT) AS DepTime",
        "FlightNum",
        "TRY_CAST(ActualElapsedTime AS INT) AS ActualElapsedTime",
        "CRSElapsedTime",
        "TRY_CAST(ArrDelay AS INT) AS ArrDelay")
    # 잘못되거나 누락된 값이 있는 행 제거
    .na.drop()
    # HHMM 형식의 DepTime을 시와 분으로 나눠 타임스탬프 생성
    .withColumn("FlightDateTime", make_timestamp_ntz(
        col("Year"), col("Month"), col("DayofMonth"),
        substr(lpad(col("DepTime"), 4, "0"), lit(1), lit(2)).try_cast("integer"),
        substr(lpad(col("DepTime"), 4, "0"), lit(3), lit(2)).try_cast("integer"),
        lit(0)))
    .drop("Year", "Month", "DayofMonth", "DepTime")
    # 경과 시간 차이
    .withColumn("ElapsedTimeDiff", col("ActualElapsedTime") - col("CRSElapsedTime"))
    .drop("ActualElapsedTime", "CRSElapsedTime")
    # 도착 지연 범주
    .withColumn("ArrDelayCategory",
        when(col("ArrDelay") <= 0, "On Time")
        .when(col("ArrDelay") <= 15, "Slight Delay")
        .when(col("ArrDelay") <= 60, "Moderate Delay")
        .otherwise("Severe Delay"))
    .drop("ArrDelay")
    # 단일 쓰기 액션
    .write.mode("overwrite").saveAsTable("cleaned_and_enriched_flights"))
```

- 변환은 지연 평가되고 새 DataFrame을 반환하므로, 전체 파이프라인을 **단일 쓰기 액션으로 끝나는 하나의 체인**으로 표현할 수 있다.
- `when` 체인은 위에서부터 처음 참인 조건의 값을 쓴다.

> 주의: 출발 시각 데이터에 `2400`(자정) 같은 값이 있으면 시가 24가 되어, ANSI 모드에서 `make_timestamp_ntz`가 오류를 낼 수 있다. 이런 값은 사전에 걸러 내거나 보정한다.

### 22-4. 정제와 보강 원칙

- `try_cast()`로 타입을 일찍 검증하고 정제한다.
- 결측값 처리 방식(삭제, 대체)을 정하고 가정을 문서화한다.
- 비즈니스 요구에 맞는 파생 컬럼을 만든다.
- 내장 함수를 우선 쓰고, 필요할 때만 UDF를 쓴다.

---

## 23. 종합 예제: 거래 데이터 분석

```python
from pyspark.sql.functions import col

# 1. 읽기와 탐색
transactions_df = spark.read.table("samples.bakehouse.sales_transactions")
transactions_df.printSchema()
display(transactions_df.limit(5))

# 2. 전체 건수 (액션)
total_transactions = transactions_df.count()

# 3. $100 초과 거래 필터 (변환)
large_transactions_df = transactions_df.filter(col("totalPrice") > 100)

# 4. 테이블로 저장 (액션)
large_transactions_df.write.mode("overwrite").saveAsTable("large_transactions")

# 5. Spark SQL로 확인
display(spark.sql("SELECT COUNT(*) AS cnt_large_transactions FROM large_transactions"))
```

- `display()`는 Databricks 노트북 함수로, 액션을 트리거해 결과를 표로 보여 준다. 시각 확인용이라 최대 10,000행 또는 2MB 중 작은 쪽까지만 보여 준다. 결과 정렬, 필터, CSV 다운로드, 데이터 프로필, 시각화를 지원한다.
- 볼륨 파일 목록은 `dbutils.fs.ls(path)`로 확인한다.

---

## 24. 핵심 정리 치트시트

### 기본 개념

| 키워드 | 핵심 |
|---|---|
| Spark | 오픈 소스 분산 통합 분석 엔진. 통합, 분산, 빠름(인메모리), 개방 |
| 역사 | 2009 Berkeley → 2013 Databricks 설립 → 2015 DataFrame → 2016 SparkSession → 2020 AQE → 2023 Spark Connect → 2025 Spark 4 |
| 스택 | 라이브러리(Streaming, MLlib, SQL, pandas API, GraphX) → DataFrame API → Spark Core(RDD) |
| Spark 4 | ANSI 기본, Spark Connect, VARIANT, `df.plot`, SQL 스크립팅, Arrow UDF |

### 아키텍처

| 키워드 | 핵심 |
|---|---|
| Driver | SparkSession 생성, DAG 구성, 태스크 스케줄링, 모니터링, 결과 반환 |
| Cluster Manager | 리소스 할당 (Databricks는 플랫폼이 담당) |
| Worker / Executor | Executor를 호스팅하는 노드 / 태스크를 실행하는 프로세스 (코어, RAM, 디스크 캐시) |
| SparkSession | 통합 진입점, Databricks에서 `spark` |
| SparkContext | 레거시. 전용 액세스 클래식에서만. Spark Connect(표준 액세스, 서버리스)에서는 불가, RDD도 불가 |
| 계층 | Application → Job(액션당) → Stage(셔플 경계) → Task(파티션당) |
| DAG | 방향성, 비순환, 그래프 |
| Spark UI | Jobs(DAG), Executors(리소스), Storage(캐시). 서버리스에서는 없음 |

### DataFrame

| 키워드 | 핵심 |
|---|---|
| DataFrame | 스키마가 있는 불변 분산 컬렉션, 파티션으로 분산 |
| 변환 | `select`, `filter`, `withColumn`, `groupBy`, `agg`, `join` → 새 DataFrame, 실행 안 함 |
| 액션 | `count`, `show`, `take`, `first`, `collect`, `write`, `display` → 실행 |
| 지연 평가 | 전체 체인을 보고 최적화, 리니지로 복구 |
| 계획 단계 | 미해결 → 분석 → 논리 최적화(푸시다운, 가지치기) → 물리 계획 → 코드 생성 |
| 확인 | `df.explain()`. SQL과 DataFrame은 같은 물리 계획 |
| Photon | 벡터화 네이티브 엔진. 서버리스/SQL 웨어하우스 기본 |
| 컬럼형 | 필요한 컬럼만 읽음, 압축 효율 |

### I/O와 스키마

| 키워드 | 핵심 |
|---|---|
| 읽기 | `spark.read.format().option().schema().load()`, `.csv`, `.parquet`, `.table` |
| 쓰기 | `df.write.format().mode().save()`, `.saveAsTable()`, `df.writeTo().createOrReplace()` |
| 저장 모드 | error(기본), overwrite, append, ignore |
| 파일 출력 | 파트 파일 디렉터리 |
| 스키마 | 추론(추가 스캔, 오추론) vs `StructType` vs DDL 문자열 → 명시 권장 |
| 자기 기술 형식 | Parquet, Delta는 추론 불필요 |
| VARIANT | 반정형 JSON용 이진 저장 |
| Spark SQL | `createOrReplaceTempView` + `spark.sql()` → DataFrame |
| 메타스토어 | Databricks는 Unity Catalog |

### 분산과 ETL

| 키워드 | 핵심 |
|---|---|
| 공유 없음 | 노드가 메모리와 디스크를 공유하지 않음 |
| 파티션 | 1 파티션 = 1 태스크 |
| 셔플 | 전체 대 전체 재분배, 가장 비쌈, 스테이지 경계 |
| 좁은 vs 넓은 | `select`/`filter` vs `groupBy`/`join` |
| 습관 | 일찍 필터하고 필요한 컬럼만 |
| MapReduce | Map(좁은 변환) → Shuffle(키 재분배) → Reduce(집계) |
| 컬럼 참조 | 문자열, 속성, `df["c"]`, `col()`, `selectExpr` |
| 결측값 | `na.drop`, `fillna`, `count(col)`은 NULL 제외 |
| ANSI | 잘못된 CAST는 오류 → `try_cast()` |
| UDF 순서 | 내장 함수 > Pandas/Arrow UDF > Python UDF |

---

## 25. 자가 점검 문제

> 답을 가리고 먼저 생각해본 뒤 펼쳐보자.

**Q1.** Apache Spark의 네 가지 특징은?
<details><summary>정답</summary>

SQL, 스트리밍, ML, 그래프를 하나의 API로 처리하는 통합, 데이터를 파티션으로 나눠 여러 머신에서 병렬 처리하는 분산, 중간 결과를 메모리에 유지하는 빠른 속도, 다양한 스토리지와 개방형 형식을 지원하는 개방성이다.
</details>

**Q2.** Spark가 Hadoop MapReduce보다 빠른 핵심 이유는?
<details><summary>정답</summary>

중간 결과를 가능한 한 메모리에 유지해, 단계마다 디스크에 쓰고 읽는 MapReduce의 반복적인 디스크 I/O를 피하기 때문이다. 특히 ML 같은 반복 작업에서 차이가 크다.
</details>

**Q3.** Spark 컴포넌트 스택의 세 계층은?
<details><summary>정답</summary>

Structured Streaming, MLlib, Spark SQL, pandas API on Spark, GraphX 같은 라이브러리가 DataFrame API 위에 있고, DataFrame API는 스케줄링, 메모리 관리, 장애 복구, RDD를 담당하는 Spark Core 위에서 실행된다.
</details>

**Q4.** Spark 4에서 ANSI 모드가 기본이 되면서 달라진 점은?
<details><summary>정답</summary>

잘못된 형변환(CAST('NA' AS INT))이나 정수 오버플로가 NULL을 조용히 반환하거나 값이 돌아가는 대신 오류를 낸다. 잘못된 값을 NULL로 바꾸려면 try_cast()를 쓴다.
</details>

**Q5.** VARIANT 타입은 무엇을 해결하는가?
<details><summary>정답</summary>

JSON 같은 반정형 데이터를 문자열로 저장하고 쿼리마다 파싱하던 방식을 대체해, 파싱된 구조를 최적화된 이진 형식으로 저장한다. 경로 표현식으로 더 편하고 빠르게 조회할 수 있다.
</details>

**Q6.** Driver, Cluster Manager, Worker, Executor의 역할은?
<details><summary>정답</summary>

Driver는 SparkSession을 만들고 DAG를 구성해 태스크를 스케줄링하고 모니터링하며 결과를 반환한다. Cluster Manager는 리소스를 할당한다. Worker는 Executor를 호스팅하는 노드이고, Executor는 태스크를 실행하는 프로세스로 코어와 메모리, 캐시를 가진다.
</details>

**Q7.** Driver는 태스크를 Cluster Manager를 거쳐 보내는가?
<details><summary>정답</summary>

아니다. Driver는 Cluster Manager와 협상해 Executor 리소스를 확보한 뒤, Executor와 직접 통신해 태스크를 스케줄링하고 결과를 받는다.
</details>

**Q8.** Spark Connect란 무엇이며 Databricks에서 어디에 쓰이는가?
<details><summary>정답</summary>

애플리케이션은 얇은 클라이언트만 갖고 Driver는 원격에서 gRPC로 쿼리 계획을 받는 클라이언트-서버 구조다. Databricks 서버리스 compute, 표준 액세스 클러스터, Databricks Connect가 이 방식이며, DataFrame 코드는 그대로 쓸 수 있다.
</details>

**Q9.** SparkSession은 무엇이며 Databricks 노트북에서 어떻게 쓰는가?
<details><summary>정답</summary>

Spark 2.0에서 도입된 모든 Spark 기능의 통합 진입점으로, SparkContext, SQLContext, HiveContext, StreamingContext를 대체한다. Databricks 노트북에서는 spark 변수로 이미 생성되어 있고, getOrCreate()는 기존 세션을 돌려준다.
</details>

**Q10.** 서버리스나 표준 액세스 compute에서 `sc`와 RDD API를 쓸 수 없는 이유는?
<details><summary>정답</summary>

이들은 Spark Connect를 사용해 클라이언트가 Driver의 JVM에 직접 접근하지 못하기 때문이다. SparkContext와 RDD API는 전용 액세스 클래식 클러스터에서만 쓸 수 있다.
</details>

**Q11.** Application, Job, Stage, Task의 관계는?
<details><summary>정답</summary>

애플리케이션은 액션마다 Job을 실행하고, Job은 셔플 경계에서 Stage로 나뉘며, Stage는 파티션마다 하나씩 Task를 병렬로 실행한다.
</details>

**Q12.** 스테이지 경계는 어디서 생기며 그 이유는?
<details><summary>정답</summary>

데이터를 클러스터 전체에 재분배해야 하는 셔플 지점에서 생긴다. 키별 집계나 조인은 같은 키의 행이 한 곳에 모여야 하므로 교환이 끝난 뒤 새 스테이지가 시작된다. 필터나 프로젝션은 한 스테이지에 파이프라인화된다.
</details>

**Q13.** DAG의 세 단어(Directed, Acyclic, Graph)는 각각 무엇을 뜻하는가?
<details><summary>정답</summary>

계산이 스테이지를 따라 한 방향으로 흐르고(Directed), 되돌아가지 않아 작업이 반드시 끝나며(Acyclic), 스테이지가 의존성 그래프로 구성된다(Graph)는 뜻이다.
</details>

**Q14.** Spark UI의 Jobs, Executors, Storage 화면은 각각 무엇을 보여 주며, 서버리스에서는?
<details><summary>정답</summary>

Jobs는 잡과 스테이지 DAG, Executors는 Executor별 코어와 메모리 사용, Storage는 캐시된 DataFrame과 메모리 사용을 보여 준다. 서버리스 compute에서는 Spark UI가 노출되지 않는다.
</details>

**Q15.** Databricks의 compute 유형 네 가지는?
<details><summary>정답</summary>

대화형 범용 클러스터, 작업 실행 시에만 뜨는 작업 클러스터, SQL에 최적화된 SQL 웨어하우스, Databricks가 운영하는 완전 관리형 서버리스다.
</details>

**Q16.** DataFrame의 정의와 주요 성질은?
<details><summary>정답</summary>

같은 스키마를 가진 레코드의 분산 컬렉션이다. 하나의 테이블처럼 보이지만 실제로는 Executor들에 파티션으로 나뉘어 있고, 불변이며, 지연 평가되고, 리니지로 내결함성을 가진다.
</details>

**Q17.** 변환과 액션의 차이와 예는?
<details><summary>정답</summary>

변환은 새 DataFrame을 만들며 무엇을 계산할지만 기술하고 실행하지 않는다(select, filter, withColumn, groupBy, join). 액션은 실제 계산을 트리거한다(count, show, take, collect, write, display).
</details>

**Q18.** 지연 평가가 주는 두 가지 이점은?
<details><summary>정답</summary>

실행 전에 전체 변환 체인을 보므로 옵티마이저가 필터를 스캔 단계로 내리는 등 연산을 재배열하고 불필요한 작업을 없앨 수 있고, 기록된 리니지로 잃어버린 파티션을 다시 계산해 내결함성을 얻는다.
</details>

**Q19.** SQL의 WHERE와 같은 DataFrame 메서드는?
<details><summary>정답</summary>

filter()이며 별칭 where()도 같다. select()는 SELECT 컬럼 목록, groupBy()는 GROUP BY, orderBy()/sort()는 ORDER BY에 해당한다.
</details>

**Q20.** 쿼리 계획 단계를 순서대로 나열하고 논리 최적화의 예를 들어라.
<details><summary>정답</summary>

미해결 논리 계획 → 분석(카탈로그로 이름과 타입 해석) → 논리 최적화 → 물리 계획 → 코드 생성 순이다. 논리 최적화에는 조건자 푸시다운, 컬럼 가지치기, 상수 폴딩이 있다.
</details>

**Q21.** 같은 의미의 Spark SQL 쿼리와 DataFrame 코드는 성능이 다른가?
<details><summary>정답</summary>

다르지 않다. 둘 다 같은 Catalyst 옵티마이저와 실행 엔진을 거쳐 같은 물리 계획으로 컴파일되며, explain()으로 확인할 수 있다. 더 명확한 쪽을 쓰면 된다.
</details>

**Q22.** AQE는 무엇을 하는가?
<details><summary>정답</summary>

실행 중 실제 셔플 출력 통계를 보고 계획을 재조정한다. 작은 셔플 파티션을 병합하고, 더 나은 조인 전략을 고르며, 치우친 파티션을 나눈다. Spark 4에서는 기본으로 켜져 있다.
</details>

**Q23.** Photon은 무엇이며 어디서 기본으로 동작하는가?
<details><summary>정답</summary>

행이 아닌 배치 단위로 처리하는 Databricks 네이티브 벡터화 쿼리 엔진이다. SQL 웨어하우스와 서버리스에서 기본으로 동작하고, 범용과 작업 클러스터에서는 선택해 켤 수 있다.
</details>

**Q24.** 컬럼형 저장이 분석에 유리한 이유는?
<details><summary>정답</summary>

같은 컬럼 값이 함께 저장되어 쿼리에 필요한 컬럼 블록만 읽고 나머지를 건너뛸 수 있고, 같은 타입의 비슷한 값이 모여 압축 효율이 높기 때문이다.
</details>

**Q25.** DataFrameReader와 DataFrameWriter는 어떻게 접근하는가?
<details><summary>정답</summary>

DataFrameReader는 spark.read로 파일, 테이블, 외부 소스를 DataFrame으로 읽고, DataFrameWriter는 df.write로 DataFrame을 파일이나 테이블에 쓴다.
</details>

**Q26.** Parquet로 저장한 결과가 파일 하나가 아니라 디렉터리인 이유는?
<details><summary>정답</summary>

각 Executor가 자기 파티션을 병렬로 써서 파티션마다 파트 파일(part-00000 등)이 생기기 때문이다.
</details>

**Q27.** 저장 모드 네 가지는?
<details><summary>정답</summary>

대상이 있으면 오류를 내는 error(기본), 덮어쓰는 overwrite, 추가하는 append, 대상이 있으면 아무것도 하지 않는 ignore다.
</details>

**Q28.** 스키마를 명시하는 두 가지 방법과 추론 대신 명시해야 하는 이유는?
<details><summary>정답</summary>

StructType/StructField 객체와 "name STRING, age INT" 같은 DDL 문자열이다. 추론은 데이터를 한 번 더 읽어야 하고, 우편번호를 정수로 읽는 것처럼 잘못 추론할 수 있어 운영 파이프라인에서는 명시한다.
</details>

**Q29.** Parquet 파일을 다시 읽을 때 스키마 추론이 필요 없는 이유는?
<details><summary>정답</summary>

Parquet와 Delta는 스키마가 파일에 함께 저장된 자기 기술 형식이기 때문이다.
</details>

**Q30.** DataFrame을 SQL로 조회하려면?
<details><summary>정답</summary>

createOrReplaceTempView()로 임시 뷰로 등록한 뒤 spark.sql()로 조회한다. 결과도 DataFrame이라 SQL과 DataFrame 코드를 섞어 쓸 수 있다.
</details>

**Q31.** 메타스토어의 역할과 Databricks에서의 구현은?
<details><summary>정답</summary>

테이블 스키마와 위치, 파티션, 테이블과 뷰와 함수의 속성을 관리한다. Databricks에서는 계정 수준의 Unity Catalog가 이 역할을 하며 catalog.schema.table 네임스페이스와 세분화된 거버넌스를 제공한다.
</details>

**Q32.** 공유 없는 아키텍처의 네 가지 특성은?
<details><summary>정답</summary>

각 노드가 자기 자원을 관리하는 독립성, 경합 없이 노드를 추가하는 확장성, 장애를 노드에 격리하는 내결함성, 데이터와 작업을 노드별로 나누는 리소스 파티셔닝이다.
</details>

**Q33.** 파티션과 태스크의 관계, 그리고 병렬도를 결정하는 것은?
<details><summary>정답</summary>

파티션 하나를 태스크 하나가 처리한다. 파티션의 수와 크기가 병렬로 실행할 수 있는 태스크 수, 즉 병렬도를 결정한다.
</details>

**Q34.** 셔플이 Spark에서 가장 비싼 연산인 이유는?
<details><summary>정답</summary>

모든 맵 쪽 파티션이 모든 키를 가질 수 있어 리듀스 쪽 파티션마다 모든 맵 파티션에서 데이터를 받아야 하는 전체 대 전체 교환이고, 데이터를 직렬화해 디스크에 쓰고 네트워크로 보내 역직렬화해야 하기 때문이다.
</details>

**Q35.** 좁은 변환과 넓은 변환의 차이와 예는?
<details><summary>정답</summary>

좁은 변환은 출력 파티션이 입력 파티션 하나에만 의존해 셔플이 없다(select, filter, withColumn). 넓은 변환은 여러 입력 파티션에 의존해 셔플과 스테이지 경계를 만든다(groupBy, join, distinct, orderBy).
</details>

**Q36.** MapReduce 관점에서 맵 단계에서 일찍 필터링하는 것이 유리한 이유는?
<details><summary>정답</summary>

셔플되어 네트워크로 이동할 데이터 양을 줄여, 가장 비싼 셔플과 그 이후의 모든 처리를 줄이기 때문이다.
</details>

**Q37.** groupBy, join, filter를 Map/Shuffle/Reduce로 나누어 설명하라.
<details><summary>정답</summary>

groupBy는 키 추출(Map), 키 기준 재분배(Shuffle), 집계(Reduce)다. join은 키 준비, 일치하는 키를 같은 위치로 모으기, 행 결합이다. filter는 조건 평가만 하고 셔플과 리듀스가 필요 없다.
</details>

**Q38.** 문자열 대신 col() Column 객체를 써야 하는 경우는?
<details><summary>정답</summary>

alias(), cast(), try_cast(), asc(), desc() 같은 컬럼 메서드를 쓰거나 비교와 산술이 들어간 복잡한 표현식을 만들 때다. 단순 선택은 문자열로 충분하다.
</details>

**Q39.** 여러 조건을 filter에 걸 때 주의할 문법은?
<details><summary>정답</summary>

Python의 and, or 대신 &, |, ~를 쓰고, 연산자 우선순위 때문에 각 조건을 괄호로 감싼다. 예: (col("amt") > 50) & (col("region") == "US").
</details>

**Q40.** count(col)과 count("*")의 차이는?
<details><summary>정답</summary>

count(col)은 해당 컬럼의 NULL이 아닌 값만 세고, count("*")는 NULL 여부와 상관없이 전체 행을 센다.
</details>

**Q41.** 결측값을 처리하는 DataFrame 메서드는?
<details><summary>정답</summary>

NULL 행을 지우는 dropna()/na.drop()(subset, how 지정 가능)과 NULL을 값으로 바꾸는 fillna()/na.fill()(컬럼별 딕셔너리 가능)이다.
</details>

**Q42.** 품질을 모르는 문자열 컬럼을 정수로 바꿀 때 cast 대신 try_cast를 쓰는 이유는?
<details><summary>정답</summary>

ANSI 모드에서는 cast가 'NA' 같은 값에서 오류를 내 작업 전체가 실패한다. try_cast는 잘못된 값을 NULL로 바꿔 COUNT_IF로 찾아내고 삭제하거나 대체하는 등 처리할 수 있게 한다.
</details>

**Q43.** 일반 Python UDF의 성능 문제와 Pandas UDF가 나은 이유는?
<details><summary>정답</summary>

일반 UDF는 Catalyst가 최적화할 수 없고 JVM과 Python 사이를 행 단위로 직렬화한다. Pandas UDF는 행 배치를 pandas Series로 받아 Apache Arrow로 주고받아 직렬화 오버헤드가 훨씬 작다. 다만 Catalyst에는 여전히 불투명하므로 내장 함수가 있으면 그것이 가장 빠르다.
</details>

**Q44.** Pandas UDF 안에서 Series의 평균과 표준편차로 z-점수를 계산하면 어떤 문제가 있는가?
<details><summary>정답</summary>

Pandas UDF가 받는 Series는 전체 컬럼이 아니라 한 배치이므로, 배치마다 다른 평균과 표준편차로 정규화되어 결과가 일관되지 않는다. 전체 기준이 필요하면 내장 집계나 윈도우 함수로 전체 통계를 먼저 구한다.
</details>

**Q45.** ETL 파이프라인을 하나의 체인으로 작성할 수 있는 이유는?
<details><summary>정답</summary>

변환이 지연 평가되고 매번 새 DataFrame을 반환하므로, 읽기부터 정제와 보강까지 변환을 이어 붙이고 마지막 쓰기 액션 하나로 실행할 수 있기 때문이다.
</details>

**Q46.** display()의 특징과 제한은?
<details><summary>정답</summary>

Databricks 노트북 함수로 액션을 트리거해 결과를 표로 보여 준다. 시각 확인용이라 최대 10,000행 또는 2MB 중 작은 쪽까지만 표시하며, 정렬, 필터, CSV 다운로드, 데이터 프로필, 시각화를 지원한다.
</details>

---

## 26. 참고 링크

- Apache Spark 문서: https://spark.apache.org/docs/latest/
- PySpark DataFrame API: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html
- PySpark Column API: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html
- PySpark 함수: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html
- Spark SQL 함수: https://spark.apache.org/docs/latest/api/sql/index.html
- PySpark 데이터 타입: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/data_types.html
- Spark SQL ANSI 모드: https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html
- Spark Connect: https://spark.apache.org/docs/latest/spark-connect-overview.html
- Spark 클러스터 모드 개요: https://spark.apache.org/docs/latest/cluster-overview.html
- Spark Web UI: https://spark.apache.org/docs/latest/web-ui.html
- Databricks의 Apache Spark: https://docs.databricks.com/aws/en/spark/
- Databricks PySpark 개요: https://docs.databricks.com/aws/en/pyspark/
- DataFrame 튜토리얼: https://docs.databricks.com/aws/en/getting-started/dataframes
- Adaptive Query Execution: https://docs.databricks.com/aws/en/optimizations/aqe
- Photon: https://docs.databricks.com/aws/en/compute/photon
- Compute 개요: https://docs.databricks.com/aws/en/compute/
- 액세스 모드 제한: https://docs.databricks.com/aws/en/compute/access-mode-limitations
- 사용자 정의 함수: https://docs.databricks.com/aws/en/udf/
- pandas UDF: https://docs.databricks.com/aws/en/udf/pandas
- VARIANT 타입: https://docs.databricks.com/aws/en/semi-structured/variant
- Unity Catalog: https://docs.databricks.com/aws/en/data-governance/unity-catalog/
