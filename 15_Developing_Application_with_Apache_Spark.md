# Apache Spark로 애플리케이션 개발하기 — 핵심 개념 정리

---

## 목차

1. [DataFrame 변환 도구 복습](#1-dataframe-변환-도구-복습)
2. [예제 ETL: 거래 데이터 검증과 보강](#2-예제-etl-거래-데이터-검증과-보강)
3. [groupBy의 동작 원리](#3-groupby의-동작-원리)
4. [집계 메서드와 agg()](#4-집계-메서드와-agg)
5. [윈도우 함수](#5-윈도우-함수)
6. [그룹화와 집계 예제](#6-그룹화와-집계-예제)
7. [조인 유형과 조인 조건](#7-조인-유형과-조인-조건)
8. [조인 예제: 프로젝션, 표현식 조인, 고아 레코드](#8-조인-예제-프로젝션-표현식-조인-고아-레코드)
9. [집합 연산](#9-집합-연산)
10. [조인 성능](#10-조인-성능)
11. [복합 데이터 타입: 배열, 구조체, 맵](#11-복합-데이터-타입-배열-구조체-맵)
12. [JSON 문자열을 구조체로 변환](#12-json-문자열을-구조체로-변환)
13. [구조체 필드 접근과 평탄화](#13-구조체-필드-접근과-평탄화)
14. [배열 함수와 explode](#14-배열-함수와-explode)
15. [컬렉션 집계와 pivot](#15-컬렉션-집계와-pivot)
16. [VARIANT 타입](#16-variant-타입)
17. [복합 데이터 성능 모범 사례](#17-복합-데이터-성능-모범-사례)
18. [핵심 정리 치트시트](#18-핵심-정리-치트시트)
19. [자가 점검 문제](#19-자가-점검-문제)
20. [참고 링크](#20-참고-링크)

---

## 1. DataFrame 변환 도구 복습

### 1-1. 변환 메서드와 SQL 대응

- DataFrame 연산은 모든 파티션에 자동으로 분산되어 병렬로 실행된다.
- 모든 변환에는 대응하는 SQL 연산이 있다.
- 변환은 기존 DataFrame을 바꾸지 않고 **새 DataFrame을 반환**한다(불변).
- 변환은 논리 계획에 쌓이기만 하고, **액션**(`display()`, `count()`, `write` 등)이 실행을 트리거한다. 그때 계획이 최적화되어 모든 파티션에서 실행된다.

| DataFrame 메서드 | SQL | 설명 |
|---|---|---|
| `select()` | `SELECT` | 컬럼 선택 |
| `filter()`, `where()` | `WHERE` | 조건으로 행 필터 |
| `groupBy()` | `GROUP BY` | 집계를 위한 그룹화 |
| `orderBy()`, `sort()` | `ORDER BY` | 정렬 |
| `join()` | `JOIN` | 키로 결합 |

### 1-2. 컬럼 참조

| 방법 | 예 | 용도 |
|---|---|---|
| 문자열 | `df.select("name")` | 기본 선택 |
| 속성 | `df.select(df.first_name)` | 이름이 유효한 Python 식별자일 때 |
| 인덱스 표현식 | `df.select(df["first-name"])` | 모든 이름, 조인 시 어느 DataFrame의 컬럼인지 지정 |
| Column 객체 | `col("name").alias("customer_name")` | 복잡한 표현식, `alias`, `cast`, `asc`, `desc` |
| SQL 표현식 | `df.selectExpr("totalPrice >= 100 AS isLargeSale")` | SQL 문법 그대로 |

주요 Column 메서드: `alias()`, `cast()`/`astype()`, `try_cast()`, `isNull()`/`isNotNull()`, `contains()`, `asc()`/`desc()`.

### 1-3. 내장 함수, 결측값, ANSI 모드, UDF

- 내장 함수(`round`, `concat`, `date_format`, `regexp_replace`, `coalesce`, `upper`, `right` 등)는 같은 이름의 SQL 함수가 있으며 옵티마이저가 이해하므로 **가장 빠르다**.
- 결측값: `isNull()`/`isNotNull()`, `count(col)`(NULL 제외), `fillna()`/`na.fill()`, `dropna()`/`na.drop()`.
- Spark 4는 ANSI 모드가 기본이라 `CAST('NA' AS INT)`와 정수 오버플로가 **오류**를 낸다. 품질을 모르는 데이터는 `try_cast()`로 잘못된 값을 NULL로 바꿔 처리한다.

| UDF 종류 | 특징 |
|---|---|
| Python UDF (`@udf`) | Catalyst가 최적화 못 함, 행 단위 JVM-Python 직렬화 |
| Arrow UDF (`@udf(..., useArrow=True)`) | Spark 4. Arrow 직렬화로 가속 |
| Pandas UDF (`@pandas_udf`) | 행 배치(pandas Series) 단위, Arrow로 교환. 일반 UDF보다 빠르지만 여전히 옵티마이저에 불투명 |

선택 순서: **내장 함수 → Pandas/Arrow UDF → 일반 Python UDF**.

---

## 2. 예제 ETL: 거래 데이터 검증과 보강

`samples.bakehouse.sales_transactions`를 검증하고 보강해 `transactions_enriched` 테이블을 만든다.

| 대상 컬럼 | 만드는 방법 |
|---|---|
| `paymentMethod` | 대문자로 정규화 |
| `cardNumberMasked` | 마지막 4자리만 남기고 마스킹 (`cardNumber` 제거) |
| `orderSize` | `quantity`로 Bulk / Large / Medium / Small |

### 2-1. 검증

```python
from pyspark.sql.functions import col, sum, when

df = spark.read.table("samples.bakehouse.sales_transactions")
df.printSchema()      # 테이블은 UC 메타데이터의 스키마를 쓰므로 추론 불필요

# 컬럼별 NULL 수: DataFrame 방식 (SQL의 COUNT_IF와 같은 물리 계획)
null_counts = df.select(
    sum(when(col("quantity").isNull(), 1).otherwise(0)).alias("null_quantity"),
    sum(when(col("cardNumber").isNull(), 1).otherwise(0)).alias("null_card"))

# 내부 일관성: 합계 = 수량 x 단가
valid_df = (df.filter(col("quantity") > 0)
              .filter(col("totalPrice") == col("quantity") * col("unitPrice")))
```

카드 번호가 `BIGINT`로 저장된 것처럼, 식별자를 숫자로 저장한 것은 데이터 품질 문제의 신호다(앞자리 0 손실 등).

### 2-2. 전체 파이프라인

```python
from pyspark.sql.functions import col, concat, lit, right, upper, when

(spark.read.table("samples.bakehouse.sales_transactions")
    .select("transactionID", "dateTime", "product", "quantity",
            "unitPrice", "totalPrice", "paymentMethod", "cardNumber")   # 필요한 컬럼만 먼저
    .filter(col("quantity") > 0)
    .filter(col("totalPrice") == col("quantity") * col("unitPrice"))    # 일관된 행만
    .withColumn("paymentMethod", upper(col("paymentMethod")))
    .withColumn("cardNumberMasked",
                concat(lit("****-"), right(col("cardNumber").cast("string"), lit(4))))
    .drop("cardNumber")
    .withColumn("orderSize",
                when(col("quantity") >= 24, "Bulk")
                .when(col("quantity") >= 12, "Large")
                .when(col("quantity") >= 6, "Medium")
                .otherwise("Small"))
    .drop("unitPrice")
    .write.mode("overwrite").saveAsTable("transactions_enriched"))
```

| 원칙 | 내용 |
|---|---|
| 검증 | 필요한 컬럼만 일찍 선택하고, 보강 전에 NULL과 내부 일관성 확인 |
| 보강 | 비즈니스 요구에 맞는 파생 컬럼, 내장 함수 우선 |
| 구성 | 변환은 지연 평가되고 새 DataFrame을 반환하므로 **하나의 체인 + 단일 쓰기 액션**으로 표현 |
| 시각화 | Spark 4는 `df.groupBy("orderSize").count().plot.bar(x="orderSize", y="count")`처럼 DataFrame에서 바로 그래프 |

> 주의: Pandas UDF 안에서 `series.mean()`, `series.std()`로 z-점수를 구하면 Series가 전체 컬럼이 아니라 **한 배치**라서 배치마다 기준이 달라진다. 전체 기준 정규화는 내장 집계나 윈도우 함수로 전체 통계를 먼저 구한다.

---

## 3. groupBy의 동작 원리

- `groupBy()`는 그룹화 컬럼을 기준으로 데이터를 노드에 다시 나눈다(**셔플**). 같은 키의 행이 한 Executor에 모여야 그 그룹을 완전히 집계할 수 있기 때문이다.
- 집계는 파티션별로 병렬 실행된다. Spark는 셔플 전에 파티션 안에서 부분 집계를 먼저 해 이동량을 줄인다.
- `groupBy()`는 **`GroupedData` 객체**를 반환한다. `count()`, `sum()`, `agg()` 같은 집계를 붙여야 DataFrame이 되며, 여전히 액션 전까지 실행되지 않는다(지연 평가).
- 여러 컬럼과 컬럼 표현식으로 그룹화할 수 있다.

```python
from pyspark.sql.functions import col, year

df.groupBy("department").count()
df.groupBy("department", "location").avg("salary")
df.groupBy(col("department"), year("hire_date")).sum("revenue")
```

Spark UI에서 보면 그룹화 키로 행을 재분배하는 **교환(exchange)** 이 두 스테이지 사이에 나타난다.

---

## 4. 집계 메서드와 agg()

### 4-1. 단축 집계 메서드

| 메서드 | SQL | 설명 |
|---|---|---|
| `count()` | `COUNT(*)` | 그룹별 행 수 |
| `sum(col)` | `SUM(col)` | 합계 |
| `avg(col)` / `mean(col)` | `AVG(col)` | 평균 |
| `min(col)` / `max(col)` | `MIN` / `MAX` | 최솟값 / 최댓값 |

단축 메서드는 `sum(salary)` 같은 자동 이름을 만든다. 이후 단계에서 참조하기 불편하다.

### 4-2. agg()로 여러 집계를 한 번에

```python
from pyspark.sql.functions import sum, avg, count, round, desc

# 함수 형식: 별칭 지정 가능 (권장)
summary_df = (df.groupBy("paymentMethod")
    .agg(round(sum("totalPrice"), 2).alias("total_revenue"),
         round(avg("totalPrice"), 2).alias("avg_transaction_value"),
         count("*").alias("transaction_count"))
    .orderBy(desc("total_revenue")))

# 딕셔너리 형식: 간결하지만 별칭 불가 → avg(trip_distance) 같은 이름
df.groupBy("pickup_zip").agg({"trip_distance": "avg", "fare_amount": "sum"})
```

- `agg()`는 **한 번의 셔플과 집계 패스**로 여러 지표를 계산한다.
- 집계가 하나뿐이어도 **별칭을 주려면** `agg()`를 쓴다.
- 집계 메서드를 `groupBy(...).sum(...).avg(...)`처럼 연달아 붙일 수는 없다.

| SQL | DataFrame |
|---|---|
| `SELECT k, COUNT(*) AS n FROM t GROUP BY k` | `df.groupBy("k").agg(count("*").alias("n"))` |
| `... HAVING SUM(x) > 100` | `.agg(sum("x").alias("s")).filter(col("s") > 100)` |

---

## 5. 윈도우 함수

### 5-1. groupBy와의 차이

| `groupBy()` | 윈도우 함수 |
|---|---|
| 각 그룹을 **한 행으로 축소** | 모든 행을 **유지**하고 계산 결과를 컬럼으로 추가 |
| 그룹 요약이 목적 | 순위, 누적 합계, 이동 평균, 이전/다음 행 비교, 행 값과 그룹 집계 비교 |

개별 행이 여전히 필요하면 윈도우 함수를 쓴다.

### 5-2. 윈도우 사양

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, dense_rank, row_number, ntile, sum, avg, lag, lead

w = Window.partitionBy("department").orderBy("salary")

(df.withColumn("rank", rank().over(w))
   .withColumn("running_total", sum("salary").over(w))
   .withColumn("prev_salary", lag("salary").over(w))     # 이전 행 값
   .withColumn("next_salary", lead("salary").over(w)))   # 다음 행 값
```

| 구성 | 역할 |
|---|---|
| `partitionBy()` | 그룹을 나눈다. 그룹이 바뀌면 계산이 다시 시작 |
| `orderBy()` | 윈도우 안의 정렬 |
| `rowsBetween()` / `rangeBetween()` | 계산에 쓸 행 범위(프레임) |

| 함수 | 동작 |
|---|---|
| `row_number()` | 동점이어도 고유 번호 (1, 2, 3, 4) |
| `rank()` | 동점은 같은 순위, 다음 순위 건너뜀 (1, 2, 2, 4) |
| `dense_rank()` | 동점은 같은 순위, 건너뛰지 않음 (1, 2, 2, 3) |
| `ntile(n)` | n개 버킷으로 나눔 (예: 5분위) |
| `lag(col, n)` / `lead(col, n)` | n행 앞/뒤 값 (기간 비교) |
| `sum`, `avg` 등 `.over(w)` | 윈도우 집계 |

> 주의
> - `orderBy`가 있는 윈도우의 기본 프레임은 "처음부터 현재 행의 **값**까지"(`RANGE`)라서, 정렬 값이 같은 행은 누적 합계에 한꺼번에 포함된다. 행 단위 누적이 필요하면 `w.rowsBetween(Window.unboundedPreceding, Window.currentRow)`처럼 명시한다.
> - `partitionBy` 없이 `orderBy`만 있는 윈도우는 **모든 행을 하나의 파티션으로 모은다**(Spark가 경고를 남긴다). 이미 집계된 작은 데이터에는 괜찮지만 원시 데이터에는 위험하다.

### 5-3. 그룹 집계와 행 비교

```python
from pyspark.sql.functions import col, round, avg

# orderBy 없는 윈도우: 각 행이 자기 버킷 전체의 평균을 참조
quintile_w = Window.partitionBy("revenue_quintile")
df.withColumn("quintile_avg", round(avg("total_fares").over(quintile_w), 2)) \
  .withColumn("vs_quintile_avg", round(col("total_fares") - col("quintile_avg"), 2))
```

---

## 6. 그룹화와 집계 예제

NYC 택시 데이터(`samples.nyctaxi.trips`)로 승차 우편번호별 요약과 순위를 만든다.

```python
from pyspark.sql.functions import avg, sum, count, round, desc, rank, ntile
from pyspark.sql.window import Window

trips_df = spark.read.table("samples.nyctaxi.trips")

# 1) 그룹 요약
zip_summary_df = (trips_df.groupBy("pickup_zip")
    .agg(count("*").alias("trip_count"),
         round(avg("trip_distance"), 2).alias("avg_distance"),
         round(sum("fare_amount"), 2).alias("total_fares"),
         round(avg("fare_amount"), 2).alias("avg_fare"))
    .orderBy(desc("total_fares")))

# 2) 집계 결과에 순위와 5분위 (집계 후라 데이터가 작아 전역 윈도우도 무방)
fares_w = Window.orderBy(desc("total_fares"))
ranked_df = (zip_summary_df
    .withColumn("revenue_rank", rank().over(fares_w))
    .withColumn("revenue_quintile", ntile(5).over(fares_w)))
```

같은 내용을 SQL로:

```sql
SELECT
  pickup_zip,
  ROUND(SUM(fare_amount), 2) AS total_fares,
  RANK() OVER (ORDER BY SUM(fare_amount) DESC) AS revenue_rank
FROM trips
GROUP BY pickup_zip
ORDER BY revenue_rank
LIMIT 10;
```

SQL에서는 집계 결과(`SUM(fare_amount)`)에 바로 윈도우를 걸 수 있다. 모든 DataFrame 집계는 같은 엔진에서 실행되는 SQL의 `GROUP BY`나 `OVER`에 대응한다.

거래 데이터 예:

```python
# 제품별 거래 수
df.groupBy("product").count().orderBy(desc("count"))

# 매출 기준 제품 순위 (groupBy로 줄인 뒤 rank로 주석)
product_rev = df.groupBy("product").agg(sum("totalPrice").alias("product_revenue"))
product_rev.withColumn("revenue_rank", rank().over(Window.orderBy(desc("product_revenue"))))
```

---

## 7. 조인 유형과 조인 조건

### 7-1. 조인 유형

| `how=` | 반환 |
|---|---|
| `inner` (기본) | 양쪽에서 키가 일치하는 행 |
| `left` (`left_outer`) | 왼쪽 전체 + 오른쪽 일치, 없으면 NULL |
| `right` (`right_outer`) | 오른쪽 전체 + 왼쪽 일치, 없으면 NULL |
| `full_outer` (`outer`, `full`) | 양쪽 전체, 불일치는 NULL |
| `left_semi` | 오른쪽에 일치가 **있는** 왼쪽 행 (왼쪽 컬럼만) |
| `left_anti` | 오른쪽에 일치가 **없는** 왼쪽 행 (왼쪽 컬럼만) |
| `cross` | 모든 행 조합 (카테시안 곱, 주의) |

`left_semi`는 SQL의 `WHERE EXISTS`/`IN`, `left_anti`는 `WHERE NOT EXISTS`/`NOT IN`에 해당한다. 조인 결과의 행 수가 왼쪽보다 늘지 않는다.

### 7-2. 조인 조건 지정 방식

```python
from pyspark.sql.functions import col

# 1) 같은 이름의 키: 문자열(또는 리스트) → 결과에 키 컬럼 하나만 남음
df1.join(df2, "user_id")
df1.join(df2, ["user_id", "region"], "left")

# 2) 다른 이름이거나 복잡한 조건: 표현식 → 양쪽 키 컬럼이 모두 남음
df1.join(df2, on=df1.user_id == df2.uid, how="left")

# 3) 별칭으로 여러 조건
users.alias("u").join(
    orders.alias("o"),
    [col("u.id") == col("o.user_id"), col("u.region") == col("o.region")],
    "inner")
```

컬럼 이름 충돌은 조인 **전에** `alias()`나 `withColumnRenamed()`로 바꾸거나 DataFrame 별칭으로 구분한다. 표현식 조인 후에는 중복 키 컬럼을 `drop()`하거나 필요한 컬럼만 선택한다.

---

## 8. 조인 예제: 프로젝션, 표현식 조인, 고아 레코드

Bakehouse 데이터: 고객이 프랜차이즈에서 구매하고(`transactions`의 `customerID`, `franchiseID`), 프랜차이즈는 공급업체에서 재료를 산다(`franchises.supplierID`).

### 8-1. 조인 전 프로젝션

```python
from pyspark.sql.functions import col, sum, round, desc

franchise_locations_df = franchises_df.select(
    "franchiseID",
    col("name").alias("franchise_name"),
    col("city").alias("franchise_city"),
    col("country").alias("franchise_country"))

sales_by_location_df = (transactions_df
    .select("transactionID", "franchiseID", "product", "totalPrice")
    .join(franchise_locations_df, on="franchiseID", how="inner"))

sales_by_location_df.groupBy("franchise_city", "franchise_country") \
    .agg(round(sum("totalPrice"), 2).alias("total_revenue")) \
    .orderBy(desc("total_revenue"))
```

조인 전에 필요한 컬럼만 고르고 이름을 바꾸면 **셔플할 데이터가 줄고 모호한 컬럼 참조도 피한다.** 비용이 거의 없는 확실한 최적화다.

### 8-2. 고아 레코드 찾기 (참조 무결성 점검)

```python
suppliers_renamed_df = (suppliers_df
    .withColumnRenamed("name", "supplier_name")
    .withColumnRenamed("city", "supplier_city"))

audit_df = (franchises_df.select("franchiseID", col("name").alias("franchise_name"), "supplierID")
    .join(suppliers_renamed_df.select("supplierID", "supplier_name"),
          on=franchises_df.supplierID == suppliers_renamed_df.supplierID,
          how="full_outer"))

orphaned_df = audit_df.filter(col("franchise_name").isNull() | col("supplier_name").isNull())
```

| 결과 | 의미 |
|---|---|
| `supplier_name`이 NULL | 존재하지 않는 공급업체를 참조하는 프랜차이즈 |
| `franchise_name`이 NULL | 어떤 프랜차이즈도 사용하지 않는 공급업체 |

외부 조인 + `isNull()` 필터는 고아 레코드를 찾는 표준 패턴이다. 한쪽만 보려면 `left_anti`가 더 간단하다.

```sql
SELECT f.franchiseID, f.name AS franchise_name, s.supplierID, s.name AS supplier_name
FROM franchises f
FULL OUTER JOIN suppliers s ON f.supplierID = s.supplierID
WHERE f.name IS NULL OR s.name IS NULL;
```

---

## 9. 집합 연산

`A = [1, 2, 3]`, `B = [2, 3, 4]`일 때:

| 연산 | 의미 | 결과 |
|---|---|---|
| 합집합 | 두 집합의 모든 요소 | `[1, 2, 3, 4]` |
| 교집합 | 둘 다에 있는 요소 | `[2, 3]` |
| 차집합 | A에만 있는 요소 | `[1]` |

| 메서드 | 설명 | 중복 |
|---|---|---|
| `union()` | 행 결합, **컬럼 위치** 기준 | 유지 (SQL `UNION ALL`) |
| `unionByName()` | **컬럼 이름** 기준 결합 | 유지 |
| `intersect()` | 양쪽 모두에 있는 행 | 제거 |
| `intersectAll()` | 교집합 | 유지 |
| `subtract()` | 첫째에만 있는 행 (SQL `EXCEPT DISTINCT`) | 제거 |
| `exceptAll()` | 차집합 | 유지 |

- 집합 연산은 행 전체를 비교하며 **스키마(컬럼 수와 타입)가 맞아야** 한다.
- `union()`은 위치로 맞추므로 컬럼 순서가 다르면 엉뚱한 컬럼끼리 **조용히** 합쳐진다. 순서가 다를 수 있으면 `unionByName()`을 쓴다. 한쪽에 없는 컬럼을 NULL로 채우려면 `unionByName(df2, allowMissingColumns=True)`.
- SQL `UNION`처럼 중복 없는 합집합은 `union().distinct()`.

```python
customer_cities = customers_df.select("city").distinct()
franchise_cities = franchises_df.select("city").distinct()

served   = customer_cities.intersect(franchise_cities)            # 둘 다 있는 도시
unserved = customer_cities.subtract(franchise_cities)             # 프랜차이즈 없는 고객 도시
all_cities = customer_cities.union(franchise_cities).distinct()   # 전체 도시
```

---

## 10. 조인 성능

### 10-1. 조인 전략

| 전략 | 동작 | 셔플 |
|---|---|---|
| **Broadcast Hash Join** | 작은 쪽을 모든 Executor에 복사하고 로컬에서 조인 | 큰 쪽 셔플 없음 |
| **Sort Merge Join** | 양쪽을 키로 재분배, 정렬 후 병합. 큰 테이블끼리의 기본 | 양쪽 셔플 |
| Shuffle Hash Join | 셔플 후 해시 테이블로 조인 | 양쪽 셔플 |

```python
from pyspark.sql.functions import broadcast

joined = transactions_df.join(broadcast(franchises_df), on="franchiseID", how="inner")
joined.explain()     # 물리 계획에 BroadcastHashJoin이 보여야 함
```

- Spark는 `spark.sql.autoBroadcastJoinThreshold`(기본 10MB)보다 작다고 추정되는 쪽을 **자동으로** 브로드캐스트한다. `broadcast()` 힌트는 이를 명시적으로 요청한다.
- AQE는 실행 중 실제 크기를 보고 Sort Merge Join을 Broadcast Join으로 바꾸거나, 치우친(skew) 파티션을 나눈다.
- 선택된 전략은 `explain()`이나 Spark UI의 SQL 화면(BroadcastHashJoin vs SortMergeJoin)과 스테이지별 셔플 지표로 확인한다.

### 10-2. 성능 체크리스트

| 항목 | 내용 |
|---|---|
| 작은 테이블 브로드캐스트 | 큰 쪽 셔플 제거. 브로드캐스트 대상은 Executor 메모리에 들어갈 만큼 작아야 함 |
| 조인 전 프로젝션과 필터 | 필요한 컬럼과 행만 남겨 셔플량 감소 |
| 데이터 스큐 | 조인 키가 일부 값에 몰리면 소수 태스크가 오래 걸림. AQE 스큐 처리, 재파티셔닝, 키 정리(NULL 키 제거 등)로 대응 |
| 메모리 | 셔플 스필 지표 모니터링. 여러 번 재사용하는 DataFrame은 캐시 검토 |
| cross 조인 | 행 수가 곱으로 늘어나므로 최적화 수단이 아님 |

> "작은 DataFrame을 먼저(왼쪽에) 두라"는 조언이 흔하지만, 현대 Spark에서 inner 조인의 빌드 쪽은 옵티마이저가 크기 추정으로 정하므로 순서 자체보다 **브로드캐스트, 통계, AQE**가 성능을 좌우한다. 외부 조인은 순서에 따라 결과 의미가 달라지므로 의미를 기준으로 정한다.

---

## 11. 복합 데이터 타입: 배열, 구조체, 맵

| 타입 | PySpark | SQL | 설명 |
|---|---|---|---|
| **Array** | `ArrayType` | `ARRAY<T>` | 같은 타입 요소의 순서 있는 목록 (`[1, 2, 3]`) |
| **Struct** | `StructType` | `STRUCT<a:T, ...>` | 이름과 타입이 **미리 정해진** 필드의 묶음. 컬럼 안의 작은 테이블 |
| **Map** | `MapType` | `MAP<K, V>` | 키-값 쌍. 키가 **미리 정해지지 않음** |

JSON 데이터, 이벤트 데이터, 중첩된 분석 결과, 특성 엔지니어링에서 흔하다. API나 이벤트 피드의 데이터는 보통 JSON 문자열로 도착한다.

---

## 12. JSON 문자열을 구조체로 변환

### 12-1. 왜 일찍 바꾸는가

| JSON 문자열 | 구조체 |
|---|---|
| 쿼리마다 다시 파싱 | 한 번 파싱해 저장 |
| 옵티마이저에 불투명 | 타입이 있어 필드 프루닝과 푸시다운 가능 |
| 메모리 낭비 | 컴팩트 |
| 타입 검증 없음 | 엔진이 타입 검증 |

### 12-2. 스키마를 아는 경우: from_json + 명시 스키마

```python
from pyspark.sql.types import StructType, StructField, LongType, MapType, StringType, DoubleType
from pyspark.sql.functions import from_json, col

schema = StructType([
    StructField("user_id", LongType()),
    StructField("features", MapType(StringType(), DoubleType()))
])
df2 = df.withColumn("parsed", from_json(col("json_data"), schema))

# DDL 문자열도 가능
df2 = df.withColumn("parsed", from_json(col("json_data"), "user_id BIGINT, features MAP<STRING, DOUBLE>"))
```

### 12-3. 스키마를 모르는 경우: 샘플 → schema_of_json → from_json

```python
from pyspark.sql.functions import col, from_json, schema_of_json, lit

raw_df = spark.read.table("raw_customer_profiles")      # profile, products, purchases가 모두 STRING

# 대표 샘플 값 하나씩
profile_sample   = raw_df.select("profile").limit(1).collect()[0][0]
purchases_sample = raw_df.select("purchases").limit(1).collect()[0][0]

# schema_of_json은 리터럴 JSON 하나에서 DDL 스키마를 추론 → lit()로 전달
profiles_df = raw_df.select(
    "customerID", "name",
    from_json(col("profile"),   schema_of_json(lit(profile_sample))).alias("profile"),
    from_json(col("purchases"), schema_of_json(lit(purchases_sample))).alias("purchases"))

profiles_df.printSchema()
# profile: struct<city, country, ...>
# purchases: array<struct<product, quantity, totalPrice, paymentMethod, dateTime>>
```

- `schema_of_json()`은 컬럼 전체에 쓸 **하나의 스키마**를 정하므로 행별 컬럼이 아니라 **리터럴 샘플**이 필요하다.
- 샘플 하나에 없는 필드는 스키마에 빠지므로, 운영에서는 스키마를 명시적으로 선언하는 경우가 많다.
- 스키마와 맞지 않는 JSON은 해당 필드가 NULL이 된다.

반대로 구조를 JSON 문자열로 만들 때는 `to_json(struct(...))`, 문자열 결합은 `concat_ws(" ", first, last)`를 쓴다.

---

## 13. 구조체 필드 접근과 평탄화

```python
from pyspark.sql.functions import col

df.select(
    col("profile.city"),                                  # 점 표기법
    col("profile").getField("country").alias("country"),  # getField
    col("user.scores")[0].alias("first_score"))           # 중첩된 배열 요소 (0 기반 인덱스)

# 구조체의 모든 필드를 최상위 컬럼으로 펼치기
df.select("customerID", "profile.*")
```

점 표기법과 `getField()`는 같은 실행 계획을 만든다. 구조체 배열을 `explode`한 뒤 각 필드를 `col("purchase.product").alias("product")`처럼 꺼내면 평탄한 테이블이 된다.

---

## 14. 배열 함수와 explode

### 14-1. 펼치지 않고 쓰는 배열 함수

| 함수 | 용도 | 예 |
|---|---|---|
| `array_contains(col, v)` | 값 포함 여부 | `array_contains("products", "Golden Gate Ginger")` |
| `size(col)` / `array_size(col)` | 요소 수 | `array_size("purchases")` |
| `element_at(col, n)` | n번째 요소 (**1 기반**, 음수는 뒤에서부터) | `element_at("items", 2)` |
| `array_distinct(col)` | 중복 제거 | |
| `sort_array`, `array_union`, `array_intersect`, `array_except` | 정렬, 집합 연산 | |
| `transform`, `filter`, `aggregate` | 고차 함수: 요소마다 처리 | `transform("items", x -> upper(x))` |

```python
from pyspark.sql.functions import col, array_size, array_contains, desc

profiles_df.select("name",
                   array_size(col("products")).alias("distinct_products"),
                   array_size(col("purchases")).alias("purchase_count")) \
           .orderBy(desc("purchase_count"))

fans_df = profiles_df.filter(array_contains(col("products"), "Golden Gate Ginger"))
```

`col("arr")[0]`은 0 기반, `element_at(arr, 1)`은 1 기반이다. ANSI 모드에서 범위를 벗어난 `element_at`은 오류를 낼 수 있어, NULL을 원하면 `try_element_at`을 쓴다.

### 14-2. explode

배열 요소마다 한 행을 만들고 **나머지 컬럼은 각 행에 그대로 복제**한다.

| id | items |
|---|---|
| 1 | ["a", "b", "c"] |
| 2 | ["x", "y"] |

`df.select("id", explode("items").alias("item"))`

| id | item |
|---|---|
| 1 | a |
| 1 | b |
| 1 | c |
| 2 | x |
| 2 | y |

```python
from pyspark.sql.functions import col, explode, sum, desc

purchases_df = profiles_df.select(
    "customerID", "name",
    col("profile.country").alias("country"),
    explode(col("purchases")).alias("purchase"))

details_df = purchases_df.select(
    "customerID", "name", "country",
    col("purchase.product").alias("product"),
    col("purchase.quantity").alias("quantity"),
    col("purchase.totalPrice").alias("totalPrice"),
    col("purchase.paymentMethod").alias("paymentMethod"))

# 펼친 뒤 구조체 필드로 바로 그룹화도 가능
purchases_df.groupBy(col("purchase.product").alias("product")) \
    .agg(sum(col("purchase.totalPrice")).alias("total_revenue")) \
    .orderBy(desc("total_revenue"))
```

| 함수 | 차이 |
|---|---|
| `explode` | NULL이거나 빈 배열인 행은 **사라진다** |
| `explode_outer` | NULL/빈 배열이면 요소를 NULL로 한 행을 남긴다 |
| `posexplode` | 요소와 함께 위치(인덱스)도 반환 |

행 수가 평균 배열 길이만큼 늘어나는 것이 `explode`의 주된 성능 위험이다.

---

## 15. 컬렉션 집계와 pivot

### 15-1. collect_list / collect_set

`explode()`의 반대로, 행을 배열로 모은다.

```python
from pyspark.sql.functions import collect_list, collect_set, array_size, col

country_products_df = (details_df.groupBy("country")
    .agg(collect_list("product").alias("all_purchases"),     # 중복 포함
         collect_set("product").alias("unique_products"))    # 고유값만
    .withColumn("purchase_count", array_size(col("all_purchases")))
    .withColumn("product_count", array_size(col("unique_products"))))
```

| 함수 | 중복 | 비고 |
|---|---|---|
| `collect_list()` | 유지 | 결과 순서는 보장되지 않는다(셔플에 따라 달라짐) |
| `collect_set()` | 제거 | 순서 없음. 중복이 필요 없으면 메모리와 셔플이 적다 |

순서가 중요하면 `array_sort(collect_list(struct(ts, value)))`처럼 정렬 키와 함께 모은 뒤 정렬한다.

### 15-2. pivot

행 값을 컬럼으로 바꿔 교차 집계표를 만든다.

```python
payment_pivot_df = (details_df
    .groupBy("country")
    .pivot("paymentMethod")                       # 또는 .pivot("paymentMethod", ["AMEX", "VISA", "MASTERCARD"])
    .count()
    .fillna(0))                                   # 조합이 없는 칸의 NULL을 0으로
```

- 값 목록을 주지 않으면 Spark가 고유값을 찾기 위해 데이터를 한 번 더 읽는다.
- 카디널리티가 높은 컬럼을 피벗하면 매우 넓은 결과가 나온다. 값을 알면 `pivot(col, [values])`로 제한한다.
- 교차 집계와 특성 엔지니어링(범주별 카운트를 컬럼으로)에 유용하다.

---

## 16. VARIANT 타입

Spark 4의 `VARIANT`는 반정형 데이터를 **스키마를 미리 선언하지 않고** 효율적인 이진 형식으로 저장한다.

| 방식 | 장점 | 단점 |
|---|---|---|
| JSON 문자열 | 단순 | 매 쿼리 파싱, 옵티마이저 불투명 |
| `from_json` 구조체 | 타입 안전, 빠름 | 스키마를 알아야 하고 구조가 바뀌면 깨짐 |
| `MapType` | 키가 유연 | 값 타입이 하나로 고정, 중첩 구조의 타입을 잃음 |
| **`VARIANT`** | 스키마 변화 허용, 경로로 조회, 문자열보다 빠름 | 필드 타입은 조회 시 지정 |

```sql
-- JSON 문자열을 VARIANT로
CREATE OR REPLACE TABLE events_v AS
SELECT parse_json(payload) AS v FROM raw_events;

-- 경로 조회와 타입 캐스팅
SELECT
  v:user.id::BIGINT          AS user_id,
  v:order.items[0].sku::STRING AS first_sku
FROM events_v;
```

```python
from pyspark.sql.functions import parse_json, variant_get, col
df_v = raw_df.select(parse_json(col("profile")).alias("v"))
df_v.select(variant_get(col("v"), "$.city", "string").alias("city"))
```

구조가 알려지지 않았거나 자주 바뀌는 페이로드에 적합하다. 구조가 안정적이면 구조체로 바꾸는 것이 가장 효율적이다.

---

## 17. 복합 데이터 성능 모범 사례

| 상황 | 주의점 |
|---|---|
| 큰 배열에 `explode()` | 1행이 N행이 되는 **데이터 폭발**로 메모리와 처리가 급증. 가능하면 `array_size`, `array_contains`, 고차 함수로 펼치지 않고 처리 |
| `collect_list()` / `collect_set()` | 그룹의 모든 값을 메모리에 모음. 그룹이 크면 메모리 부담 |
| 중복과 순서가 필요 없음 | `collect_set()`이 메모리와 셔플이 적다 |
| JSON 문자열 | 수집 단계에서 일찍 구조체로 변환 |
| 고카디널리티 pivot | 값 목록을 지정해 컬럼 수 제한 |

---

## 18. 핵심 정리 치트시트

### 기초

| 키워드 | 핵심 |
|---|---|
| 변환 vs 액션 | 변환은 새 DataFrame, 지연 / 액션이 실행 |
| 컬럼 참조 | 문자열, 속성, `df["c"]`, `col()`, `selectExpr` |
| ANSI | 잘못된 CAST 오류 → `try_cast()` |
| UDF 순서 | 내장 함수 > Pandas/Arrow UDF > Python UDF |
| ETL 체인 | 필요한 컬럼 먼저 → 검증 → 보강 → 단일 쓰기 |

### 그룹화와 윈도우

| 키워드 | 핵심 |
|---|---|
| `groupBy()` | 셔플 발생, `GroupedData` 반환, 지연 평가 |
| 단축 메서드 | `count`, `sum`, `avg`, `min`, `max`. 자동 이름 |
| `agg()` | 한 패스로 여러 집계 + `alias()`. 딕셔너리 형식은 별칭 불가 |
| HAVING | `agg` 후 `filter` |
| groupBy vs 윈도우 | 행 축소 vs 행 유지 + 컬럼 추가 |
| 윈도우 사양 | `Window.partitionBy().orderBy().rowsBetween()` |
| 순위 | `row_number`(고유), `rank`(1,2,2,4), `dense_rank`(1,2,2,3), `ntile` |
| 오프셋 | `lag`(이전), `lead`(다음) |
| 주의 | partitionBy 없는 윈도우는 단일 파티션. orderBy 기본 프레임은 RANGE |

### 조인과 집합

| 키워드 | 핵심 |
|---|---|
| 조인 유형 | inner(기본), left, right, full_outer, left_semi, left_anti, cross |
| 키 지정 | 같은 이름은 문자열(키 하나 남음), 다른 이름은 표현식(둘 다 남음) |
| 이름 충돌 | 조인 전 `alias`/`withColumnRenamed` |
| 고아 레코드 | full_outer + `isNull()` 필터, 또는 left_anti |
| `union` | 위치 기준, 중복 유지(UNION ALL) |
| `unionByName` | 이름 기준, `allowMissingColumns` |
| `intersect`/`subtract` | 중복 제거. 유지는 `intersectAll`/`exceptAll` |
| 브로드캐스트 | `broadcast(small)`, 자동 임계값 10MB, `explain()`에서 BroadcastHashJoin |
| 성능 | 조인 전 프로젝션, 스큐 대응, cross 조인 지양 |

### 복합 데이터

| 키워드 | 핵심 |
|---|---|
| 타입 | Array(순서 목록), Struct(정해진 필드), Map(정해지지 않은 키) |
| JSON 변환 | `from_json(col, schema)`. 스키마 모르면 샘플 + `schema_of_json(lit(sample))` |
| 구조체 접근 | `col("s.f")`, `getField("f")`, `"s.*"` |
| 배열 함수 | `array_contains`, `array_size`, `element_at`(1 기반), `array_distinct` |
| explode | 요소당 한 행, 다른 컬럼 복제. NULL/빈 배열은 `explode_outer` |
| 컬렉션 | `collect_list`(중복 유지, 순서 비보장), `collect_set`(고유) |
| pivot | `groupBy().pivot(col, [values]).count().fillna(0)` |
| VARIANT | `parse_json`, `v:path::type`, 스키마 없이 반정형 저장 |

---

## 19. 자가 점검 문제

> 답을 가리고 먼저 생각해본 뒤 펼쳐보자.

**Q1.** DataFrame 변환의 세 가지 성질은?
<details><summary>정답</summary>

모든 파티션에 자동으로 분산되고, 기존 DataFrame을 바꾸지 않고 새 DataFrame을 반환하며(불변), 액션이 실행될 때까지 논리 계획에 쌓이기만 한다(지연 평가). 또한 SQL 연산에 그대로 대응한다.
</details>

**Q2.** 거래 데이터의 내부 일관성을 검증하는 예를 들어라.
<details><summary>정답</summary>

quantity > 0이고 totalPrice == quantity * unitPrice인 행만 남기는 필터다. 보강 전에 NULL 수와 이런 일관성을 확인해 잘못된 행을 걸러 낸다.
</details>

**Q3.** 카드 번호를 마지막 4자리만 남기고 마스킹하는 내장 함수 조합은?
<details><summary>정답</summary>

concat(lit("****-"), right(col("cardNumber").cast("string"), lit(4)))로 만든 뒤 원래 cardNumber 컬럼을 drop한다. 숫자로 저장된 카드 번호는 문자열로 바꿔야 자를 수 있다.
</details>

**Q4.** groupBy()가 셔플을 일으키는 이유와 반환 타입은?
<details><summary>정답</summary>

같은 키의 행이 한 Executor에 모여야 그룹을 완전히 집계할 수 있어 키 기준으로 데이터를 재분배하기 때문이다. GroupedData 객체를 반환하며, count()나 agg()를 붙여야 DataFrame이 되고 액션 전까지 실행되지 않는다.
</details>

**Q5.** 단축 집계 메서드 대신 agg()를 쓰는 이유 두 가지는?
<details><summary>정답</summary>

한 번의 셔플과 집계 패스로 여러 지표를 계산할 수 있고, alias()로 결과 컬럼 이름을 정할 수 있기 때문이다. 단축 메서드는 sum(salary) 같은 자동 이름을 만든다.
</details>

**Q6.** agg()의 딕셔너리 형식의 단점은?
<details><summary>정답</summary>

간결하지만 별칭을 지정할 수 없어 avg(trip_distance) 같은 이름이 생긴다. 이후 단계에서 참조할 결과는 함수 형식과 alias()를 쓴다.
</details>

**Q7.** SQL의 HAVING을 DataFrame으로 표현하려면?
<details><summary>정답</summary>

groupBy().agg()로 별칭이 있는 집계 컬럼을 만든 뒤 그 컬럼에 filter()를 건다.
</details>

**Q8.** groupBy와 윈도우 함수의 핵심 차이는?
<details><summary>정답</summary>

groupBy는 각 그룹을 한 행으로 축소하고, 윈도우 함수는 모든 행을 유지한 채 순위나 누적 합계 같은 계산 결과를 컬럼으로 추가한다.
</details>

**Q9.** rank(), dense_rank(), row_number()의 차이는?
<details><summary>정답</summary>

row_number는 동점이어도 고유 번호(1,2,3,4), rank는 동점에 같은 순위를 주고 다음을 건너뛰며(1,2,2,4), dense_rank는 건너뛰지 않는다(1,2,2,3).
</details>

**Q10.** 이전 행의 값과 비교하려면 어떤 윈도우 함수를 쓰는가?
<details><summary>정답</summary>

lag()이다. 다음 행 값은 lead()를 쓴다. 기간별 증감 비교에 유용하다.
</details>

**Q11.** partitionBy 없이 orderBy만 있는 윈도우의 위험은?
<details><summary>정답</summary>

전체 데이터를 하나의 윈도우로 보므로 모든 행이 단일 파티션으로 모인다. 이미 집계된 작은 데이터에는 괜찮지만 원시 데이터에서는 한 태스크에 부하가 몰려 느려지거나 메모리가 부족해진다.
</details>

**Q12.** orderBy가 있는 윈도우로 누적 합계를 구할 때 동점 값에서 생기는 문제와 해결은?
<details><summary>정답</summary>

기본 프레임이 처음부터 현재 행의 값까지인 RANGE라서 정렬 값이 같은 행들이 한꺼번에 누적에 포함된다. 행 단위 누적이 필요하면 rowsBetween(Window.unboundedPreceding, Window.currentRow)로 프레임을 명시한다.
</details>

**Q13.** 각 행을 자기 그룹의 평균과 비교하는 방법은?
<details><summary>정답</summary>

orderBy 없이 partitionBy만 있는 윈도우로 avg(col).over(Window.partitionBy("group"))를 구해 각 행에 붙이고, 행 값과의 차이를 계산한다.
</details>

**Q14.** 집계 결과에 순위를 매기는 SQL을 한 쿼리로 쓰는 방법은?
<details><summary>정답</summary>

GROUP BY와 함께 RANK() OVER (ORDER BY SUM(fare_amount) DESC)처럼 집계 표현식에 바로 윈도우를 건다.
</details>

**Q15.** left_semi와 left_anti 조인은 무엇을 반환하는가?
<details><summary>정답</summary>

left_semi는 오른쪽에 일치가 있는 왼쪽 행, left_anti는 일치가 없는 왼쪽 행을 반환하며, 둘 다 왼쪽 컬럼만 남는다. SQL의 EXISTS와 NOT EXISTS에 해당한다.
</details>

**Q16.** 조인 키를 문자열로 줄 때와 표현식으로 줄 때 결과 컬럼은 어떻게 다른가?
<details><summary>정답</summary>

양쪽 키 이름이 같아 문자열로 주면 결과에 키 컬럼이 하나만 남는다. 표현식(df1.a == df2.b)으로 주면 양쪽 키 컬럼이 모두 남으므로 하나를 drop하거나 필요한 컬럼만 선택한다.
</details>

**Q17.** 두 테이블에 name, city 같은 같은 이름의 컬럼이 있을 때 조인 전에 할 일은?
<details><summary>정답</summary>

alias()나 withColumnRenamed()로 이름을 바꾸거나 DataFrame 별칭으로 구분한다. 그대로 조인하면 이후 참조가 모호해진다.
</details>

**Q18.** 조인 전에 프로젝션하는 이점 두 가지는?
<details><summary>정답</summary>

필요한 컬럼만 남겨 셔플할 데이터 양이 줄고, 겹치는 컬럼 이름을 미리 정리해 모호한 참조를 피한다.
</details>

**Q19.** 프랜차이즈와 공급업체 사이의 고아 레코드를 찾는 패턴은?
<details><summary>정답</summary>

두 테이블을 full_outer 조인한 뒤 한쪽 컬럼이 NULL인 행만 필터한다. supplier_name이 NULL이면 없는 공급업체를 참조하는 프랜차이즈, franchise_name이 NULL이면 아무도 쓰지 않는 공급업체다.
</details>

**Q20.** union()과 unionByName()의 차이와 위험은?
<details><summary>정답</summary>

union은 컬럼 위치로 맞추므로 컬럼 순서가 다르면 엉뚱한 컬럼끼리 조용히 합쳐진다. unionByName은 이름으로 맞추며, allowMissingColumns=True면 없는 컬럼을 NULL로 채운다.
</details>

**Q21.** Spark의 union()은 SQL의 UNION과 같은가?
<details><summary>정답</summary>

아니다. 중복을 유지하는 UNION ALL처럼 동작한다. 중복 없는 합집합은 union().distinct()로 만든다.
</details>

**Q22.** intersect()와 subtract()의 중복 처리와, 중복을 유지하는 대안은?
<details><summary>정답</summary>

둘 다 중복을 제거한다. 중복을 유지하려면 intersectAll()과 exceptAll()을 쓴다.
</details>

**Q23.** 집합 연산의 전제 조건은?
<details><summary>정답</summary>

행 전체를 비교하므로 두 DataFrame의 스키마(컬럼 수와 타입)가 맞아야 한다. 그래서 비교할 컬럼만 프로젝션해 맞춘 뒤 수행한다.
</details>

**Q24.** 브로드캐스트 조인은 어떻게 동작하며 언제 자동으로 쓰이는가?
<details><summary>정답</summary>

작은 DataFrame을 모든 Executor에 복사해 큰 쪽을 셔플하지 않고 로컬에서 조인한다. 크기 추정이 spark.sql.autoBroadcastJoinThreshold(기본 10MB) 이하면 자동으로 쓰이고, broadcast() 힌트로 명시할 수 있다.
</details>

**Q25.** 선택된 조인 전략을 확인하는 방법은?
<details><summary>정답</summary>

explain()의 물리 계획에서 BroadcastHashJoin인지 SortMergeJoin인지 보거나, Spark UI의 SQL 화면과 스테이지별 셔플 지표를 확인한다.
</details>

**Q26.** 조인 성능 최적화로 권장되지 않는 것은? 데이터 스큐는 어떻게 다루는가?
<details><summary>정답</summary>

cross 조인은 행 조합이 곱으로 늘어나 최적화가 아니다. 스큐는 조인 키가 일부 값에 몰리는 것으로, AQE의 스큐 처리, 재파티셔닝, NULL 같은 불필요한 키 제거로 대응한다.
</details>

**Q27.** inner 조인에서 작은 DataFrame을 왼쪽에 두면 항상 빨라지는가?
<details><summary>정답</summary>

아니다. 현대 Spark에서는 옵티마이저가 크기 추정으로 빌드 쪽과 전략을 정하므로 순서보다 브로드캐스트, 정확한 통계, AQE가 성능을 좌우한다. 외부 조인은 순서가 결과 의미를 바꾸므로 의미 기준으로 정한다.
</details>

**Q28.** Array, Struct, Map의 차이는?
<details><summary>정답</summary>

Array는 같은 타입 요소의 순서 있는 목록, Struct는 이름과 타입이 미리 정해진 필드 묶음, Map은 미리 정해지지 않은 키를 가진 키-값 쌍이다.
</details>

**Q29.** JSON 문자열을 일찍 구조체로 바꿔야 하는 이유는?
<details><summary>정답</summary>

JSON 문자열은 쿼리마다 다시 파싱되고 옵티마이저에 불투명하며 메모리를 낭비한다. 구조체는 타입이 검증되고 컴팩트하며 필드 프루닝과 푸시다운이 가능하다.
</details>

**Q30.** 스키마를 모르는 JSON 컬럼을 구조체로 바꾸는 절차는?
<details><summary>정답</summary>

컬럼에서 대표 샘플 값을 하나 수집하고, schema_of_json(lit(sample))로 스키마를 추론한 뒤, from_json(col, 그 스키마)로 변환한다.
</details>

**Q31.** schema_of_json()에 컬럼이 아니라 리터럴 샘플을 넘기는 이유는?
<details><summary>정답</summary>

컬럼 전체에 적용할 하나의 스키마를 정해야 하므로 행마다 다른 컬럼 값이 아니라 대표 리터럴 JSON 하나가 필요하기 때문이다. 운영에서는 샘플에 없는 필드가 빠질 수 있어 스키마를 명시하는 경우가 많다.
</details>

**Q32.** JSON 객체 배열 문자열을 from_json으로 바꾸면 어떤 타입이 되는가?
<details><summary>정답</summary>

array<struct<...>>, 즉 요소가 product, quantity 같은 타입 있는 필드를 가진 구조체인 배열이 된다.
</details>

**Q33.** 구조체 필드에 접근하는 방법과 모든 필드를 펼치는 방법은?
<details><summary>정답</summary>

col("profile.city") 같은 점 표기법이나 col("profile").getField("city")를 쓰며 둘은 같은 계획을 만든다. select("profile.*")로 모든 필드를 최상위 컬럼으로 펼친다.
</details>

**Q34.** 배열을 펼치지 않고 쓸 수 있는 함수 세 가지는?
<details><summary>정답</summary>

값 포함 여부를 보는 array_contains, 요소 수를 세는 array_size(size), n번째 요소를 꺼내는 element_at(1 기반)이다. array_distinct와 transform, filter 같은 고차 함수도 있다.
</details>

**Q35.** element_at()과 col("arr")[n]의 인덱스 차이는?
<details><summary>정답</summary>

element_at은 1 기반이고 음수면 뒤에서부터 센다. col("arr")[n]은 0 기반이다.
</details>

**Q36.** explode()가 행 수와 다른 컬럼에 미치는 영향은?
<details><summary>정답</summary>

배열 요소마다 한 행을 만들어 행 수가 평균 배열 길이만큼 늘어나고, 다른 컬럼 값은 각 행에 그대로 복제된다. 큰 배열에서는 데이터 폭발로 메모리와 처리 부담이 커진다.
</details>

**Q37.** explode와 explode_outer의 차이는?
<details><summary>정답</summary>

explode는 배열이 NULL이거나 비어 있는 행을 결과에서 없애고, explode_outer는 요소를 NULL로 한 행을 남긴다.
</details>

**Q38.** collect_list와 collect_set의 차이와 collect_set이 유리한 경우는?
<details><summary>정답</summary>

collect_list는 중복을 유지하고 collect_set은 고유값만 남긴다. 각 값이 한 번만 필요하고 순서가 중요하지 않으면 collect_set이 메모리와 셔플이 적다.
</details>

**Q39.** collect_list 결과의 순서를 믿을 수 있는가?
<details><summary>정답</summary>

없다. 셔플에 따라 순서가 달라질 수 있다. 순서가 필요하면 정렬 키와 함께 struct로 모은 뒤 array_sort로 정렬한다.
</details>

**Q40.** 국가별 결제 수단별 구매 횟수표를 만드는 코드와 주의점은?
<details><summary>정답</summary>

groupBy("country").pivot("paymentMethod").count().fillna(0)이다. 값 목록을 주지 않으면 고유값을 찾으려 데이터를 한 번 더 읽고, 카디널리티가 높으면 매우 넓어지므로 pivot(col, [values])로 제한한다.
</details>

**Q41.** VARIANT가 JSON 문자열, 구조체, MapType보다 나은 상황은?
<details><summary>정답</summary>

구조가 알려지지 않았거나 자주 바뀌는 페이로드다. 스키마를 미리 선언하지 않고 이진 형식으로 저장해 문자열보다 빠르고, 경로 표기(v:path::type)로 조회하며, Map처럼 중첩 구조의 타입을 잃지 않는다.
</details>

**Q42.** 복합 데이터를 다룰 때 메모리 위험이 있는 두 연산은?
<details><summary>정답</summary>

큰 배열에 대한 explode(행 폭증)와, 큰 그룹에 대한 collect_list/collect_set(그룹의 모든 값을 메모리에 보유)이다.
</details>

**Q43.** 원시 데이터 컬럼을 JSON 문자열로 만드는 함수는?
<details><summary>정답</summary>

여러 컬럼을 struct()로 묶고 to_json()으로 문자열로 만든다. collect_list(struct(...))를 to_json으로 감싸면 JSON 배열 문자열이 된다.
</details>

**Q44.** Pandas UDF로 z-점수를 계산할 때의 함정은?
<details><summary>정답</summary>

Pandas UDF가 받는 Series는 한 배치일 뿐이라 배치마다 다른 평균과 표준편차로 계산된다. 전체 기준이 필요하면 내장 집계나 윈도우 함수로 전체 통계를 먼저 구한다.
</details>

---

## 20. 참고 링크

- PySpark DataFrame API: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html
- PySpark Column API: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html
- PySpark 함수: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html
- PySpark 그룹화 API: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html
- PySpark 윈도우: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/window.html
- Spark SQL 함수: https://spark.apache.org/docs/latest/api/sql/index.html
- Spark SQL 윈도우 함수: https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html
- Spark SQL 조인: https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-join.html
- Spark SQL 집합 연산: https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-setops.html
- Spark 조인 전략 힌트: https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-hints.html
- Spark 성능 튜닝 (AQE, 브로드캐스트): https://spark.apache.org/docs/latest/sql-performance-tuning.html
- Spark SQL ANSI 모드: https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html
- Databricks 윈도우 함수: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-window-functions
- Databricks JOIN: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-qry-select-join
- Databricks 복합 데이터 타입 변환: https://docs.databricks.com/aws/en/semi-structured/complex-types
- Databricks JSON 문자열 조회: https://docs.databricks.com/aws/en/semi-structured/json
- from_json: https://docs.databricks.com/aws/en/sql/language-manual/functions/from_json
- schema_of_json: https://docs.databricks.com/aws/en/sql/language-manual/functions/schema_of_json
- explode: https://docs.databricks.com/aws/en/sql/language-manual/functions/explode
- VARIANT 타입: https://docs.databricks.com/aws/en/semi-structured/variant
- Adaptive Query Execution: https://docs.databricks.com/aws/en/optimizations/aqe
- pandas UDF: https://docs.databricks.com/aws/en/udf/pandas
