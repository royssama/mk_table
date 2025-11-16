# 프로시저 테이블 사용 분석 (타입별)

## SELECT 문에서 사용된 테이블

### WITH 절 (CTE)에서 사용
- **Departments** (d)
- **Employees** (e)
- **Orders** (o)
- **OrderDetails** (od)
- **Products** (p)
- **Categories** (c, cat)
- **Suppliers** (s, sup)
- **Customers** (c)
- **Shippers** (sh)
- **Regions** (r)
- **OrderCancellations** (oc)

### 메인 SELECT 문에서 사용
- **#TempProcessingData** (tpd)
- **#TempValidationResults** (tvr)
- **#TempAggregatedData** (tad)
- **CustomerAnalysis** (ca) - CTE
- **EmployeePerformance** (ep) - CTE
- **DepartmentHierarchy** (dh) - CTE
- **ProductSalesMetrics** (psm) - CTE
- **RegionalSalesSummary** (rss) - CTE
- **ComplexDataTransformation** (cdt) - CTE
- **EmployeeRanking** (er) - CTE
- **CustomerSegmentation** (cs) - CTE
- **RecursiveDepartmentBudget** (rdb) - CTE
- **DepartmentPerformanceMetrics** (dpm) - CTE
- **Orders** (o, o2, o3)
- **OrderDetails** (od, od2, od3, od4, od5)
- **Products** (p, p2, p3, p4)
- **Categories** (c, c2, c3, cat)
- **Customers** (c, c2, c3, c4, c5, cus)
- **Employees** (e)
- **Departments** (d)
- **Regions** (r)
- **Suppliers** (s)
- **Shippers** (sh)
- **Discounts** (d)
- **OrderCancellations** (oc)
- **SalesPerformanceReport** (spr)
- **CustomerSegmentAnalysis** (csa)
- **DepartmentPerformanceReport** (dpr)

---

## INSERT 문에서 사용된 테이블

### 임시 테이블
1. **#TempProcessingData**
   - INSERT 위치: 라인 194-273
   - 소스: CustomerAnalysis, Orders, OrderDetails, ProductSalesMetrics, Employees, EmployeePerformance, DepartmentHierarchy, RegionalSalesSummary, Products, Categories, Suppliers

2. **#TempValidationResults**
   - INSERT 위치: 라인 578-613, 615-662
   - 소스: #TempProcessingData, OrderDetails, Orders, Customers

3. **#TempAggregatedData**
   - INSERT 위치: 라인 533-573
   - 소스: #TempProcessingData, Employees, EmployeePerformance, DepartmentHierarchy, Customers, Orders, OrderDetails, Products, Categories, RegionalSalesSummary

### 영구 테이블
4. **SalesPerformanceReport**
   - INSERT 위치: 라인 456-528
   - 소스: #TempProcessingData, Customers, CustomerAnalysis, Orders, OrderDetails, Products, ProductSalesMetrics, Categories, Suppliers, Employees, EmployeePerformance, DepartmentHierarchy, RegionalSalesSummary, Shippers

5. **CustomerSegmentAnalysis**
   - INSERT 위치: 라인 941-1000
   - 소스: CustomerSegmentation, Orders, Employees, EmployeeRanking

6. **DepartmentPerformanceReport**
   - INSERT 위치: 라인 1052-1082
   - 소스: DepartmentPerformanceMetrics (CTE)

7. **ProcessExecutionLog**
   - INSERT 위치: 라인 699-728
   - 소스: #TempProcessingData, #TempValidationResults

8. **ProcessStatistics**
   - INSERT 위치: 라인 1137-1189
   - 소스: #TempProcessingData, Products, OrderDetails, Orders, Categories, Suppliers, Regions, Customers

9. **ErrorLog**
   - INSERT 위치: 라인 1217-1224
   - 소스: 시스템 함수 및 변수

---

## UPDATE 문에서 사용된 테이블

1. **#TempProcessingData** (tpd)
   - UPDATE 위치: 라인 280-338
   - 조건: OrderDetails, Orders, Discounts, Customers 사용
   - 업데이트 필드: Amount, Status

2. **#TempProcessingData** (tpd) - 2번째 UPDATE
   - UPDATE 위치: 라인 667-694
   - 조건: #TempValidationResults, Orders 사용
   - 업데이트 필드: IsProcessed, Status, ErrorMessage

3. **#TempProcessingData** (tpd) - 3번째 UPDATE
   - UPDATE 위치: 라인 921-938
   - 조건: ComplexDataTransformation (CTE) 사용
   - 업데이트 필드: Status

4. **Orders** (o)
   - UPDATE 위치: 라인 1087-1132
   - 조건: Customers, OrderDetails, Products, Categories, Employees, Departments, Regions, #TempProcessingData 사용
   - 업데이트 필드: RequiredDate, ShippedDate

---

## DELETE 문에서 사용된 테이블

1. **OrderProcessingQueue**
   - DELETE 위치: 라인 415-451
   - 조건: Orders, Customers, OrderDetails, Products, Categories, Suppliers, Employees, Departments, Regions, #TempProcessingData, OrderCancellations 사용

2. **CustomerOrderSummary** (MERGE 문 내부)
   - DELETE 위치: 라인 399-403 (MERGE의 WHEN NOT MATCHED BY SOURCE 절)
   - 조건: MERGE 문의 조건에 따라 삭제

---

## MERGE 문에서 사용된 테이블

1. **CustomerOrderSummary** (target)
   - MERGE 위치: 라인 343-410
   - SOURCE: #TempProcessingData
   - 동작:
     - WHEN MATCHED: UPDATE
     - WHEN NOT MATCHED BY TARGET: INSERT
     - WHEN NOT MATCHED BY SOURCE: DELETE
   - OUTPUT: #TempMergeResults

---

## 요약 통계

### 전체 사용 테이블 목록 (중복 제거)

**영구 테이블:**
1. Departments
2. Employees
3. Orders
4. OrderDetails
5. Products
6. Categories
7. Suppliers
8. Customers
9. Shippers
10. Regions
11. OrderCancellations
12. Discounts
13. CustomerOrderSummary
14. OrderProcessingQueue
15. SalesPerformanceReport
16. CustomerSegmentAnalysis
17. DepartmentPerformanceReport
18. ProcessExecutionLog
19. ProcessStatistics
20. ErrorLog

**임시 테이블:**
1. #TempProcessingData
2. #TempValidationResults
3. #TempAggregatedData
4. #TempMergeResults

### 타입별 테이블 개수
- **SELECT**: 20개 영구 테이블 + 4개 임시 테이블
- **INSERT**: 9개 영구 테이블 + 3개 임시 테이블
- **UPDATE**: 2개 영구 테이블 + 1개 임시 테이블
- **DELETE**: 2개 영구 테이블
- **MERGE**: 1개 영구 테이블 + 1개 임시 테이블

---

## 상세 분석

### 가장 많이 사용된 테이블 (SELECT 기준)
1. **Orders** - 15회 이상
2. **OrderDetails** - 15회 이상
3. **Products** - 12회 이상
4. **Customers** - 10회 이상
5. **Categories** - 10회 이상
6. **Employees** - 8회 이상
7. **Departments** - 6회 이상

### 복잡도가 높은 쿼리
1. **라인 194-273**: 10개 이상 테이블 조인 (INSERT INTO #TempProcessingData)
2. **라인 456-528**: 10개 이상 테이블 조인 (INSERT INTO SalesPerformanceReport)
3. **라인 1087-1132**: 7개 테이블 조인 (UPDATE Orders)
4. **라인 415-451**: 9개 테이블 조인 (DELETE FROM OrderProcessingQueue)

