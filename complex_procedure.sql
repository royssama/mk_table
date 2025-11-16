-- =============================================
-- 복잡한 비즈니스 로직 처리 프로시저
-- 작성일: 2024
-- 설명: 다중 테이블 조인 및 복잡한 데이터 처리
-- =============================================
CREATE PROCEDURE [dbo].[sp_ComplexBusinessProcess]
    @ProcessDate DATETIME = NULL,
    @DepartmentId INT = NULL,
    @EmployeeId INT = NULL,
    @Status VARCHAR(50) = NULL,
    @BatchSize INT = 1000,
    @DebugMode BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
    
    DECLARE @ErrorCode INT = 0;
    DECLARE @ErrorMessage NVARCHAR(MAX) = '';
    DECLARE @RowCount INT = 0;
    DECLARE @TransactionCount INT = 0;
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @EndTime DATETIME;
    DECLARE @ProcessId UNIQUEIDENTIFIER = NEWID();
    
    -- 변수 선언
    DECLARE @CurrentDate DATETIME = ISNULL(@ProcessDate, GETDATE());
    DECLARE @PreviousMonth DATETIME = DATEADD(MONTH, -1, @CurrentDate);
    DECLARE @NextMonth DATETIME = DATEADD(MONTH, 1, @CurrentDate);
    DECLARE @YearStart DATETIME = DATEFROMPARTS(YEAR(@CurrentDate), 1, 1);
    DECLARE @YearEnd DATETIME = DATEFROMPARTS(YEAR(@CurrentDate), 12, 31);
    
    -- 임시 테이블 생성
    CREATE TABLE #TempProcessingData (
        Id INT IDENTITY(1,1) PRIMARY KEY,
        CustomerId INT,
        OrderId INT,
        ProductId INT,
        EmployeeId INT,
        DepartmentId INT,
        Amount DECIMAL(18,2),
        Status VARCHAR(50),
        ProcessDate DATETIME,
        IsProcessed BIT DEFAULT 0,
        ErrorMessage NVARCHAR(MAX)
    );
    
    CREATE TABLE #TempValidationResults (
        RecordId INT,
        ValidationType VARCHAR(100),
        IsValid BIT,
        ValidationMessage NVARCHAR(MAX)
    );
    
    CREATE TABLE #TempAggregatedData (
        DepartmentId INT,
        EmployeeId INT,
        TotalAmount DECIMAL(18,2),
        RecordCount INT,
        AverageAmount DECIMAL(18,2),
        MaxAmount DECIMAL(18,2),
        MinAmount DECIMAL(18,2)
    );
    
    BEGIN TRY
        BEGIN TRANSACTION;
        SET @TransactionCount = @@TRANCOUNT;
        
        -- ============================================
        -- 1단계: WITH 절을 사용한 복잡한 CTE 쿼리
        -- ============================================
        WITH DepartmentHierarchy AS (
            SELECT 
                d.DepartmentId,
                d.DepartmentName,
                d.ParentDepartmentId,
                d.ManagerId,
                d.BudgetAmount,
                d.LocationId,
                0 AS Level,
                CAST(d.DepartmentName AS VARCHAR(MAX)) AS HierarchyPath
            FROM Departments d
            WHERE d.ParentDepartmentId IS NULL
            
            UNION ALL
            
            SELECT 
                d.DepartmentId,
                d.DepartmentName,
                d.ParentDepartmentId,
                d.ManagerId,
                d.BudgetAmount,
                d.LocationId,
                dh.Level + 1,
                CAST(dh.HierarchyPath + ' > ' + d.DepartmentName AS VARCHAR(MAX))
            FROM Departments d
            INNER JOIN DepartmentHierarchy dh ON d.ParentDepartmentId = dh.DepartmentId
        ),
        EmployeePerformance AS (
            SELECT 
                e.EmployeeId,
                e.EmployeeName,
                e.DepartmentId,
                e.ManagerId,
                e.HireDate,
                e.Salary,
                e.PositionId,
                COUNT(DISTINCT o.OrderId) AS TotalOrders,
                SUM(od.Quantity * od.UnitPrice) AS TotalSales,
                AVG(od.Quantity * od.UnitPrice) AS AvgOrderValue,
                MAX(o.OrderDate) AS LastOrderDate,
                MIN(o.OrderDate) AS FirstOrderDate
            FROM Employees e
            LEFT JOIN Orders o ON e.EmployeeId = o.SalesPersonId
            LEFT JOIN OrderDetails od ON o.OrderId = od.OrderId
            LEFT JOIN Products p ON od.ProductId = p.ProductId
            LEFT JOIN Categories c ON p.CategoryId = c.CategoryId
            LEFT JOIN Suppliers s ON p.SupplierId = s.SupplierId
            WHERE e.IsActive = 1
            GROUP BY 
                e.EmployeeId, e.EmployeeName, e.DepartmentId, 
                e.ManagerId, e.HireDate, e.Salary, e.PositionId
        ),
        CustomerAnalysis AS (
            SELECT 
                c.CustomerId,
                c.CustomerName,
                c.CustomerTypeId,
                c.RegionId,
                c.CreditLimit,
                COUNT(DISTINCT o.OrderId) AS OrderCount,
                SUM(od.Quantity * od.UnitPrice * (1 - od.Discount)) AS TotalSpent,
                AVG(od.Quantity * od.UnitPrice * (1 - od.Discount)) AS AvgOrderAmount,
                MAX(o.OrderDate) AS LastPurchaseDate,
                DATEDIFF(DAY, MAX(o.OrderDate), @CurrentDate) AS DaysSinceLastPurchase
            FROM Customers c
            LEFT JOIN Orders o ON c.CustomerId = o.CustomerId
            LEFT JOIN OrderDetails od ON o.OrderId = od.OrderId
            LEFT JOIN Products p ON od.ProductId = p.ProductId
            LEFT JOIN Categories cat ON p.CategoryId = cat.CategoryId
            LEFT JOIN Suppliers sup ON p.SupplierId = sup.SupplierId
            LEFT JOIN Shippers sh ON o.ShipVia = sh.ShipperId
            WHERE c.IsActive = 1
            GROUP BY 
                c.CustomerId, c.CustomerName, c.CustomerTypeId, 
                c.RegionId, c.CreditLimit
        ),
        ProductSalesMetrics AS (
            SELECT 
                p.ProductId,
                p.ProductName,
                p.CategoryId,
                p.SupplierId,
                p.UnitPrice,
                p.UnitsInStock,
                p.UnitsOnOrder,
                p.ReorderLevel,
                COUNT(DISTINCT od.OrderId) AS OrderCount,
                SUM(od.Quantity) AS TotalQuantitySold,
                SUM(od.Quantity * od.UnitPrice * (1 - od.Discount)) AS TotalRevenue,
                AVG(od.UnitPrice * (1 - od.Discount)) AS AvgSellingPrice,
                MAX(o.OrderDate) AS LastSaleDate
            FROM Products p
            LEFT JOIN OrderDetails od ON p.ProductId = od.ProductId
            LEFT JOIN Orders o ON od.OrderId = o.OrderId
            LEFT JOIN Categories c ON p.CategoryId = c.CategoryId
            LEFT JOIN Suppliers s ON p.SupplierId = s.SupplierId
            GROUP BY 
                p.ProductId, p.ProductName, p.CategoryId, p.SupplierId,
                p.UnitPrice, p.UnitsInStock, p.UnitsOnOrder, p.ReorderLevel
        ),
        RegionalSalesSummary AS (
            SELECT 
                r.RegionId,
                r.RegionName,
                r.CountryId,
                COUNT(DISTINCT c.CustomerId) AS CustomerCount,
                COUNT(DISTINCT o.OrderId) AS OrderCount,
                SUM(od.Quantity * od.UnitPrice * (1 - od.Discount)) AS TotalSales,
                AVG(od.Quantity * od.UnitPrice * (1 - od.Discount)) AS AvgOrderValue,
                COUNT(DISTINCT e.EmployeeId) AS EmployeeCount
            FROM Regions r
            LEFT JOIN Customers c ON r.RegionId = c.RegionId
            LEFT JOIN Orders o ON c.CustomerId = o.CustomerId
            LEFT JOIN OrderDetails od ON o.OrderId = od.OrderId
            LEFT JOIN Employees e ON o.SalesPersonId = e.EmployeeId
            LEFT JOIN Departments d ON e.DepartmentId = d.DepartmentId
            LEFT JOIN Products p ON od.ProductId = p.ProductId
            LEFT JOIN Categories cat ON p.CategoryId = cat.CategoryId
            GROUP BY r.RegionId, r.RegionName, r.CountryId
        )
        
        -- 복잡한 조인과 서브쿼리를 사용한 데이터 삽입
        INSERT INTO #TempProcessingData (
            CustomerId, OrderId, ProductId, EmployeeId, DepartmentId, 
            Amount, Status, ProcessDate
        )
        SELECT 
            ca.CustomerId,
            o.OrderId,
            psm.ProductId,
            ep.EmployeeId,
            dh.DepartmentId,
            (SELECT SUM(od2.Quantity * od2.UnitPrice * (1 - od2.Discount))
             FROM OrderDetails od2
             WHERE od2.OrderId = o.OrderId
               AND od2.ProductId IN (
                   SELECT p2.ProductId
                   FROM Products p2
                   WHERE p2.CategoryId IN (
                       SELECT c2.CategoryId
                       FROM Categories c2
                       WHERE c2.CategoryName IN (
                           SELECT TOP 5 cat.CategoryName
                           FROM Categories cat
                           INNER JOIN Products pr ON cat.CategoryId = pr.CategoryId
                           INNER JOIN OrderDetails od3 ON pr.ProductId = od3.ProductId
                           GROUP BY cat.CategoryName
                           ORDER BY SUM(od3.Quantity * od3.UnitPrice) DESC
                       )
                   )
               )
            ) AS Amount,
            CASE 
                WHEN (SELECT COUNT(*)
                      FROM Orders o2
                      WHERE o2.CustomerId = ca.CustomerId
                        AND o2.OrderDate >= @PreviousMonth
                        AND o2.OrderDate < @CurrentDate) > 10 
                THEN 'VIP'
                WHEN (SELECT AVG(od4.Quantity * od4.UnitPrice)
                      FROM OrderDetails od4
                      INNER JOIN Orders o3 ON od4.OrderId = o3.OrderId
                      WHERE o3.CustomerId = ca.CustomerId) > 1000
                THEN 'Premium'
                ELSE 'Standard'
            END AS Status,
            @CurrentDate
        FROM CustomerAnalysis ca
        INNER JOIN Orders o ON ca.CustomerId = o.CustomerId
        INNER JOIN OrderDetails od ON o.OrderId = od.OrderId
        INNER JOIN ProductSalesMetrics psm ON od.ProductId = psm.ProductId
        INNER JOIN Employees e ON o.SalesPersonId = e.EmployeeId
        INNER JOIN EmployeePerformance ep ON e.EmployeeId = ep.EmployeeId
        INNER JOIN DepartmentHierarchy dh ON e.DepartmentId = dh.DepartmentId
        INNER JOIN RegionalSalesSummary rss ON ca.RegionId = rss.RegionId
        INNER JOIN Products p ON od.ProductId = p.ProductId
        INNER JOIN Categories c ON p.CategoryId = c.CategoryId
        INNER JOIN Suppliers s ON p.SupplierId = s.SupplierId
        WHERE o.OrderDate >= @PreviousMonth
          AND o.OrderDate < @CurrentDate
          AND (ISNULL(@DepartmentId, dh.DepartmentId) = dh.DepartmentId)
          AND (ISNULL(@EmployeeId, ep.EmployeeId) = ep.EmployeeId)
          AND EXISTS (
              SELECT 1
              FROM OrderDetails od5
              WHERE od5.OrderId = o.OrderId
                AND od5.ProductId IN (
                    SELECT p3.ProductId
                    FROM Products p3
                    WHERE p3.UnitsInStock < (
                        SELECT AVG(p4.UnitsInStock)
                        FROM Products p4
                        WHERE p4.CategoryId = p3.CategoryId
                    )
                )
          )
          AND NOT EXISTS (
              SELECT 1
              FROM OrderCancellations oc
              WHERE oc.OrderId = o.OrderId
                AND oc.CancellationDate >= @PreviousMonth
          );
        
        SET @RowCount = @@ROWCOUNT;
        
        -- ============================================
        -- 2단계: 복잡한 서브쿼리를 사용한 업데이트
        -- ============================================
        UPDATE tpd
        SET 
            Amount = (
                SELECT 
                    SUM(od.Quantity * od.UnitPrice * (1 - od.Discount)) * 
                    (1 + (SELECT ISNULL(SUM(d.DiscountPercent), 0) / 100.0
                          FROM Discounts d
                          WHERE d.CustomerId = tpd.CustomerId
                            AND d.StartDate <= @CurrentDate
                            AND d.EndDate >= @CurrentDate
                            AND d.IsActive = 1))
                FROM OrderDetails od
                INNER JOIN Orders o ON od.OrderId = o.OrderId
                WHERE o.OrderId = tpd.OrderId
                  AND od.ProductId = tpd.ProductId
            ),
            Status = (
                SELECT 
                    CASE 
                        WHEN COUNT(DISTINCT o2.OrderId) > (
                            SELECT AVG(CAST(OrderCount AS FLOAT))
                            FROM (
                                SELECT COUNT(DISTINCT o3.OrderId) AS OrderCount
                                FROM Orders o3
                                INNER JOIN Customers c2 ON o3.CustomerId = c2.CustomerId
                                WHERE c2.RegionId = (
                                    SELECT c3.RegionId
                                    FROM Customers c3
                                    WHERE c3.CustomerId = tpd.CustomerId
                                )
                                GROUP BY c2.CustomerId
                            ) AS SubQuery
                        ) * 1.5
                        THEN 'High Value'
                        WHEN (
                            SELECT SUM(od2.Quantity * od2.UnitPrice)
                            FROM OrderDetails od2
                            WHERE od2.OrderId = tpd.OrderId
                        ) > (
                            SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY TotalAmount)
                            FROM (
                                SELECT 
                                    SUM(od3.Quantity * od3.UnitPrice) AS TotalAmount
                                FROM OrderDetails od3
                                GROUP BY od3.OrderId
                            ) AS PercentileQuery
                        )
                        THEN 'Above Average'
                        ELSE 'Standard'
                    END
            )
        FROM #TempProcessingData tpd
        WHERE tpd.IsProcessed = 0
          AND EXISTS (
              SELECT 1
              FROM Orders o
              WHERE o.OrderId = tpd.OrderId
                AND o.OrderDate >= @PreviousMonth
          );
        
        -- ============================================
        -- 3단계: 복잡한 MERGE 문
        -- ============================================
        MERGE CustomerOrderSummary AS target
        USING (
            SELECT 
                tpd.CustomerId,
                tpd.DepartmentId,
                COUNT(DISTINCT tpd.OrderId) AS OrderCount,
                SUM(tpd.Amount) AS TotalAmount,
                AVG(tpd.Amount) AS AvgAmount,
                MAX(tpd.ProcessDate) AS LastProcessDate,
                MIN(tpd.ProcessDate) AS FirstProcessDate,
                COUNT(DISTINCT tpd.ProductId) AS ProductCount,
                COUNT(DISTINCT tpd.EmployeeId) AS EmployeeCount,
                STRING_AGG(DISTINCT tpd.Status, ', ') AS StatusList
            FROM #TempProcessingData tpd
            WHERE tpd.IsProcessed = 0
            GROUP BY tpd.CustomerId, tpd.DepartmentId
        ) AS source
        ON target.CustomerId = source.CustomerId
           AND target.DepartmentId = source.DepartmentId
           AND target.ProcessYear = YEAR(@CurrentDate)
           AND target.ProcessMonth = MONTH(@CurrentDate)
        WHEN MATCHED AND (
            target.TotalAmount <> source.TotalAmount
            OR target.OrderCount <> source.OrderCount
            OR target.LastProcessDate < source.LastProcessDate
        ) THEN
            UPDATE SET
                OrderCount = source.OrderCount,
                TotalAmount = source.TotalAmount,
                AvgAmount = source.AvgAmount,
                LastProcessDate = source.LastProcessDate,
                FirstProcessDate = CASE 
                    WHEN target.FirstProcessDate > source.FirstProcessDate 
                    THEN source.FirstProcessDate 
                    ELSE target.FirstProcessDate 
                END,
                ProductCount = source.ProductCount,
                EmployeeCount = source.EmployeeCount,
                StatusList = source.StatusList,
                UpdatedDate = @CurrentDate,
                UpdatedBy = SYSTEM_USER
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                CustomerId, DepartmentId, ProcessYear, ProcessMonth,
                OrderCount, TotalAmount, AvgAmount, LastProcessDate,
                FirstProcessDate, ProductCount, EmployeeCount, StatusList,
                CreatedDate, CreatedBy, UpdatedDate, UpdatedBy
            )
            VALUES (
                source.CustomerId, source.DepartmentId,
                YEAR(@CurrentDate), MONTH(@CurrentDate),
                source.OrderCount, source.TotalAmount, source.AvgAmount,
                source.LastProcessDate, source.FirstProcessDate,
                source.ProductCount, source.EmployeeCount, source.StatusList,
                @CurrentDate, SYSTEM_USER, @CurrentDate, SYSTEM_USER
            )
        WHEN NOT MATCHED BY SOURCE 
            AND target.ProcessYear = YEAR(@CurrentDate)
            AND target.ProcessMonth = MONTH(@CurrentDate)
            AND target.LastProcessDate < DATEADD(DAY, -90, @CurrentDate) THEN
            DELETE
        OUTPUT 
            $action AS ActionType,
            INSERTED.CustomerId AS NewCustomerId,
            DELETED.CustomerId AS OldCustomerId,
            INSERTED.TotalAmount AS NewTotalAmount,
            DELETED.TotalAmount AS OldTotalAmount
        INTO #TempMergeResults;
        
        -- ============================================
        -- 4단계: 복잡한 DELETE 문 (조건부 삭제)
        -- ============================================
        DELETE FROM OrderProcessingQueue
        WHERE QueueId IN (
            SELECT opq.QueueId
            FROM OrderProcessingQueue opq
            INNER JOIN Orders o ON opq.OrderId = o.OrderId
            INNER JOIN Customers c ON o.CustomerId = c.CustomerId
            INNER JOIN OrderDetails od ON o.OrderId = od.OrderId
            INNER JOIN Products p ON od.ProductId = p.ProductId
            INNER JOIN Categories cat ON p.CategoryId = cat.CategoryId
            INNER JOIN Suppliers s ON p.SupplierId = s.SupplierId
            INNER JOIN Employees e ON o.SalesPersonId = e.EmployeeId
            INNER JOIN Departments d ON e.DepartmentId = d.DepartmentId
            INNER JOIN Regions r ON c.RegionId = r.RegionId
            WHERE opq.Status = 'Processed'
              AND opq.ProcessedDate < DATEADD(DAY, -30, @CurrentDate)
              AND EXISTS (
                  SELECT 1
                  FROM #TempProcessingData tpd
                  WHERE tpd.OrderId = opq.OrderId
                    AND tpd.IsProcessed = 1
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM OrderCancellations oc
                  WHERE oc.OrderId = opq.OrderId
              )
              AND (
                  SELECT COUNT(*)
                  FROM OrderDetails od2
                  WHERE od2.OrderId = opq.OrderId
                    AND od2.ProductId IN (
                        SELECT p2.ProductId
                        FROM Products p2
                        WHERE p2.Discontinued = 1
                    )
              ) = 0
        );
        
        -- ============================================
        -- 5단계: 복잡한 INSERT 문 (다중 테이블 조인)
        -- ============================================
        INSERT INTO SalesPerformanceReport (
            ReportDate, DepartmentId, EmployeeId, CustomerId,
            OrderCount, TotalSales, AvgOrderValue, ProductCount,
            CategoryCount, SupplierCount, RegionId, Status,
            CreatedDate, CreatedBy
        )
        SELECT 
            @CurrentDate AS ReportDate,
            dh.DepartmentId,
            ep.EmployeeId,
            ca.CustomerId,
            COUNT(DISTINCT o.OrderId) AS OrderCount,
            SUM(od.Quantity * od.UnitPrice * (1 - od.Discount)) AS TotalSales,
            AVG(od.Quantity * od.UnitPrice * (1 - od.Discount)) AS AvgOrderValue,
            COUNT(DISTINCT psm.ProductId) AS ProductCount,
            COUNT(DISTINCT c.CategoryId) AS CategoryCount,
            COUNT(DISTINCT s.SupplierId) AS SupplierCount,
            rss.RegionId,
            CASE 
                WHEN SUM(od.Quantity * od.UnitPrice * (1 - od.Discount)) > (
                    SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY TotalSales)
                    FROM (
                        SELECT 
                            SUM(od2.Quantity * od2.UnitPrice * (1 - od2.Discount)) AS TotalSales
                        FROM OrderDetails od2
                        INNER JOIN Orders o2 ON od2.OrderId = o2.OrderId
                        WHERE o2.OrderDate >= @PreviousMonth
                        GROUP BY o2.SalesPersonId, o2.CustomerId
                    ) AS SalesPercentile
                )
                THEN 'Top Performer'
                WHEN SUM(od.Quantity * od.UnitPrice * (1 - od.Discount)) > (
                    SELECT AVG(TotalSales)
                    FROM (
                        SELECT 
                            SUM(od3.Quantity * od3.UnitPrice * (1 - od3.Discount)) AS TotalSales
                        FROM OrderDetails od3
                        INNER JOIN Orders o3 ON od3.OrderId = o3.OrderId
                        WHERE o3.OrderDate >= @PreviousMonth
                        GROUP BY o3.SalesPersonId, o3.CustomerId
                    ) AS AvgSales
                )
                THEN 'Above Average'
                ELSE 'Standard'
            END AS Status,
            @CurrentDate AS CreatedDate,
            SYSTEM_USER AS CreatedBy
        FROM #TempProcessingData tpd
        INNER JOIN Customers cus ON tpd.CustomerId = cus.CustomerId
        INNER JOIN CustomerAnalysis ca ON cus.CustomerId = ca.CustomerId
        INNER JOIN Orders o ON tpd.OrderId = o.OrderId
        INNER JOIN OrderDetails od ON o.OrderId = od.OrderId
        INNER JOIN Products p ON od.ProductId = p.ProductId
        INNER JOIN ProductSalesMetrics psm ON p.ProductId = psm.ProductId
        INNER JOIN Categories c ON p.CategoryId = c.CategoryId
        INNER JOIN Suppliers s ON p.SupplierId = s.SupplierId
        INNER JOIN Employees e ON tpd.EmployeeId = e.EmployeeId
        INNER JOIN EmployeePerformance ep ON e.EmployeeId = ep.EmployeeId
        INNER JOIN DepartmentHierarchy dh ON e.DepartmentId = dh.DepartmentId
        INNER JOIN RegionalSalesSummary rss ON ca.RegionId = rss.RegionId
        INNER JOIN Shippers sh ON o.ShipVia = sh.ShipperId
        WHERE tpd.IsProcessed = 0
          AND tpd.ProcessDate >= @PreviousMonth
          AND NOT EXISTS (
              SELECT 1
              FROM SalesPerformanceReport spr
              WHERE spr.ReportDate = @CurrentDate
                AND spr.DepartmentId = dh.DepartmentId
                AND spr.EmployeeId = ep.EmployeeId
                AND spr.CustomerId = ca.CustomerId
          )
        GROUP BY 
            dh.DepartmentId, ep.EmployeeId, ca.CustomerId, rss.RegionId;
        
        -- ============================================
        -- 6단계: 복잡한 집계 및 검증
        -- ============================================
        INSERT INTO #TempAggregatedData (
            DepartmentId, EmployeeId, TotalAmount, RecordCount,
            AverageAmount, MaxAmount, MinAmount
        )
        SELECT 
            dh.DepartmentId,
            ep.EmployeeId,
            SUM(tpd.Amount) AS TotalAmount,
            COUNT(*) AS RecordCount,
            AVG(tpd.Amount) AS AverageAmount,
            MAX(tpd.Amount) AS MaxAmount,
            MIN(tpd.Amount) AS MinAmount
        FROM #TempProcessingData tpd
        INNER JOIN Employees e ON tpd.EmployeeId = e.EmployeeId
        INNER JOIN EmployeePerformance ep ON e.EmployeeId = ep.EmployeeId
        INNER JOIN DepartmentHierarchy dh ON e.DepartmentId = dh.DepartmentId
        INNER JOIN Customers c ON tpd.CustomerId = c.CustomerId
        INNER JOIN Orders o ON tpd.OrderId = o.OrderId
        INNER JOIN OrderDetails od ON o.OrderId = od.OrderId
        INNER JOIN Products p ON od.ProductId = p.ProductId
        INNER JOIN Categories cat ON p.CategoryId = cat.CategoryId
        WHERE tpd.IsProcessed = 0
          AND EXISTS (
              SELECT 1
              FROM RegionalSalesSummary rss
              WHERE rss.RegionId = c.RegionId
                AND rss.TotalSales > (
                    SELECT AVG(TotalSales)
                    FROM RegionalSalesSummary
                )
          )
        GROUP BY dh.DepartmentId, ep.EmployeeId
        HAVING SUM(tpd.Amount) > (
            SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY TotalAmount)
            FROM (
                SELECT 
                    SUM(tpd2.Amount) AS TotalAmount
                FROM #TempProcessingData tpd2
                GROUP BY tpd2.DepartmentId, tpd2.EmployeeId
            ) AS MedianQuery
        );
        
        -- ============================================
        -- 7단계: 복잡한 검증 로직
        -- ============================================
        INSERT INTO #TempValidationResults (
            RecordId, ValidationType, IsValid, ValidationMessage
        )
        SELECT 
            tpd.Id,
            'Amount Validation',
            CASE 
                WHEN tpd.Amount > 0 
                     AND tpd.Amount <= (
                         SELECT MAX(od.Quantity * od.UnitPrice * (1 - od.Discount))
                         FROM OrderDetails od
                         INNER JOIN Orders o ON od.OrderId = o.OrderId
                         WHERE o.OrderId = tpd.OrderId
                     ) * 1.1
                THEN 1
                ELSE 0
            END,
            CASE 
                WHEN tpd.Amount <= 0 THEN 'Amount must be greater than zero'
                WHEN tpd.Amount > (
                    SELECT MAX(od.Quantity * od.UnitPrice * (1 - od.Discount))
                    FROM OrderDetails od
                    INNER JOIN Orders o ON od.OrderId = o.OrderId
                    WHERE o.OrderId = tpd.OrderId
                ) * 1.1
                THEN 'Amount exceeds maximum allowed value'
                ELSE 'Valid'
            END
        FROM #TempProcessingData tpd
        WHERE tpd.IsProcessed = 0
          AND EXISTS (
              SELECT 1
              FROM Orders o
              WHERE o.OrderId = tpd.OrderId
                AND o.OrderDate >= @PreviousMonth
          );
        
        INSERT INTO #TempValidationResults (
            RecordId, ValidationType, IsValid, ValidationMessage
        )
        SELECT 
            tpd.Id,
            'Customer Status Validation',
            CASE 
                WHEN EXISTS (
                    SELECT 1
                    FROM Customers c
                    WHERE c.CustomerId = tpd.CustomerId
                      AND c.IsActive = 1
                      AND c.CreditLimit >= (
                          SELECT SUM(od.Quantity * od.UnitPrice * (1 - od.Discount))
                          FROM OrderDetails od
                          INNER JOIN Orders o ON od.OrderId = o.OrderId
                          WHERE o.CustomerId = tpd.CustomerId
                            AND o.OrderDate >= @PreviousMonth
                      )
                )
                THEN 1
                ELSE 0
            END,
            CASE 
                WHEN NOT EXISTS (
                    SELECT 1
                    FROM Customers c
                    WHERE c.CustomerId = tpd.CustomerId
                      AND c.IsActive = 1
                )
                THEN 'Customer is not active'
                WHEN EXISTS (
                    SELECT 1
                    FROM Customers c
                    WHERE c.CustomerId = tpd.CustomerId
                      AND c.CreditLimit < (
                          SELECT SUM(od.Quantity * od.UnitPrice * (1 - od.Discount))
                          FROM OrderDetails od
                          INNER JOIN Orders o ON od.OrderId = o.OrderId
                          WHERE o.CustomerId = tpd.CustomerId
                            AND o.OrderDate >= @PreviousMonth
                      )
                )
                THEN 'Customer credit limit exceeded'
                ELSE 'Valid'
            END
        FROM #TempProcessingData tpd
        WHERE tpd.IsProcessed = 0;
        
        -- ============================================
        -- 8단계: 오류 처리 및 상태 업데이트
        -- ============================================
        UPDATE tpd
        SET 
            IsProcessed = 1,
            Status = CASE 
                WHEN EXISTS (
                    SELECT 1
                    FROM #TempValidationResults tvr
                    WHERE tvr.RecordId = tpd.Id
                      AND tvr.IsValid = 0
                )
                THEN 'Validation Failed'
                WHEN tpd.Amount IS NULL OR tpd.Amount <= 0
                THEN 'Invalid Amount'
                ELSE 'Processed'
            END,
            ErrorMessage = (
                SELECT STRING_AGG(tvr.ValidationMessage, '; ')
                FROM #TempValidationResults tvr
                WHERE tvr.RecordId = tpd.Id
                  AND tvr.IsValid = 0
            )
        FROM #TempProcessingData tpd
        WHERE tpd.IsProcessed = 0
          AND EXISTS (
              SELECT 1
              FROM Orders o
              WHERE o.OrderId = tpd.OrderId
          );
        
        -- ============================================
        -- 9단계: 최종 집계 및 리포트 생성
        -- ============================================
        INSERT INTO ProcessExecutionLog (
            ProcessId, ProcessName, StartTime, EndTime,
            RecordsProcessed, RecordsSucceeded, RecordsFailed,
            TotalAmount, AverageAmount, DepartmentCount,
            EmployeeCount, CustomerCount, ErrorCount, Status
        )
        SELECT 
            @ProcessId,
            'ComplexBusinessProcess',
            @StartTime,
            GETDATE(),
            COUNT(*) AS RecordsProcessed,
            SUM(CASE WHEN tpd.Status = 'Processed' THEN 1 ELSE 0 END) AS RecordsSucceeded,
            SUM(CASE WHEN tpd.Status <> 'Processed' THEN 1 ELSE 0 END) AS RecordsFailed,
            SUM(tpd.Amount) AS TotalAmount,
            AVG(tpd.Amount) AS AverageAmount,
            COUNT(DISTINCT tpd.DepartmentId) AS DepartmentCount,
            COUNT(DISTINCT tpd.EmployeeId) AS EmployeeCount,
            COUNT(DISTINCT tpd.CustomerId) AS CustomerCount,
            COUNT(DISTINCT tvr.RecordId) AS ErrorCount,
            CASE 
                WHEN SUM(CASE WHEN tpd.Status <> 'Processed' THEN 1 ELSE 0 END) = 0
                THEN 'Success'
                WHEN SUM(CASE WHEN tpd.Status <> 'Processed' THEN 1 ELSE 0 END) < COUNT(*) * 0.1
                THEN 'Partial Success'
                ELSE 'Failed'
            END AS Status
        FROM #TempProcessingData tpd
        LEFT JOIN #TempValidationResults tvr ON tpd.Id = tvr.RecordId AND tvr.IsValid = 0
        WHERE tpd.IsProcessed = 1;
        
        -- ============================================
        -- 10단계: 디버그 모드 출력
        -- ============================================
        IF @DebugMode = 1
        BEGIN
            SELECT 
                'Processing Summary' AS InfoType,
                COUNT(*) AS TotalRecords,
                SUM(CASE WHEN IsProcessed = 1 THEN 1 ELSE 0 END) AS ProcessedRecords,
                SUM(CASE WHEN IsProcessed = 0 THEN 1 ELSE 0 END) AS UnprocessedRecords,
                SUM(Amount) AS TotalAmount,
                AVG(Amount) AS AverageAmount,
                MIN(Amount) AS MinAmount,
                MAX(Amount) AS MaxAmount
            FROM #TempProcessingData;
            
            SELECT 
                'Department Summary' AS InfoType,
                tad.DepartmentId,
                COUNT(DISTINCT tad.EmployeeId) AS EmployeeCount,
                tad.TotalAmount,
                tad.RecordCount,
                tad.AverageAmount
            FROM #TempAggregatedData tad
            ORDER BY tad.TotalAmount DESC;
            
            SELECT 
                'Validation Results' AS InfoType,
                tvr.ValidationType,
                COUNT(*) AS TotalValidations,
                SUM(CASE WHEN tvr.IsValid = 1 THEN 1 ELSE 0 END) AS ValidCount,
                SUM(CASE WHEN tvr.IsValid = 0 THEN 1 ELSE 0 END) AS InvalidCount
            FROM #TempValidationResults tvr
            GROUP BY tvr.ValidationType;
        END;
        
        -- ============================================
        -- 10-1단계: 프로시저 호출 예제
        -- ============================================
        -- 데이터 검증 프로시저 호출
        IF EXISTS (SELECT 1 FROM sys.procedures WHERE name = 'sp_ValidateProcessingData')
        BEGIN
            EXEC sp_ValidateProcessingData 
                @ProcessId = @ProcessId,
                @ProcessDate = @CurrentDate;
        END;
        
        -- 통계 계산 프로시저 호출
        IF EXISTS (SELECT 1 FROM sys.procedures WHERE name = 'sp_CalculateStatistics')
        BEGIN
            EXEC dbo.sp_CalculateStatistics 
                @DepartmentId = @DepartmentId,
                @StartDate = @PreviousMonth,
                @EndDate = @CurrentDate;
        END;
        
        -- 로그 정리 프로시저 호출 (조건부)
        IF @DebugMode = 0
        BEGIN
            EXEC [dbo].[sp_CleanupOldLogs] 
                @RetentionDays = 90,
                @ProcessType = 'ComplexBusinessProcess';
        END;
        
        -- CALL 키워드를 사용한 프로시저 호출 (Oracle/MySQL 스타일)
        IF EXISTS (SELECT 1 FROM sys.procedures WHERE name = 'sp_UpdateCustomerMetrics')
        BEGIN
            CALL sp_UpdateCustomerMetrics(
                @ProcessId,
                @CurrentDate
            );
        END;
        
        -- CALL 키워드로 스키마 포함 프로시저 호출
        IF EXISTS (SELECT 1 FROM sys.procedures WHERE name = 'sp_GenerateReport')
        BEGIN
            CALL dbo.sp_GenerateReport(
                @DepartmentId = @DepartmentId,
                @StartDate = @PreviousMonth,
                @EndDate = @CurrentDate
            );
        END;
        
        -- ============================================
        -- 11단계: 추가 복잡한 데이터 마이그레이션 및 변환
        -- ============================================
        WITH ComplexDataTransformation AS (
            SELECT 
                tpd.Id,
                tpd.CustomerId,
                tpd.OrderId,
                tpd.ProductId,
                tpd.EmployeeId,
                tpd.DepartmentId,
                tpd.Amount,
                tpd.Status,
                (SELECT 
                    COUNT(DISTINCT o2.OrderId)
                 FROM Orders o2
                 INNER JOIN OrderDetails od2 ON o2.OrderId = od2.OrderId
                 INNER JOIN Products p2 ON od2.ProductId = p2.ProductId
                 INNER JOIN Categories c2 ON p2.CategoryId = c2.CategoryId
                 WHERE o2.CustomerId = tpd.CustomerId
                   AND o2.OrderDate >= @PreviousMonth
                   AND c2.CategoryId IN (
                       SELECT c3.CategoryId
                       FROM Categories c3
                       INNER JOIN Products p3 ON c3.CategoryId = p3.CategoryId
                       WHERE p3.ProductId = tpd.ProductId
                   )
                ) AS SimilarOrderCount,
                (SELECT 
                    AVG(od3.Quantity * od3.UnitPrice * (1 - od3.Discount))
                 FROM OrderDetails od3
                 INNER JOIN Orders o3 ON od3.OrderId = o3.OrderId
                 INNER JOIN Products p3 ON od3.ProductId = p3.ProductId
                 WHERE p3.CategoryId = (
                     SELECT p4.CategoryId
                     FROM Products p4
                     WHERE p4.ProductId = tpd.ProductId
                 )
                   AND o3.OrderDate >= @PreviousMonth
                   AND o3.CustomerId IN (
                       SELECT c4.CustomerId
                       FROM Customers c4
                       WHERE c4.RegionId = (
                           SELECT c5.RegionId
                           FROM Customers c5
                           WHERE c5.CustomerId = tpd.CustomerId
                       )
                   )
                ) AS RegionalCategoryAverage
            FROM #TempProcessingData tpd
            WHERE tpd.IsProcessed = 1
        ),
        EmployeeRanking AS (
            SELECT 
                e.EmployeeId,
                e.DepartmentId,
                e.EmployeeName,
                ROW_NUMBER() OVER (
                    PARTITION BY e.DepartmentId 
                    ORDER BY (
                        SELECT SUM(od.Quantity * od.UnitPrice * (1 - od.Discount))
                        FROM Orders o
                        INNER JOIN OrderDetails od ON o.OrderId = od.OrderId
                        WHERE o.SalesPersonId = e.EmployeeId
                          AND o.OrderDate >= @PreviousMonth
                    ) DESC
                ) AS SalesRank,
                (SELECT 
                    SUM(od.Quantity * od.UnitPrice * (1 - od.Discount))
                 FROM Orders o
                 INNER JOIN OrderDetails od ON o.OrderId = od.OrderId
                 INNER JOIN Products p ON od.ProductId = p.ProductId
                 INNER JOIN Categories c ON p.CategoryId = c.CategoryId
                 WHERE o.SalesPersonId = e.EmployeeId
                   AND o.OrderDate >= @PreviousMonth
                   AND c.CategoryId IN (
                       SELECT TOP 3 c2.CategoryId
                       FROM Categories c2
                       INNER JOIN Products p2 ON c2.CategoryId = p2.CategoryId
                       INNER JOIN OrderDetails od2 ON p2.ProductId = od2.ProductId
                       INNER JOIN Orders o2 ON od2.OrderId = o2.OrderId
                       WHERE o2.SalesPersonId = e.EmployeeId
                       GROUP BY c2.CategoryId
                       ORDER BY SUM(od2.Quantity * od2.UnitPrice * (1 - od2.Discount)) DESC
                   )
                ) AS TopCategorySales
            FROM Employees e
            INNER JOIN Departments d ON e.DepartmentId = d.DepartmentId
            WHERE e.IsActive = 1
        ),
        CustomerSegmentation AS (
            SELECT 
                c.CustomerId,
                c.CustomerName,
                c.RegionId,
                CASE 
                    WHEN (SELECT 
                              SUM(od.Quantity * od.UnitPrice * (1 - od.Discount))
                          FROM Orders o
                          INNER JOIN OrderDetails od ON o.OrderId = od.OrderId
                          WHERE o.CustomerId = c.CustomerId
                            AND o.OrderDate >= @PreviousMonth
                          ) > (
                              SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY TotalSpent)
                              FROM (
                                  SELECT 
                                      SUM(od2.Quantity * od2.UnitPrice * (1 - od2.Discount)) AS TotalSpent
                                  FROM Orders o2
                                  INNER JOIN OrderDetails od2 ON o2.OrderId = od2.OrderId
                                  WHERE o2.OrderDate >= @PreviousMonth
                                  GROUP BY o2.CustomerId
                              ) AS CustomerSpending
                          )
                    THEN 'Platinum'
                    WHEN (SELECT 
                              COUNT(DISTINCT o.OrderId)
                          FROM Orders o
                          WHERE o.CustomerId = c.CustomerId
                            AND o.OrderDate >= @PreviousMonth
                          ) > (
                              SELECT AVG(CAST(OrderCount AS FLOAT))
                              FROM (
                                  SELECT 
                                      COUNT(DISTINCT o2.OrderId) AS OrderCount
                                  FROM Orders o2
                                  WHERE o2.OrderDate >= @PreviousMonth
                                  GROUP BY o2.CustomerId
                              ) AS OrderCounts
                          ) * 1.5
                    THEN 'Gold'
                    WHEN (SELECT 
                              COUNT(DISTINCT p.ProductId)
                          FROM Orders o
                          INNER JOIN OrderDetails od ON o.OrderId = od.OrderId
                          INNER JOIN Products p ON od.ProductId = p.ProductId
                          WHERE o.CustomerId = c.CustomerId
                            AND o.OrderDate >= @PreviousMonth
                          ) > 10
                    THEN 'Silver'
                    ELSE 'Bronze'
                END AS CustomerSegment,
                (SELECT 
                    STRING_AGG(DISTINCT cat.CategoryName, ', ')
                 FROM Orders o
                 INNER JOIN OrderDetails od ON o.OrderId = od.OrderId
                 INNER JOIN Products p ON od.ProductId = p.ProductId
                 INNER JOIN Categories cat ON p.CategoryId = cat.CategoryId
                 WHERE o.CustomerId = c.CustomerId
                   AND o.OrderDate >= @PreviousMonth
                 ) AS PreferredCategories
            FROM Customers c
            WHERE c.IsActive = 1
        )
        
        -- 복잡한 데이터 업데이트 및 삽입
        UPDATE cdt
        SET 
            Status = CASE 
                WHEN cdt.SimilarOrderCount > (
                    SELECT AVG(CAST(SimilarOrderCount AS FLOAT))
                    FROM ComplexDataTransformation
                ) * 1.2
                THEN 'High Frequency'
                WHEN cdt.RegionalCategoryAverage > (
                    SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY RegionalCategoryAverage)
                    FROM ComplexDataTransformation
                )
                THEN 'Above Regional Average'
                ELSE cdt.Status
            END
        FROM #TempProcessingData tpd
        INNER JOIN ComplexDataTransformation cdt ON tpd.Id = cdt.Id
        WHERE tpd.IsProcessed = 1;
        
        -- 추가 복잡한 INSERT 문
        INSERT INTO CustomerSegmentAnalysis (
            AnalysisDate, CustomerId, CustomerSegment, PreferredCategories,
            TotalOrders, TotalSpent, AvgOrderValue, EmployeeId, DepartmentId,
            SalesRank, TopCategorySales, CreatedDate, CreatedBy
        )
        SELECT 
            @CurrentDate AS AnalysisDate,
            cs.CustomerId,
            cs.CustomerSegment,
            cs.PreferredCategories,
            (SELECT COUNT(DISTINCT o.OrderId)
             FROM Orders o
             WHERE o.CustomerId = cs.CustomerId
               AND o.OrderDate >= @PreviousMonth
            ) AS TotalOrders,
            (SELECT SUM(od.Quantity * od.UnitPrice * (1 - od.Discount))
             FROM Orders o
             INNER JOIN OrderDetails od ON o.OrderId = od.OrderId
             WHERE o.CustomerId = cs.CustomerId
               AND o.OrderDate >= @PreviousMonth
            ) AS TotalSpent,
            (SELECT AVG(od.Quantity * od.UnitPrice * (1 - od.Discount))
             FROM Orders o
             INNER JOIN OrderDetails od ON o.OrderId = od.OrderId
             WHERE o.CustomerId = cs.CustomerId
               AND o.OrderDate >= @PreviousMonth
            ) AS AvgOrderValue,
            (SELECT TOP 1 o.SalesPersonId
             FROM Orders o
             WHERE o.CustomerId = cs.CustomerId
               AND o.OrderDate >= @PreviousMonth
             GROUP BY o.SalesPersonId
             ORDER BY COUNT(DISTINCT o.OrderId) DESC
            ) AS EmployeeId,
            (SELECT TOP 1 e.DepartmentId
             FROM Orders o
             INNER JOIN Employees e ON o.SalesPersonId = e.EmployeeId
             WHERE o.CustomerId = cs.CustomerId
               AND o.OrderDate >= @PreviousMonth
             GROUP BY e.DepartmentId
             ORDER BY COUNT(DISTINCT o.OrderId) DESC
            ) AS DepartmentId,
            er.SalesRank,
            er.TopCategorySales,
            @CurrentDate AS CreatedDate,
            SYSTEM_USER AS CreatedBy
        FROM CustomerSegmentation cs
        LEFT JOIN Orders o ON cs.CustomerId = o.CustomerId
        LEFT JOIN Employees e ON o.SalesPersonId = e.EmployeeId
        LEFT JOIN EmployeeRanking er ON e.EmployeeId = er.EmployeeId
        WHERE o.OrderDate >= @PreviousMonth
          AND NOT EXISTS (
              SELECT 1
              FROM CustomerSegmentAnalysis csa
              WHERE csa.CustomerId = cs.CustomerId
                AND csa.AnalysisDate = @CurrentDate
          )
        GROUP BY 
            cs.CustomerId, cs.CustomerSegment, cs.PreferredCategories,
            er.SalesRank, er.TopCategorySales;
        
        -- ============================================
        -- 12단계: 복잡한 재귀적 데이터 처리
        -- ============================================
        WITH RecursiveDepartmentBudget AS (
            SELECT 
                d.DepartmentId,
                d.DepartmentName,
                d.ParentDepartmentId,
                d.BudgetAmount AS DirectBudget,
                d.BudgetAmount AS TotalBudget,
                0 AS Level
            FROM Departments d
            WHERE d.ParentDepartmentId IS NULL
            
            UNION ALL
            
            SELECT 
                d.DepartmentId,
                d.DepartmentName,
                d.ParentDepartmentId,
                d.BudgetAmount AS DirectBudget,
                rdb.TotalBudget + d.BudgetAmount AS TotalBudget,
                rdb.Level + 1
            FROM Departments d
            INNER JOIN RecursiveDepartmentBudget rdb ON d.ParentDepartmentId = rdb.DepartmentId
        ),
        DepartmentPerformanceMetrics AS (
            SELECT 
                rdb.DepartmentId,
                rdb.DepartmentName,
                rdb.TotalBudget,
                COUNT(DISTINCT e.EmployeeId) AS EmployeeCount,
                SUM(od.Quantity * od.UnitPrice * (1 - od.Discount)) AS TotalSales,
                AVG(od.Quantity * od.UnitPrice * (1 - od.Discount)) AS AvgOrderValue,
                COUNT(DISTINCT o.OrderId) AS OrderCount,
                COUNT(DISTINCT c.CustomerId) AS CustomerCount,
                (SUM(od.Quantity * od.UnitPrice * (1 - od.Discount)) / NULLIF(rdb.TotalBudget, 0)) * 100 AS SalesToBudgetRatio
            FROM RecursiveDepartmentBudget rdb
            LEFT JOIN Employees e ON rdb.DepartmentId = e.DepartmentId
            LEFT JOIN Orders o ON e.EmployeeId = o.SalesPersonId
            LEFT JOIN OrderDetails od ON o.OrderId = od.OrderId
            LEFT JOIN Customers c ON o.CustomerId = c.CustomerId
            LEFT JOIN Products p ON od.ProductId = p.ProductId
            LEFT JOIN Categories cat ON p.CategoryId = cat.CategoryId
            WHERE o.OrderDate >= @PreviousMonth OR o.OrderDate IS NULL
            GROUP BY 
                rdb.DepartmentId, rdb.DepartmentName, rdb.TotalBudget
        )
        
        -- 부서 성과 리포트 삽입
        INSERT INTO DepartmentPerformanceReport (
            ReportDate, DepartmentId, DepartmentName, TotalBudget,
            EmployeeCount, TotalSales, AvgOrderValue, OrderCount,
            CustomerCount, SalesToBudgetRatio, Status, CreatedDate, CreatedBy
        )
        SELECT 
            @CurrentDate AS ReportDate,
            dpm.DepartmentId,
            dpm.DepartmentName,
            dpm.TotalBudget,
            dpm.EmployeeCount,
            dpm.TotalSales,
            dpm.AvgOrderValue,
            dpm.OrderCount,
            dpm.CustomerCount,
            dpm.SalesToBudgetRatio,
            CASE 
                WHEN dpm.SalesToBudgetRatio >= 120 THEN 'Exceeding Target'
                WHEN dpm.SalesToBudgetRatio >= 100 THEN 'Meeting Target'
                WHEN dpm.SalesToBudgetRatio >= 80 THEN 'Near Target'
                ELSE 'Below Target'
            END AS Status,
            @CurrentDate AS CreatedDate,
            SYSTEM_USER AS CreatedBy
        FROM DepartmentPerformanceMetrics dpm
        WHERE NOT EXISTS (
            SELECT 1
            FROM DepartmentPerformanceReport dpr
            WHERE dpr.DepartmentId = dpm.DepartmentId
              AND dpr.ReportDate = @CurrentDate
        );
        
        -- ============================================
        -- 13단계: 복잡한 데이터 정합성 검증 및 수정
        -- ============================================
        UPDATE o
        SET 
            o.RequiredDate = CASE 
                WHEN o.RequiredDate < o.OrderDate THEN DATEADD(DAY, 7, o.OrderDate)
                WHEN o.RequiredDate > DATEADD(DAY, 90, o.OrderDate) THEN DATEADD(DAY, 30, o.OrderDate)
                ELSE o.RequiredDate
            END,
            o.ShippedDate = CASE 
                WHEN o.ShippedDate IS NOT NULL 
                     AND o.ShippedDate < o.OrderDate 
                THEN NULL
                WHEN o.ShippedDate IS NOT NULL
                     AND o.ShippedDate > DATEADD(DAY, 180, o.OrderDate)
                     AND NOT EXISTS (
                         SELECT 1
                         FROM OrderDetails od
                         WHERE od.OrderId = o.OrderId
                           AND od.ProductId IN (
                               SELECT p.ProductId
                               FROM Products p
                               WHERE p.Discontinued = 1
                           )
                     )
                THEN DATEADD(DAY, 14, o.OrderDate)
                ELSE o.ShippedDate
            END
        FROM Orders o
        INNER JOIN Customers c ON o.CustomerId = c.CustomerId
        INNER JOIN OrderDetails od ON o.OrderId = od.OrderId
        INNER JOIN Products p ON od.ProductId = p.ProductId
        INNER JOIN Categories cat ON p.CategoryId = cat.CategoryId
        INNER JOIN Employees e ON o.SalesPersonId = e.EmployeeId
        INNER JOIN Departments d ON e.DepartmentId = d.DepartmentId
        INNER JOIN Regions r ON c.RegionId = r.RegionId
        WHERE o.OrderDate >= @PreviousMonth
          AND (
              o.RequiredDate < o.OrderDate
              OR o.RequiredDate > DATEADD(DAY, 90, o.OrderDate)
              OR (o.ShippedDate IS NOT NULL AND o.ShippedDate < o.OrderDate)
              OR (o.ShippedDate IS NOT NULL AND o.ShippedDate > DATEADD(DAY, 180, o.OrderDate))
          )
          AND EXISTS (
              SELECT 1
              FROM #TempProcessingData tpd
              WHERE tpd.OrderId = o.OrderId
          );
        
        -- ============================================
        -- 14단계: 최종 통계 및 요약 정보 생성
        -- ============================================
        INSERT INTO ProcessStatistics (
            ProcessId, ProcessDate, TotalRecords, SuccessfulRecords,
            FailedRecords, TotalAmount, AverageAmount, DepartmentCount,
            EmployeeCount, CustomerCount, ProductCount, CategoryCount,
            SupplierCount, RegionCount, ProcessingTimeSeconds, Status
        )
        SELECT 
            @ProcessId AS ProcessId,
            @CurrentDate AS ProcessDate,
            COUNT(*) AS TotalRecords,
            SUM(CASE WHEN tpd.Status = 'Processed' THEN 1 ELSE 0 END) AS SuccessfulRecords,
            SUM(CASE WHEN tpd.Status <> 'Processed' THEN 1 ELSE 0 END) AS FailedRecords,
            SUM(tpd.Amount) AS TotalAmount,
            AVG(tpd.Amount) AS AverageAmount,
            COUNT(DISTINCT tpd.DepartmentId) AS DepartmentCount,
            COUNT(DISTINCT tpd.EmployeeId) AS EmployeeCount,
            COUNT(DISTINCT tpd.CustomerId) AS CustomerCount,
            (SELECT COUNT(DISTINCT p.ProductId)
             FROM Products p
             INNER JOIN OrderDetails od ON p.ProductId = od.ProductId
             INNER JOIN Orders o ON od.OrderId = o.OrderId
             WHERE o.OrderDate >= @PreviousMonth
            ) AS ProductCount,
            (SELECT COUNT(DISTINCT c.CategoryId)
             FROM Categories c
             INNER JOIN Products p ON c.CategoryId = p.CategoryId
             INNER JOIN OrderDetails od ON p.ProductId = od.ProductId
             INNER JOIN Orders o ON od.OrderId = o.OrderId
             WHERE o.OrderDate >= @PreviousMonth
            ) AS CategoryCount,
            (SELECT COUNT(DISTINCT s.SupplierId)
             FROM Suppliers s
             INNER JOIN Products p ON s.SupplierId = p.SupplierId
             INNER JOIN OrderDetails od ON p.ProductId = od.ProductId
             INNER JOIN Orders o ON od.OrderId = o.OrderId
             WHERE o.OrderDate >= @PreviousMonth
            ) AS SupplierCount,
            (SELECT COUNT(DISTINCT r.RegionId)
             FROM Regions r
             INNER JOIN Customers c ON r.RegionId = c.RegionId
             INNER JOIN Orders o ON c.CustomerId = o.CustomerId
             WHERE o.OrderDate >= @PreviousMonth
            ) AS RegionCount,
            DATEDIFF(SECOND, @StartTime, GETDATE()) AS ProcessingTimeSeconds,
            CASE 
                WHEN SUM(CASE WHEN tpd.Status <> 'Processed' THEN 1 ELSE 0 END) = 0
                THEN 'Success'
                WHEN SUM(CASE WHEN tpd.Status <> 'Processed' THEN 1 ELSE 0 END) < COUNT(*) * 0.05
                THEN 'Warning'
                ELSE 'Error'
            END AS Status
        FROM #TempProcessingData tpd
        WHERE tpd.IsProcessed = 1;
        
        -- 커밋 트랜잭션
        IF @TransactionCount = 0
            COMMIT TRANSACTION;
        
        SET @EndTime = GETDATE();
        
        -- 성공 알림 프로시저 호출
        IF EXISTS (SELECT 1 FROM sys.procedures WHERE name = 'sp_SendProcessNotification')
        BEGIN
            EXECUTE sp_SendProcessNotification 
                @ProcessId = @ProcessId,
                @Status = 'Success',
                @RecordsProcessed = @RowCount,
                @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
        END;
        
        -- 성공 메시지
        SELECT 
            @ProcessId AS ProcessId,
            @StartTime AS StartTime,
            @EndTime AS EndTime,
            DATEDIFF(SECOND, @StartTime, @EndTime) AS DurationSeconds,
            @RowCount AS RecordsProcessed,
            'Success' AS Status,
            NULL AS ErrorMessage;
        
    END TRY
    BEGIN CATCH
        -- 오류 처리
        IF @TransactionCount = 0
            ROLLBACK TRANSACTION;
        
        SET @ErrorCode = ERROR_NUMBER();
        SET @ErrorMessage = ERROR_MESSAGE();
        
        -- 오류 알림 프로시저 호출
        IF EXISTS (SELECT 1 FROM sys.procedures WHERE name = 'sp_SendErrorNotification')
        BEGIN
            EXEC [dbo].[sp_SendErrorNotification]
                @ProcessId = @ProcessId,
                @ErrorNumber = @ErrorCode,
                @ErrorMessage = @ErrorMessage,
                @ErrorLine = ERROR_LINE();
        END;
        
        -- 오류 로그 기록
        INSERT INTO ErrorLog (
            ProcessId, ErrorNumber, ErrorMessage, ErrorSeverity,
            ErrorState, ErrorLine, ErrorProcedure, CreatedDate
        )
        VALUES (
            @ProcessId, @ErrorCode, @ErrorMessage, ERROR_SEVERITY(),
            ERROR_STATE(), ERROR_LINE(), ERROR_PROCEDURE(), GETDATE()
        );
        
        -- 오류 정보 반환
        SELECT 
            @ProcessId AS ProcessId,
            @StartTime AS StartTime,
            GETDATE() AS EndTime,
            DATEDIFF(SECOND, @StartTime, GETDATE()) AS DurationSeconds,
            0 AS RecordsProcessed,
            'Failed' AS Status,
            @ErrorMessage AS ErrorMessage;
        
        -- 오류 재발생
        THROW;
    END CATCH;
    
    -- 임시 테이블 정리
    DROP TABLE IF EXISTS #TempProcessingData;
    DROP TABLE IF EXISTS #TempValidationResults;
    DROP TABLE IF EXISTS #TempAggregatedData;
    DROP TABLE IF EXISTS #TempMergeResults;
END;
GO

-- 프로시저 권한 설정
GRANT EXECUTE ON [dbo].[sp_ComplexBusinessProcess] TO [public];
GO

-- 프로시저 설명 추가
EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'복잡한 비즈니스 로직을 처리하는 프로시저. 다중 테이블 조인, 서브쿼리, WITH 절, MERGE/DELETE/UPDATE/INSERT 문을 포함합니다.',
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'PROCEDURE', @level1name = N'sp_ComplexBusinessProcess';
GO

