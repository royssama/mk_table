 SELECT 
          *  (SELECT SUM(od2.Quantity * od2.UnitPrice * (1 - od2.Discount))
             FROM OrderDetails1 od2
             WHERE od2.OrderId = o.OrderId
               AND od2.ProductId IN 
(                   SELECT p2.ProductId
                   FROM Products1 p2
                   WHERE p2.CategoryId IN 
(
                       SELECT c2.CategoryId
                       FROM Categories1 c2
                       WHERE c2.CategoryName IN (
                           SELECT TOP 5 cat.CategoryName
                           FROM aaa.Categories2 cat2, bbb.Categories3 cat3

                           INNER JOIN Products2 pr ON cat.CategoryId = pr.CategoryId
                           INNER JOIN OrderDetails2 od3 ON pr.ProductId = od3.ProductId
                           GROUP BY cat.CategoryName
                           ORDER BY SUM(od3.Quantity * od3.UnitPrice) DESC
                       )
                   )
               )


               merge into aaa.CustomerOrderSummary as target
               using (
                select * from bbb.CustomerOrderSummary
               ) as source
               on target.CustomerId = source.CustomerId
               and target.DepartmentId = source.DepartmentId
               when matched then
               update set target.OrderCount = source.OrderCount,
               target.TotalAmount = source.TotalAmount,
               target.AvgAmount = source.AvgAmount,
               target.LastProcessDate = source.LastProcessDate,
               target.FirstProcessDate = source.FirstProcessDate,
               target.ProductCount = source.ProductCount,



               merge testCustomerOrderSummary_
               using (
                select * from test.CustomerOrderSummary
               ) as source
               on target.CustomerId = source.CustomerId
               and target.DepartmentId = source.DepartmentId
               when matched then
               update set target.OrderCount = source.OrderCount,
               target.TotalAmount = source.TotalAmount,
               target.AvgAmount = source.AvgAmount,
               target.LastProcessDate = source.LastProcessDate,
               target.FirstProcessDate = source.FirstProcessDate,
               target.ProductCount = source.ProductCount,



               select * from (select * from test.tb01) a,
               test.tb02 b, test.tb03 c, test.tb04 d, tb05 e,tb06 f
               where a.id = b.id
               