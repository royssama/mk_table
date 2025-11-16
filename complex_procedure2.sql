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
                           FROM Categories2 cat2, Categories3 cat3

                           INNER JOIN Products2 pr ON cat.CategoryId = pr.CategoryId
                           INNER JOIN OrderDetails2 od3 ON pr.ProductId = od3.ProductId
                           GROUP BY cat.CategoryName
                           ORDER BY SUM(od3.Quantity * od3.UnitPrice) DESC
                       )
                   )
               )