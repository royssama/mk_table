 select * from (select * from test.tb01) a,
test.tb02 b, test.tb03 c, test.tb04 d, tb05 e,tb06 f
where a.id = b.id;
               
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
target.ProductCount = source.ProductCount;



select * from (select * from test.tb01) a,
test.tb02 b, test.tb03 c, test.tb04 d, tb05 e,tb06 f
where a.id = b.id;
               
merge into testCustomerOrderSummary_aaa
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
target.ProductCount = source.ProductCount;