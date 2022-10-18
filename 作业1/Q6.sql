select id,orderdate,case when preorderdate<>0 then preorderdate else orderdate end,round((julianday(orderdate)-julianday(case when preorderdate<>0 then preorderdate else orderdate end)),2)
from (select id,orderdate,lag(orderdate,1,0) over (order by orderdate)preorderdate
from 'order'
where customerid='BLONP')
limit 10;