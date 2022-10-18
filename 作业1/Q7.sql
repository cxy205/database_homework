select customerid,comname,summ
from (select customerid,ifnull(companyname,'MISSING_NAME') as comname,sum(unitprice*quantity) as summ,NTILE (4) OVER ( ORDER BY sum(unitprice*quantity) ) num
from orderdetail left join ('order' left join customer on 'order'.customerid=customer.id) on orderdetail.orderid='order'.id
group by customerid
order by sum(unitprice*quantity))
where num=1;