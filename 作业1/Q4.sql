select category.categoryname,count(*),round(avg(unitprice),2),min(unitprice),max(unitprice),sum(unitsonorder)
from product,category
where category.id=product.categoryid
group by categoryid
having count(*)>10;