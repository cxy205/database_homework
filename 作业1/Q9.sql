 select group_concat(productname,',')
 from(
  select productname
  from product,orderdetail,customer,'order'
  where product.id=orderdetail.productid and orderdetail.orderid='order'.id and 'order'.customerid=customer.id and substr('order'.orderdate,1,instr('order'.orderdate,' ')-1)='2014-12-25' and customer.companyname='Queen Cozinha'
  order by product.id);
