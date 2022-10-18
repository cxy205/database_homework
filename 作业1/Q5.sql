Select t.ProductName, t.CompanyName, t.ContactName
From (
  Select *,row_number() over (partition by product.productname order by 'Order'.OrderDate asc) as n
  From Product,'Order',Customer,OrderDetail
  Where product.ID=OrderDetail.ProductID and OrderDetail.OrderID='Order'.ID and 'Order'.CustomerID=Customer.ID and product.discontinued=1
)t
Where t.n=1;
