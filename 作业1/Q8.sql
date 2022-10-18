select t.regiondescription,t.firstname,t.lastname,t.birthdate
from(
  Select *,row_number() over (partition by regiondescription order by birthdate desc) as n
  from employee,employeeTerritory,region,territory
  where region.id=territory.regionid and territory.id=employeeterritory.territoryid and employeeterritory.employeeid=employee.id)t
where t.n=1
order by regionid;
