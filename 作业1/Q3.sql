Select shipper.companyname,round(count(case when shippeddate>requireddate then shipvia end)*100.0/count(*),2)
from 'order',shipper
where shipper.id='order'.shipvia
group by shipvia;