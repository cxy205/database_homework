Select Id,ShipCountry,case when ShipCountry= 'USA' or  ShipCountry= 'Mexico' or ShipCountry= 'Canada' then 'NorthAmerica' else 'OtherPlace' end as a
From 'order'
where id>=15445 and id<=15465
Order by Id;