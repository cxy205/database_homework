Select ShipName,substr(ShipName,1,instr(ShipName,'-')-1)
From  'order'
group by ShipName
having substr(ShipName,1,instr(ShipName,'-')-1)<>'';