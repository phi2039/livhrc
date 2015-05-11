-- Clear previous hierarchy
update ${destSchema}.customer_master
set gparent=null, ggparent=null, gggparent=null;

-- Parents (Eliminate self-parentage)
update ${destSchema}.customer_master c
set c.parent=null where c.parent=c.custid;

-- Grandparents
update ${destSchema}.customer_master c
left join ${destSchema}.customer_master p on c.parent=p.custid
set c.gparent=p.parent where p.parent!=p.custid;

-- Great grandparents
update ${destSchema}.customer_master c
left join ${destSchema}.customer_master g on c.gparent=g.custid
set c.ggparent=g.parent where g.parent!=g.custid;

-- Great-great grandparents
update ${destSchema}.customer_master c
left join ${destSchema}.customer_master gg on c.ggparent=gg.custid
set c.gggparent=gg.parent where gg.parent=gg.custid;

truncate table ${destSchema}.cust_hierarchy;

-- Hierarchy Levels
insert into ${destSchema}.cust_hierarchy(custid, l1, l2, l3, l4, l5)
select custid,
(case 
	when (parent is not null and gparent is not null and ggparent is not null and gggparent is not null) then gggparent 
	when (parent is not null and gparent is not null and ggparent is not null and gggparent is null) then ggparent 
	when (parent is not null and gparent is not null and ggparent is null and gggparent is null) then gparent 
	when (parent is not null and gparent is null and ggparent is null and gggparent is null) then parent 
	when (parent is null and gparent is null and ggparent is null and gggparent is null) then custid 
	else null
end) as l1,
(case 
	when (parent is not null and gparent is not null and ggparent is not null and gggparent is not null) then ggparent 
	when (parent is not null and gparent is not null and ggparent is not null and gggparent is null) then gparent 
	when (parent is not null and gparent is not null and ggparent is null and gggparent is null) then parent 
	when (parent is not null and gparent is null and ggparent is null and gggparent is null) then custid 
	else null
end) as l2,
(case 
	when (parent is not null and gparent is not null and ggparent is not null and gggparent is not null) then gparent 
	when (parent is not null and gparent is not null and ggparent is not null and gggparent is null) then parent 
	when (parent is not null and gparent is not null and ggparent is null and gggparent is null) then custid 
	else null
end) as l3,
(case 
	when (parent is not null and gparent is not null and ggparent is not null and gggparent is not null) then parent 
	when (parent is not null and gparent is not null and ggparent is not null and gggparent is null) then custid 
	else null
end) as l4,
(case 
	when (parent is not null and gparent is not null and ggparent is not null and gggparent is not null) then custid 
	else null
end) as l5
from ${destSchema}.customer_master;