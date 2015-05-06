-- Clear previous hierarchy
update customers
set gparent=null, ggparent=null, gggparent=null;

-- Parents (Eliminate self-parentage)
update customers c
set c.parent = null where c.parent = c.custid;

-- Grandparents
update customers c
left join customers p on c.parent = p.custid
set c.gparent = p.parent where p.parent != p.custid;

-- Great grandparents
update customers c
left join customers g on c.gparent = g.custid
set c.ggparent = g.parent where g.parent != g.custid;

-- Great-great grandparents
update customers c
left join customers gg on c.ggparent = gg.custid
set c.gggparent = gg.parent where gg.parent != gg.custid;

-- Hierarchy Levels
insert into cust_hierarchy(custid, l1, l2, l3, l4, l5)
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
from customers;