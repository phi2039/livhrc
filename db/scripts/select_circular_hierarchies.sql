SELECT 
C.CUSTID,
L1.CUSTID AS P1,
L2.CUSTID AS P2,
L3.CUSTID AS P3,
L4.CUSTID AS P4,
L5.CUSTID AS P5
FROM
STAGING.CUST_SNAP C
LEFT JOIN STAGING.CUST_SNAP L1 ON C.PARENT = L1.CUSTID
LEFT JOIN STAGING.CUST_SNAP L2 ON L1.PARENT = L2.CUSTID
LEFT JOIN STAGING.CUST_SNAP L3 ON L2.PARENT = L3.CUSTID
LEFT JOIN STAGING.CUST_SNAP L4 ON L3.PARENT = L4.CUSTID
LEFT JOIN STAGING.CUST_SNAP L5 ON L4.PARENT = L5.CUSTID
WHERE
(C.CUSTID IS NOT NULL AND
	(C.CUSTID = L1.CUSTID
  OR C.CUSTID = L2.CUSTID
  OR C.CUSTID = L3.CUSTID
  OR C.CUSTID = L4.CUSTID  
  OR C.CUSTID = L5.CUSTID))
OR (L1.CUSTID IS NOT NULL AND
  (L1.CUSTID = L2.CUSTID
  OR L1.CUSTID = L3.CUSTID
  OR L1.CUSTID = L4.CUSTID  
  OR L1.CUSTID = L5.CUSTID)) 
OR (L2.CUSTID IS NOT NULL AND
  (L2.CUSTID = L3.CUSTID
  OR L2.CUSTID = L4.CUSTID  
  OR L2.CUSTID = L5.CUSTID))
OR (L3.CUSTID IS NOT NULL AND
  (L3.CUSTID = L4.CUSTID  
  OR L3.CUSTID = L5.CUSTID))
OR (L4.CUSTID IS NOT NULL AND
  (L4.CUSTID = L5.CUSTID))
;
