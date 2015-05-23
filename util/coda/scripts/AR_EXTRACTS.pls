create or replace PACKAGE BODY AR_EXTRACTS AS

-- TODO: Factor duplicate subqueries for consistency and maintenance
  PROCEDURE CUSTOMER_MASTER_FULL (CUSTOMER_MASTER_CUR OUT SYS_REFCURSOR) AS
  BEGIN
    OPEN CUSTOMER_MASTER_CUR FOR
      SELECT 
          MASTER.ACCT AS CUSTID,
          DETAIL.NAME,
          DETAIL.ADD1,
          DETAIL.ADD2,
          DETAIL.ADD3,
          DETAIL.CITY,
          DETAIL.STATE AS STATEPROV,
          DETAIL.COUNTRY,
          DETAIL.POSTCODE,
          DETAIL.CREATE_DATE AS CREATEDATE,
          DETAIL.TERMS,
          (CASE WHEN MASTER.PARENT IS NOT NULL THEN NULL ELSE DETAIL.CREDIT_LIMIT END) AS CREDITLIM, -- Set credit limit to NULL for captions
          NULL AS TTMREV,
          NULL AS SFID,
          DETAIL.CORP_CODE AS CORPCODE,
          DETAIL.CRLIMDATE AS CRLIMDATE,
          DETAIL.ACCT_STAT AS ACCTSTAT,
          DETAIL.ACCT_TYPE AS ACCTTYPE,
          DETAIL.BOND_TYPE AS BONDTYPE,
          DETAIL.PREV_CO AS PREVCO,
          DETAIL.SIC,
          DETAIL.CST,
          DETAIL.CSM,
          DETAIL.MSD,
          DETAIL.SC,
          DETAIL.CBSA_WAIVER AS WAIVERTYPE,
          MASTER.CURR AS INVCURR,
          DETAIL.CREDIT_CODE AS CREDITCODE,
          NULL AS LANGUAGE,  
          MASTER.PARENT,
          NULL AS REMITTO,
          NULL AS REMARKS1,
          NULL AS REMARKS2
      FROM
      (
          -- SELECTS a distinct list of customers (6-9 digits) from the element list
          -- Results are sorted by MODDATE to retrieve the most recently modified 'version' of a customer record
          SELECT 
              EL.CMPCODE,
              EL.CODE,
              GET_CUSTID(EL.CODE) AS ACCT, 
              GET_PARENTID(EL.CODE) AS PARENT,
              GRP.GRPCODE AS CURR, -- NOTE: "Equivalent currency option" is defined at invoice level, not customer level, so this has limited meaning...
              EL.MODDATE, 
              row_number() OVER (PARTITION BY GET_CUSTID(EL.CODE) ORDER BY EL.MODDATE DESC) AS ROW_NO
          FROM 
              OAS_ELEMENT EL
          LEFT OUTER JOIN 
              OAS_GRPLIST GRP
                  ON  EL.CODE = GRP.CODE
                  AND EL.CMPCODE = GRP.CMPCODE    
          WHERE
              EL.CMPCODE IN ('C310','C165','U125')
              AND EL.CODE LIKE 'C%'
              AND EL.CODE NOT LIKE 'CER%'
              AND EL.ELMLEVEL = 5 -- Only clients
              AND EL.DELDATE IS NULL -- Only active records
              AND GRP.GRPCODE IN ('CAD','USD','USE') -- Valid "client currency groups"
      ) MASTER
      LEFT OUTER JOIN
      (SELECT
          EL.CMPCODE,
          EL.CODE,
          EL.NAME,
          BT.ADD1,
          BT.ADD2,
          BT.ADD3,
          BT.ADD5 AS CITY,
          BT.ADD6 AS STATE,
          BT.COUNTRY,
          BT.POSTCODE,
          EL.DATEACCOPENED AS CREATE_DATE,
          (CASE WHEN SUBSTR(EL.TERMS, 1, 1) = 'D' THEN TO_NUMBER(SUBSTR(EL.TERMS, 2, 3)) ELSE (CASE WHEN EL.TERMS = '00LL' THEN
              DECODE(EL.CRREF, 
              'Direct Bond without DSL', 96,
              'Month End Settlement', 98, 
              -- Cannot recreate terms code '99' -> These are combined with '98'
              'Direct GST', 22,
              'Direct Bond', 22, -1)  -- Unknown Terms Code
               ELSE -1 END) END) TERMS, -- Convert to GQL coding
          EL.TERMS AS TC,
          EL.CRLIM AS CREDIT_LIMIT,
          EL.REPCODE1 AS CORP_CODE,
          EL.CRLIMDATE,
          DECODE (EL.ELMSTAT,
              'ACTIVE', '',
              'CANCEL', 'C',
              'CREDIT HOLD', 'H',
              EL.ELMSTAT) AS ACCT_STAT, -- Convert to GQL coding
          EL.STATUSER AS ACCT_TYPE,
          DECODE(EL.CRREF,
              'Direct Bond without DSL', 'I',
              'Direct GST', 'G',
              'Self Clear', 'C',
              'Open Terms', 'B',
              'Direct Bond', 'D',
              'Month End Settlement', 'M',
              '') AS BOND_TYPE, -- Convert to GQL coding
          EL.STATMEMO AS PREV_CO,
          EL.SIC,
          BT.LANG AS CST,
          OP.ADD1 AS CSM,
          OP.ADD3 AS MSD,
          OP.ADD2 AS SC,
          EL.CRAGENCY AS CBSA_WAIVER,
          EL.CUR AS CLIENT_CURR,
          EL.PAYMENTINDEX AS CREDIT_CODE
      FROM
          OAS_ELEMENT EL
      LEFT OUTER JOIN OAS_ELMADDRLIST BT
          ON EL.CMPCODE = BT.CMPCODE
          AND EL.CODE = BT.ELMCODE
          AND EL.ELMLEVEL = BT.ELMLEVEL
          AND BT.CATAGORY = 'BILL TO'
      LEFT OUTER JOIN OAS_ELMADDRLIST  OP
          ON EL.CMPCODE = OP.CMPCODE
          AND EL.CODE = OP.ELMCODE
          AND EL.ELMLEVEL = OP.ELMLEVEL
          AND OP.CATAGORY = 'SALES REP'
      WHERE
          EL.ELMLEVEL = 5 -- Prevent inter-level joins
      ) DETAIL
      ON 
          MASTER.CODE = DETAIL.CODE
          AND MASTER.CMPCODE = DETAIL.CMPCODE
      WHERE 
          MASTER.ROW_NO = 1
      ORDER BY
          MASTER.ACCT
      ;
  END CUSTOMER_MASTER_FULL;

  PROCEDURE OPEN_ITEMS_FULL(OPEN_ITEM_CUR OUT SYS_REFCURSOR) AS
  BEGIN
    OPEN OPEN_ITEM_CUR FOR
    -- Single-client, high volume documents (invoices)
      SELECT 
          HEAD.CMPCODE, 
          GET_CUSTID(ORG.CUST) AS CUSTID,
          HEAD.DOCCODE AS DOCTYPE,
          ORG.REF4 AS DOCSUBTYPE,
          TRIM(HEAD.DOCNUM) AS DOCNUM,
          GET_SOURCESYS(HEAD.DOCCODE, ORG.CUST) AS SOURCESYS,
          ORG.REF3 AS INVOICEREF,
          ORG.GLACCT,
          HEAD.DOCDATE AS DOCDATE,
          HEAD.INPDATE AS POSTDATE,
          NULL AS CLEARDATE,
          ORG.DESCR AS DOCDESC,
          ORG.REF1 AS XACTREF,
          ORG.REF2 AS CUSTREF,
          ORG.REF5 AS EXTREF5,
          -- Currencies
          HEAD.CURDOC AS DOCCURR,
          CMP.HOMECUR AS HOMECURR,
          (CASE WHEN ORG.EQUIV IS NULL THEN NULL ELSE CMP.DUALCUR END) AS ALTCURR,
          -- Original Amounts
          GET_ORG_MASK(HEAD.DOCCODE) * ORG.VALUEDOC AS ORGVAL_DOC,
          GET_ORG_MASK(HEAD.DOCCODE) * ORG.VALUEHOME AS ORGVAL_HOME,
          (CASE WHEN ORG.EQUIV IS NULL THEN NULL ELSE GET_ORG_MASK(HEAD.DOCCODE) * ORG.VALUEDUAL END) AS ORGVAL_ALT,
          GET_ORG_MASK(HEAD.DOCCODE) * NORMALIZE_VALUE(ORG.VALUEHOME, ORG.VALUEDUAL, CMP.HOMECUR, 'USD') AS ORGVAL_NORM,
          -- Open Amounts
          OPN.TOTALDOC AS OPENVAL_DOC,
          OPN.TOTALHOME AS OPENVAL_HOME,
          (CASE WHEN ORG.EQUIV IS NULL THEN NULL ELSE OPN.TOTALDUAL END) AS OPENVAL_ALT, -- Alternate currency amount is only valid for clients with equivalent status
          NORMALIZE_VALUE(OPN.TOTALHOME, OPN.TOTALDUAL, CMP.HOMECUR, 'USD') AS OPENVAL_NORM,
          PAY.TOTALDOC AS PAYVAL_DOC,
          PAY.TOTALHOME AS PAYVAL_HOME,
          (CASE WHEN ORG.EQUIV IS NULL THEN NULL ELSE PAY.TOTALDUAL END) AS PAYVAL_ALT, -- Alternate currency amount is only valid for clients with equivalent status
          NORMALIZE_VALUE(PAY.TOTALHOME, PAY.TOTALDUAL, CMP.HOMECUR, 'USD') AS PAYVAL_NORM
      FROM
          -- Open amount (total of all items with 'Available' status)
          (SELECT 
              CMPCODE,
              DOCCODE,
              DOCNUM,
              SUM(VALUEDOC) AS TOTALDOC,
              SUM(VALUEHOME) AS TOTALHOME,
              SUM(VALUEDUAL) AS TOTALDUAL
          FROM
              OAS_DOCLINE
          WHERE
              (EL1 = '112111' OR EL1 = '112121') -- AR lines only
              AND STATPAY = 84
              AND MATCHLEVEL = 5
          GROUP BY CMPCODE, DOCCODE, DOCNUM) OPN
      INNER JOIN
          -- Document header information
          OAS_DOCHEAD HEAD ON HEAD.CMPCODE = OPN.CMPCODE AND HEAD.DOCCODE = OPN.DOCCODE AND HEAD.DOCNUM = OPN.DOCNUM
      INNER JOIN
          -- Company information (for currencies)
          OAS_COMPANY CMP ON CMP.CODE = OPN.CMPCODE
      INNER JOIN
          -- Original amount (first line posted to 1121*1 account) 
          (SELECT
              DL.CMPCODE,
              DOCCODE,
              DOCNUM,
              EL5 AS CUST,
              VALUEDOC,
              VALUEHOME,
              VALUEDUAL,
              REF1,
              REF2,
              REF3,
              REF4,
              REF5,
              REF6,
              DESCR,
              EL1 AS GLACCT,
              GET_ALTCUR(DL.CMPCODE, GRP.GRPCODE) AS EQUIV,
              row_number() OVER (PARTITION BY DL.CMPCODE, DOCCODE, DOCNUM ORDER BY DOCLINENUM ASC) AS ROW_NO
          FROM 
              OAS_DOCLINE DL
          LEFT  JOIN 
              OAS_GRPLIST GRP ON DL.EL5 = GRP.CODE  AND DL.CMPCODE = GRP.CMPCODE
          WHERE
              (EL1 = '112111' OR EL1 = '112121') -- AR lines only
              AND MATCHLEVEL = 5
              AND GRP.GRPCODE IN ('CAD','USD','USE')) ORG -- LOCUS client currencies
      ON ORG.CMPCODE = OPN.CMPCODE AND ORG.DOCCODE = OPN.DOCCODE AND ORG.DOCNUM = OPN.DOCNUM
      LEFT JOIN
          -- Settled amount (total of all items with 'Paid' status)
          (SELECT 
              CMPCODE,
              DOCCODE,
              DOCNUM,
              MAX(MODDATE) AS LASTMOD,
              SUM(VALUEDOC) AS TOTALDOC,
              SUM(VALUEHOME) AS TOTALHOME,
              SUM(VALUEDUAL) AS TOTALDUAL
          FROM
              OAS_DOCLINE
          WHERE
              (EL1 = '112111' OR EL1 = '112121') -- AR lines only
              AND STATPAY = 89
              AND MATCHLEVEL = 5
          GROUP BY CMPCODE, DOCCODE, DOCNUM) PAY
      ON PAY.CMPCODE = OPN.CMPCODE AND PAY.DOCCODE = OPN.DOCCODE AND PAY.DOCNUM = OPN.DOCNUM
      WHERE
          ORG.ROW_NO = 1
          AND ORG.VALUEDOC <> 0 -- Ignore zero-value docs posted by interface
          AND HEAD.CMPCODE IN ('C165','C310','U125')
          AND HEAD.DOCCODE <> 'DISPERSE'
          AND HEAD.DOCCODE NOT LIKE 'YB%'
          AND HEAD.STATUS = 78 -- Posted
          AND HEAD.DOCCODE IN (
            'SALE-INV-CDN',
            'SALE-INV-USD'
          )
          AND HEAD.INPDATE < TRUNC(SYSDATE) -- Only fetch items created before today (only full days)
          
      UNION ALL
      
    -- Multi-client and/or low volume documents
      SELECT 
          HEAD.CMPCODE, 
          GET_CUSTID(ORG.CUST) AS CUSTID,
          HEAD.DOCCODE AS DOCTYPE,
          ORG.REF4 AS DOCSUBTYPE,
          TRIM(HEAD.DOCNUM) AS DOCNUM,
          GET_SOURCESYS(HEAD.DOCCODE, ORG.CUST) AS SOURCESYS,
          ORG.REF3 AS INVOICEREF,
          ORG.GLACCT,
          HEAD.DOCDATE AS DOCDATE,
          HEAD.INPDATE AS POSTDATE,
          NULL AS CLEARDATE,
          ORG.DESCR AS DOCDESC,
          ORG.REF1 AS XACTREF,
          ORG.REF2 AS CUSTREF,
          ORG.REF5 AS EXTREF5,
          -- Currencies
          HEAD.CURDOC AS DOCCURR,
          CMP.HOMECUR AS HOMECURR,
          (CASE WHEN ORG.EQUIV IS NULL THEN NULL ELSE CMP.DUALCUR END) AS ALTCURR,
          -- Original Amounts
          GET_ORG_MASK(HEAD.DOCCODE) * ORG.VALUEDOC AS ORGVAL_DOC,
          GET_ORG_MASK(HEAD.DOCCODE) * ORG.VALUEHOME AS ORGVAL_HOME,
          (CASE WHEN ORG.EQUIV IS NULL THEN NULL ELSE GET_ORG_MASK(HEAD.DOCCODE) * ORG.VALUEDUAL END) AS ORGVAL_ALT,
          GET_ORG_MASK(HEAD.DOCCODE) * NORMALIZE_VALUE(ORG.VALUEHOME, ORG.VALUEDUAL, CMP.HOMECUR, 'USD') AS ORGVAL_NORM,
          -- Open Amounts
          OPN.TOTALDOC AS OPENVAL_DOC,
          OPN.TOTALHOME AS OPENVAL_HOME,
          (CASE WHEN ORG.EQUIV IS NULL THEN NULL ELSE OPN.TOTALDUAL END) AS OPENVAL_ALT, -- Alternate currency amount is only valid for clients with equivalent status
          NORMALIZE_VALUE(OPN.TOTALHOME, OPN.TOTALDUAL, CMP.HOMECUR, 'USD') AS OPENVAL_NORM,
          PAY.TOTALDOC AS PAYVAL_DOC,
          PAY.TOTALHOME AS PAYVAL_HOME,
          (CASE WHEN ORG.EQUIV IS NULL THEN NULL ELSE PAY.TOTALDUAL END) AS PAYVAL_ALT, -- Alternate currency amount is only valid for clients with equivalent status
          NORMALIZE_VALUE(PAY.TOTALHOME, PAY.TOTALDUAL, CMP.HOMECUR, 'USD') AS PAYVAL_NORM
      FROM
          -- Open amount (total of all items with 'Available' status)
          (SELECT 
              CMPCODE,
              DOCCODE,
              DOCNUM,
              EL5 AS CUST,
              SUM(VALUEDOC) AS TOTALDOC,
              SUM(VALUEHOME) AS TOTALHOME,
              SUM(VALUEDUAL) AS TOTALDUAL
          FROM
              OAS_DOCLINE
          WHERE
              (EL1 = '112111' OR EL1 = '112121') -- AR lines only
              AND STATPAY = 84
              AND MATCHLEVEL = 5
          GROUP BY CMPCODE, DOCCODE, DOCNUM, EL5) OPN
      INNER JOIN
          -- Document header information
          OAS_DOCHEAD HEAD ON HEAD.CMPCODE = OPN.CMPCODE AND HEAD.DOCCODE = OPN.DOCCODE AND HEAD.DOCNUM = OPN.DOCNUM
      INNER JOIN
          -- Company information (for currencies)
          OAS_COMPANY CMP ON CMP.CODE = OPN.CMPCODE
      INNER JOIN
          -- Original amount (first line posted to 1121*1 account) 
          (SELECT
              DL.CMPCODE,
              DOCCODE,
              DOCNUM,
              EL5 AS CUST,
              VALUEDOC,
              VALUEHOME,
              VALUEDUAL,
              REF1,
              REF2,
              REF3,
              REF4,
              REF5,
              REF6,
              DESCR,
              EL1 AS GLACCT,
              GET_ALTCUR(DL.CMPCODE, GRP.GRPCODE) AS EQUIV,
              row_number() OVER (PARTITION BY DL.CMPCODE, DOCCODE, DOCNUM, EL5 ORDER BY DOCLINENUM ASC) AS ROW_NO
          FROM 
              OAS_DOCLINE DL
          LEFT  JOIN 
              OAS_GRPLIST GRP ON DL.EL5 = GRP.CODE  AND DL.CMPCODE = GRP.CMPCODE
          WHERE
              (EL1 = '112111' OR EL1 = '112121') -- AR lines only
              AND MATCHLEVEL = 5
              AND GRP.GRPCODE IN ('CAD','USD','USE')) ORG -- LOCUS client currencies
      ON ORG.CMPCODE = OPN.CMPCODE AND ORG.DOCCODE = OPN.DOCCODE AND ORG.DOCNUM = OPN.DOCNUM AND ORG.CUST = OPN.CUST
      LEFT JOIN
      -- Settled amount (total of all items with 'Paid' status)
      (SELECT 
        CMPCODE,
              DOCCODE,
              DOCNUM,
              EL5 AS CUST,
              MAX(MODDATE) AS LASTMOD,
              SUM(VALUEDOC) AS TOTALDOC,
              SUM(VALUEHOME) AS TOTALHOME,
              SUM(VALUEDUAL) AS TOTALDUAL
          FROM
              OAS_DOCLINE
          WHERE
              (EL1 = '112111' OR EL1 = '112121') -- AR lines only
              AND STATPAY = 89
              AND MATCHLEVEL = 5
          GROUP BY CMPCODE, DOCCODE, DOCNUM, EL5) PAY
      ON OPN.CMPCODE = PAY.CMPCODE AND OPN.DOCCODE = PAY.DOCCODE AND OPN.DOCNUM = PAY.DOCNUM AND OPN.CUST = PAY.CUST      
      WHERE
          ORG.ROW_NO = 1
          AND ORG.VALUEDOC <> 0 -- Ignore zero-value docs posted by interface
          AND HEAD.CMPCODE IN ('C165','C310','U125')
          AND HEAD.DOCCODE <> 'DISPERSE'
          AND HEAD.DOCCODE NOT LIKE 'YB%'
          AND HEAD.STATUS = 78 -- Posted
          AND HEAD.DOCCODE NOT IN (
            'SALE-INV-CDN',
            'SALE-INV-USD'
          )
          AND HEAD.INPDATE < TRUNC(SYSDATE) -- Only fetch items created before today (only full days)
          ;
  END OPEN_ITEMS_FULL;

  PROCEDURE OPEN_ITEMS_DELTA(P_START_DATE DATE, P_DAYS INT, OPEN_ITEM_CUR OUT SYS_REFCURSOR) AS
    V_NUM_DAYS INT := 1; -- Minimum of one day
  BEGIN
  -- Set-up the date interval
    IF P_DAYS < 0 THEN -- A negative value indicates that all records modified since the referenced date should be fetched
      V_NUM_DAYS := TRUNC(SYSDATE) - P_START_DATE;
    ELSIF P_DAYS > 0 THEN
      V_NUM_DAYS := P_DAYS;
    END IF;
    OPEN OPEN_ITEM_CUR FOR

    -- Retrieves documents modified since P_START_DATE (if P_DAYS = -1)
    --  Alternatively retrieves only documents modified within P_DAYS of P_START_DATE
    --  Only retrieves full-days if P_START_DATE < TRUNC(SYSDATE)
    --  Retrieves the current day's activity if P_START_DATE = TRUNC(SYSDATE)
    
    -- Single-client, high volume documents
      SELECT 
          HEAD.CMPCODE, 
          GET_CUSTID(ORG.CUST) AS CUSTID,
          HEAD.DOCCODE AS DOCTYPE,
          ORG.REF4 AS DOCSUBTYPE,
          TRIM(HEAD.DOCNUM) AS DOCNUM,
          GET_SOURCESYS(HEAD.DOCCODE, ORG.CUST) AS SOURCESYS,
          ORG.REF3 AS INVOICEREF,
          ORG.GLACCT,
          HEAD.DOCDATE AS DOCDATE,
          HEAD.INPDATE AS POSTDATE,
          (CASE WHEN OPN.TOTALDOC IS NULL THEN PAY.LASTMOD ELSE NULL END) AS CLEARDATE,
          ORG.DESCR AS DOCDESC,
          ORG.REF1 AS XACTREF,
          ORG.REF2 AS CUSTREF,
          ORG.REF5 AS EXTREF5,
          -- Currencies
          HEAD.CURDOC AS DOCCURR,
          CMP.HOMECUR AS HOMECURR,
          (CASE WHEN ORG.EQUIV IS NULL THEN NULL ELSE CMP.DUALCUR END) AS ALTCURR,
          -- Original Amounts
          GET_ORG_MASK(HEAD.DOCCODE) * ORG.VALUEDOC AS ORGVAL_DOC,
          GET_ORG_MASK(HEAD.DOCCODE) * ORG.VALUEHOME AS ORGVAL_HOME,
          (CASE WHEN ORG.EQUIV IS NULL THEN NULL ELSE GET_ORG_MASK(HEAD.DOCCODE) * ORG.VALUEDUAL END) AS ORGVAL_ALT,
          GET_ORG_MASK(HEAD.DOCCODE) * NORMALIZE_VALUE(ORG.VALUEHOME, ORG.VALUEDUAL, CMP.HOMECUR, 'USD') AS ORGVAL_NORM,
          -- Open Amounts
          OPN.TOTALDOC AS OPENVAL_DOC,
          OPN.TOTALHOME AS OPENVAL_HOME,
          (CASE WHEN ORG.EQUIV IS NULL THEN NULL ELSE OPN.TOTALDUAL END) AS OPENVAL_ALT, -- Alternate currency amount is only valid for clients with equivalent status
          NORMALIZE_VALUE(OPN.TOTALHOME, OPN.TOTALDUAL, CMP.HOMECUR, 'USD') AS OPENVAL_NORM,
          PAY.TOTALDOC AS PAYVAL_DOC,
          PAY.TOTALHOME AS PAYVAL_HOME,
          (CASE WHEN ORG.EQUIV IS NULL THEN NULL ELSE PAY.TOTALDUAL END) AS PAYVAL_ALT, -- Alternate currency amount is only valid for clients with equivalent status
          NORMALIZE_VALUE(PAY.TOTALHOME, PAY.TOTALDUAL, CMP.HOMECUR, 'USD') AS PAYVAL_NORM
      FROM
          -- List of documents (lines, actually) modified in the last day
          (SELECT CMPCODE, DOCCODE, DOCNUM, COUNT(1) AS MODLINES 
          FROM OAS_DOCLINE
          WHERE
              MODDATE >= P_START_DATE
              AND MODDATE < P_START_DATE + V_NUM_DAYS
              AND (EL1 = '112111' OR EL1 = '112121') -- AR lines only
              AND MATCHLEVEL = 5 -- Clients only
          GROUP BY CMPCODE, DOCCODE, DOCNUM) MODS
      INNER JOIN
          -- Document header information
          OAS_DOCHEAD HEAD ON HEAD.CMPCODE = MODS.CMPCODE AND HEAD.DOCCODE = MODS.DOCCODE AND HEAD.DOCNUM = MODS.DOCNUM
      INNER JOIN
          -- Company information (for currencies)
          OAS_COMPANY CMP ON MODS.CMPCODE = CMP.CODE
      INNER JOIN
          -- Original amount (first line posted to 1121*1 account) 
          (SELECT
              DL.CMPCODE,
              DOCCODE,
              DOCNUM,
              EL5 AS CUST,
              VALUEDOC,
              VALUEHOME,
              VALUEDUAL,
              REF1,
              REF2,
              REF3,
              REF4,
              REF5,
              REF6,
              DESCR,
              EL1 AS GLACCT,
              DOCLINENUM AS FIRST_LINE,
              (CASE WHEN SUBSTR(DL.CMPCODE,1,1) = 'U' THEN
                DECODE(GRP.GRPCODE,
                  'USD', NULL,
                  'CAD', 'CAD',
                  NULL) ELSE
                DECODE(GRP.GRPCODE,
                  'USD', NULL,
                  'CAD', NULL,
                  'USE', 'USD',
                  NULL) END) EQUIV,        
              row_number() OVER (PARTITION BY DL.CMPCODE, DOCCODE, DOCNUM ORDER BY DOCLINENUM ASC) AS ROW_NO
          FROM 
              OAS_DOCLINE DL
          LEFT  JOIN 
              OAS_GRPLIST GRP ON DL.EL5 = GRP.CODE  AND DL.CMPCODE = GRP.CMPCODE     
          WHERE
              (EL1 = '112111' OR EL1 = '112121') -- AR lines only
              AND MATCHLEVEL = 5
              AND GRP.GRPCODE IN ('CAD','USD','USE')) ORG -- LOCUS client currencies
      ON MODS.CMPCODE = ORG.CMPCODE AND MODS.DOCCODE = ORG.DOCCODE AND MODS.DOCNUM = ORG.DOCNUM
      LEFT JOIN
          -- Open amount (total of all items with 'Available' status)
          (SELECT 
              CMPCODE,
              DOCCODE,
              DOCNUM,
              COUNT(1) AS LINES,
              SUM(VALUEDOC) AS TOTALDOC,
              SUM(VALUEHOME) AS TOTALHOME,
              SUM(VALUEDUAL) AS TOTALDUAL
          FROM
              OAS_DOCLINE
          WHERE
              (EL1 = '112111' OR EL1 = '112121') -- AR lines only
              AND STATPAY = 84
              AND MATCHLEVEL = 5
          GROUP BY CMPCODE, DOCCODE, DOCNUM) OPN
      ON MODS.CMPCODE = OPN.CMPCODE AND MODS.DOCCODE = OPN.DOCCODE AND MODS.DOCNUM = OPN.DOCNUM
      LEFT JOIN
          -- Settled amount (total of all items with 'Paid' status)
          (SELECT 
              CMPCODE,
              DOCCODE,
              DOCNUM,
              MAX(MODDATE) AS LASTMOD,
              SUM(VALUEDOC) AS TOTALDOC,
              SUM(VALUEHOME) AS TOTALHOME,
              SUM(VALUEDUAL) AS TOTALDUAL
          FROM
              OAS_DOCLINE
          WHERE
              (EL1 = '112111' OR EL1 = '112121') -- AR lines only
              AND STATPAY = 89
              AND MATCHLEVEL = 5
          GROUP BY CMPCODE, DOCCODE, DOCNUM) PAY
      ON MODS.CMPCODE = PAY.CMPCODE AND MODS.DOCCODE = PAY.DOCCODE AND MODS.DOCNUM = PAY.DOCNUM
      WHERE
          ORG.ROW_NO = 1
          AND HEAD.CMPCODE IN ('C165','C310','U125')
          AND ORG.VALUEDOC <> 0 -- Ignore zero-value docs posted by interface
          AND HEAD.STATUS = 78
          AND HEAD.DOCCODE IN (
            'SALE-INV-CDN',
            'SALE-INV-USD'
          )          
          
          UNION ALL
          
    -- Multi-client and/or low volume documents
      SELECT 
          HEAD.CMPCODE, 
          GET_CUSTID(ORG.CUST) AS CUSTID,
          HEAD.DOCCODE AS DOCTYPE,
          ORG.REF4 AS DOCSUBTYPE,
          TRIM(HEAD.DOCNUM) AS DOCNUM,
          GET_SOURCESYS(HEAD.DOCCODE, ORG.CUST) AS SOURCESYS,
          ORG.REF3 AS INVOICEREF,
          ORG.GLACCT,
          HEAD.DOCDATE AS DOCDATE,
          HEAD.INPDATE AS POSTDATE,
          (CASE WHEN OPN.TOTALDOC IS NULL THEN PAY.LASTMOD ELSE NULL END) AS CLEARDATE,
          ORG.DESCR AS DOCDESC,
          ORG.REF1 AS XACTREF,
          ORG.REF2 AS CUSTREF,
          ORG.REF5 AS EXTREF5,
          -- Currencies
          HEAD.CURDOC AS DOCCURR,
          CMP.HOMECUR AS HOMECURR,
          (CASE WHEN ORG.EQUIV IS NULL THEN NULL ELSE CMP.DUALCUR END) AS ALTCURR,
          -- Original Amounts
          GET_ORG_MASK(HEAD.DOCCODE) * ORG.VALUEDOC AS ORGVAL_DOC,
          GET_ORG_MASK(HEAD.DOCCODE) * ORG.VALUEHOME AS ORGVAL_HOME,
          (CASE WHEN ORG.EQUIV IS NULL THEN NULL ELSE GET_ORG_MASK(HEAD.DOCCODE) * ORG.VALUEDUAL END) AS ORGVAL_ALT,
          GET_ORG_MASK(HEAD.DOCCODE) * NORMALIZE_VALUE(ORG.VALUEHOME, ORG.VALUEDUAL, CMP.HOMECUR, 'USD') AS ORGVAL_NORM,
          -- Open Amounts
          OPN.TOTALDOC AS OPENVAL_DOC,
          OPN.TOTALHOME AS OPENVAL_HOME,
          (CASE WHEN ORG.EQUIV IS NULL THEN NULL ELSE OPN.TOTALDUAL END) AS OPENVAL_ALT, -- Alternate currency amount is only valid for clients with equivalent status
          NORMALIZE_VALUE(OPN.TOTALHOME, OPN.TOTALDUAL, CMP.HOMECUR, 'USD') AS OPENVAL_NORM,
          PAY.TOTALDOC AS PAYVAL_DOC,
          PAY.TOTALHOME AS PAYVAL_HOME,
          (CASE WHEN ORG.EQUIV IS NULL THEN NULL ELSE PAY.TOTALDUAL END) AS PAYVAL_ALT, -- Alternate currency amount is only valid for clients with equivalent status
          NORMALIZE_VALUE(PAY.TOTALHOME, PAY.TOTALDUAL, CMP.HOMECUR, 'USD') AS PAYVAL_NORM
      FROM
          -- List of documents (lines, actually) modified since P_START_DATE
          (SELECT CMPCODE, DOCCODE, DOCNUM, EL5 AS CUST, COUNT(1) AS MODLINES 
          FROM OAS_DOCLINE
          WHERE
              MODDATE >= P_START_DATE
              AND MODDATE < P_START_DATE + V_NUM_DAYS -- Line timestamp
              AND (EL1 = '112111' OR EL1 = '112121') -- AR lines only
              AND MATCHLEVEL = 5 -- Client lines only
          GROUP BY CMPCODE, DOCCODE, DOCNUM, EL5) MODS
      INNER JOIN
          -- Document header information
          OAS_DOCHEAD HEAD ON HEAD.CMPCODE = MODS.CMPCODE AND HEAD.DOCCODE = MODS.DOCCODE AND HEAD.DOCNUM = MODS.DOCNUM
      INNER JOIN
          -- Company information (for currencies)
          OAS_COMPANY CMP ON MODS.CMPCODE = CMP.CODE
      INNER JOIN
          -- Original amount (first line posted to 1121*1 account) 
          (SELECT
              DL.CMPCODE,
              DOCCODE,
              DOCNUM,
              EL5 AS CUST,
              VALUEDOC,
              VALUEHOME,
              VALUEDUAL,
              REF1,
              REF2,
              REF3,
              REF4,
              REF5,
              REF6,
              DESCR,
              EL1 AS GLACCT,
              DOCLINENUM AS FIRST_LINE,
              (CASE WHEN SUBSTR(DL.CMPCODE,1,1) = 'U' THEN
                DECODE(GRP.GRPCODE,
                  'USD', NULL,
                  'CAD', 'CAD',
                  NULL) ELSE
                DECODE(GRP.GRPCODE,
                  'USD', NULL,
                  'CAD', NULL,
                  'USE', 'USD',
                  NULL) END) EQUIV,        
              row_number() OVER (PARTITION BY DL.CMPCODE, DOCCODE, DOCNUM, EL5 ORDER BY DOCLINENUM ASC) AS ROW_NO
          FROM 
              OAS_DOCLINE DL
          LEFT  JOIN 
              OAS_GRPLIST GRP ON DL.EL5 = GRP.CODE  AND DL.CMPCODE = GRP.CMPCODE     
          WHERE
              (EL1 = '112111' OR EL1 = '112121') -- AR lines only
              AND MATCHLEVEL = 5
              AND GRP.GRPCODE IN ('CAD','USD','USE')) ORG -- LOCUS client currencies
      ON MODS.CMPCODE = ORG.CMPCODE AND MODS.DOCCODE = ORG.DOCCODE AND MODS.DOCNUM = ORG.DOCNUM AND MODS.CUST = ORG.CUST
      LEFT JOIN
          -- Open amount (total of all items with 'Available' status)
          (SELECT 
              CMPCODE,
              DOCCODE,
              DOCNUM,
              EL5 AS CUST,
              COUNT(1) AS LINES,
              SUM(VALUEDOC) AS TOTALDOC,
              SUM(VALUEHOME) AS TOTALHOME,
              SUM(VALUEDUAL) AS TOTALDUAL
          FROM
              OAS_DOCLINE
          WHERE
              (EL1 = '112111' OR EL1 = '112121') -- AR lines only
              AND STATPAY = 84
              AND MATCHLEVEL = 5
          GROUP BY CMPCODE, DOCCODE, DOCNUM, EL5) OPN
      ON MODS.CMPCODE = OPN.CMPCODE AND MODS.DOCCODE = OPN.DOCCODE AND MODS.DOCNUM = OPN.DOCNUM AND MODS.CUST = OPN.CUST
      LEFT JOIN
          -- Settled amount (total of all items with 'Paid' status)
          (SELECT 
              CMPCODE,
                    DOCCODE,
                    DOCNUM,
                    EL5 AS CUST,
                    MAX(MODDATE) AS LASTMOD,
                    SUM(VALUEDOC) AS TOTALDOC,
                    SUM(VALUEHOME) AS TOTALHOME,
                    SUM(VALUEDUAL) AS TOTALDUAL
                FROM
                    OAS_DOCLINE
                WHERE
                    (EL1 = '112111' OR EL1 = '112121') -- AR lines only
                    AND STATPAY = 89
                    AND MATCHLEVEL = 5
                GROUP BY CMPCODE, DOCCODE, DOCNUM, EL5) PAY
            ON MODS.CMPCODE = PAY.CMPCODE AND MODS.DOCCODE = PAY.DOCCODE AND MODS.DOCNUM = PAY.DOCNUM AND MODS.CUST = PAY.CUST
            WHERE
                ORG.ROW_NO = 1 -- First posting line only
                AND HEAD.CMPCODE IN ('C165','C310','U125')
                AND ORG.VALUEDOC <> 0 -- Ignore zero-value docs posted by interface
                AND HEAD.DOCCODE <> 'DISPERSE'
                AND HEAD.DOCCODE NOT LIKE 'YB%'
                AND HEAD.STATUS = 78 -- Only 'Posted' documents
                AND HEAD.DOCCODE NOT IN (
                  'SALE-INV-CDN',
                  'SALE-INV-USD'
                )          
            ;
  END OPEN_ITEMS_DELTA;

  PROCEDURE ACCT_BALANCES_NET(P_PERIOD INT, P_YEAR INT, OPEN_ITEM_CUR OUT SYS_REFCURSOR) AS
  BEGIN
    OPEN OPEN_ITEM_CUR FOR
      SELECT
        P_YEAR AS BAL_YR,
        P_PERIOD AS BAL_PERIOD,
        GET_CUSTID(EL5) AS ACCT, 
        CURCODE,
        SUM(BAL.FULL_VALUE) AS NET_BAL 
      FROM 
        OAS_BALANCE BAL
      WHERE EL5 != ' '
      AND (YR = P_YEAR AND PERIOD <= P_PERIOD)
      AND BALCODE = 'ACTUAL'
--      AND CURFLAG = 138
      AND REPBASIS = 0
      AND CMPCODE IN ('C165','C310','U125')
      AND EL1 IN ('112111','112121')
      AND EL5 LIKE 'C%'
      AND CURCODE = 'USD'
      GROUP BY 
        GET_CUSTID(EL5), CURCODE   
    ;
  END ACCT_BALANCES_NET;

  PROCEDURE SETTLED_ITEMS_DAYS(P_START_DATE DATE, P_DAYS INT, SETTLED_ITEM_CUR OUT SYS_REFCURSOR) AS
   V_NUM_DAYS INT := 1; -- Minimum of one day
  BEGIN
  -- Set-up the date interval
    IF P_DAYS < 0 THEN -- A negative value indicates that all items settled since the referenced date should be fetched
      V_NUM_DAYS := TRUNC(SYSDATE) - P_START_DATE;
    ELSIF P_DAYS > 0 THEN
      V_NUM_DAYS := P_DAYS;
    END IF;
    OPEN SETTLED_ITEM_CUR FOR
      SELECT 
        PL.CMPCODE,
        PL.DOCCODE AS DOCTYPE,
        PL.DOCNUM,
        GET_CUSTID(DL.EL5) AS CUSTID,
        TRUNC(PL.MODDATE) AS PAYDATE,
        PL.MATCHREF,
        DH.CURDOC AS DOCCURR,
        SUM(DL.VALUEDOC) PAYVAL_DOC
      FROM
        OAS_PAYLINE PL
        JOIN OAS_DOCLINE DL
          ON DL.CMPCODE = PL.CMPCODE AND DL.DOCCODE = PL.DOCCODE AND DL.DOCNUM = PL.DOCNUM AND DL.DOCLINENUM = PL.DOCLINENUM
        JOIN OAS_DOCHEAD DH
          ON DH.CMPCODE = PL.CMPCODE AND DH.DOCCODE = PL.DOCCODE AND DH.DOCNUM = PL.DOCNUM
      WHERE
        PL.MATCHREF != 0
        AND PL.MODDATE >= P_START_DATE
        AND PL.MODDATE < P_START_DATE + V_NUM_DAYS
        AND PL.CMPCODE IN ('C165','C310','U125')
        AND (DL.EL1 = '112111' OR DL.EL1 = '112121') -- AR lines only
        AND DL.MATCHLEVEL = 5 -- Clients only
        AND DH.DOCCODE NOT LIKE 'YB%'
        AND DH.DOCCODE != 'DISPERSE'
      GROUP BY
        PL.CMPCODE,
        PL.DOCCODE,
        PL.DOCNUM,
        GET_CUSTID(DL.EL5),
        PL.MATCHREF,
        TRUNC(PL.MODDATE),
        DH.CURDOC 
      ;
  END SETTLED_ITEMS_DAYS;
  
  PROCEDURE SETTLED_ITEMS_RANGE(P_START_DATE DATE, P_END_DATE DATE, SETTLED_ITEM_CUR OUT SYS_REFCURSOR) AS
  BEGIN
    OPEN SETTLED_ITEM_CUR FOR
      SELECT 
        PL.CMPCODE,
        PL.DOCCODE,
        PL.DOCNUM,
        GET_CUSTID(DL.EL5) AS CUSTID,
        TRUNC(PL.MODDATE) AS PAYDATE,
        PL.MATCHREF,
        DH.CURDOC AS DOCCURR,
        SUM(DL.VALUEDOC) PAYVAL_DOC
      FROM
        OAS_PAYLINE PL
        JOIN OAS_DOCLINE DL
          ON DL.CMPCODE = PL.CMPCODE AND DL.DOCCODE = PL.DOCCODE AND DL.DOCNUM = PL.DOCNUM AND DL.DOCLINENUM = PL.DOCLINENUM
        JOIN OAS_DOCHEAD DH
          ON DH.CMPCODE = PL.CMPCODE AND DH.DOCCODE = PL.DOCCODE AND DH.DOCNUM = PL.DOCNUM
      WHERE
        PL.MATCHREF != 0
        AND PL.MODDATE >= P_START_DATE
        AND PL.MODDATE < P_END_DATE + 1
        AND PL.CMPCODE IN ('C165','C310','U125')
        AND (DL.EL1 = '112111' OR DL.EL1 = '112121') -- AR lines only
        AND DL.MATCHLEVEL = 5 -- Clients only
        AND DH.DOCCODE NOT LIKE 'YB%'
        AND DH.DOCCODE != 'DISPERSE'
      GROUP BY
        PL.CMPCODE,
        PL.DOCCODE,
        PL.DOCNUM,
        GET_CUSTID(DL.EL5),
        PL.MATCHREF,
        TRUNC(PL.MODDATE),
        DH.CURDOC 
      ;
  END SETTLED_ITEMS_RANGE;
    
  PROCEDURE AR_ITEMS_HISTORY(P_START_DATE DATE, P_DAYS INT, AR_ITEM_CUR OUT SYS_REFCURSOR) AS
    V_NUM_DAYS INT := 1; -- Minimum of one day
  BEGIN
  -- Set-up the date interval
    IF P_DAYS < 0 THEN -- A negative value indicates that all records modified since the referenced date should be fetched
      V_NUM_DAYS := TRUNC(SYSDATE) - P_START_DATE;
    ELSIF P_DAYS > 0 THEN
      V_NUM_DAYS := P_DAYS;
    END IF;
    OPEN AR_ITEM_CUR FOR

    -- Retrieves documents closed since P_START_DATE (if P_DAYS = -1)
    --  Alternatively retrieves only documents closed within P_DAYS of P_START_DATE
    --  Only retrieves full-days if P_START_DATE < TRUNC(SYSDATE)
    --  Retrieves the current day's activity if P_START_DATE == TRUNC(SYSDATE)
    
    -- Single-client, high volume documents
      SELECT 
          HEAD.CMPCODE, 
          GET_CUSTID(ORG.CUST) AS CUSTID,
          HEAD.DOCCODE AS DOCTYPE,
          ORG.REF4 AS DOCSUBTYPE,
          TRIM(HEAD.DOCNUM) AS DOCNUM,
          GET_SOURCESYS(HEAD.DOCCODE, ORG.CUST) AS SOURCESYS,
          ORG.REF3 AS INVOICEREF,
          ORG.GLACCT,
          HEAD.DOCDATE AS DOCDATE,
          HEAD.INPDATE AS POSTDATE,
          (CASE WHEN OPN.TOTALDOC IS NULL THEN PAY.LASTMOD ELSE NULL END) AS CLEARDATE,
          ORG.DESCR AS DOCDESC,
          ORG.REF1 AS XACTREF,
          ORG.REF2 AS CUSTREF,
          ORG.REF5 AS EXTREF5,
          -- Currencies
          HEAD.CURDOC AS DOCCURR,
          CMP.HOMECUR AS HOMECURR,
          (CASE WHEN ORG.EQUIV IS NULL THEN NULL ELSE CMP.DUALCUR END) AS ALTCURR,
          -- Original Amounts
          GET_ORG_MASK(HEAD.DOCCODE) * ORG.VALUEDOC AS ORGVAL_DOC,
          GET_ORG_MASK(HEAD.DOCCODE) * ORG.VALUEHOME AS ORGVAL_HOME,
          (CASE WHEN ORG.EQUIV IS NULL THEN NULL ELSE GET_ORG_MASK(HEAD.DOCCODE) * ORG.VALUEDUAL END) AS ORGVAL_ALT,
          GET_ORG_MASK(HEAD.DOCCODE) * NORMALIZE_VALUE(ORG.VALUEHOME, ORG.VALUEDUAL, CMP.HOMECUR, 'USD') AS ORGVAL_NORM,
          -- Open Amounts
          OPN.TOTALDOC AS OPENVAL_DOC,
          OPN.TOTALHOME AS OPENVAL_HOME,
          (CASE WHEN ORG.EQUIV IS NULL THEN NULL ELSE OPN.TOTALDUAL END) AS OPENVAL_ALT, -- Alternate currency amount is only valid for clients with equivalent status
          NORMALIZE_VALUE(OPN.TOTALHOME, OPN.TOTALDUAL, CMP.HOMECUR, 'USD') AS OPENVAL_NORM,
          PAY.TOTALDOC AS PAYVAL_DOC,
          PAY.TOTALHOME AS PAYVAL_HOME,
          (CASE WHEN ORG.EQUIV IS NULL THEN NULL ELSE PAY.TOTALDUAL END) AS PAYVAL_ALT, -- Alternate currency amount is only valid for clients with equivalent status
          NORMALIZE_VALUE(PAY.TOTALHOME, PAY.TOTALDUAL, CMP.HOMECUR, 'USD') AS PAYVAL_NORM
      FROM
          -- List of documents (lines, actually) modified in the last day
          (SELECT CMPCODE, DOCCODE, DOCNUM, COUNT(1) AS MODLINES 
          FROM OAS_DOCLINE
          WHERE
              MODDATE >= P_START_DATE
              AND MODDATE < P_START_DATE + V_NUM_DAYS
              AND (EL1 = '112111' OR EL1 = '112121') -- AR lines only
              AND MATCHLEVEL = 5 -- Clients only
          GROUP BY CMPCODE, DOCCODE, DOCNUM) MODS
      INNER JOIN
          -- Document header information
          OAS_DOCHEAD HEAD ON HEAD.CMPCODE = MODS.CMPCODE AND HEAD.DOCCODE = MODS.DOCCODE AND HEAD.DOCNUM = MODS.DOCNUM
      INNER JOIN
          -- Company information (for currencies)
          OAS_COMPANY CMP ON MODS.CMPCODE = CMP.CODE
      INNER JOIN
          -- Original amount (first line posted to 1121*1 account) 
          (SELECT
              DL.CMPCODE,
              DOCCODE,
              DOCNUM,
              EL5 AS CUST,
              VALUEDOC,
              VALUEHOME,
              VALUEDUAL,
              REF1,
              REF2,
              REF3,
              REF4,
              REF5,
              REF6,
              DESCR,
              EL1 AS GLACCT,
              DOCLINENUM AS FIRST_LINE,
              (CASE WHEN SUBSTR(DL.CMPCODE,1,1) = 'U' THEN
                DECODE(GRP.GRPCODE,
                  'USD', NULL,
                  'CAD', 'CAD',
                  NULL) ELSE
                DECODE(GRP.GRPCODE,
                  'USD', NULL,
                  'CAD', NULL,
                  'USE', 'USD',
                  NULL) END) EQUIV,        
              row_number() OVER (PARTITION BY DL.CMPCODE, DOCCODE, DOCNUM ORDER BY DOCLINENUM ASC) AS ROW_NO
          FROM 
              OAS_DOCLINE DL
          LEFT  JOIN 
              OAS_GRPLIST GRP ON DL.EL5 = GRP.CODE  AND DL.CMPCODE = GRP.CMPCODE     
          WHERE
              (EL1 = '112111' OR EL1 = '112121') -- AR lines only
              AND MATCHLEVEL = 5
              AND GRP.GRPCODE IN ('CAD','USD','USE')) ORG -- LOCUS client currencies
      ON MODS.CMPCODE = ORG.CMPCODE AND MODS.DOCCODE = ORG.DOCCODE AND MODS.DOCNUM = ORG.DOCNUM
      LEFT JOIN
          -- Open amount (total of all items with 'Available' status)
          (SELECT 
              CMPCODE,
              DOCCODE,
              DOCNUM,
              COUNT(1) AS LINES,
              SUM(VALUEDOC) AS TOTALDOC,
              SUM(VALUEHOME) AS TOTALHOME,
              SUM(VALUEDUAL) AS TOTALDUAL
          FROM
              OAS_DOCLINE
          WHERE
              (EL1 = '112111' OR EL1 = '112121') -- AR lines only
              AND STATPAY = 84
              AND MATCHLEVEL = 5
          GROUP BY CMPCODE, DOCCODE, DOCNUM) OPN
      ON MODS.CMPCODE = OPN.CMPCODE AND MODS.DOCCODE = OPN.DOCCODE AND MODS.DOCNUM = OPN.DOCNUM
      LEFT JOIN
          -- Settled amount (total of all items with 'Paid' status)
          (SELECT 
              CMPCODE,
              DOCCODE,
              DOCNUM,
              MAX(MODDATE) AS LASTMOD,
              SUM(VALUEDOC) AS TOTALDOC,
              SUM(VALUEHOME) AS TOTALHOME,
              SUM(VALUEDUAL) AS TOTALDUAL
          FROM
              OAS_DOCLINE
          WHERE
              (EL1 = '112111' OR EL1 = '112121') -- AR lines only
              AND STATPAY = 89
              AND MATCHLEVEL = 5
          GROUP BY CMPCODE, DOCCODE, DOCNUM) PAY
      ON MODS.CMPCODE = PAY.CMPCODE AND MODS.DOCCODE = PAY.DOCCODE AND MODS.DOCNUM = PAY.DOCNUM
      WHERE
          ORG.ROW_NO = 1
          AND HEAD.CMPCODE IN ('C165','C310','U125')
          AND ORG.VALUEDOC <> 0 -- Ignore zero-value docs posted by interface
          AND HEAD.STATUS = 78
          AND HEAD.DOCCODE IN (
            'SALE-INV-CDN',
            'SALE-INV-USD'
          )          
          AND OPN.TOTALDOC IS NULL
          
          UNION ALL
          
    -- Multi-client and/or low volume documents
      SELECT 
          HEAD.CMPCODE, 
          GET_CUSTID(ORG.CUST) AS CUSTID,
          HEAD.DOCCODE AS DOCTYPE,
          ORG.REF4 AS DOCSUBTYPE,
          TRIM(HEAD.DOCNUM) AS DOCNUM,
          GET_SOURCESYS(HEAD.DOCCODE, ORG.CUST) AS SOURCESYS,
          ORG.REF3 AS INVOICEREF,
          ORG.GLACCT,
          HEAD.DOCDATE AS DOCDATE,
          HEAD.INPDATE AS POSTDATE,
          (CASE WHEN OPN.TOTALDOC IS NULL THEN PAY.LASTMOD ELSE NULL END) AS CLEARDATE,
          ORG.DESCR AS DOCDESC,
          ORG.REF1 AS XACTREF,
          ORG.REF2 AS CUSTREF,
          ORG.REF5 AS EXTREF5,
          -- Currencies
          HEAD.CURDOC AS DOCCURR,
          CMP.HOMECUR AS HOMECURR,
          (CASE WHEN ORG.EQUIV IS NULL THEN NULL ELSE CMP.DUALCUR END) AS ALTCURR,
          -- Original Amounts
          GET_ORG_MASK(HEAD.DOCCODE) * ORG.VALUEDOC AS ORGVAL_DOC,
          GET_ORG_MASK(HEAD.DOCCODE) * ORG.VALUEHOME AS ORGVAL_HOME,
          (CASE WHEN ORG.EQUIV IS NULL THEN NULL ELSE GET_ORG_MASK(HEAD.DOCCODE) * ORG.VALUEDUAL END) AS ORGVAL_ALT,
          GET_ORG_MASK(HEAD.DOCCODE) * NORMALIZE_VALUE(ORG.VALUEHOME, ORG.VALUEDUAL, CMP.HOMECUR, 'USD') AS ORGVAL_NORM,
          -- Open Amounts
          OPN.TOTALDOC AS OPENVAL_DOC,
          OPN.TOTALHOME AS OPENVAL_HOME,
          (CASE WHEN ORG.EQUIV IS NULL THEN NULL ELSE OPN.TOTALDUAL END) AS OPENVAL_ALT, -- Alternate currency amount is only valid for clients with equivalent status
          NORMALIZE_VALUE(OPN.TOTALHOME, OPN.TOTALDUAL, CMP.HOMECUR, 'USD') AS OPENVAL_NORM,
          PAY.TOTALDOC AS PAYVAL_DOC,
          PAY.TOTALHOME AS PAYVAL_HOME,
          (CASE WHEN ORG.EQUIV IS NULL THEN NULL ELSE PAY.TOTALDUAL END) AS PAYVAL_ALT, -- Alternate currency amount is only valid for clients with equivalent status
          NORMALIZE_VALUE(PAY.TOTALHOME, PAY.TOTALDUAL, CMP.HOMECUR, 'USD') AS PAYVAL_NORM
      FROM
          -- List of documents (lines, actually) modified since P_START_DATE
          (SELECT CMPCODE, DOCCODE, DOCNUM, EL5 AS CUST, COUNT(1) AS MODLINES 
          FROM OAS_DOCLINE
          WHERE
              MODDATE >= P_START_DATE
              AND MODDATE < P_START_DATE + V_NUM_DAYS -- Line timestamp
              AND (EL1 = '112111' OR EL1 = '112121') -- AR lines only
              AND MATCHLEVEL = 5 -- Client lines only
          GROUP BY CMPCODE, DOCCODE, DOCNUM, EL5) MODS
      INNER JOIN
          -- Document header information
          OAS_DOCHEAD HEAD ON HEAD.CMPCODE = MODS.CMPCODE AND HEAD.DOCCODE = MODS.DOCCODE AND HEAD.DOCNUM = MODS.DOCNUM
      INNER JOIN
          -- Company information (for currencies)
          OAS_COMPANY CMP ON MODS.CMPCODE = CMP.CODE
      INNER JOIN
          -- Original amount (first line posted to 1121*1 account) 
          (SELECT
              DL.CMPCODE,
              DOCCODE,
              DOCNUM,
              EL5 AS CUST,
              VALUEDOC,
              VALUEHOME,
              VALUEDUAL,
              REF1,
              REF2,
              REF3,
              REF4,
              REF5,
              REF6,
              DESCR,
              EL1 AS GLACCT,
              DOCLINENUM AS FIRST_LINE,
              (CASE WHEN SUBSTR(DL.CMPCODE,1,1) = 'U' THEN
                DECODE(GRP.GRPCODE,
                  'USD', NULL,
                  'CAD', 'CAD',
                  NULL) ELSE
                DECODE(GRP.GRPCODE,
                  'USD', NULL,
                  'CAD', NULL,
                  'USE', 'USD',
                  NULL) END) EQUIV,        
              row_number() OVER (PARTITION BY DL.CMPCODE, DOCCODE, DOCNUM, EL5 ORDER BY DOCLINENUM ASC) AS ROW_NO
          FROM 
              OAS_DOCLINE DL
          LEFT  JOIN 
              OAS_GRPLIST GRP ON DL.EL5 = GRP.CODE  AND DL.CMPCODE = GRP.CMPCODE     
          WHERE
              (EL1 = '112111' OR EL1 = '112121') -- AR lines only
              AND MATCHLEVEL = 5
              AND GRP.GRPCODE IN ('CAD','USD','USE')) ORG -- LOCUS client currencies
      ON MODS.CMPCODE = ORG.CMPCODE AND MODS.DOCCODE = ORG.DOCCODE AND MODS.DOCNUM = ORG.DOCNUM AND MODS.CUST = ORG.CUST
      LEFT JOIN
          -- Open amount (total of all items with 'Available' status)
          (SELECT 
              CMPCODE,
              DOCCODE,
              DOCNUM,
              EL5 AS CUST,
              COUNT(1) AS LINES,
              SUM(VALUEDOC) AS TOTALDOC,
              SUM(VALUEHOME) AS TOTALHOME,
              SUM(VALUEDUAL) AS TOTALDUAL
          FROM
              OAS_DOCLINE
          WHERE
              (EL1 = '112111' OR EL1 = '112121') -- AR lines only
              AND STATPAY = 84
              AND MATCHLEVEL = 5
          GROUP BY CMPCODE, DOCCODE, DOCNUM, EL5) OPN
      ON MODS.CMPCODE = OPN.CMPCODE AND MODS.DOCCODE = OPN.DOCCODE AND MODS.DOCNUM = OPN.DOCNUM AND MODS.CUST = OPN.CUST
      LEFT JOIN
          -- Settled amount (total of all items with 'Paid' status)
          (SELECT 
              CMPCODE,
                    DOCCODE,
                    DOCNUM,
                    EL5 AS CUST,
                    MAX(MODDATE) AS LASTMOD,
                    SUM(VALUEDOC) AS TOTALDOC,
                    SUM(VALUEHOME) AS TOTALHOME,
                    SUM(VALUEDUAL) AS TOTALDUAL
                FROM
                    OAS_DOCLINE
                WHERE
                    (EL1 = '112111' OR EL1 = '112121') -- AR lines only
                    AND STATPAY = 89
                    AND MATCHLEVEL = 5
                GROUP BY CMPCODE, DOCCODE, DOCNUM, EL5) PAY
            ON MODS.CMPCODE = PAY.CMPCODE AND MODS.DOCCODE = PAY.DOCCODE AND MODS.DOCNUM = PAY.DOCNUM AND MODS.CUST = PAY.CUST
            WHERE
                ORG.ROW_NO = 1 -- First posting line only
                AND HEAD.CMPCODE IN ('C165','C310','U125')
                AND ORG.VALUEDOC <> 0 -- Ignore zero-value docs posted by interface
                AND HEAD.DOCCODE <> 'DISPERSE'
                AND HEAD.DOCCODE NOT LIKE 'YB%'
                AND HEAD.STATUS = 78 -- Only 'Posted' documents
                AND HEAD.DOCCODE NOT IN (
                  'SALE-INV-CDN',
                  'SALE-INV-USD'
                )          
            AND OPN.TOTALDOC IS NULL
          ;
  END AR_ITEMS_HISTORY;
  
  FUNCTION GET_CUSTID(ELMCODE IN VARCHAR2) RETURN VARCHAR2 AS
    V_CUSTID VARCHAR2(12) := NULL;
  BEGIN
    IF SUBSTR(ELMCODE, 1, 3) = 'CJN' THEN
      V_CUSTID := ELMCODE;
    ELSE
      IF SUBSTR(ELMCODE, 10, 3) = '000' THEN 
        V_CUSTID := SUBSTR(ELMCODE, 4, 6);
      ELSE
        V_CUSTID := SUBSTR(ELMCODE, 4, 9);
      END IF;
    END IF;

    RETURN V_CUSTID;
  END;
  
  FUNCTION GET_PARENTID(ELMCODE IN VARCHAR2) RETURN VARCHAR2 AS
    V_PARENTID VARCHAR2(6) := NULL;
  BEGIN 
    IF SUBSTR(ELMCODE, 1, 3) = 'CJN' THEN
      V_PARENTID := NULL;
    ELSE
      IF SUBSTR(ELMCODE, 10, 3) = '000' THEN 
        V_PARENTID := NULL;
      ELSE
        V_PARENTID := SUBSTR(ELMCODE, 4, 6);
      END IF;
    END IF;

    RETURN V_PARENTID;  
  END;

  -- Returns the value for the specified currency
  FUNCTION NORMALIZE_VALUE(HOMEVAL IN NUMBER, DUALVAL IN NUMBER, HOMECUR IN VARCHAR, NORMCUR IN VARCHAR2) RETURN NUMBER AS
  BEGIN 
    RETURN ((CASE WHEN HOMECUR = NORMCUR THEN HOMEVAL ELSE DUALVAL END));
  END;

  -- Determine alternate payment currency, where appropriate. Return NULL when no alternate currency applies
  FUNCTION GET_ALTCUR(CMPCODE IN VARCHAR2, GRPCODE IN VARCHAR2) RETURN VARCHAR2 AS
    V_CUR VARCHAR2(3) := NULL;
  BEGIN 
    IF SUBSTR(CMPCODE,1,1) = 'U' THEN
      CASE GRPCODE
        WHEN 'USD' THEN V_CUR := NULL;
        WHEN 'CAD' THEN V_CUR := 'CAD';
        ELSE V_CUR := NULL;
      END CASE;
    ELSE
      CASE GRPCODE
        WHEN 'USD' THEN V_CUR := NULL;
        WHEN 'CAD' THEN V_CUR := NULL;
        WHEN 'USE' THEN V_CUR := 'USD';
        ELSE V_CUR := NULL;
      END CASE;
    END IF;
              
    RETURN V_CUR;
  END;

  -- Identify the originating system for a document, given the DOCCODE and ELEMENT code
  FUNCTION GET_SOURCESYS(DOCCODE IN VARCHAR2, ELMCODE IN VARCHAR2) RETURN VARCHAR2 AS
    V_SYS VARCHAR2(32);
  BEGIN
    IF DOCCODE IN ('SALE-INV-CDN','SALE-INV-USD') THEN
      CASE SUBSTR(ELMCODE,1,3)
        WHEN 'CLO' THEN
          V_SYS := 'LOCUS';
        WHEN 'CAL' THEN
          V_SYS := 'Alliance';
        WHEN 'CSB' THEN
          V_SYS := 'SmartBorder';
        WHEN 'CJN' THEN
          V_SYS := 'Jensen';
        WHEN 'CAS' THEN
          V_SYS := 'AS400';
        ELSE
          V_SYS := 'Unknown';
      END CASE;
    ELSE 
      V_SYS := 'CODA';
    END IF;

  RETURN  V_SYS;
  END;

  -- Payment and invoice DOCCODEs have an "original value" all others do not
  FUNCTION GET_ORG_MASK(DOCCODE IN VARCHAR2) RETURN NUMBER AS
  BEGIN
    IF (DOCCODE LIKE 'AR %' OR DOCCODE IN('SALE-INV-CDN', 'SALE-INV-USD', 'K84 GST CDN')) THEN
      RETURN 1;
    ELSE
      RETURN 0;
    END IF;
  END;
  
END AR_EXTRACTS;