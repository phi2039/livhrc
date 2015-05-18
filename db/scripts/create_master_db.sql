DROP SCHEMA MASTER_DATA;

CREATE SCHEMA MASTER_DATA;

CREATE CACHED TABLE MASTER_DATA.AR_ITEM_MASTER
(
   CMPCODE       VARCHAR(4)    NOT NULL,
   CUSTID        VARCHAR(32)   NOT NULL,
   DOCTYPE       VARCHAR(32)   NOT NULL,
   DOCSUBTYPE    VARCHAR(8),
   DOCNUM        BIGINT        NOT NULL,
   SOURCESYS     VARCHAR(32),
   INVOICEREF    VARCHAR(32),
   GLACCT        BIGINT,
   DOCDATE       DATE           NOT NULL,
   POSTDATE      DATE           NOT NULL,
   CLEARDATE     TIMESTAMP     DEFAULT NULL,
   DOCDESC       VARCHAR(48),
   XACTREF       VARCHAR(32),
   CUSTREF       VARCHAR(32),
   EXTREF5       VARCHAR(32),
   DOCCURR       VARCHAR(3),
   HOMECURR      VARCHAR(3),
   ALTCURR       VARCHAR(3),
   ORGVAL_DOC    DECIMAL(20,2),
   ORGVAL_HOME   DECIMAL(20,2),
   ORGVAL_ALT    DECIMAL(20,2),
   ORGVAL_NORM   DECIMAL(20,2),
   OPENVAL_DOC   DECIMAL(20,2),
   OPENVAL_HOME  DECIMAL(20,2),
   OPENVAL_ALT   DECIMAL(20,2),
   OPENVAL_NORM  DECIMAL(20,2),
   PAYVAL_DOC    DECIMAL(20,2),
   PAYVAL_HOME   DECIMAL(20,2),
   PAYVAL_ALT    DECIMAL(20,2),
   PAYVAL_NORM   DECIMAL(20,2),
   K84ACCTDATE   DATE           DEFAULT NULL,
   DUEDATE       DATE           DEFAULT NULL,
   LASTMOD       TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE MASTER_DATA.AR_ITEM_MASTER
   ADD PRIMARY KEY (CMPCODE, DOCTYPE, DOCNUM, CUSTID);

CREATE INDEX MASTER_DATA.IDX_AR_ITEM_CLEARED
   ON MASTER_DATA.AR_ITEM_MASTER (CLEARDATE ASC);

CREATE INDEX MASTER_DATA.IDX_AR_ITEM_XACT
   ON MASTER_DATA.AR_ITEM_MASTER (XACTREF ASC);

CREATE INDEX MASTER_DATA.IDX_AR_ITEM_CURR
   ON MASTER_DATA.AR_ITEM_MASTER (DOCCURR ASC, HOMECURR ASC, ALTCURR ASC);

CREATE INDEX MASTER_DATA.IDX_AR_ITEM_MOD
   ON MASTER_DATA.AR_ITEM_MASTER (LASTMOD DESC);
   
CREATE INDEX MASTER_DATA.IDX_AR_ITEM_DUE
   ON MASTER_DATA.AR_ITEM_MASTER (DUEDATE DESC);
   
CREATE INDEX MASTER_DATA.IDX_AR_ITEM_K84
   ON MASTER_DATA.AR_ITEM_MASTER (k84ACCTDATE DESC);
      
CREATE CACHED TABLE MASTER_DATA.CUSTOMER_MASTER
(
   CUSTID      VARCHAR(32)     NOT NULL,
   NAME        VARCHAR(1024),
   ADD1        VARCHAR(1024),
   ADD2        VARCHAR(1024),
   ADD3        VARCHAR(1024),
   CITY        VARCHAR(1024),
   STATEPROV   VARCHAR(16),
   COUNTRY     VARCHAR(32),
   POSTCODE    VARCHAR(32),
   CREATEDATE  DATE             DEFAULT CURRENT_DATE,
   TERMS       TINYINT,
   CREDITLIM   DECIMAL(20,2),
   TTMREV      DECIMAL(20,2),
   SFID        VARCHAR(64),
   CORPCODE    CHAR(1),
   CRLIMDATE   DATE,
   ACCTSTAT    CHAR(1),
   ACCTTYPE    CHAR(1),
   BONDTYPE    CHAR(1),
   PREVCO      CHAR(2),
   SIC         CHAR(4),
   CST         VARCHAR(32),
   CSM         VARCHAR(128),
   MSD         VARCHAR(128),
   SC          VARCHAR(128),
   WAIVERTYPE  VARCHAR(256),
   INVCURR     CHAR(3),
   CREDITCODE  VARCHAR(3),
   LANGUAGE    VARCHAR(32),
   PARENT      VARCHAR(32),
   REMITTO     TINYINT,
   REMARKS1    VARCHAR(128),
   REMARKS2    VARCHAR(128),
   LASTARACT   TIMESTAMP     DEFAULT NULL,
   LASTMOD     TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE MASTER_DATA.CUSTOMER_MASTER
   ADD PRIMARY KEY (CUSTID);

CREATE INDEX MASTER_DATA.IDX_CUST_PARENT
   ON MASTER_DATA.CUSTOMER_MASTER (PARENT ASC);

CREATE INDEX MASTER_DATA.IDX_CUST_MOD
   ON MASTER_DATA.CUSTOMER_MASTER (LASTMOD DESC);

CREATE CACHED TABLE MASTER_DATA.K84_MASTER
(
   XACTID    VARCHAR(32)    NOT NULL,
   RPTDATE   TIMESTAMP,
   ACCTDATE  TIMESTAMP,
   SECID     INT             NOT NULL,
   LASTMOD   TIMESTAMP      DEFAULT CURRENT_TIMESTAMP   
);

ALTER TABLE MASTER_DATA.K84_MASTER
   ADD PRIMARY KEY (SECID, XACTID);
   
CREATE INDEX MASTER_DATA.IDX_K84_MOD
   ON MASTER_DATA.K84_MASTER (LASTMOD DESC);   

CREATE TABLE MASTER_DATA.AR_SETTLEMENT_MASTER
(
   CMPCODE    VARCHAR(4)    NOT NULL,
   CUSTID     VARCHAR(32)   NOT NULL,
   DOCTYPE    VARCHAR(32)   NOT NULL,
   DOCNUM     BIGINT        NOT NULL,
   MATCHREF   BIGINT        NOT NULL,
   PAYDATE    TIMESTAMP,
   PAYVAL_DOC DOUBLE        DEFAULT 0,
   DOCCURR    VARCHAR(3),
   LASTMOD    TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE MASTER_DATA.AR_SETTLEMENT_EXT
   ADD PRIMARY KEY (CMPCODE, CUSTID, DOCTYPE, DOCNUM, MATCHREF);
   
CREATE INDEX MASTER_DATA.IDX_SETTLEMENT_MOD
   ON MASTER_DATA.AR_SETTLEMENT_MASTER (LASTMOD DESC);   

CREATE TABLE MASTER_DATA.MONTH_END_CAL
(
   YEAR       SMALLINT       NOT NULL,
   PERIOD     TINYINT        NOT NULL,
   STARTDATE  DATE           NOT NULL,
   CUTOFFDATE DATE           NOT NULL,
   CLOSEDATE  DATE           NOT NULL,
   LASTMOD    TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE MASTER_DATA.MONTH_END_CAL
   ADD PRIMARY KEY (YEAR, PERIOD);