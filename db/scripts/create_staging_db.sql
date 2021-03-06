DROP SCHEMA STAGING;

CREATE SCHEMA STAGING;

CREATE CACHED TABLE STAGING.AR_ITEM_EXT
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
   DUEDATE       DATE           DEFAULT NULL
);

ALTER TABLE STAGING.AR_ITEM_EXT
   ADD PRIMARY KEY (CMPCODE, DOCTYPE, DOCNUM, CUSTID);

CREATE CACHED TABLE STAGING.CUST_SNAP
(
   CUSTID      VARCHAR(32)            NOT NULL,
   NAME        VARCHAR(1024),
   ADD1        VARCHAR(1024),
   ADD2        VARCHAR(1024),
   ADD3        VARCHAR(1024),
   CITY        VARCHAR(1024),
   STATEPROV   VARCHAR(16),
   COUNTRY     VARCHAR(32),
   POSTCODE    VARCHAR(32),
   CREATEDATE  DATE,
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
   LASTSTATE   VARCHAR(32)
);

ALTER TABLE STAGING.CUST_SNAP
   ADD PRIMARY KEY (CUSTID ASC);

-- For hierarchy updates
CREATE INDEX STAGING.IDX_CUST_PARENT
   ON STAGING.CUST_SNAP (PARENT ASC);
   
CREATE CACHED TABLE STAGING.CUST_EXT_CODA
(
   CUSTID      VARCHAR(32)            NOT NULL,
   NAME        VARCHAR(1024),
   ADD1        VARCHAR(1024),
   ADD2        VARCHAR(1024),
   ADD3        VARCHAR(1024),
   CITY        VARCHAR(1024),
   STATEPROV   VARCHAR(16),
   COUNTRY     VARCHAR(32),
   POSTCODE    VARCHAR(32),
   CREATEDATE  DATE,
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
   REMARKS2    VARCHAR(128)
);

ALTER TABLE STAGING.CUST_EXT_CODA
   ADD PRIMARY KEY (CUSTID ASC);
   
CREATE CACHED TABLE STAGING.CUST_EXT_GQL
(
   CUSTID      VARCHAR(32)            NOT NULL,
   NAME        VARCHAR(1024),
   ADD1        VARCHAR(1024),
   ADD2        VARCHAR(1024),
   ADD3        VARCHAR(1024),
   CITY        VARCHAR(1024),
   STATEPROV   VARCHAR(16),
   COUNTRY     VARCHAR(32),
   POSTCODE    VARCHAR(32),
   CREATEDATE  DATE,
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
   REMARKS2    VARCHAR(128)
);

ALTER TABLE STAGING.CUST_EXT_GQL
   ADD PRIMARY KEY (CUSTID ASC);   

CREATE CACHED TABLE STAGING.K84_EXT
(
   XACTID    VARCHAR(32)    NOT NULL,
   RPTDATE   TIMESTAMP,
   ACCTDATE  TIMESTAMP,
   SECID     INT             NOT NULL
);

ALTER TABLE STAGING.K84_EXT
   ADD PRIMARY KEY (SECID, XACTID);

CREATE TABLE STAGING.AR_SETTLEMENT_EXT
(
   CMPCODE    VARCHAR(4)    NOT NULL,
   CUSTID     VARCHAR(32)   NOT NULL,
   DOCTYPE    VARCHAR(32)   NOT NULL,
   DOCNUM     BIGINT        NOT NULL,
   MATCHREF   BIGINT        NOT NULL,
   PAYDATE    TIMESTAMP,
   PAYVAL_DOC DOUBLE        DEFAULT 0,
   DOCCURR    VARCHAR(3),
);

ALTER TABLE STAGING.AR_SETTLEMENT_EXT
   ADD PRIMARY KEY (CMPCODE, CUSTID, DOCTYPE, DOCNUM, MATCHREF);
   
CREATE SCHEMA VALIDATION;

CREATE TABLE VALIDATION.CUST_HIER_ERRORS
(
   CUSTID VARCHAR(32) NOT NULL,
   H1     VARCHAR(32),
   H2     VARCHAR(32),
   H3     VARCHAR(32),
   H4     VARCHAR(32),
   H5     VARCHAR(32)
);

ALTER TABLE VALIDATION.CUST_HIER_ERRORS
   ADD PRIMARY KEY (CUSTID);