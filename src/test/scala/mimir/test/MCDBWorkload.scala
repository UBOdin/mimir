package mimir.test

import java.io._
import mimir.algebra._

object MCDBWorkload
{
  def isDownloaded = new File("test/mcdb").exists()

   val attributes = Seq(
    //  Table Name               Create DDL                                                               Schema                            Load Timeout (s)
    ("customer",                 """CREATE TABLE CUSTOMER ( C_CUSTKEY     INTEGER NOT NULL,
                                                             C_NAME        VARCHAR(25) NOT NULL,
                                                             C_ADDRESS     VARCHAR(40) NOT NULL,
                                                             C_NATIONKEY   INTEGER NOT NULL,
                                                             C_PHONE       CHAR(15) NOT NULL,
                                                             C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
                                                             C_MKTSEGMENT  CHAR(10) NOT NULL,
                                                             C_COMMENT     VARCHAR(117) NOT NULL);""",   Seq(
                                                                                                           ("C_CUSTKEY", TInt()),
                                                                                                           ("C_NAME", TString()),
                                                                                                           ("C_ADDRESS",TString()),
                                                                                                           ("C_NATIONKEY",TInt()),
                                                                                                           ("C_PHONE",TString()),
                                                                                                           ("C_ACCTBAL",TFloat()),
                                                                                                           ("C_MKTSEGMENT",TString()),
                                                                                                           ("C_COMMENT",TString())),         5.000),
                                                             
    ("supplier",                 """CREATE TABLE SUPPLIER ( S_SUPPKEY     INTEGER NOT NULL,
                                                             S_NAME        CHAR(25) NOT NULL,
                                                             S_ADDRESS     VARCHAR(40) NOT NULL,
                                                             S_NATIONKEY   INTEGER NOT NULL,
                                                             S_PHONE       CHAR(15) NOT NULL,
                                                             S_ACCTBAL     DECIMAL(15,2) NOT NULL,
                                                             S_COMMENT     VARCHAR(101) NOT NULL);""",   Seq(
                                                                                                           ("S_SUPPKEY", TInt()),
                                                                                                           ("S_NAME", TString()),
                                                                                                           ("S_ADDRESS",TString()),
                                                                                                           ("S_NATIONKEY",TInt()),
                                                                                                           ("S_PHONE",TString()),
                                                                                                           ("S_ACCTBAL",TFloat()),
                                                                                                           ("C_COMMENT",TString())),         5.000),
                                                             
    ("lineitem",                 """CREATE TABLE LINEITEM ( L_ORDERKEY    INTEGER NOT NULL,
                                                             L_PARTKEY     INTEGER NOT NULL,
                                                             L_SUPPKEY     INTEGER NOT NULL,
                                                             L_LINENUMBER  INTEGER NOT NULL,
                                                             L_QUANTITY    DECIMAL(15,2) NOT NULL,
                                                             L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
                                                             L_DISCOUNT    DECIMAL(15,2) NOT NULL,
                                                             L_TAX         DECIMAL(15,2) NOT NULL,
                                                             L_RETURNFLAG  CHAR(1) NOT NULL,
                                                             L_LINESTATUS  CHAR(1) NOT NULL,
                                                             L_SHIPDATE    DATE NOT NULL,
                                                             L_COMMITDATE  DATE NOT NULL,
                                                             L_RECEIPTDATE DATE NOT NULL,
                                                             L_SHIPINSTRUCT CHAR(25) NOT NULL,
                                                             L_SHIPMODE     CHAR(10) NOT NULL,
                                                             L_COMMENT      VARCHAR(44) NOT NULL);""",   Seq(
                                                                                                           ("L_ORDERKEY", TInt()),
                                                                                                           ("L_PARTKEY", TInt()),
                                                                                                           ("L_SUPPKEY", TInt()),
                                                                                                           ("L_LINENUMBER", TInt()),
                                                                                                           ("L_QUANTITY", TFloat()),
                                                                                                           ("L_EXTENDEDPRICE", TFloat()),
                                                                                                           ("L_DISCOUNT", TFloat()),
                                                                                                           ("L_TAX", TFloat()),
                                                                                                           ("L_RETURNFLAG", TString()),
                                                                                                           ("L_LINESTATUS",TString()),
                                                                                                           ("S_PHONE",TString()),
                                                                                                           ("L_SHIPDATE",TDate()),
                                                                                                           ("L_COMMITDATE",TDate()),
                                                                                                           ("L_RECEIPTDATE",TDate()),
                                                                                                           ("L_SHIPINSTRUCT",TString()),
                                                                                                           ("L_SHIPMODE",TString()),
                                                                                                           ("L_COMMENT",TString())),         5.000),
                                                             
    ("nation",                   """CREATE TABLE NATION ( N_NATIONKEY  INTEGER NOT NULL,
                                                             N_NAME       CHAR(25) NOT NULL,
                                                             N_REGIONKEY  INTEGER NOT NULL,
                                                             N_COMMENT    VARCHAR(152));""",             Seq(
                                                                                                           ("N_NATIONKEY", TInt()),
                                                                                                           ("N_NAME", TString()),
                                                                                                           ("N_REGIONKEY",TInt()),
                                                                                                           ("N_COMMENT",TString())),         5.000),
                                                             
    ("orders",                   """CREATE TABLE ORDERS ( O_ORDERKEY       INTEGER NOT NULL,
                                                             O_CUSTKEY        INTEGER NOT NULL,
                                                             O_ORDERSTATUS    CHAR(1) NOT NULL,
                                                             O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
                                                             O_ORDERDATE      DATE NOT NULL,
                                                             O_ORDERPRIORITY  CHAR(15) NOT NULL,  
                                                             O_CLERK          CHAR(15) NOT NULL, 
                                                             O_SHIPPRIORITY   INTEGER NOT NULL,
                                                             O_COMMENT        VARCHAR(79) NOT NULL);""", Seq(
                                                                                                           ("O_ORDERKEY", TInt()),
                                                                                                           ("O_CUSTKEY", TInt()),
                                                                                                           ("O_ORDERSTATUS", TString()),
                                                                                                           ("O_TOTALPRICE", TFloat()),
                                                                                                           ("O_ORDERDATE",TDate()),
                                                                                                           ("O_ORDERPRIORITY",TString()),
                                                                                                           ("O_CLERK",TString()),
                                                                                                           ("O_SHIPPRIORITY", TInt()),
                                                                                                           ("O_COMMENT",TString())),          5.000),
                                                             
    ("part",                     """CREATE TABLE PART  ( P_PARTKEY     INTEGER NOT NULL,
                                                             P_NAME        VARCHAR(55) NOT NULL,
                                                             P_MFGR        CHAR(25) NOT NULL,
                                                             P_BRAND       CHAR(10) NOT NULL,
                                                             P_TYPE        VARCHAR(25) NOT NULL,
                                                             P_SIZE        INTEGER NOT NULL,
                                                             P_CONTAINER   CHAR(10) NOT NULL,
                                                             P_RETAILPRICE DECIMAL(15,2) NOT NULL,
                                                             P_COMMENT     VARCHAR(23) NOT NULL );""",  Seq(
                                                                                                           ("P_PARTKEY", TInt()),
                                                                                                           ("P_NAME", TString()),
                                                                                                           ("P_MFGR", TString()),
                                                                                                           ("P_BRAND", TString()),
                                                                                                           ("P_TYPE",TString()),
                                                                                                           ("P_SIZE",TInt()),
                                                                                                           ("P_CONTAINER",TString()),
                                                                                                           ("P_RETAILPRICE", TFloat()),
                                                                                                           ("P_COMMENT",TString())),          5.000),
                                                             
    ("partsupp",                 """CREATE TABLE PARTSUPP ( PS_PARTKEY     INTEGER NOT NULL,
                                                             PS_SUPPKEY     INTEGER NOT NULL,
                                                             PS_AVAILQTY    INTEGER NOT NULL,
                                                             PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
                                                             PS_COMMENT     VARCHAR(199) NOT NULL );""",Seq(
                                                                                                           ("PS_PARTKEY", TInt()),
                                                                                                           ("PS_SUPPKEY", TInt()),
                                                                                                           ("PS_AVAILQTY", TInt()),
                                                                                                           ("PS_SUPPLYCOST",TFloat()),
                                                                                                           ("PS_COMMENT",TString())),          5.000),
                                                             
    ("region",                   """CREATE TABLE REGION  ( R_REGIONKEY  INTEGER NOT NULL,
                                                             R_NAME       CHAR(25) NOT NULL,
                                                             R_COMMENT    VARCHAR(152));""",            Seq(
                                                                                                           ("R_REGIONKEY", TInt()),
                                                                                                           ("R_NAME", TString()),
                                                                                                           ("R_COMMENT",TString())),          5.000)
  )
}

