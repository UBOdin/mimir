package mimir.test

import java.io._
import mimir.algebra._

object PDBench
{
  def isDownloaded = new File("test/pdbench").exists()

  val attributes = Seq(
    //  Attribute Table Name     Attribute        Type       Load Timeout
    ("cust_c_acctbal",           "acctbal",       TFloat(),  5000000),
    ("cust_c_address",           "address",       TString(), 5000000),
    ("cust_c_comment",           "comment",       TString(), 5000000),
    ("cust_c_custkey",           "custkey",       TInt(),    5000000),
    ("cust_c_mktsegment",        "mktsegment",    TString(), 5000000),
    ("cust_c_name",              "name",          TString(), 5000000),
    ("cust_c_nationkey",         "nationkey",     TInt(),    5000000),
    ("cust_c_phone",             "phone",         TString(), 5000000),

    ("lineitem_l_comment",       "comment",       TString(), 5000000),
    ("lineitem_l_commitdate",    "commitdate",    TDate(),   5000000),
    ("lineitem_l_discount",      "discount",      TFloat(),  5000000),
    ("lineitem_l_extendedprice", "extendedprice", TFloat(),  5000000),
    ("lineitem_l_linenumber",    "linenumber",    TInt(),    5000000),
    ("lineitem_l_linestatus",    "linestatus",    TString(), 5000000),
    ("lineitem_l_orderkey",      "orderkey",      TInt(),    5000000),
    ("lineitem_l_partkey",       "partkey",       TInt(),    5000000),
    ("lineitem_l_quantity",      "quantity",      TInt(),    5000000),
    ("lineitem_l_receiptdate",   "receiptdate",   TDate(),   5000000),
    ("lineitem_l_returnflag",    "returnflag",    TString(), 5000000),
    ("lineitem_l_shipdate",      "shipdate",      TDate(),   5000000),
    ("lineitem_l_shipinstruct",  "shipinstruct",  TString(), 5000000),
    ("lineitem_l_shipmode",      "shipmode",      TString(), 5000000),
    ("lineitem_l_suppkey",       "suppkey",       TInt(),    5000000),
    ("lineitem_l_tax",           "tax",           TFloat(),  5000000),

    ("nation_n_comment",         "comment",       TString(), 5000000),
    ("nation_n_name",            "name",          TString(), 5000000),
    ("nation_n_nationkey",       "nationkey",     TInt(),    5000000),
    ("nation_n_regionkey",       "regionkey",     TInt(),    5000000),

    ("orders_o_clerk",           "clerk",         TString(), 5000000),
    ("orders_o_comment",         "comment",       TString(), 5000000),
    ("orders_o_custkey",         "custkey",       TInt(),    5000000),
    ("orders_o_orderdate",       "orderdate",     TDate(),   5000000),
    ("orders_o_orderkey",        "orderkey",      TInt(),    5000000),
    ("orders_o_orderpriority",   "orderpriority", TString(), 5000000),
    ("orders_o_orderstatus",     "orderstatus",   TString(), 5000000),
    ("orders_o_shippriority",    "shippriority",  TString(), 5000000),
    ("orders_o_totalprice",      "totalprice",    TFloat(), 5000000),

    ("part_p_brand",             "brand",         TString(), 5000000),
    ("part_p_comment",           "comment",       TString(), 5000000),
    ("part_p_container",         "container",     TString(), 5000000),
    ("part_p_mfgr",              "mfgr",          TString(), 5000000),
    ("part_p_name",              "name",          TString(), 5000000),
    ("part_p_partkey",           "partkey",       TInt(),    5000000),
    ("part_p_retailprice",       "retailprice",   TFloat(),  5000000),
    ("part_p_size",              "size",          TString(), 5000000),
    ("part_p_type",              "type",          TString(), 5000000),

    ("psupp_ps_availqty",        "availqty",      TInt(),    5000000),
    ("psupp_ps_comment",         "comment",       TString(), 5000000),
    ("psupp_ps_partkey",         "partkey",       TInt(),    5000000),
    ("psupp_ps_suppkey",         "suppkey",       TInt(),    5000000),
    ("psupp_ps_supplycost",      "supplycost",    TFloat(),  5000000),

    ("region_r_comment",         "comment",       TString(), 5000000),
    ("region_r_name",            "name",          TString(), 5000000),
    ("region_r_regionkey",       "regionkey",     TInt(),    5000000),

    ("supp_s_acctbal",           "acctbal",       TFloat(),  5000000),
    ("supp_s_address",           "address",       TString(), 5000000),
    ("supp_s_comment",           "comment",       TString(), 5000000),
    ("supp_s_name",              "name",          TString(), 5000000),
    ("supp_s_nationkey",         "nationkey",     TInt(),    5000000),
    ("supp_s_phone",             "phone",         TString(), 5000000),
    ("supp_s_suppkey",           "suppkey",       TInt(),    5000000)
  )
}

