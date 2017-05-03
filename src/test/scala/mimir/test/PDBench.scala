package mimir.test

import java.io._
import mimir.algebra._

object PDBench
{
  def isDownloaded = new File("test/pdbench").exists()

  val tables = Map[String, (Seq[String], Seq[(String,String,Type,Double)])](
    "customer" -> (
      Seq("custkey"),
      Seq(
    //    Attribute Table Name     Attribute        Type       Load Timeout (s)
        ("cust_c_acctbal",           "acctbal",       TFloat(),  5.000),
        ("cust_c_address",           "address",       TString(), 5.000),
        ("cust_c_comment",           "comment",       TString(), 5.000),
        ("cust_c_custkey",           "custkey",       TInt(),    5.000),
        ("cust_c_mktsegment",        "mktsegment",    TString(), 5.000),
        ("cust_c_name",              "name",          TString(), 5.000),
        ("cust_c_nationkey",         "nationkey",     TInt(),    5.000),
        ("cust_c_phone",             "phone",         TString(), 5.000)
      )
    ),
    "lineitem" -> (
      Seq("orderkey", "linenumber"),
      Seq(
        ("lineitem_l_comment",       "comment",       TString(), 5.000),
        ("lineitem_l_commitdate",    "commitdate",    TDate(),   5.000),
        ("lineitem_l_discount",      "discount",      TFloat(),  5.000),
        ("lineitem_l_extendedprice", "extendedprice", TFloat(),  5.000),
        ("lineitem_l_linenumber",    "linenumber",    TInt(),    5.000),
        ("lineitem_l_linestatus",    "linestatus",    TString(), 5.000),
        ("lineitem_l_orderkey",      "orderkey",      TInt(),    5.000),
        ("lineitem_l_partkey",       "partkey",       TInt(),    5.000),
        ("lineitem_l_quantity",      "quantity",      TInt(),    5.000),
        ("lineitem_l_receiptdate",   "receiptdate",   TDate(),   5.000),
        ("lineitem_l_returnflag",    "returnflag",    TString(), 5.000),
        ("lineitem_l_shipdate",      "shipdate",      TDate(),   5.000),
        ("lineitem_l_shipinstruct",  "shipinstruct",  TString(), 5.000),
        ("lineitem_l_shipmode",      "shipmode",      TString(), 5.000),
        ("lineitem_l_suppkey",       "suppkey",       TInt(),    5.000),
        ("lineitem_l_tax",           "tax",           TFloat(),  5.000)
      )
    ),
    "nation" -> (
      Seq("nationkey"),
      Seq(
        ("nation_n_comment",         "comment",       TString(), 5.000),
        ("nation_n_name",            "name",          TString(), 5.000),
        ("nation_n_nationkey",       "nationkey",     TInt(),    5.000),
        ("nation_n_regionkey",       "regionkey",     TInt(),    5.000)
      )
    ),
    "orders" -> (
      Seq("orderkey"),
      Seq(
        ("orders_o_clerk",           "clerk",         TString(), 5.000),
        ("orders_o_comment",         "comment",       TString(), 5.000),
        ("orders_o_custkey",         "custkey",       TInt(),    5.000),
        ("orders_o_orderdate",       "orderdate",     TDate(),   5.000),
        ("orders_o_orderkey",        "orderkey",      TInt(),    5.000),
        ("orders_o_orderpriority",   "orderpriority", TString(), 5.000),
        ("orders_o_orderstatus",     "orderstatus",   TString(), 5.000),
        ("orders_o_shippriority",    "shippriority",  TString(), 5.000),
        ("orders_o_totalprice",      "totalprice",    TFloat(),  5.000)
      )
    ),
    "part" -> (
      Seq("partkey"),
      Seq(
        ("part_p_brand",             "brand",         TString(), 5.000),
        ("part_p_comment",           "comment",       TString(), 5.000),
        ("part_p_container",         "container",     TString(), 5.000),
        ("part_p_mfgr",              "mfgr",          TString(), 5.000),
        ("part_p_name",              "name",          TString(), 5.000),
        ("part_p_partkey",           "partkey",       TInt(),    5.000),
        ("part_p_retailprice",       "retailprice",   TFloat(),  5.000),
        ("part_p_size",              "size",          TString(), 5.000),
        ("part_p_type",              "type",          TString(), 5.000)
      )
    ),
    "partsupp" -> (
      Seq("partkey", "suppkey"),
      Seq(
        ("psupp_ps_availqty",        "availqty",      TInt(),    5.000),
        ("psupp_ps_comment",         "comment",       TString(), 5.000),
        ("psupp_ps_partkey",         "partkey",       TInt(),    5.000),
        ("psupp_ps_suppkey",         "suppkey",       TInt(),    5.000),
        ("psupp_ps_supplycost",      "supplycost",    TFloat(),  5.000)
      )
    ),
    "region" -> (
      Seq("regionkey"),
      Seq(
        ("region_r_comment",         "comment",       TString(), 5.000),
        ("region_r_name",            "name",          TString(), 5.000),
        ("region_r_regionkey",       "regionkey",     TInt(),    5.000)
      )
    ),
    "supplier" -> (
      Seq("suppkey"),
      Seq(
        ("supp_s_acctbal",           "acctbal",       TFloat(),  5.000),
        ("supp_s_address",           "address",       TString(), 5.000),
        ("supp_s_comment",           "comment",       TString(), 5.000),
        ("supp_s_name",              "name",          TString(), 5.000),
        ("supp_s_nationkey",         "nationkey",     TInt(),    5.000),
        ("supp_s_phone",             "phone",         TString(), 5.000),
        ("supp_s_suppkey",           "suppkey",       TInt(),    5.000)
      )
    )
  )

  val attributes = tables.values.flatMap { _._2 }.toSeq

}

