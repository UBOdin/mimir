package mimir.optimizer

import org.specs2.mutable._
import mimir.algebra._
import mimir.test._

object OperatorOptimizerRegressions
  extends Specification
  with DumbRAParsers
  // extends SQLTestSpecification("OperOptRegressions")
{

  "MakeSafeJoin" should {

    "Correctly rename aggregates" >> {
      val tree =
        Aggregate(
          Seq(Var("TID_4")),
          Seq(
            AggFunction("FIRST", false, Seq(Var("CUSTKEY")), "CUSTKEY"),
            AggFunction("COUNT", true, Seq(Var("CUSTKEY")), "MIMIR_KR_COUNT_CUSTKEY"),
            AggFunction("JSON_GROUP_ARRAY", false, Seq(Var("CUSTKEY")), "MIMIR_KR_HINT_COL_CUSTKEY")
          ),
          Table(
            "ORDERS_O_CUSTKEY",
            Seq(
              ("VAR_ID", TInt()), 
              ("WORLD_ID", TInt()), 
              ("TID_4", TInt()), 
              ("CUSTKEY", TInt()) 
            ),
            Seq(
              ("MIMIR_ROWID", Var("ROWID"), TRowId())
            )
          )
        )

      val (rename, replaced) = OperatorUtils.makeColumnNameUnique("CUSTKEY", tree.columnNames.toSet, tree)
      replaced must beAnInstanceOf[Aggregate]
      val child = replaced.asInstanceOf[Aggregate].source
      child.columnNames must contain("CUSTKEY")
      child.columnNames must not contain(rename)
      replaced.expressions.flatMap { ExpressionUtils.getColumns(_) }.toSet must not contain(rename)
    }

  }

  "PropagateConditions" should {

    "Not clobber expressions" >> {
      PropagateConditions(
        expr("CUSTKEY_0=CUSTKEY"), 
        Seq(expr("MKTSEGMENT = 'BUILDING'"))
      ) must be equalTo(expr("CUSTKEY_0=CUSTKEY"))

    }

    "Not clobber predicates" >> {
      val problemQuery = 
        Select(expr("CUSTKEY_0=CUSTKEY"),
          Join(
            Select(expr("MKTSEGMENT = 'BUILDING'"),
              Table("CUSTOMER_RUN_1",
                Seq(
                  ("CUSTKEY", TInt()),
                  ("MKTSEGMENT", TString())
                ), Seq()
              )
            ),
            Select(expr("ORDERDATE<DATE('1995-03-15')"),
              Table("ORDERS_RUN_1",
                Seq(
                  ("ORDERKEY", TInt()),
                  ("CUSTKEY_0", TInt()),
                  ("ORDERDATE", TDate())
                ), Seq()
              )
            )
          )
        )
      val ret = PropagateConditions(problemQuery)
      ret must beAnInstanceOf[Select]
      ret.asInstanceOf[Select].condition must be equalTo(expr("CUSTKEY_0=CUSTKEY"))
    }

    "Push deep into expressions" >> {
      val testBit = (i:Int) => { Comparison(Cmp.Eq, Var("MIMIR_DET_BIT_VECTOR"), IntPrimitive(i)) }
      val and = ExpressionUtils.makeAnd(_:Expression,_:Expression)
      val or = ExpressionUtils.makeOr(_:Expression,_:Expression)

      val problemExpr = expr("((((MIMIR_DET_BIT_VECTOR & 2) =2)  AND  ( (MIMIR_DET_BIT_VECTOR_0 & 4) =4) )  AND  (CUSTKEY !=  CUSTKEY_0) )")
      // and(
      //   testBit(64), 
      //   testBit(128)
      // )
//       val problemExpr = expr("""
// ( 
//   ( 
//     ( 
//       ( 
//         ( 
//           ( 
//             ( 
//               ( 
//                 ( 
//                   ( (MIMIR_DET_BIT_VECTOR_1 & 64) =64)  AND  ( (MIMIR_DET_BIT_VECTOR_1 & 128) =128) 
//                 )  AND  ( 
//                   ( 
//                     ( 
//                       ( 
//                         ( 
//                           ( 
//                             ( 
//                               ( 
//                                 ( 
//                                   ( 
//                                     ( 
//                                       ( 
//                                         ( 
//                                           ( 
//                                             ( 
//                                               ( 
//                                                 ( 
//                                                   (MIMIR_DET_BIT_VECTOR & 2) =2)  AND  ( (MIMIR_DET_BIT_VECTOR_0 & 4) =4) )  
//                                                   AND  (CUSTKEY<>CUSTKEY_0) )  OR  ( ( ( (MIMIR_DET_BIT_VECTOR_1 & 2) =2)  AND  ( (MIMIR_DET_BIT_VECTOR_0 & 2) =2) )  AND  (ORDERKEY_0<>ORDERKEY) ) )  AND  ( (CUSTKEY<>CUSTKEY_0)  OR  (ORDERKEY_0<>ORDERKEY) ) )  OR  ( ( ( (MIMIR_DET_BIT_VECTOR_1 & 8) =8)  AND  ( (MIMIR_DET_BIT_VECTOR_2 & 2) =2) )  AND  (SUPPKEY<>SUPPKEY_0) ) )  AND  ( ( (CUSTKEY<>CUSTKEY_0)  OR  (ORDERKEY_0<>ORDERKEY) )  OR  (SUPPKEY<>SUPPKEY_0) ) )  OR  ( ( ( (MIMIR_DET_BIT_VECTOR & 16) =16)  AND  ( (MIMIR_DET_BIT_VECTOR_2 & 16) =16) )  AND  (NATIONKEY<>NATIONKEY_0) ) )  AND  ( ( ( (CUSTKEY<>CUSTKEY_0)  OR  (ORDERKEY_0<>ORDERKEY) )  OR  (SUPPKEY<>SUPPKEY_0) )  OR  (NATIONKEY<>NATIONKEY_0) ) )  OR  ( ( ( (MIMIR_DET_BIT_VECTOR_2 & 16) =16)  AND  ( (MIMIR_DET_BIT_VECTOR_3 & 2) =2) )  AND  (NATIONKEY_0<>NATIONKEY_1) ) )  AND  ( ( ( ( (CUSTKEY<>CUSTKEY_0)  OR  (ORDERKEY_0<>ORDERKEY) )  OR  (SUPPKEY<>SUPPKEY_0) )  OR  (NATIONKEY<>NATIONKEY_0) )  OR  (NATIONKEY_0<>NATIONKEY_1) ) )  OR  ( ( (MIMIR_DET_BIT_VECTOR_3 & 8) =8)  AND  (REGIONKEY<>REGIONKEY_0) ) )  AND  ( ( ( ( ( (CUSTKEY<>CUSTKEY_0)  OR  (ORDERKEY_0<>ORDERKEY) )  OR  (SUPPKEY<>SUPPKEY_0) )  OR  (NATIONKEY<>NATIONKEY_0) )  OR  (NATIONKEY_0<>NATIONKEY_1) )  OR  (REGIONKEY<>REGIONKEY_0) ) )  AND  ( ( ( ( ( (CUSTKEY<>CUSTKEY_0)  OR  (ORDERKEY_0<>ORDERKEY) )  OR  (SUPPKEY<>SUPPKEY_0) )  OR  (NATIONKEY<>NATIONKEY_0) )  OR  (NATIONKEY_0<>NATIONKEY_1) )  OR  (REGIONKEY<>REGIONKEY_0) ) )  OR  ( ( (MIMIR_DET_BIT_VECTOR_0 & 32) =32)  AND  (ORDERDATE<DATE '1994-01-01') ) )  AND  ( ( ( ( ( ( (CUSTKEY<>CUSTKEY_0)  OR  (ORDERKEY_0<>ORDERKEY) )  OR  (SUPPKEY<>SUPPKEY_0) )  OR  (NATIONKEY<>NATIONKEY_0) )  OR  (NATIONKEY_0<>NATIONKEY_1) )  OR  (REGIONKEY<>REGIONKEY_0) )  OR  (ORDERDATE<DATE '1994-01-01') ) )  OR  ( ( (MIMIR_DET_BIT_VECTOR_0 & 32) =32)  AND  (ORDERDATE>=DATE '1995-01-01') ) ) )  AND  ( (MIMIR_DET_BIT_VECTOR & 1) =1) )  AND  ( (MIMIR_DET_BIT_VECTOR_0 & 1) =1) )  AND  ( (MIMIR_DET_BIT_VECTOR_1 & 1) =1) )  AND  ( (MIMIR_DET_BIT_VECTOR_2 & 1) =1) )  AND  ( (MIMIR_DET_BIT_VECTOR_3 & 1) =1) )  AND  ( (MIMIR_DET_BIT_VECTOR_3 & 4) =4) ) )
//       """)
      PropagateConditions(problemExpr, Seq(expr("CUSTKEY_0=CUSTKEY"))) must be equalTo(BoolPrimitive(false))
    }

  }

}