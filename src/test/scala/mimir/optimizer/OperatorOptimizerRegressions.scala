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
              Table("CUSTOMER_RUN_1","CUSTOMER_RUN_1",
                Seq(
                  ("CUSTKEY", TInt()),
                  ("MKTSEGMENT", TString())
                ), Seq()
              )
            ),
            Select(expr("ORDERDATE<DATE('1995-03-15')"),
              Table("ORDERS_RUN_1","ORDERS_RUN_1",
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
      val problemExpr = 
        ExpressionUtils.makeAnd(
          Seq(
            Comparison(Cmp.Eq, expr("MIMIR_DET_BIT_VECTOR & 2"), IntPrimitive(2)),
            Comparison(Cmp.Eq, expr("MIMIR_DET_BIT_VECTOR_0 & 4"), IntPrimitive(4)),
            expr("CUSTKEY !=  CUSTKEY_0")
          )
        )
      Eval.simplify(
        PropagateConditions(problemExpr, Seq(expr("CUSTKEY_0=CUSTKEY")))
      ) must be equalTo(BoolPrimitive(false))
    }

    "Propagate IsNull" >> {
      val problemExpr = 
        Arithmetic(Arith.Or,
          Var("C"),
          Arithmetic(Arith.And,
            Not(IsNullExpression(Var("REGIONKEY_0"))),
            Conditional(
              IsNullExpression(Var("REGIONKEY_0")),
              Var("A"),
              Var("B")
            )
          )
        )
      PropagateConditions(problemExpr) must be equalTo(
        Arithmetic(Arith.Or,
          Var("C"),
          Arithmetic(Arith.And,
            Not(IsNullExpression(Var("REGIONKEY_0"))),
            Var("B")
          )
        )
      )
    }

    "Propagate IsNull deep into expressions" >> {
      val r = Table("R", "R", Seq("A" -> TString(), "B" -> TInt(), "C" -> TInt()), Seq( ("MIMIR_ROWID", Var("ROWID"), TRowId())))

      val problemExpr =
        r .filter(  ( Var("MIMIR_ROWID").eq(StringPrimitive("3")) ) and ( Var("C").isNull ) )
          .project( "A", "B", "C", "MIMIR_ROWID" )
          .addColumn( "MIMIR_ROW_DET" -> Var("MIMIR_ROWID").neq(StringPrimitive("3")).or ( Not(Var("C").isNull) ) )
          .limit(4)
          .project( "A", "MIMIR_ROW_DET", "MIMIR_ROWID", "B", "C" )

      val propagated = PropagateConditions(problemExpr)
      propagated.toString must not contain("NOT(C IS NULL)")

    }

  }

}