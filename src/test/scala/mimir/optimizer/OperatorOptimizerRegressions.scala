package mimir.optimizer

import org.specs2.mutable._
import mimir.algebra._
import mimir.test._
import mimir.optimizer.expression._
import mimir.optimizer.operator._

object OperatorOptimizerRegressions
  extends Specification
  with RAParsers
  with RASimplify
  // extends SQLTestSpecification("OperOptRegressions")
{

  "MakeSafeJoin" should {

    "Correctly rename aggregates" >> {
      val tree =
        Aggregate(
          Seq(Var(ID("TID_4"))),
          Seq(
            AggFunction(ID("first"), false, Seq(Var(ID("CUSTKEY"))), ID("CUSTKEY")),
            AggFunction(ID("count"), true, Seq(Var(ID("CUSTKEY"))), ID("MIMIR_KR_COUNT_CUSTKEY")),
            AggFunction(ID("json_group_array"), false, Seq(Var(ID("CUSTKEY"))), ID("MIMIR_KR_HINT_COL_CUSTKEY"))
          ),
          Table(
            ID("ORDERS_O_CUSTKEY"),
            ID("ORDERS_O_CUSTKEY"),
            Seq(
              ID("VAR_ID") -> TInt(), 
              ID("WORLD_ID") -> TInt(), 
              ID("TID_4") -> TInt(), 
              ID("CUSTKEY") -> TInt() 
            ),
            Seq(
              (ID("MIMIR_ROWID"), Var(ID("ROWID")), TRowId())
            )
          )
        )

      val (rename, replaced) = OperatorUtils.makeColumnNameUnique(ID("CUSTKEY"), tree.columnNames.toSet, tree)
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
              Table(ID("CUSTOMER_RUN_1"),ID("CUSTOMER_RUN_1"),
                Seq(
                  ID("CUSTKEY") -> TInt(),
                  ID("MKTSEGMENT") -> TString()
                ), Seq()
              )
            ),
            Select(expr("ORDERDATE<DATE('1995-03-15')"),
              Table(ID("ORDERS_RUN_1"),ID("ORDERS_RUN_1"),
                Seq(
                  ID("ORDERKEY") -> TInt(),
                  ID("CUSTKEY_0") -> TInt(),
                  ID("ORDERDATE") -> TDate()
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
      simplify(
        PropagateConditions(problemExpr, Seq(expr("CUSTKEY_0=CUSTKEY")))
      ) must be equalTo(BoolPrimitive(false))
    }

    "Propagate IsNull" >> {
      val problemExpr = 
        Arithmetic(Arith.Or,
          Var(ID("C")),
          Arithmetic(Arith.And,
            Not(IsNullExpression(Var(ID("REGIONKEY_0")))),
            Conditional(
              IsNullExpression(Var(ID("REGIONKEY_0"))),
              Var(ID("A")),
              Var(ID("B"))
            )
          )
        )
      simplify(PropagateConditions(problemExpr)) must be equalTo(
        Arithmetic(Arith.Or,
          Var(ID("C")),
          Arithmetic(Arith.And,
            Not(IsNullExpression(Var(ID("REGIONKEY_0")))),
            Var(ID("B"))
          )
        )
      )
    }

    "Propagate IsNull deep into expressions" >> {
      val r = Table(
        ID("R"), 
        ID("R"), 
        Seq(
          ID("A") -> TString(), 
          ID("B") -> TInt(), 
          ID("C") -> TInt()
        ), 
        Seq( 
          (ID("MIMIR_ROWID"), Var(ID("ROWID")), TRowId())
        )
      )

      val problemExpr =
        r .filter(  ( Var(ID("MIMIR_ROWID")).eq(RowIdPrimitive("3")) ) and ( Var(ID("C")).isNull ) )
          .project( "A", "B", "C", "MIMIR_ROWID" )
          .addColumns( "MIMIR_ROW_DET" -> Var(ID("MIMIR_ROWID")).neq(RowIdPrimitive("3")).or ( Not(Var(ID("C")).isNull) ) )
          .limit(4)
          .project( "A", "MIMIR_ROW_DET", "MIMIR_ROWID", "B", "C" )

      val propagated = PropagateConditions(problemExpr)
      propagated.toString must not contain("NOT(C IS NULL)")

    }

  }

}