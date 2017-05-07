package mimir.optimizer

import org.specs2.mutable._
import mimir.algebra._
import mimir.test._

object OperatorOptimizerRegressions
  extends Specification
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

}