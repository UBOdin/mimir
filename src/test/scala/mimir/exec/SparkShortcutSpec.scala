package mimir.exec

import org.specs2.mutable._
import org.specs2.specification._

import mimir.algebra._
import mimir.test.{ RAParsers, DBTestInstances, SQLTestSpecification }
import mimir.exec.spark.{ RAToSpark, SparkEval, MimirSpark }
import mimir.exec.result._
import mimir.exec.shortcut._

import org.apache.spark.sql.catalyst.expressions.{ GenericInternalRow, Attribute }
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReference
import org.apache.spark.sql.catalyst.expressions.aggregate.{
  Count,
  Sum,
  DeclarativeAggregate,
  ImperativeAggregate,
  AggregateFunction
}

class SparkShortcutSpec
  extends SQLTestSpecification("SparkShortcuts")
{
  def makeSource(): ResultIterator = 
  {
    new HardTableIterator(
      Seq(
        ID("a") -> TInt(),
        ID("b") -> TInt()
      ),
      Seq(
        Seq(IntPrimitive(1), IntPrimitive(3)),
        Seq(IntPrimitive(1), IntPrimitive(4)),
        Seq(IntPrimitive(2), IntPrimitive(1))
      )
    )
  }

  def aggregateOne(agg: AggFunction, source:ResultIterator = makeSource()): PrimitiveValue =
  {
    val mkEvalAgg = ComputeAggregate.compile(agg, source, db)
    val evalAgg = mkEvalAgg()

    for(row <- source){
      evalAgg.update(new GenericInternalRow(row.tuple.map { 
        RAToSpark.mimirPrimitiveToSparkInternalRowValue(_)
      }.toArray))
    }
    evalAgg.finish()
  }

  "Aggregate shortcuts" should {

    "compile correctly" >> {

      aggregateOne(
        AggFunction(
          ID("count"),
          false,
          Seq(),
          ID("TOT")
        )
      ) must beEqualTo(IntPrimitive(3))

      aggregateOne(
        AggFunction(
          ID("sum"),
          false,
          Seq(Var(ID("a"))),
          ID("TOT")
        )
      ) must beEqualTo(IntPrimitive(4))

      aggregateOne(
        AggFunction(
          ID("sum"),
          false,
          Seq(Var(ID("a")).add(IntPrimitive(1))),
          ID("TOT")
        )
      ) must beEqualTo(IntPrimitive(7))

      aggregateOne(
        AggFunction(
          ID("count"),
          true,
          Seq(Var(ID("a"))),
          ID("TOT")
        )
      ) must beEqualTo(IntPrimitive(2))

      aggregateOne(
        AggFunction(
          ID("approx_percentile"),
          true,
          Seq(
            CastExpression(Var(ID("a")), TFloat()),
            FloatPrimitive(0.2),
            IntPrimitive(100)
          ),
          ID("TOT")
        )
      ).asDouble must beCloseTo(2.0, 2.0)

    }

  }

}