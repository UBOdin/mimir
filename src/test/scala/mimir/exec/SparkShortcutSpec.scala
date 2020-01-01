package mimir.exec

import org.specs2.mutable._
import org.specs2.specification._
import org.specs2.concurrent.ExecutionEnv

import scala.concurrent.duration._

import mimir.algebra._
import mimir.test.{ RAParsers, DBTestInstances, SQLTestSpecification }
import mimir.exec.spark.{ RAToSpark, SparkEval, MimirSpark }
import mimir.exec.result._
import mimir.exec.shortcut._
import mimir.util.ExperimentalOptions

import org.apache.spark.sql.catalyst.expressions.{ GenericInternalRow, Attribute }
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReference
import org.apache.spark.sql.catalyst.expressions.aggregate.{
  Count,
  Sum,
  DeclarativeAggregate,
  ImperativeAggregate,
  AggregateFunction
}

class SparkShortcutSpec(implicit ee: ExecutionEnv)
  extends SQLTestSpecification("SparkShortcuts")
  with AroundTimeout
  with BeforeAll
{
  def beforeAll
  {
    loadCSV(
      targetTable = "R", 
      sourceFile = "test/r_test/r.csv", 
      detectHeaders = false, 
      inferTypes = false,
      targetSchema = Seq("A", "B", "C")
    )
    // warm up the cache
    query("SELECT * FROM R") { ret => ret.toIndexedSeq }
  }

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

  def aggregateOne(
    agg: AggFunction, 
    source:ResultIterator = makeSource()
  ): PrimitiveValue =
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

  def aggregateAll(
    group: Seq[ID], 
    agg:Seq[AggFunction], 
    source:ResultIterator = makeSource()
  ): Seq[Seq[PrimitiveValue]] =
  {
    ComputeAggregate(group, agg, source, db)
  }

  sequential

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

    "compute aggregates correctly" >> {
      aggregateAll(
        Seq(),
        Seq(AggFunction(
          ID("count"),
          false,
          Seq(),
          ID("TOT_COUNT")
        ),AggFunction(
          ID("sum"),
          false,
          Seq(Var(ID("a"))),
          ID("TOT_SUM")
        ))
      ) must contain(eachOf( Seq[PrimitiveValue](IntPrimitive(3), IntPrimitive(4)) ))
      aggregateAll(
        Seq(ID("a")),
        Seq(AggFunction(
          ID("count"),
          false,
          Seq(),
          ID("TOT_COUNT")
        ),AggFunction(
          ID("sum"),
          false,
          Seq(Var(ID("b"))),
          ID("TOT_SUM")
        ))
      ) must contain(eachOf( 
        Seq[PrimitiveValue](IntPrimitive(1), IntPrimitive(2), IntPrimitive(7)),
        Seq[PrimitiveValue](IntPrimitive(2), IntPrimitive(1), IntPrimitive(1)) 
      ))
    }

    "compute queries correctly" >> {
      upTo(1.second) {
        ExperimentalOptions.withEnabled(Seq("ALLOW-SPARK-SHORTCUT")) { 
          query("SELECT * FROM R WHERE CAST(A AS int) > 1") { ret => ret.toIndexedSeq }
        }
        ok
      }
    }

  }

}