package mimir.exec

import org.specs2.mutable._
import org.specs2.specification._

import mimir.algebra._
import mimir.exec.spark.{ RAToSpark, SparkEval, MimirSpark }
import mimir.test.{ RAParsers, DBTestInstances }

import org.apache.spark.sql.catalyst.expressions.{ GenericInternalRow, Attribute }
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReference
import org.apache.spark.sql.catalyst.expressions.aggregate.{
  Count,
  Sum,
  DeclarativeAggregate,
  ImperativeAggregate,
  AggregateFunction
}

class SparkEvalSpec
  extends Specification
  with RAParsers
  with BeforeAll
{

  def beforeAll = {
    DBTestInstances.initSpark
  }

  def aggregate(agg: DeclarativeAggregate, schema:Seq[Attribute])
               (data: Seq[PrimitiveValue]*): Seq[PrimitiveValue] =
  {
    val attrs = schema ++ agg.aggBufferAttributes
    val update = agg.updateExpressions.map { bindReference(_, attrs) }
    val emptyRow = new GenericInternalRow(Array[Any]())

    val init = agg.initialValues.map { _.eval(emptyRow) }

    val ret: Seq[Any] = data.foldLeft(init) { (accum, curr) =>
      val input = new GenericInternalRow(
        ((curr.map { 
          RAToSpark.mimirPrimitiveToSparkInternalRowValue(_)
        })++accum).toArray
      )

      update.map { _.eval(input) }
    }

    ret.map { RAToSpark.sparkInternalRowValueToMimirPrimitive(_) }
  }

  lazy val sch = Seq(
    ID("test1") -> TInt(),
    ID("test2") -> TInt()
  ).map { RAToSpark.mimirColumnToAttribute(_) }

  lazy val testData = Seq(
      Seq(IntPrimitive(1), IntPrimitive(4)),
      Seq(NullPrimitive(), IntPrimitive(5)),
      Seq(IntPrimitive(3), NullPrimitive())
    )

  "SparkEval" should {

    "compute counts " >> {
      aggregate(Count(Seq()), Seq())(
        Seq(),
        Seq(),
        Seq()
      ) must beEqualTo(Seq(IntPrimitive(3)))


      aggregate(Count(Seq(sch(0))), sch)(
        testData:_*
      ) must beEqualTo(Seq(IntPrimitive(2)))
    }

    "compute sums" >> {
      aggregate(Sum(sch(0)), sch)(
        testData:_*
      ) must beEqualTo(Seq(IntPrimitive(4)))
    }

  }

  "Spark Aggregate Functions" should {

    "Expose Types" >> {

      val countfn = MimirSpark.getFunction(ID("count"))
      countfn.toString must startWith("Function[name='count'")

      countfn.className must endWith("Count")
      val clazz = Class.forName(countfn.className)
      classOf[AggregateFunction].isAssignableFrom(clazz) must beTrue
      classOf[DeclarativeAggregate].isAssignableFrom(clazz) must beTrue
      classOf[ImperativeAggregate].isAssignableFrom(clazz) must beFalse

    }

    "Allow Instantiation" >> {
      val countagg = MimirSpark.makeAggregate(ID("count"), Seq())
      countagg must beLike {
        case agg: DeclarativeAggregate => 
          aggregate(agg, Seq())(
            Seq(),
            Seq(),
            Seq()
          ) must beEqualTo(Seq(IntPrimitive(3)))
      } 

      val sumagg = MimirSpark.makeAggregate(ID("sum"), Seq(sch(1)))
      sumagg must beLike {
        case agg: DeclarativeAggregate => 
          aggregate(agg, sch)(
            testData:_*
          ) must beEqualTo(Seq(IntPrimitive(9)))
      } 

    }

  }

}