package mimir.exec.result

import mimir.exec._
import mimir.algebra._
import com.typesafe.scalalogging.slf4j.LazyLogging
import math.{abs,sqrt}

abstract class AggregateValue
{
  def fold(input: Row): Unit
  def result: PrimitiveValue
}

class LongSumAggregate(arg: Row => Long) extends AggregateValue
{
  var t: Long = 0
  def fold(input: Row) { t = t + arg(input) }
  def result = IntPrimitive(t)
}

class DoubleSumAggregate(arg: Row => Double) extends AggregateValue
{
  var t: Double = 0.0
  def fold(input: Row) { t = t + arg(input) }
  def result = FloatPrimitive(t)
}

class LongAvgAggregate(arg: Row => Long) extends AggregateValue
{
  var t: Long = 0
  var count = 0
  def fold(input: Row) { t = t + arg(input); count += 1 }
  def result = FloatPrimitive(t.toDouble / count)
}

class DoubleAvgAggregate(arg: Row => Double) extends AggregateValue
{
  var t: Double = 0.0
  var count = 0
  def fold(input: Row) { t = t + arg(input); count += 1 }
  def result = FloatPrimitive(t / count)
}

class CountAggregate() extends AggregateValue
{
  var count = 0;
  def fold(input: Row) { count += 1 }
  def result = IntPrimitive(count)
}

class LongStdDevAggregate(arg: Row => Long) extends AggregateValue
{
  var tSq: Long = 0
  var t: Long = 0
  var count = 0
  def fold(input: Row) { 
    val curr = arg(input); 
    t = t + curr
    tSq = tSq + curr * curr
    count += 1 
  }
  def result = FloatPrimitive(sqrt(abs(t.toDouble*t.toDouble - tSq) / (count*count)))
}

class DoubleStdDevAggregate(arg: Row => Double) extends AggregateValue
{
  var tSq: Double = 0.0
  var t: Double = 0.0
  var count = 0
  def fold(input: Row) { 
    val curr = arg(input); 
    t = t + curr
    tSq = tSq + curr * curr
    count += 1 
  }
  def result = FloatPrimitive(sqrt(abs(t*t - tSq) / (count*count)))
}

class GroupAndAggregate(arg: Row => Boolean) extends AggregateValue
{
  var t = true
  def fold(input: Row) { t = t && arg(input) }
  def result = BoolPrimitive(t)
}

class GroupOrAggregate(arg: Row => Boolean) extends AggregateValue
{
  var t = false
  def fold(input: Row) { t = t || arg(input) }
  def result = BoolPrimitive(t)
}

class FirstAggregate(arg: Row => PrimitiveValue) extends AggregateValue
{
  var t:PrimitiveValue = NullPrimitive();
  def fold(input: Row) { if(t.isInstanceOf[NullPrimitive]){ t = arg(input) } }
  def result = t
}

class AggregateResultIterator(
  groupByColumns: Seq[Var],
  aggregates: Seq[AggFunction],
  inputSchema: Seq[(String,Type)],
  src: ResultIterator
) 
  extends ResultIterator
  with LazyLogging
{
  private val typeOfInputColumn: Map[String, Type] = inputSchema.toMap
  private val typechecker = new ExpressionChecker(typeOfInputColumn)
  val tupleSchema: Seq[(String,Type)] = 
    groupByColumns.map { col => (col.name, typeOfInputColumn(col.name)) } ++ 
    aggregates.map { fn => 
      (
        fn.alias, 
        AggregateRegistry.typecheck(fn.function, fn.args.map { typechecker.typeOf(_) })
      ) 
    }
  val annotationSchema: Seq[(String,Type)] = Seq()

  val aggNames =
    aggregates.map { _.alias }
  val aggTypes =
    aggregates.map { agg => (agg, agg.args.map { typechecker.typeOf(_) }) }

  private val aggEvalScope: Map[String,(Type, Row => PrimitiveValue)] =
    inputSchema.zipWithIndex.map { 
      case ((name, t), idx) => 
        logger.debug(s"For $name ($t) using idx = $idx")
        (name, (t, (r:Row) => { logger.trace(s"read $name @ $idx"); r(idx) })) 
    }.toMap
  private val aggEval = new EvalInlined[Row](aggEvalScope)

  val groupConstructor: Seq[() => AggregateValue] = 
    aggTypes.map { agg =>
      logger.debug(s"Compiling: $agg")
      agg match {
        case (AggFunction("SUM", false, Seq(input), _), Seq(TInt())) => {
          val i = aggEval.compileForLong(input);
          { () => new LongSumAggregate(i) }
        }
        case (AggFunction("SUM", false, Seq(input), _), Seq(TFloat())) => {
          val i = aggEval.compileForDouble(input);
          { () => new DoubleSumAggregate(i) }
        }
        case (AggFunction("AVG", false, Seq(input), _), Seq(TInt())) => {
          val i = aggEval.compileForLong(input);
          { () => new LongAvgAggregate(i) }
        }
        case (AggFunction("AVG", false, Seq(input), _), Seq(TFloat())) => {
          val i = aggEval.compileForDouble(input);
          { () => new DoubleAvgAggregate(i) }
        }
        case (AggFunction("STDDEV", false, Seq(input), _), Seq(TInt())) => {
          val i = aggEval.compileForLong(input);
          { () => new LongStdDevAggregate(i) }
        }
        case (AggFunction("STDDEV", false, Seq(input), _), Seq(TFloat())) => {
          val i = aggEval.compileForDouble(input);
          { () => new DoubleStdDevAggregate(i) }
        }
        case (AggFunction("COUNT", false, Seq(), _), Seq()) => {
          { () => new CountAggregate() }
        }
        case (AggFunction("GROUP_AND", false, Seq(input), _), Seq(TBool())) => {
          val i = aggEval.compileForBool(input);
          { () => new GroupAndAggregate(i) }        
        }
        case (AggFunction("GROUP_OR", false, Seq(input), _), Seq(TBool())) => {
          val i = aggEval.compileForBool(input);
          { () => new GroupOrAggregate(i) }        
        }
        case (AggFunction("FIRST", false, Seq(input), _), _) => {
          val i = aggEval.compile(input);
          { () => new FirstAggregate(i) }        
        }
      }
    }
  def makeGroup = groupConstructor.map{ _() }
  var groupValues = scala.collection.mutable.Map[Seq[PrimitiveValue], Seq[AggregateValue]]()
  val extractGroup = groupByColumns.map { _.name }.map { aggEvalScope(_)._2 }

  type GroupRecord = (Seq[PrimitiveValue], Seq[AggregateValue])
  lazy val groupIterator: Iterator[GroupRecord] = build()

  def build(): Iterator[GroupRecord] = 
  {
    logger.debug(s"Starting to aggregate $groupByColumns <- $aggregates")
    var i = 0;
    for(row <- src){
      i += 1
      logger.trace(s"$i : $row")
      val group = extractGroup.map { _(row) }
      val aggs = groupValues.getOrElseUpdate(group, makeGroup)
      for(agg <- aggs){
        agg.fold(row)
      }
      if(i % 10000 == 0) { logger.trace(s"%i rows aggregated") }
    }
    logger.debug(s"Done aggregating $groupByColumns <- $aggregates")
    return groupValues.iterator
  }

  def close(): Unit =
    src.close()

  def hasNext() =
    groupIterator.hasNext

  def next(): Row =
  {
    val group = groupIterator.next()
    new ExplicitRow(group._1 ++ group._2.map { _.result }, Seq(), this)
  }
}