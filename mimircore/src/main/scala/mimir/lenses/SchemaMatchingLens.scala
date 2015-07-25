package mimir.lenses

import java.sql.SQLException

import mimir.Database
import mimir.algebra.Type.T
import mimir.algebra._
import mimir.ctables.Model

import scala.tools.nsc.util.ShowPickled

/**
 * Created by vinayak on 7/20/15.
 */
class SchemaMatchingLens(name: String, args: List[Expression], source: Operator)
  extends Lens(name, args, source) {

  var targetSchema: Map[String, Type.T] = null
  //var cols: Map[String, Type.T] = null
  var sourceSchema: Map[String, Type.T] = source.schema.toMap
  var db: Database = null
  var model: Model = null

  def init() = {
    if (args.length % 2 != 0)
      throw new SQLException("Incorrect parameters for " + lensType + " Lens")
    if (targetSchema == null) {
      targetSchema = Map[String, Type.T]()
      //cols = Map[String, Type.T]()
      var i = 0
      while (i < args.length) {
        //val qualified = name + "_" + args(i).toString.toUpperCase
        val col = args(i).toString.toUpperCase
        i += 1
        val t = Type.fromString(args(i).toString)
        i += 1
        targetSchema += (col -> t)
        //cols += (col -> t)
      }
    }
  }

  def lensType = "SCHEMA_MATCHING"

  /**
   * `view` emits an Operator that defines the Virtual C-Table for the lens
   */
  override def view: Operator = {
    // TODO change for idx
    Project(
      targetSchema.keys.toList.map(a => ProjectArg(
        a,
        CaseExpression(
          sourceSchema.filter(_._2 == targetSchema(a)).keys.toList.map(b => WhenThenClause(
            Comparison(Cmp.Eq, model.mostLikelyExpr(0, List(StringPrimitive(a), StringPrimitive(b))), BoolPrimitive(true)),
            Var(b))
          ),
          NullPrimitive()
        )
      )),
      source
    )
  }

  /**
   * Initialize the lens' model by building it from scratch.  Typically this involves
   * using `db` to evaluate `source`
   */
  override def build(db: Database): Unit = {
    init()
    this.db = db
    model = new SchemaMatchingModel(this)
    model.asInstanceOf[SchemaMatchingModel].learn(targetSchema, sourceSchema)
  }
}

case class SchemaAnalysis(model: SchemaMatchingModel, idx: Int, args: List[Expression])
  extends Proc(args) {
  override def get(v: List[PrimitiveValue]): PrimitiveValue = model.mostLikelyValue(idx, v)

  override def rebuild(c: List[Expression]): Expression = SchemaAnalysis(model, idx, c)

  override def exprType(bindings: Map[String, T]): T = Type.TBool
}

class SchemaMatchingModel(lens: SchemaMatchingLens) extends Model {

  var schema: Map[String, Type.T] = null
  val colMapping = collection.mutable.Map[String, String]()

  def editDistance(s1: String, s2: String, i: Int, j: Int, dp: collection.mutable.Map[(Int, Int), Int]): Int = {
    var res = 0
    if (dp.contains((i, j))) res = dp((i, j))
    else if (i < 0 && j < 0) res = 0
    else if (i < 0) res = j + 1
    else if (j < 0) res = i + 1
    else {
      val d = if (s1(i) == s2(j)) 0 else 1
      val l = List(editDistance(s1, s2, i - 1, j - 1, dp) + d, editDistance(s1, s2, i - 1, j, dp) + 1,
        editDistance(s1, s2, i, j - 1, dp) + 1)
      res = l.min
    }
    if (!dp.contains((i, j)))
      dp += ((i, j) -> res)
    res
  }

  def learn(targetSchema: Map[String, Type.T], sourceSchema: Map[String, Type.T]) = {
    for(i <- targetSchema.keys.toList){
      var dist = List[(String, Int)]()
      for(j <- sourceSchema.filter(_._2 == targetSchema(i)).keys.toList){
        dist ::= (j, editDistance(i, j, i.length - 1, j.length - 1, collection.mutable.Map[(Int, Int), Int]()))
      }
      colMapping += (i -> dist.minBy(_._2)._1)
    }
    schema = targetSchema
  }

  override def varTypes: List[T] = schema.map(_._2).toList

  override def sample(seed: Long, idx: Int, args: List[PrimitiveValue]): PrimitiveValue = mostLikelyValue(idx, args)

  override def sampleGenerator(idx: Int, args: List[PrimitiveValue]): PrimitiveValue = sample(0, idx, args)

  override def mostLikelyValue(idx: Int, args: List[PrimitiveValue]): PrimitiveValue = {
    val targetCol = args(0).asString
    val sourceCol = args(1).asString
    if(colMapping(targetCol).equals(sourceCol))
      BoolPrimitive(true)
    else BoolPrimitive(false)
  }

  override def upperBoundExpr(idx: Int, args: List[Expression]): Expression = SchemaAnalysis(this, idx, args)

  override def upperBound(idx: Int, args: List[PrimitiveValue]): PrimitiveValue = mostLikelyValue(idx, args)

  override def sampleGenExpr(idx: Int, args: List[Expression]): Expression = SchemaAnalysis(this, idx, args)

  override def mostLikelyExpr(idx: Int, args: List[Expression]): Expression = SchemaAnalysis(this, idx, args)

  override def lowerBoundExpr(idx: Int, args: List[Expression]): Expression = SchemaAnalysis(this, idx, args)

  override def lowerBound(idx: Int, args: List[PrimitiveValue]): PrimitiveValue = mostLikelyValue(idx, args)

  override def reason(idx: Int): String = "Mapping columns " + colMapping.mkString(", ")
}
