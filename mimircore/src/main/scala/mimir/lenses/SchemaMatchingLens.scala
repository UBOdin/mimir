package mimir.lenses

import java.sql.SQLException

import mimir.Database
import mimir.algebra.Type.T
import mimir.algebra._
import mimir.ctables.{VGTerm, Model}
import org.apache.lucene.search.spell.{NGramDistance, LevensteinDistance, JaroWinklerDistance, StringDistance}

import scala.tools.nsc.util.ShowPickled

/**
 * Created by vinayak on 7/20/15.
 */
class SchemaMatchingLens(name: String, args: List[Expression], source: Operator)
  extends Lens(name, args, source) {

  var targetSchema: Map[String, Type.T] = null
  var sourceSchema: Map[String, Type.T] = source.schema.toMap
  var db: Database = null
  var model: Model = null

  def init() = {
    if (args.length % 2 != 0)
      throw new SQLException("Incorrect parameters for " + lensType + " Lens")
    if (targetSchema == null) {
      targetSchema = Map[String, Type.T]()
      var i = 0
      while (i < args.length) {
        val col = args(i).toString.toUpperCase
        i += 1
        val t = Type.fromString(args(i).toString)
        i += 1
        targetSchema += (col -> t)
      }
    }
  }

  def lensType = "SCHEMA_MATCHING"

  /**
   * `view` emits an Operator that defines the Virtual C-Table for the lens
   */
  override def view: Operator = {
    Project(
      targetSchema.keys.toList.zipWithIndex.map { case (key, idx) => ProjectArg(
        key,
        CaseExpression(
          sourceSchema.filter(_._2 == targetSchema(key)).keys.toList.map(b => WhenThenClause(
            Comparison(Cmp.Eq,
              VGTerm((name, model), idx, List(StringPrimitive(key), StringPrimitive(b))),
              BoolPrimitive(true)),
            Var(b))
          ),
          NullPrimitive()
        ))
      },
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
  var colMapping: Map[String, Map[String, Double]] = null

  def getSchemaMatching(criteria: String, targetColumn: String, sourceColumns: List[String]): Map[String, Double] = {
    val matcher: StringDistance = criteria match {
      case "JaroWinklerDistance" => new JaroWinklerDistance()
      case "LevensteinDistance" => new LevensteinDistance()
      case "NGramDistance" => new NGramDistance()
      case _ => null
    }
    var total = 0.0
    // calculate distance
    val sorted = sourceColumns.map(a => {
      val dist = matcher.getDistance(targetColumn, a)
      total += dist
      (a, dist)
    }).sortBy(_._2).toMap
    // normalize
    sorted.map { case (k, v) => (k, v / total) }
  }

  def learn(targetSchema: Map[String, Type.T], sourceSchema: Map[String, Type.T]) = {
    colMapping = targetSchema.map { case (k, v) =>
      (k, getSchemaMatching("NGramDistance", k, sourceSchema.filter(_._2 == v).keys.toList))
    }
    schema = targetSchema
  }

  override def varTypes: List[T] = List.fill(schema.size)(Type.TBool)

  override def sample(seed: Long, idx: Int, args: List[PrimitiveValue]): PrimitiveValue = mostLikelyValue(idx, args)

  override def sampleGenerator(idx: Int, args: List[PrimitiveValue]): PrimitiveValue = sample(0, idx, args)

  override def mostLikelyValue(idx: Int, args: List[PrimitiveValue]): PrimitiveValue = {
    val targetCol = args(0).asString
    val sourceCol = args(1).asString
    if (colMapping(targetCol).maxBy(_._2)._1.equals(sourceCol))
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
