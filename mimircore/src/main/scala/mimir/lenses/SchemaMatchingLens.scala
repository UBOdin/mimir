package mimir.lenses

import java.sql.SQLException
import scala.util._

import mimir.Database
import mimir.algebra.Type.T
import mimir.algebra._
import mimir.ctables.{Model, VGTerm}
import mimir.optimizer.{InlineVGTerms}
import org.apache.lucene.search.spell.{JaroWinklerDistance, LevensteinDistance, NGramDistance, StringDistance}

class SchemaMatchingLens(name: String, args: List[Expression], source: Operator)
  extends Lens(name, args, source) {

  var targetSchema: Map[String, Type.T] = null
  var sourceSchema: Map[String, Type.T] = Typechecker.schemaOf(InlineVGTerms.optimize(source)).toMap
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

  def schema(): List[(String, Type.T)] = targetSchema.toList

  def lensType = "SCHEMA_MATCHING"

  def isTypeCompatible(a: T, b: T): Boolean = 
  {
    (a,b) match {
      case ((Type.TInt|Type.TFloat),  (Type.TInt|Type.TFloat)) => true
      case (Type.TAny, _) => true
      case (_, Type.TAny) => true
      case _ => a == b
    }

  }

  /**
   * `view` emits an Operator that defines the Virtual C-Table for the lens
   */
  override def view: Operator = {
    // println("SourceSchema: "+sourceSchema)
    // println("SourceSchema: "+targetSchema)
    Project(
      targetSchema.keys.toList.zipWithIndex.map { case (key, idx) => ProjectArg(
        key,
        ExpressionUtils.makeCaseExpression(
          sourceSchema.filter(
            (src) => isTypeCompatible(src._2, targetSchema(key))
          ).keys.toList.map(b => (
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
  def build(db: Database): Unit = {
    init()
    this.db = db
    model = new SchemaMatchingModel(this)
    model.asInstanceOf[SchemaMatchingModel].learn(targetSchema, sourceSchema)
  }
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
      (k, getSchemaMatching("NGramDistance", k, sourceSchema.filter( (src) => lens.isTypeCompatible(src._2, v)).keys.toList))
    }
    schema = targetSchema
  }

  def varTypes: List[T] = List.fill(schema.size)(Type.TBool)

  def sample(idx: Int, randomness: Random, args: List[PrimitiveValue]): PrimitiveValue = 
    bestGuess(idx, args)

  def bestGuess(idx: Int, args: List[PrimitiveValue]): PrimitiveValue = {
    val targetCol = args(0).asString
    val sourceCol = args(1).asString
    if (colMapping(targetCol).maxBy(_._2)._1.equals(sourceCol))
      BoolPrimitive(true)
    else BoolPrimitive(false)
  }

  def reason(idx: Int, args: List[Expression]): String = {
    val target = schema.keys.toList(idx)
    val source = colMapping(target).maxBy(_._2)
    ("I assumed that " + source._1 + " maps to " + target + " ("+ (source._2 * 100).toInt +"% likely)")
  }
}
