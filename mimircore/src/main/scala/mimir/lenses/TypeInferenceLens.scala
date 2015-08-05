package mimir.lenses

import java.sql.{ResultSet, SQLException}

import mimir.Database
import mimir.algebra.Type.T
import mimir.algebra._
import mimir.ctables._

class TypeInferenceLens(name: String, args: List[Expression], source: Operator)
  extends Lens(name, args, source) {

  var orderedSourceSchema: List[(String,Type.T)] = null
  var model: Model = null
  var db: Database = null

  def sourceSchema() = {
    if(orderedSourceSchema == null){
      orderedSourceSchema =
        source.schema.toList.map( _ match { case (n,t) => (n.toUpperCase,t) } )
    }
    orderedSourceSchema
  }

  def allKeys() = { sourceSchema.map(_._1) }

  def lensType = "TYPE_INFERENCE"

  /**
   * `view` emits an Operator that defines the Virtual C-Table for the lens
   */
  override def view: Operator = {
    Project(
      allKeys().
        zipWithIndex.
          map{ case (k, i) => {
            ProjectArg(k, rowVar(i))
          }
        },
        source
    )
  }

  /**
   * Initialize the lens' model by building it from scratch.  Typically this involves
   * using `db` to evaluate `source`
   */
  override def build(db: Database): Unit = {
    this.db = db
    val results = db.backend.execute(db.convert(source))

    model = new TypeInferenceModel(this)
    model.asInstanceOf[TypeInferenceModel].init(results)
  }
}

class TypeInferenceModel(lens: TypeInferenceLens) extends Model
{
  var inferredTypeMap = List[(String, Type.T, Double)]()
  var threshold: Double = lens.args.head.asInstanceOf[FloatPrimitive].asDouble

  class TypeInferrer {
    private val votes =
      scala.collection.mutable.Map(Type.TInt -> 0,
                                    Type.TFloat -> 0,
                                    Type.TDate -> 0,
                                    Type.TBool -> 0)
    private var totalVotes = 0
    def detectAndVoteType(v: String): Unit = {
      if(v != null) {
        if(v.matches("(\\+|-)?([0-9]+)"))
          votes(Type.TInt) += 1
        if(v.matches("(\\+|-)?([0-9]*(\\.[0-9]+))"))
          votes(Type.TFloat) += 1
        if(v.matches("[0-9]{4}\\-[0-9]{2}\\-[0-9]{2}"))
          votes(Type.TDate) += 1
        if(v.matches("(?i:true|false)"))
          votes(Type.TBool) += 1
      }
      else {
        votes.foreach{ case (t, v) => votes(t) += 1 }
      }

      totalVotes += 1
    }
    def infer(): (Type.T, Double) = {
      if(totalVotes == 0)
        return (Type.TString, 0)

      val max = votes.maxBy(_._2)
      val ratio: Double = (max._2.toFloat / totalVotes)
      if(ratio >= threshold) {
        (max._1, ratio)
      } else {
        (Type.TString, 0)
      }
    }
  }


  def init(data: ResultSet): Unit = {
    inferredTypeMap = learn(lens.sourceSchema(), data)
  }

  def learn(sch: List[(String, T)],
                 data: ResultSet): List[(String, T, Double)] = {

    /**
     * Count votes for each type
     */
    val inferClasses =
      sch.map{ case(k, t) => (k, new TypeInferrer)}

    while(data.next()) {
      sch.indices.foreach( (i) =>
        inferClasses(i)._2.detectAndVoteType(data.getString(i+1))
      )
    }

    data.close()

    /**
     * Now infer types
     */
    inferClasses.map{
      case(k, inferClass) =>
        val inferred = inferClass.infer()
        (k, inferred._1, inferred._2)
    }
  }

  def getValue(idx: Int, rowid: PrimitiveValue): PrimitiveValue =
  {
    val rowValues = lens.db.query(
      CTPercolator.percolate(
        Select(
          Comparison(Cmp.Eq, Var("ROWID"), rowid),
          lens.source
        )
      )
    )
    if(!rowValues.getNext()){
      throw new SQLException("Invalid Source Data ROWID: '" +rowid+"'")
    }

    rowValues(idx+1)
  }

  def cast(v: String, t: T): PrimitiveValue = {
    try {
      t match {
        case Type.TBool =>
          new BoolPrimitive(v.toBoolean)

        case Type.TDate => {
          val (y, m, d) = parseDate(v)
          new DatePrimitive(y, m, d)
        }

        case Type.TFloat =>
          new FloatPrimitive(v.toDouble)

        case Type.TInt =>
          new IntPrimitive(v.toInt)

        case _ =>
          new StringPrimitive(v)
      }
    } catch {
      case _: Exception => new NullPrimitive
        // TODO More can be done for coercion here
    }
  }

  def parseDate(date: String): (Int, Int, Int) = {
    val split = date.split("-")
    (split(0).toInt, split(1).toInt, split(2).toInt)
  }

  // Model Implementation
  override def varTypes: List[T] = {
    inferredTypeMap.map(_._2)
  }

  override def sample(seed: Long, idx: Int, args: List[PrimitiveValue]): PrimitiveValue = {
    mostLikelyValue(idx, args)
  }

  override def sampleGenerator(idx: Int, args: List[PrimitiveValue]): PrimitiveValue = {
    mostLikelyValue(idx, args)
  }

  override def mostLikelyValue(idx: Int, args: List[PrimitiveValue]): PrimitiveValue = {
    getValue(idx, args(0)) match {
      case v: NullPrimitive => v
      case v => cast(v.asString, varTypes(idx))
    }
  }

  override def upperBoundExpr(idx: Int, args: List[Expression]): Expression = {
    new TypeInferenceAnalysis(this, idx, args)
  }

  override def upperBound(idx: Int, args: List[PrimitiveValue]): PrimitiveValue = {
    mostLikelyValue(idx, args)
  }

  override def sampleGenExpr(idx: Int, args: List[Expression]): Expression = {
    new TypeInferenceAnalysis(this, idx, args)
  }

  override def mostLikelyExpr(idx: Int, args: List[Expression]): Expression = {
    new TypeInferenceAnalysis(this, idx, args)
  }

  override def lowerBoundExpr(idx: Int, args: List[Expression]): Expression = {
    new TypeInferenceAnalysis(this, idx, args)
  }

  override def lowerBound(idx: Int, args: List[PrimitiveValue]): PrimitiveValue = {
    mostLikelyValue(idx, args)
  }

  override def reason(idx: Int): String = {
    val percentage = (inferredTypeMap(idx)._3 * 100).round

    if(percentage == 0) {
      return "I assumed that the type of " + inferredTypeMap(idx)._1 +
        " is string"
    }

      "I assumed that the type of " + inferredTypeMap(idx)._1 +
      " is " + prettyTypeString(inferredTypeMap(idx)._2) +
      " with " + percentage.toString + "% of the data conforming to the expected type"
  }

  private def prettyTypeString(t: Type.T): String = {
    t match {
      case Type.TBool => "Boolean"
      case Type.TDate => "Date"
      case Type.TFloat => "Double"
      case Type.TInt => "Integer"
      case Type.TString => "String"
      case _ => "Unknown"
    }
  }


}

case class TypeInferenceAnalysis(model: TypeInferenceModel,
                                 idx: Int,
                                 args: List[Expression])
extends Proc(args) {

  def get(args: List[PrimitiveValue]): PrimitiveValue = {
    model.mostLikelyValue(idx, args)
  }
  def exprType(bindings: Map[String,Type.T]) = model.varTypes(idx)
  def rebuild(c: List[Expression]) = new TypeInferenceAnalysis(model, idx, c)

}