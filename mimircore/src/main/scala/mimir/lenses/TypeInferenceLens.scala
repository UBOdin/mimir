package mimir.lenses

import java.sql.SQLException
import scala.util._

import mimir.Database
import mimir.algebra.Type.T
import mimir.algebra._
import mimir.ctables._
import mimir.exec.ResultIterator
import mimir.util.TypeUtils

class TypeInferenceLens(name: String, args: List[Expression], source: Operator)
  extends Lens(name, args, source) {

  var orderedSourceSchema: List[(String,Type.T)] = null
  var model: Model = null
  var db: Database = null

  def sourceSchema() = {
    if(orderedSourceSchema == null){
      orderedSourceSchema =
        source.schema.map( _ match { case (n,t) => (n.toUpperCase,t) } )
    }
    orderedSourceSchema
  }

  def schema(): List[(String, Type.T)] =
    model.asInstanceOf[TypeInferenceModel].inferredTypeMap.map( x => (x._1, x._2))

  def allKeys() = { sourceSchema.map(_._1) }

  def lensType = "TYPE_INFERENCE"

  /**
   * `view` emits an Operator that defines the Virtual C-Table for the lens
   */
  override def view: Operator = {
    Project(
      allKeys().
        zipWithIndex.
          map{ case (k, i) =>
            ProjectArg(
              k,
              Function(
                "CAST",
                List(Var(k), VGTerm((name, model), i, List()))
              )
            )
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
    val results = db.query(source)

    model = new TypeInferenceModel(this)
    model.asInstanceOf[TypeInferenceModel].init(results)
  }
}

class TypeInferenceModel(lens: TypeInferenceLens) extends Model
{
  val numVars = lens.sourceSchema.length
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
      val ratio: Double = max._2.toFloat / totalVotes
      if(ratio >= threshold) {
        (max._1, ratio)
      } else {
        (Type.TString, 0)
      }
    }
  }


  def init(data: ResultIterator): Unit = {
    inferredTypeMap = learn(lens.sourceSchema(), data.allRows())
  }

  def learn(sch: List[(String, T)],
                 data: List[List[PrimitiveValue]]): List[(String, T, Double)] = {

    /**
     * Count votes for each type
     */
    val inferClasses =
      sch.map{ case(k, t) => (k, new TypeInferrer)}

    data.foreach( (row) =>
      sch.indices.foreach( (i) =>
        inferClasses(i)._2.detectAndVoteType(
          try{
            row(i).asString
          } catch {
            case e:TypeException => ""
          }
        )
      )
    )

    /**
     * Now infer types
     */
    inferClasses.map{
      case(k, inferClass) =>
        val inferred = inferClass.infer()
        (k, inferred._1, inferred._2)
    }
  }

  // Model Implementation
  override def varType(idx: Int, argTypes: List[Type.T]): T = Type.TType

  def sample(idx: Int, randomness: Random, args: List[PrimitiveValue]): PrimitiveValue = {
    bestGuess(idx, args)
  }
  def bestGuess(idx: Int, args: List[PrimitiveValue]): PrimitiveValue = {
    TypePrimitive(inferredTypeMap(idx)._2)
  }
  def reason(idx: Int, args: List[Expression]): (String) = {
    val percentage = (inferredTypeMap(idx)._3 * 100).round

    if(percentage == 0) {
      "I assumed that the type of " + inferredTypeMap(idx)._1 +
        " is string"
    } else {
      "I assumed that the type of " + inferredTypeMap(idx)._1 +
        " is " + Type.toString(inferredTypeMap(idx)._2) +
        " with " + percentage.toString + "% of the data conforming to the expected type"
    }
  }
}
