package mimir.lenses

import java.sql.{SQLException, ResultSet}

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
  def init(data: ResultSet): Unit = {
    // TODO Generate a mapping for new types and store in varTypes
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

  // Model Implementation
  override def varTypes: List[T] = {
    val types = new Array[T](lens.allKeys().length)
    types.map(_ => Type.TInt)
    types.toList
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
      case v => new IntPrimitive(v.asLong)
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

  override def reason(idx: Int): String = "Types were inferred"
}

case class TypeInferenceAnalysis(model: TypeInferenceModel,
                                 idx: Int,
                                 args: List[Expression])
extends Proc(args) {

  def get(args: List[PrimitiveValue]): PrimitiveValue = {
    model.mostLikelyValue(idx, args)
  }
  def exprType(bindings: Map[String,Type.T]) = model.varTypes(idx)
  def rebuild(c: List[Expression]) = new TypeInferenceAnalysis(model, idx, args)

}