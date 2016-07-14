package mimir.provenance;

import org.specs2.mutable._

import mimir._
import mimir.ctables._
import mimir.parser._
import mimir.algebra._
import mimir.sql._
import mimir.optimizer._
import mimir.exec._
import mimir.provenance._

object ProvenanceSpec extends Specification {

  val baseModel = JointSingleVarModel(List(
    UniformDistribution,
    UniformDistribution,
    UniformDistribution,
    UniformDistribution,
    UniformDistribution
  ))
  val schema = Map[String,Map[String,Type.T]](
    ("R", Map( 
      ("R_A", Type.TInt), 
      ("R_B", Type.TInt), 
      ("R_C", Type.TInt)
    )),
    ("S", Map( 
      ("S_C", Type.TInt), 
      ("S_D", Type.TFloat)
    ))
  )

  def parser = new OperatorParser(
    (x: String) => baseModel,
    schema(_).toList
  )
  def expr = parser.expr _
  def oper:(String => Operator) = parser.operator _

  def prov(x: String) = Provenance.compile(oper(x))

  def checkProv(x: String, expectedColCount:Integer) = {
    val (provOper:Operator, provCols:List[String]) = prov("R(A, B)")
    provCols must not be empty
    if(expectedColCount > 0){
      provCols must have size expectedColCount
    }
    val provSchema = provOper.schema
    provCols.map( col => 
      provSchema must contain (col, Type.TRowId)
    )
  }

  "The Provenance Compiler" should {

    "Work with one relation" >> {
      checkProv("R(A, B)", 1)
    }

  }

}