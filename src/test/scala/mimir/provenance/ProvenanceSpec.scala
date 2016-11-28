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
import mimir.models._

object ProvenanceSpec extends Specification {

  val baseModel = IndependentVarsModel("TEST", List(
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

  def checkProv(expectedColCount: Integer, oper:String) = {
    val (provOper:Operator, provCols:List[String]) = prov(oper);
    val provSchema = provOper.schema.toMap
    val initial = (if(expectedColCount > 0) {
      provCols must have size expectedColCount
    } else {
      provCols must not be empty 
    })
    provCols.map( col => 
      provSchema must havePair(col -> Type.TRowId)
    ).fold(initial)( _ and _ )
  }

  "The Provenance Compiler" should {

    "Work with one relation" >> {
      checkProv(1, "R(A, B, C)")
      checkProv(1, "S(C, D)")
    }

    "Work with projection" >> {
      checkProv(1, """
        PROJECT[A<=A](R(A, B, C))
      """)
      checkProv(1, """
        PROJECT[A<=A, Z<=C](R(A, B, C))
      """)
    }

    "Work with selection" >> {
      checkProv(1, """
        SELECT[A > B](R(A, B, C))
      """)
    }

    "Work with joins" >> {
      checkProv(2, """
        JOIN(R(A, B, C), S(D, E))
      """)
      checkProv(2, """
        JOIN(PROJECT[A <= A](R(A, B, C)), SELECT[D>E](S(D, E)))
      """)
    }

    "Work with unions" >> {
      checkProv(2, """
        UNION(PROJECT[D <= A, E <= B](R(A, B, C)), S(D, E))
      """)
      checkProv(3, """
        JOIN(R(A, B, C), UNION(PROJECT[D <= A, E <= B](R(A, B, C)), S(D, E)))
      """)
    }

    "Work with misaligned unions" >> {
      checkProv(3, """
        UNION(PROJECT[D <= A, E <= E](JOIN(R(A, B, C), S(D, E))), S(D, E))
      """)
    }

  }

}