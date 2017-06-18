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

  val schema = Map[String,Seq[(String,Type)]](
    ("R", Seq( 
      ("R_A", TInt()),
      ("R_B", TInt()),
      ("R_C", TInt())
    )),
    ("S", Seq( 
      ("S_C", TInt()),
      ("S_D", TFloat())
    ))
  )
  def table(name: String): Operator =
    Table(name, name, schema(name), Seq())

  def typechecker = new Typechecker
  def expr = ExpressionParser.expr _

  def prov(x: Operator) = Provenance.compile(x)

  def checkProv(expectedColCount: Integer, oper:Operator) = {
    val (provOper:Operator, provCols:List[String]) = prov(oper);
    val provSchema = typechecker.schemaOf(provOper).toMap
    val initial = (if(expectedColCount > 0) {
      provCols must have size expectedColCount
    } else {
      provCols must not be empty 
    })
    provCols.map( col => 
      provSchema must havePair(col -> TRowId())
    ).fold(initial)( _ and _ )
  }

  "The Provenance Compiler" should {

    "Work with one relation" >> {
      checkProv(1, table("R"))
      checkProv(1, table("S"))
    }

    "Work with projection" >> {
      checkProv(1, 
        table("R")
          .project("A")
      )
      checkProv(1, 
        table("R")
          .mapParsed( ("A", "A"), ("Z", "C") )
      )
    }

    "Work with selection" >> {
      checkProv(1, 
        table("R")
          .filterParsed("A > B")
      )
    }

    "Work with joins" >> {
      checkProv(2, 
        table("R")
          .join(table("S"))
      )
      checkProv(2, 
        table("R")
          .project("A")
          .join(
            table("S")
              .filterParsed("D > E")
          )
      )
    }

    "Work with unions" >> {
      checkProv(2, 
        table("R")
          .mapParsed( ("D", "A"), ("E", "B") )
          .union(table("S"))
      )
      checkProv(3, 
        table("R").
          join(
            table("R")
              .mapParsed( ("D", "A"), ("E", "B") )
              .union(table("S"))
          )
      )
    }

    "Work with misaligned unions" >> {
      checkProv(3, 
        table("R")
          .join(table("S"))
          .mapParsed( ("D", "A"), ("E", "E") )
          .union(table("S"))
      )
    }
  }

}