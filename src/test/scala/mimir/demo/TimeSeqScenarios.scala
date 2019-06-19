package mimir.demo

import java.io._
import org.specs2.reporter.LineLogger
import org.specs2.specification.core.{Fragment,Fragments}

import mimir.test._
import mimir.util._
import mimir.algebra.ID

object TimeSeqScenarios
  extends SQLTestSpecification("TimeSeq")
{

  sequential

  "The Trivial Time Series" should {

    "Load correctly" >> {
      update("LOAD 'test/data/seq.csv'")
      ok
    }

    "run limit queries" >> {
      query("select T, A, B from seq limit 20"){ _.toSeq must have size(20) }
    }

    "run order-by limit queries" >> {
      query("select T, A, B from seq order by t limit 20"){ 
        _.toSeq.reverse.head(ID("T")).asLong must beEqualTo(20)
      }
      query("select T, A, B from seq order by t desc limit 20"){
        _.toSeq.reverse.head(ID("T")).asLong must beEqualTo(9980)
      }
    }

    "generate categories correctly" >> {
      query("""
        select T, A, B, 
          case when a is not null then 'A' 
               when b is not null then 'B' 
               else 'C' 
          end as cat from seq limit 20
      """) { _.map { _(3).asString }.toSet must contain("A", "B", "C") }
    }

  }

}
