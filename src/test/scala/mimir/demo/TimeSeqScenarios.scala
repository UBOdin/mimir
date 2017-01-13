package mimir.demo

import java.io._
import org.specs2.reporter.LineLogger
import org.specs2.specification.core.{Fragment,Fragments}

import mimir.test._
import mimir.util._

object TimeSeqScenarios
  extends SQLTestSpecification("TimeSeq")
{

  sequential

  "The Trivial Time Series" should {

    "Load correctly" >> {
      update("LOAD 'test/data/seq.csv'")
      ok
    }

    "EXTEND correctly" >> {
      query("select T, A, B from seq limit 20").allRows must have size(20)
      val r1 = query("""
        extend set cat=
          case when a is not null then 'A' 
               when b is not null then 'B' 
               else 'C' 
          end
      """).allRows
      r1.map(_(3)).toSet must contain(eachOf(str("A"), str("B"), str("C")))

    }

  }

}
