package mimir.exec

import org.specs2.mutable._
import org.specs2.specification._

import mimir.test._
import mimir.algebra.HardTable
import mimir.algebra.TInt
import mimir.algebra.IntPrimitive

object PerformanceTuningSpec 
  extends SQLTestSpecification("PerformanceTuningSpec")
  with BeforeAll
  with TestTimer
{

  def inputSize = 1000000

  def beforeAll = {
    
  }

  "ProjectionResultIterator" should {

    "Not have absurdly high extraction costs" >> {
      val queryStartTime = getTime
      db.query(
        HardTable(
          Seq(("value",TInt())), 
          Seq((1 to inputSize toSeq).map(IntPrimitive(_)))
        )
      ) { results =>
        val queryRunTime = (getTime - queryStartTime)
        var count = 0;

        val (_, queryFetchTime) =
          time { results.foreach { row => count += 1; } }

        val perTupleOverhead = queryFetchTime / inputSize
        val percentOverhead  = (queryFetchTime / (queryRunTime+queryFetchTime))

        println(f"Total time: $queryRunTime%1.3f s run; $queryFetchTime%1.3f s fetch")
        println(f"Overhead: ${perTupleOverhead*1000000}%1.3f µs/tuple; ${percentOverhead*100}%3.1f${"%"} of total runtime")
        // Limit overhead to no more than 1µs per tuple
        queryFetchTime / inputSize should be lessThan (0.0000033)
      }
    }

  }


}