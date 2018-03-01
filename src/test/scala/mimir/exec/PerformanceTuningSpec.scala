package mimir.exec

import org.specs2.mutable._
import org.specs2.specification._

import mimir.test._

object PerformanceTuningSpec 
  extends SQLTestSpecification("PerformanceTuningSpec")
  with BeforeAll
  with TestTimer
{

  def inputSize = 1000000

  def beforeAll = {
    /*db.backend.update(s"""
      CREATE TEMPORARY TABLE TEST_SEQUENCE AS
        WITH RECURSIVE generate_series(value) AS (
          SELECT 1 UNION ALL 
          SELECT value+1 
            FROM generate_series 
            WHERE value+1 <= $inputSize
        )
        SELECT cast(value as int) as value FROM generate_series;
    """)*/
  }

  "ProjectionResultIterator" should {

    "Not have absurdly high extraction costs" >> {
      val queryStartTime = getTime
      db.query("""
        SELECT 
          value                                AS A, 
          value*10                             AS B, 
          (value+21)*100                       AS C, 
          random()*value                       AS D, 
          CASE WHEN (value*value*value) < 100 
               THEN value*50 ELSE random() END AS E 
        FROM TEST_SEQUENCE;
      """) { results =>
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