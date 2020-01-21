package mimir.algebra;

import org.specs2.specification._

import mimir.algebra._
import mimir.util._
import mimir.test._

object SamplingSpec
  extends SQLTestSpecification("Sampling")
  with BeforeAll 
{
  def beforeAll = 
  {
    loadCSV(
      targetTable = "DATA", 
      sourceFile = "test/data/Bestbuy_raw.csv",
      detectHeaders = true
    )
  }

  "The Mimir Sampler" >> {

    "draw uniform samples correctly" >> {

      val query = db.table("DATA")
          .sampleUniformly(0.1, seed = Some(42l))
          .count()

      val count = 
        db.query(query) { result =>
          val row = result.next()
          row(ID("COUNT")).asLong.toInt
        }

      // 345 rows in the input file.  10% sampling rate ~= 34-35 rows.
      count should beCloseTo(34, 10)

      val recount = 
        db.query(query) { result =>
          val row = result.next()
          row(ID("COUNT")).asLong.toInt
        }

      // With an identical seed, the sample sets should be IDENTICAL
      recount must be equalTo(count)

    }


    "draw stratified samples correctly" >> {

      val stratificationColumn = "MANUFACTURER"
      schemaLookup("DATA").map { _._1 } must contain(ID(stratificationColumn))

      val query = db.table("DATA")
        .stratifiedSample(
          stratificationColumn,
          TInt(),
          Map(
            IntPrimitive(0) -> 1.0,
            IntPrimitive(1) -> 0.1
          ), 
          seed = Some(42l)
        )
        .groupBy(Var(ID(stratificationColumn)))(
          AggFunction(ID("count"), false, Seq(), ID("COUNT"))
        )

      val counts = 
        db.query(query) { result => 
          result.map { row => 
            row(ID("MANUFACTURER")).asLong.toInt -> 
              row(ID("COUNT")).asLong.toInt
          }.toIndexedSeq.toMap
        }

      // All unspecified strata must be empty
      counts.size must be equalTo(2)
      // 33 rows have a manufacturer of '0'
      counts(0) must be equalTo(33)
      // 77 rows have a manufacturer of '1' (@10% ~= 8)
      counts(1) must beCloseTo(8, 4)

    }

    "create named samples" >> {

      update("CREATE SAMPLE VIEW sample FROM data WITH FRACTION 0.5;")
      update("CREATE SAMPLE VIEW stratified FROM data WITH STRATIFIED ON manufacturer (0 ~ 1.0, 1 ~ 0.1);")
      update("CREATE OR REPLACE SAMPLE VIEW sample FROM data WITH FRACTION 0.1;")
      
      val count = 
        db.query(db.table("SAMPLE").count()) { result =>
          val row = result.next()
          row(ID("COUNT")).asLong.toInt
        }
      // 345 rows in the input file.  10% sampling rate ~= 34-35 rows.
      count should beCloseTo(34, 10)
    }
  }
}