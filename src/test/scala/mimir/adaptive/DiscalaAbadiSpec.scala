package mimir.adaptive

import java.io._

import org.specs2.specification._
import org.specs2.mutable._

import mimir.algebra._
import mimir.test._

object DiscalaAbadiSpec
  extends SQLTestSpecification("DiscalaAbadi")
  with BeforeAll
{
  def beforeAll =
  {
    loadCSV("SHIPPING", new File("test/data/cureSource.csv"))
  }

  sequential

  "The Discala-Abadi Normalizer" should {

    "Support Creation" >> {
      update("""
        CREATE ADAPTIVE SCHEMA SHIPPING
          AS SELECT * FROM SHIPPING
          WITH DISCALA_ABADI()
      """)

      querySingleton("""
        SELECT COUNT(*) FROM MIMIR_DA_FDG_SHIPPING
      """).asLong must be greaterThan(20l)
      querySingleton("""
        SELECT NAME FROM MIMIR_ADAPTIVE_SCHEMAS
      """) must be equalTo(StringPrimitive("SHIPPING"))
    }


  }


}