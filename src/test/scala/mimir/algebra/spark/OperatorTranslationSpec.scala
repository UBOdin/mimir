package mimir.algebra.spark

import org.specs2.specification.BeforeAll
import mimir.test.SQLTestSpecification

object OperatorTranslationSpec 
  extends SQLTestSpecification("SparkOperatorTranslationSpec",Map("reset" -> "NO", "cleanup" -> "NO"))
  with BeforeAll
{

  def beforeAll = 
  {
    db.loadTable("test/r_test/r.csv")
  }
  
  "Spark" should {
    "Be able to query imported CSV" >> {
      val result = query("""
        SELECT * FROM R
      """)(_.toList.map(_.tuple))
      
      println(result)
      1 must be equalTo 1
    }
  }
}