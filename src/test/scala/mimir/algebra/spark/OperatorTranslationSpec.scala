package mimir.algebra.spark

import org.specs2.specification.BeforeAll
import mimir.test.SQLTestSpecification
import mimir.algebra.NullPrimitive
import mimir.algebra.RowIdVar
import mimir.algebra.RowIdPrimitive
import mimir.algebra.Var
import mimir.algebra.StringPrimitive
import mimir.algebra.TInt

object OperatorTranslationSpec 
  extends SQLTestSpecification("SparkOperatorTranslationSpec",Map("cleanup" -> "NO"))
  with BeforeAll
{

  def beforeAll = 
  {
    db.loadTable("test/r_test/r.csv")
  }
  
  "Spark" should {
    sequential
    "Be able to query imported CSV" >> {
      val result = query("""
        SELECT * FROM R
      """)(_.toList.map(_.tuple))
      println(result)
      
      result must contain (
       List(i(1), i(2), i(3)), 
       List(i(1), i(3), i(1)), 
       List(i(2), NullPrimitive(), i(1)), 
       List(i(1), i(2), NullPrimitive()), 
       List(i(1), i(4), i(2)), 
       List(i(2), i(2), i(1)), 
       List(i(4), i(2), i(4))   
      )
    }
    
    "Do ROWIDs" >> {
      val result = db.query(db.table("R").addColumn(("ROWID", RowIdVar())).project("ROWID"))(_.toList.map(_.tuple))
      println(result)
      result must contain (
       List(RowIdPrimitive("0")), 
       List(RowIdPrimitive("1")), 
       List(RowIdPrimitive("2")), 
       List(RowIdPrimitive("3")), 
       List(RowIdPrimitive("4")), 
       List(RowIdPrimitive("5")), 
       List(RowIdPrimitive("6"))   
      )
    }
    
    "Be able create and query missing value lens" >> {
      update("""
				CREATE LENS MV_R
				  AS SELECT * FROM R
				  WITH MISSING_VALUE('COLUMN_2')
 			""")
      val result = query("""
        SELECT * FROM MV_R
      """)(_.toList.map(_.tuple))
      
      result must contain (
       List(i(1), i(3), i(1)), 
       List(i(2), i(2), i(1)), 
       List(i(1), i(2), i(1)), 
       List(i(1), i(4), i(2)), 
       List(i(2), i(2), i(1)), 
       List(i(4), i(2), i(4))   
      )
    }
  }
}