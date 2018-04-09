package mimir.algebra.spark

import org.specs2.specification.BeforeAll
import mimir.test.SQLTestSpecification
import mimir.algebra.NullPrimitive
import mimir.algebra.RowIdVar
import mimir.algebra.RowIdPrimitive
import mimir.algebra.Var
import mimir.algebra.StringPrimitive
import mimir.algebra.TInt
import java.io.File
import mimir.algebra.Function
import mimir.algebra.AggFunction

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
    /*"Be able to query imported CSV" >> {
      val result = query("""
        SELECT * FROM R
      """)(_.toList.map(_.tuple))
      
      result must be equalTo List(
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
      
      result must be equalTo List(
       List(RowIdPrimitive("1")), 
       List(RowIdPrimitive("2")), 
       List(RowIdPrimitive("3")), 
       List(RowIdPrimitive("4")), 
       List(RowIdPrimitive("5")), 
       List(RowIdPrimitive("6")),
       List(RowIdPrimitive("7"))
      )
    }
    
    "Be able create and query missing value lens" >> {
      update("""
				CREATE LENS MV_R
				  AS SELECT * FROM R
				  WITH MISSING_VALUE('COLUMN_2')
 			""")
 			val query = db.table("MV_R")
      val result = db.query(query)(_.toList.map(_.tuple))
      
      //mimir.MimirVizier.db = db
      //println( mimir.MimirVizier.explainEverything(query).map(_.all(db).map(_.reason)) )
      
      result must be equalTo List(
       List(i(1), i(2), i(3)), 
       List(i(1), i(3), i(1)), 
       List(i(2), NullPrimitive(), i(1)), 
       List(i(1), i(2), i(1)), 
       List(i(1), i(4), i(2)), 
       List(i(2), i(2), i(1)), 
       List(i(4), i(2), i(4))   
      )
    }
    
    "Be able to do Aggregates" >> {
      loadCSV("U",Seq(("A","int"),("B","int"),("C","int")), new File("test/r_test/r.csv"))
      val aggQuery = db.table("U").aggregate(AggFunction("JSON_GROUP_ARRAY", false, Seq(Var("A")), "F"))
      val result = db.query(aggQuery){ _.map { row => 
          row.tuple(0)
        
      }}.toList
      result must be equalTo List(str("[1,1,2,1,1,2,4]"))
    }*/
    
    "Be Able to do RepairKey" >> {
      loadCSV("S",Seq(("A","int"),("B","int"),("C","int")), new File("test/r_test/r.csv"))
      update("""
        CREATE LENS S_UNIQUE_A 
          AS SELECT * FROM S
        WITH KEY_REPAIR(A)
      """);

      val result = query("""
        SELECT A, B, C FROM S_UNIQUE_A
      """){ _.map { row => 
        row("A").asInt -> (
          row("B").asInt, 
          row("C").asInt, 
          row.isColDeterministic("A"),
          row.isColDeterministic("B"),
          row.isColDeterministic("C"),
          row.isDeterministic()
        )
      }.toMap[Int, (Int,Int, Boolean, Boolean, Boolean, Boolean)] }

      result.keys must contain(eachOf(1, 2, 4))
      result must have size(3)

      // The input is deterministic, so the key column "A" should also be deterministic
      result(1)._3 must be equalTo true
      // The input is deterministic, so the row itself is deterministic
      result(1)._6 must be equalTo true
      // There are a number of possibilities for <A:1> on both columns B and C
      result(1)._4 must be equalTo false
      result(1)._5 must be equalTo false

      // The chosen values for <A:1> in columns B and C are arbitrary, but selected
      // from a finite set of possibilities based on what's in R.
      result(1)._1 must be oneOf(2, 3, 4)
      result(1)._2 must be oneOf(1, 2, 3)

      // There is only one populated value for <A:2>[B], the other is null
      result(2)._1 must be equalTo 2
      result(2)._4 must be equalTo true

      // There are two populated values for <A:2>[C], but they're both identical
      result(2)._2 must be equalTo 1
      result(2)._5 must be equalTo true
    }
  }
}