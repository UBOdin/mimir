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
import mimir.algebra.ID
import mimir.algebra.BoolPrimitive
import mimir.test.TestTimer

object OperatorTranslationSpec 
  extends SQLTestSpecification("SparkOperatorTranslationSpec",Map("cleanup" -> "YES"))
  with BeforeAll
  with TestTimer
{

  def beforeAll = 
  {
    loadCSV(
      sourceFile = "test/r_test/r.csv",
      detectHeaders = false,
      targetSchema = Seq("COLUMN_0", "COLUMN_1", "COLUMN_2")
    )
  }
  
  "Spark" should {
    sequential
    "Do ROWIDs" >> {
      val result = db.query(
        db.table("R")
          .addColumns( "ROWID" -> RowIdVar() )
          .project("ROWID")
        )(_.toList.map(_.tuple))
      
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
      loadCSV(
        targetTable = "U", 
        sourceFile = "test/r_test/r.csv",
        targetSchema = Seq("A", "B", "C")
      )
      val aggQuery = 
        db.table("U")
          .aggregate(AggFunction(
            ID("json_group_array"), 
            false, 
            Seq(Var(ID("A"))), ID("F"))
          )
      val result = db.query(aggQuery){ _.map { row => 
          row.tuple(0)
        
      }}.toList
      result must be equalTo List(str("[1,1,2,1,1,2,4]"))
    }
    
    "Be Able to do RepairKey" >> {
      loadCSV(
        targetTable = "S", 
        sourceFile = "test/r_test/r.csv",
        targetSchema = Seq("A", "B", "C")
      )
      update("""
        CREATE LENS S_UNIQUE_A 
          AS SELECT * FROM S
        WITH KEY_REPAIR(A)
      """);

      val result = query("""
        SELECT A, B, C FROM S_UNIQUE_A
      """){ _.map { row => 
        row(ID("A")).asInt -> (
          row(ID("B")).asInt, 
          row(ID("C")).asInt, 
          row.isColDeterministic(ID("A")),
          row.isColDeterministic(ID("B")),
          row.isColDeterministic(ID("C")),
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
    
   
    
    /*"Be able create and query missing value lens" >> {
      loadCSV("MOCKDATA", new File("test/data/mockData.csv"))
      val timeForCreate = time { 
        update("""
  				CREATE LENS MV_MOCKDATA
  				  AS SELECT * FROM MOCKDATA
  				  WITH MISSING_VALUE('AGE')
   			""")
      }
   		val timeForQuery = time {
   		  val query = db.table("MV_MOCKDATA")
        val result = db.query(query)(_.length)
        
        //mimir.MimirVizier.db = db
        //println( mimir.MimirVizier.explainEverything(query).map(_.all(db).map(_.reason)) )
        
        result must be equalTo 41000
      }
      println(s"Create Time:${timeForCreate._2} seconds, Query Time: ${timeForQuery._2} seconds <- MissingValueLens")
      timeForQuery._1   
    }*/
    
    
  }
}