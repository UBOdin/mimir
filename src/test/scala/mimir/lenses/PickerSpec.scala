package mimir.lenses

import java.io._
import mimir._
import mimir.algebra._
import mimir.test._

import org.specs2.specification._

object PickerSpec 
  extends SQLTestSpecification("PickerTest")
  with BeforeAll
{

  def beforeAll = {
    update("CREATE TABLE R(A integer, B integer)")
    update("INSERT INTO R (A, B) VALUES(1, 1)")
    update("INSERT INTO R (A, B) VALUES(1, 2)")
    update("INSERT INTO R (A, B) VALUES(NULL, 3)")
    update("INSERT INTO R (A, B) VALUES(4, 4)")
  }
  
  "The Picker Lens" should {

    "Be able to create and query picker lenses" >> {
 
     update("""
        CREATE LENS PICKER_1 
          AS SELECT * FROM R
        WITH PICKER(PICK_FROM(A,B),PICK_AS(DINOSAURS_WERNT_JUST_GIANT_LIZARDS))
      """);

      val result = query("""
        SELECT DINOSAURS_WERNT_JUST_GIANT_LIZARDS FROM PICKER_1
      """)(results => results.toList.map( row =>  { 
        (
          row("DINOSAURS_WERNT_JUST_GIANT_LIZARDS"), 
          row.isColDeterministic("DINOSAURS_WERNT_JUST_GIANT_LIZARDS"),
          row.isDeterministic(),
          row.provenance.asString
        )
      }))
      
      
      result(0)._1 must be equalTo IntPrimitive(1)
      result(0)._2 must be equalTo true
      result(1)._1 must be equalTo IntPrimitive(1)
      result(1)._2 must be equalTo false
      result(2)._1 must be equalTo IntPrimitive(3)
      result(2)._2 must be equalTo false
      result(3)._1 must be equalTo IntPrimitive(4)
      result(3)._2 must be equalTo true
      
    }

  }


}
