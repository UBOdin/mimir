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
    loadCSV(targetTable = "R", sourceFile = "test/data/pick.csv")
  }
  
  "The Picker Lens" should {

    "Be able to create and query picker lenses" >> {
 
     update("""
        CREATE LENS PICKER_1 
          AS SELECT A, B FROM R
        WITH PICKER(PICK_FROM(A,B),PICK_AS(DINOSAURS_WERNT_JUST_GIANT_LIZARDS))
      """);
 
     val result = query("""
        SELECT DINOSAURS_WERNT_JUST_GIANT_LIZARDS FROM PICKER_1
      """)(results => results.toList.map( row =>  { 
        (
          row(ID("DINOSAURS_WERNT_JUST_GIANT_LIZARDS")), 
          row.isColDeterministic(ID("DINOSAURS_WERNT_JUST_GIANT_LIZARDS")),
          row.isDeterministic(),
          row.provenance.asString
        )
      }))
      println(result)
      
      result(0)._1 must be equalTo FloatPrimitive(4.0)
      result(0)._2 must be equalTo false
      result(1)._1 must be equalTo FloatPrimitive(4.0)
      result(1)._2 must be equalTo false
      result(2)._1 must be equalTo FloatPrimitive(4.0)
      result(2)._2 must be equalTo false
      result(3)._1 must be equalTo FloatPrimitive(6.4)
      result(3)._2 must be equalTo false
      
    }

  }


}
