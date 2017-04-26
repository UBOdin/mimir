package mimir.demo;

import org.specs2.mutable._
import mimir.test._
import mimir.util._

object PlantScenario
  extends SQLTestSpecification("PlantDemoScript")
{

  "The Plant Scenario" should {

    "Be able to load Barnes" >> {
      db.loadTable("test/plants/Barnes.csv"); ok
    }
    "Be able to load GBIF (which has garbled data)" >> {
      // disable data import warnings
      LoggerUtils.error("mimir.util.NonStrictCSVParser"){
        db.loadTable("test/plants/GBIF.csv"); 
      }

      // Lines 469 and 1212 of the file GBIF.csv have some garbling.
      // For example, line 469, where
      //    E_E_DATASET = 'A_EDDMapS' AND OBJECT_ID = 86101
      // has one field (including quotations):
      //    "Alligator Creek Channel "H" Detention""lligator Cre"
      // 
      // The following two tests ensure that the preceeding
      // and following rows are unaffected by the data bug.
      querySingleton("""
        SELECT LONG2
        FROM GBIF 
        WHERE E_E_DATASET = 'A_EDDMapS' 
        AND OBJECT_ID = 1005634
      """) must be equalTo f(-82.77)
      querySingleton("""
        SELECT LONG2
        FROM GBIF 
        WHERE E_E_DATASET = 'A_EDDMapS' 
        AND OBJECT_ID = 1005553
      """) must be equalTo f(-82.74)
    }
    "Be able to load EDD" >> {
      db.loadTable("test/plants/EDD.csv"); ok
    }

  } 

}