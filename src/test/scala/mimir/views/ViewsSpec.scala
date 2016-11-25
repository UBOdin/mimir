package mimir.views;

import mimir.algebra._
import mimir.util._

object ViewsSpec extends SQLTestSpecification("ViewsTest")
{

  sequential

  "The View Manager" should {
    "Not interfere with table creation and inserts" >> {
      update("CREATE TABLE R(A int, B int, C int)")
      update("INSERT INTO R(A,B,C) VALUES (1,2,3)")
      update("INSERT INTO R(A,B,C) VALUES (1,3,1)")
      update("INSERT INTO R(A,B,C) VALUES (1,4,2)")
      update("INSERT INTO R(A,B,C) VALUES (2,2,1)")
      update("INSERT INTO R(A,B,C) VALUES (4,2,4)")

      update("CREATE TABLE S(C int, D int)")
      update("INSERT INTO S(C,D) VALUES (1,2)")
      update("INSERT INTO S(C,D) VALUES (1,3)")
      update("INSERT INTO S(C,D) VALUES (1,2)")
      update("INSERT INTO S(C,D) VALUES (1,4)")
      update("INSERT INTO S(C,D) VALUES (2,2)")
      update("INSERT INTO S(C,D) VALUES (4,2)")
      true
    }

    "Support Simple SELECTs" >> {
      db.views.createView("RAB", select("SELECT A, B FROM R"))
      val result = query("SELECT A FROM RAB").allRows.flatten 

      result must contain(eachOf(i(1), i(1), i(1), i(2), i(4)))
    }

    "Support Joins" >> {
      db.views.createView("RS", select("SELECT A, B, R.C, D FROM R, S WHERE R.C = S.C"))

      val result = query("SELECT B FROM RS").allRows.flatten
      result must contain(eachOf(i(3),i(3),i(3),i(3),i(2),i(2)))
      
    }

    "Process CREATE VIEW statements" >> {
      update("CREATE VIEW TEST1 AS SELECT A, B FROM R")
      val result = query("SELECT A FROM TEST1").allRows.flatten 

      result must contain(eachOf(i(1), i(1), i(1), i(2), i(4)))
    }

  }

}