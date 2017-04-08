package mimir.views;

import org.specs2.specification._
import mimir.algebra._
import mimir.util._
import mimir.test._
import mimir.ctables._
import mimir.provenance._

object ViewsSpec 
  extends SQLTestSpecification("ViewsTest")
  with BeforeAll
{

  def beforeAll = {
    update("CREATE TABLE R(A int, B int, C int)")
    update("INSERT INTO R(A,B,C) VALUES (1,2,3)")
    update("INSERT INTO R(A,B,C) VALUES (1,3,1)")
    update("INSERT INTO R(A,B,C) VALUES (1,4,2)")
    update("INSERT INTO R(A,B,C) VALUES (2,2,1)")
    update("INSERT INTO R(A,B,C) VALUES (4,2,4)")
  }

  sequential

  "The View Manager" should {
    "Not interfere with table creation and inserts" >> {

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
      db.views.create("RAB", select("SELECT A, B FROM R"))
      val result = query("SELECT A FROM RAB").allRows.flatten 

      result must contain(eachOf(i(1), i(1), i(1), i(2), i(4)))
    }

    "Support Joins" >> {
      db.views.create("RS", select("SELECT A, B, R.C, D FROM R, S WHERE R.C = S.C"))

      val result = query("SELECT B FROM RS").allRows.flatten
      result must contain(eachOf(i(3),i(3),i(3),i(3),i(2),i(2)))
      
    }

    "Process CREATE VIEW statements" >> {
      update("CREATE VIEW TEST1 AS SELECT A, B FROM R")
      val result = query("SELECT A FROM TEST1").allRows.flatten 

      result must contain(eachOf(i(1), i(1), i(1), i(2), i(4)))
    }

  }

  "Materialized Views" should {

    "Be creatable" >> {

      update("CREATE VIEW MATTEST AS SELECT A, B FROM R WHERE C = 1")

      db.views("MATTEST").operator must be equalTo
        View("MATTEST",
          oper("PROJECT[A,B](SELECT[C=1](R))")
        )

      update("ALTER VIEW MATTEST MATERIALIZE")
      val results = 
        db.backend.resultRows(s"""
          SELECT A, B, 
            ${CTPercolator.mimirColDeterministicColumnPrefix}A, 
            ${CTPercolator.mimirColDeterministicColumnPrefix}B,
            ${CTPercolator.mimirRowDeterministicColumnName},
            MIMIR_ROWID_0
           FROM MATTEST
        """).map { row => 
          ( row(0).asLong, row(1).asLong, row(2).asLong, row(3).asLong, row(4).asLong, row(5).asLong ) 
        }

      results must contain(eachOf(
        (1l, 3l, 1l, 1l, 1l, db.backend.resultValue("SELECT ROWID FROM R WHERE A = 1 AND B = 3 AND C = 1").asLong),
        (2l, 2l, 1l, 1l, 1l, db.backend.resultValue("SELECT ROWID FROM R WHERE A = 2 AND B = 2 AND C = 1").asLong)
      ))

    }

    "Support raw queries" >> {

      db.views.resolve(db.views("MATTEST").operator) must be equalTo(
        Project(Seq(ProjectArg("A", Var("A")),ProjectArg("B", Var("B"))),
          Table("MATTEST", db.views("MATTEST").fullSchema, Seq())
        )
      )

    }

    "Support provenance-compiled queries" >> {

      val (query, rowidCols) = Provenance.compile(db.views("MATTEST").operator)

      db.views.resolve(query) must be equalTo(
        Project(Seq(
            ProjectArg("A", Var("A")),
            ProjectArg("B", Var("B"))
          ) ++ rowidCols.map { col => ProjectArg(col, Var(col)) },
          Table("MATTEST", db.views("MATTEST").fullSchema, Seq())
        )
      )

    }

    "Support provenance and bestguess-compiled queries" >> {

      val (query, rowidCols) = Provenance.compile(db.views("MATTEST").operator)

      db.views.resolve(CTPercolator.percolateLite(query)._1) must be equalTo(
        Project(Seq(
            ProjectArg("A", Var("A")),
            ProjectArg("B", Var("B")),
            ProjectArg(CTPercolator.mimirColDeterministicColumnPrefix+"A", Var(CTPercolator.mimirColDeterministicColumnPrefix+"A")),
            ProjectArg(CTPercolator.mimirColDeterministicColumnPrefix+"B", Var(CTPercolator.mimirColDeterministicColumnPrefix+"B")),
            ProjectArg(CTPercolator.mimirRowDeterministicColumnName, Var(CTPercolator.mimirRowDeterministicColumnName))
          ) ++ rowidCols.map { col => ProjectArg(col, Var(col)) },
          Table("MATTEST", db.views("MATTEST").fullSchema, Seq())
        )
      )
      
    }
  }

}