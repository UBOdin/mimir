package mimir.views;

import org.specs2.specification._
import mimir.exec._
import mimir.algebra._
import mimir.util._
import mimir.test._
import mimir.ctables._
import mimir.provenance._
import java.io.File

object ViewsSpec 
  extends SQLTestSpecification("ViewsTest")
  with BeforeAll
{

  def beforeAll = {
    loadCSV("R",Seq(("A", "int"), ("B", "int"), ("C", "int")), new File("test/data/views_r.csv"))
  }

  sequential

  "The View Manager" should {
    "Not interfere with CSV Imports" >> {
      loadCSV("S",Seq(("C", "int"), ("D", "int")), new File("test/data/views_s.csv"))
      true
    }

    "Support Simple SELECTs" >> {
      db.views.create("RAB", select("SELECT A, B FROM R"))
      queryOneColumn("SELECT A FROM RAB"){ 
        _.toSeq must contain(i(1), i(1), i(1), i(2), i(4))
      }
    }

    "Support Joins" >> {
      db.views.create("RS", select("SELECT A, B, R.C, D FROM R, S WHERE R.C = S.C"))

      queryOneColumn("SELECT B FROM RS"){ 
        _.toSeq must contain(i(3),i(3),i(3),i(3),i(2),i(2))
      }
      
    }

    "Process CREATE VIEW statements" >> {
      update("CREATE VIEW TEST1 AS SELECT A, B FROM R")
      queryOneColumn("SELECT A FROM TEST1"){ 
        _.toSeq must contain(i(1), i(1), i(1), i(2), i(4))
      }
    }

  }

  "Materialized Views" should {

    "Be creatable" >> {

      update("CREATE VIEW MATTEST AS SELECT A, B FROM R WHERE C = 1")

      db.views("MATTEST").operator must be equalTo
        View("MATTEST",
          db.
            table("R").
            filter(expr("C = 1")).
            project("A", "B")
        )

      update("ALTER VIEW MATTEST MATERIALIZE")
      val results = 
        db.query(s"""
          SELECT A, B, 
            ${ViewAnnotation.taintBitVectorColumn}, 
            MIMIR_ROWID_0
           FROM MATTEST
        """)(result => result.toList.map(_.tuple)).map { row => 
          ( row(0).asLong, row(1).asLong, row(2).asLong, row(3).asLong ) 
        }

      results must contain(eachOf(
        (1l, 3l, 7l, db.query("SELECT ROWID FROM R WHERE A = 1 AND B = 3 AND C = 1")(_.toList.head(0).asLong)),
        (2l, 2l, 7l, db.query("SELECT ROWID FROM R WHERE A = 2 AND B = 2 AND C = 1")(_.toList.head(0).asLong))
      ))

    }

    "Support raw queries" >> {

      db.compiler.optimize(
        db.views.resolve(db.views("MATTEST").operator)
      ) must be equalTo(
        Project(Seq(ProjectArg("A", Var("A")),ProjectArg("B", Var("B"))),
          Table("MATTEST","MATTEST", db.views("MATTEST").materializedSchema, Seq())
        )
      )

    }

    "Support provenance-compiled queries" >> {

      val (query, rowidCols) = Provenance.compile(db.views("MATTEST").operator)

      db.compiler.optimize(
        db.views.resolve(query)
      ) must be equalTo(
        Project(Seq(
            ProjectArg("A", Var("A")),
            ProjectArg("B", Var("B"))
          ) ++ rowidCols.map { col => ProjectArg(col, Var(col)) },
          Table("MATTEST","MATTEST", db.views("MATTEST").materializedSchema, Seq())
        )
      )

    }

    "Support provenance and bestguess-compiled queries" >> {

      val (query, rowidCols) = Provenance.compile(db.views("MATTEST").operator)

      db.compiler.optimize(
        db.views.resolve(CTPercolator.percolateLite(query, db.models.get(_))._1) 
      ) must be equalTo(
        Project(Seq(
            ProjectArg("A", Var("A")),
            ProjectArg("B", Var("B")),
            ProjectArg(CTPercolator.mimirColDeterministicColumnPrefix+"A", Comparison(Cmp.Eq, Arithmetic(Arith.BitAnd, Var(ViewAnnotation.taintBitVectorColumn), IntPrimitive(2)), IntPrimitive(2))),
            ProjectArg(CTPercolator.mimirColDeterministicColumnPrefix+"B", Comparison(Cmp.Eq, Arithmetic(Arith.BitAnd, Var(ViewAnnotation.taintBitVectorColumn), IntPrimitive(4)), IntPrimitive(4))),
            ProjectArg(CTPercolator.mimirColDeterministicColumnPrefix+"MIMIR_ROWID_0", Comparison(Cmp.Eq, Arithmetic(Arith.BitAnd, Var(ViewAnnotation.taintBitVectorColumn), IntPrimitive(8)), IntPrimitive(8))),
            ProjectArg(CTPercolator.mimirRowDeterministicColumnName, Comparison(Cmp.Eq, Arithmetic(Arith.BitAnd, Var(ViewAnnotation.taintBitVectorColumn), IntPrimitive(1)), IntPrimitive(1)))
          ) ++ rowidCols.map { col => ProjectArg(col, Var(col)) },
          Table("MATTEST","MATTEST", db.views("MATTEST").materializedSchema, Seq())
        )
      )
      
    }
  }

}
