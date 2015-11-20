package mimir.ctables;

import java.io.{StringReader,FileReader}

import mimir.parser.{MimirJSqlParser}
import org.specs2.mutable._

import mimir._
import mimir.parser._
import mimir.algebra._
import mimir.sql._

object CTPercolateLite extends Specification {
  
  val boundsSpecModel = JointSingleVarModel(List(
    UniformDistribution,
    UniformDistribution,
    UniformDistribution,
    UniformDistribution,
    UniformDistribution
  ))
  val schema = Map[String,List[(String,Type.T)]](
    ("R", List( 
      ("A", Type.TInt), 
      ("B", Type.TInt)
    )),
    ("S", List( 
      ("C", Type.TInt), 
      ("D", Type.TInt)
    ))
  )

  def db = Database("testdb", null);
  def parser = new OperatorParser((x: String) => boundsSpecModel, schema(_))
  def expr = parser.expr _
  def oper = parser.operator _

  def percolite(x:String) = 
    CTPercolator.percolateLite(
      CTPercolator.propagateRowIDs(oper(x)))

  "The Percolator (Lite)" should {

    "Handle Base Relations" in {
      percolite("R(A, B)") must be equalTo ((
        oper("R(A, B)"),
        Map( 
          ("A", expr("true")),
          ("B", expr("true"))
        ),
        expr("true")
      ))
      percolite("R(A, B // ROWID:String)") must be equalTo ((
        oper("R(A, B // ROWID:String)"),
        Map( 
          ("A", expr("true")),
          ("B", expr("true"))
        ),
        expr("true")
      ))
    }

    "Handle Deterministic Projection" in {
      percolite("PROJECT[A <= A](R(A, B))") must be equalTo ((
        oper("PROJECT[A <= A](R(A, B))"),
        Map( 
          ("A", expr("true"))
        ),
        expr("true")
      ))
    }

    "Handle Data-Independent Non-Deterministic Projection" in {
      percolite("PROJECT[A <= A, B <= {{X_1[ROWID]}}](R(A, B))") must be equalTo ((
        oper("PROJECT[A <= A, B <= {{X_1[ROWID]}}](R(A, B // ROWID:rowid))"),
        Map( 
          ("A", expr("true")),
          ("B", expr("false"))
        ),
        expr("true")
      ))
    }
    "Handle Data-Dependent Non-Deterministic Projection" in {
      percolite("""
        PROJECT[A <= A, 
                B <= CASE WHEN B IS NULL THEN {{X_1[ROWID]}} ELSE B END
               ](R(A, B))""") must be equalTo ((
        oper("""
          PROJECT[A <= A, 
                  B <= CASE WHEN B IS NULL THEN {{X_1[ROWID]}} ELSE B END, 
                  MIMIR_COL_DET_B <= 
                       CASE WHEN B IS NULL THEN FALSE ELSE TRUE END
                ](R(A, B // ROWID:rowid))"""),
        Map( 
          ("A", expr("true")),
          ("B", expr("MIMIR_COL_DET_B"))
        ),
        expr("true")
      ))
    }
    "Handle Data-Independent Non-Deterministic Inline Selection" in {
      percolite("SELECT[{{X_1[ROWID]}} = 3](R(A, B))") must be equalTo ((
        oper("SELECT[{{X_1[ROWID]}} = 3](R(A, B // ROWID:rowid))"),
        Map( 
          ("A", expr("true")),
          ("B", expr("true"))
        ),
        expr("false")
      ))
    }
    "Handle Data-Dependent Non-Deterministic Projection" in {
      percolite("""
        SELECT[B = 3](
          PROJECT[A <= A, 
                  B <= CASE WHEN B IS NULL THEN {{X_1[ROWID]}} ELSE B END
                 ](R(A, B)))""") must be equalTo ((
        oper("""
        PROJECT[A <= A, B <= B, MIMIR_COL_DET_B <= MIMIR_COL_DET_B, MIMIR_ROW_DET <= MIMIR_COL_DET_B](
          SELECT[B = 3](
            PROJECT[A <= A, 
                    B <= CASE WHEN B IS NULL THEN {{X_1[ROWID]}} ELSE B END, 
                    MIMIR_COL_DET_B <= 
                         CASE WHEN B IS NULL THEN FALSE ELSE TRUE END
                  ](R(A, B // ROWID:rowid))))"""),
        Map( 
          ("A", expr("true")),
          ("B", expr("MIMIR_COL_DET_B"))
        ),
        expr("MIMIR_ROW_DET")
      ))
    }
    "Handle Deterministic Join" in {
      percolite("""
        JOIN(R(A,B), S(C,D))
      """) must be equalTo ((
        oper("""
          JOIN(R(A,B), S(C,D))          
        """),
        Map( 
          ("A", expr("true")),
          ("B", expr("true")),
          ("C", expr("true")),
          ("D", expr("true"))
        ),
        expr("true")
      ))
    }
    "Handle Non-Deterministic Join" in {
      percolite("""
        JOIN(
          PROJECT[A <= {{X_1[ROWID,A]}}](R(A,B)), 
          S(C,D)
        )
      """) must be equalTo ((
        oper("""
          JOIN(
            PROJECT[A <= {{X_1[ROWID,A]}}](R(A,B//ROWID:rowid)), 
            S(C,D)
          )
        """),
        Map( 
          ("A", expr("false")),
          ("C", expr("true")),
          ("D", expr("true"))
        ),
        expr("true")
      ))
    }

  }
}