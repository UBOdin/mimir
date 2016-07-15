package mimir.ctables;

import java.io.{StringReader,FileReader}

import mimir.parser.{MimirJSqlParser}
import org.specs2.mutable._

import mimir._
import mimir.ctables._
import mimir.parser._
import mimir.algebra._
import mimir.sql._
import mimir.optimizer._
import mimir.exec._
import mimir.provenance._

object CTPercolatorSpec extends Specification {
  
  val baseModel = JointSingleVarModel(List(
    UniformDistribution,
    UniformDistribution,
    UniformDistribution,
    UniformDistribution,
    UniformDistribution
  ))
  val schema = Map[String,Map[String,Type.T]](
    ("R", Map( 
      ("R_A", Type.TInt), 
      ("R_B", Type.TInt), 
      ("R_C", Type.TInt)
    )),
    ("S", Map( 
      ("S_C", Type.TInt), 
      ("S_D", Type.TFloat)
    ))
  )

  def parser = new OperatorParser(
    (x: String) => baseModel,
    schema(_).toList
  )
  def expr = parser.expr _
  def oper:(String => Operator) = parser.operator _
  def project(cols: List[(String,String)], src: Operator): Operator =
    Project(cols.map( { case (name,e) => ProjectArg(name, expr(e))}), src) 
  def analyze(s: String) = new Compiler(null).compileAnalysis(oper(s))

  def percolite(x:String) = 
    CTPercolator.percolateLite(oper(x))

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
        oper("PROJECT[A <= A, B <= {{X_1[ROWID]}}](R(A, B))"),
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
                B <= IF B IS NULL THEN {{X_1[ROWID]}} ELSE B END
               ](R(A, B))""") must be equalTo ((
        oper("""
          PROJECT[A <= A, 
                  B <= IF B IS NULL THEN {{X_1[ROWID]}} ELSE B END, 
                  MIMIR_COL_DET_B <= 
                       IF B IS NULL THEN FALSE ELSE TRUE END
                ](R(A, B))"""),
        Map( 
          ("A", expr("true")),
          ("B", expr("MIMIR_COL_DET_B"))
        ),
        expr("true")
      ))
    }
    "Handle Data-Independent Non-Deterministic Inline Selection" in {
      percolite("SELECT[{{X_1[ROWID]}} = 3](R(A, B))") must be equalTo ((
        oper("SELECT[{{X_1[ROWID]}} = 3](R(A, B))"),
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
                  B <= IF B IS NULL THEN {{X_1[ROWID]}} ELSE B END
                 ](R(A, B)))""") must be equalTo ((
        oper("""
        PROJECT[A <= A, B <= B, MIMIR_COL_DET_B <= MIMIR_COL_DET_B, MIMIR_ROW_DET <= MIMIR_COL_DET_B](
          SELECT[B = 3](
            PROJECT[A <= A, 
                    B <= IF B IS NULL THEN {{X_1[ROWID]}} ELSE B END, 
                    MIMIR_COL_DET_B <= 
                         IF B IS NULL THEN FALSE ELSE TRUE END
                  ](R(A, B))))"""),
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
    "Handle Non-Deterministic Joins" in {
      percolite("""
        JOIN(
          PROJECT[A <= {{X_1[ROWID,A]}}](R(A,B)), 
          S(C,D)
        )
      """) must be equalTo ((
        oper("""
          JOIN(
            PROJECT[A <= {{X_1[ROWID,A]}}](R(A,B)), 
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
    "Handle Non-Deterministic Joins With Row Non-Determinism" in {
      percolite("""
        JOIN(
          SELECT[B < IF A < 3 THEN {{X_1[A]}} ELSE 3 END](R(A,B)), 
          SELECT[C < IF D > 5 THEN {{X_2[D]}} ELSE 5 END](S(C,D))
        )
      """) must be equalTo ((
        oper("""
          JOIN(
            PROJECT[A <= A, B <= B, MIMIR_ROW_DET_LEFT <= MIMIR_ROW_DET](
              PROJECT[A <= A, B <= B, MIMIR_ROW_DET <= IF A < 3 THEN FALSE ELSE TRUE END](
                SELECT[B < IF A < 3 THEN {{X_1[A]}} ELSE 3 END](R(A,B)))), 
            PROJECT[C <= C, D <= D, MIMIR_ROW_DET_RIGHT <= MIMIR_ROW_DET](
              PROJECT[C <= C, D <= D, MIMIR_ROW_DET <= IF D > 5 THEN FALSE ELSE TRUE END](
                SELECT[C < IF D > 5 THEN {{X_2[D]}} ELSE 5 END](S(C,D))))
          )
        """),
        Map( 
          ("A", expr("true")),
          ("B", expr("true")),
          ("C", expr("true")),
          ("D", expr("true"))
        ),
        expr("MIMIR_ROW_DET_LEFT AND MIMIR_ROW_DET_RIGHT")
      ))
    }
    "Percolate projections over non-deterministic rows" >> {
      percolite("""
        PROJECT[A <= A, B <= B](
          SELECT[IF A < 5 THEN {{X_1[A]}} ELSE A END > 5](R(A,B))
        )
      """) must be equalTo ((
        oper("""
          PROJECT[A <= A, B <= B, MIMIR_ROW_DET <= MIMIR_ROW_DET](
            PROJECT[A <= A, B <= B, MIMIR_ROW_DET <= IF A < 5 THEN FALSE ELSE TRUE END](
              SELECT[IF A < 5 THEN {{X_1[A]}} ELSE A END > 5](R(A,B))
            )
          )
        """),
        Map(
          ("A", expr("true")),
          ("B", expr("true"))
        ),
        expr("MIMIR_ROW_DET")
      ))
    }

  }
}