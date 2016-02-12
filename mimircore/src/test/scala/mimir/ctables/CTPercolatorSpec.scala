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
  def percolate(s: String) = CTPercolator.percolate(oper(s))
  def analyze(s: String) = new Compiler(null).compileAnalysis(oper(s))

  def percolite(x:String) = 
    CTPercolator.percolateLite(
      CTPercolator.propagateRowIDs(oper(x)))

  "The Percolator" should { 
    
    "work on deterministic P queries" in {
      percolate(
        "PROJECT[A <= R_A, B <= R_A*R_B](R)"
      ) must be equalTo
        oper("PROJECT[A <= R_A, B <= R_A*R_B](R)")
    }
    "work on deterministic PS queries" in {
      percolate(
        "PROJECT[A <= R_A, B <= R_A*R_B](SELECT[R_A = R_B](R))"
      ) must be equalTo 
        oper("PROJECT[A <= R_A, B <= R_A*R_B](SELECT[R_A = R_B](R))")
    }
    "work on deterministic SPJ queries" in {
      percolate(
        """PROJECT[A <= R_A, D <= S_D](
            SELECT[R_B = S_C](
              JOIN(R, S)
          ))"""
      ) must be equalTo oper(
        """PROJECT[A <= R_A, D <= S_D](
            SELECT[R_B = S_C](
              JOIN(R, S)
          ))"""
      )
    }
    "work on deterministic SP queries" in {
      percolate(
        """SELECT[A = D](
            PROJECT[A <= R_A, D <= R_A * R_B](R)
          )"""
      ) must be equalTo oper(
        """PROJECT[A <= R_A, D <= R_A * R_B](
            SELECT[R_A = R_A * R_B](R)
        )"""
      )
    }
    "work on deterministic PSP queries" in {
      percolate(
        """PROJECT[A <= A, E <= D](
            SELECT[A = D](
              PROJECT[A <= R_A, D <= R_A * R_B](R)
        ))"""
      ) must be equalTo oper(
        """PROJECT[A <= R_A, E <= R_A * R_B](
            SELECT[R_A = R_A * R_B](R)
        )"""
      )
    }
    "work on nondeterministic P queries" in {
      percolate(
        "PROJECT[A <= R_A, B <= R_B, D <= {{ test_0[A, B] }}](R)"
      ) must be equalTo
        oper("PROJECT[A <= R_A, B <= R_B, D <= {{ test_0[A, B] }}](R)")
    }
    "work on nondeterministic SP queries" in {
      percolate(
        """PROJECT[A <= R_A, B <= R_B, D <= {{ test_0[A, B] }}](
            SELECT[R_A = R_B](R)
        )"""
      ) must be equalTo oper(
        """PROJECT[A <= R_A, B <= R_B, D <= {{ test_0[A, B] }}](
            SELECT[R_A = R_B](R)
        )""")
    }
    "work on nondeterministic PSP queries" in {
      percolate(
        """PROJECT[A <= A, E <= D](
            SELECT[A = B](
              PROJECT[A <= R_A, B <= R_B, D <= {{ test_0[R_A, R_B] }}](R)
            )
          )"""
      ) must be equalTo oper(
        """PROJECT[A <= R_A, E <= {{ test_0[R_A, R_B] }}](
            SELECT[R_A = R_B](R)
          )"""
      )
    }
    "handle nondeterministic selections" in {
      percolate(
        """PROJECT[A <= A, E <= D](
            SELECT[A = D](
              PROJECT[A <= R_A, B <= R_B, D <= {{ test_0[R_A, R_B] }}](R)
            )
          )"""
      ) must be equalTo oper(
        """PROJECT[A <= R_A, E <= {{ test_0[R_A,R_B] }}, 
                   __MIMIR_CONDITION <= R_A = {{ test_0[R_A, R_B] }}](R)"""
      )
    }
    "handle selections that are both deterministic and nondeterministic" in {
      percolate(
        """PROJECT[A <= A, E <= D](
            SELECT[(A = D) & (A = B)](
              PROJECT[A <= R_A, B <= R_B, D <= {{ test_0[R_A, R_B] }}](R)
            )
          )"""
      ) must be equalTo oper(
        """PROJECT[A <= R_A, E <= {{ test_0[R_A, R_B] }}, 
                   __MIMIR_CONDITION <= R_A = {{ test_0[R_A, R_B] }}](
            SELECT[R_A = R_B](R)
        )"""
      )
    }
    "handle left-nondeterministic joins" in {
      percolate(
        """SELECT[C = S_C](
              JOIN(
                PROJECT[A <= R_A, C <= R_C, N <= {{ test_0[A, B] }}](R),
                S
              )
            )
        """
      ) must be equalTo oper(
        """PROJECT[A <= R_A, C <= R_C, N <= {{test_0[A, B]}}, 
                   S_C <= S_C, S_D <= S_D](
            SELECT[R_C = S_C](JOIN(R, S))
        )"""
      )
    }
    "handle right-nondeterministic joins" in {
      percolate(
        """SELECT[R_C = C](
            JOIN(
              R, 
              PROJECT[C <= S_C, D <= S_D, N <= {{ test_0[A, B] }}](S)
            )
          )
        """
      ) must be equalTo oper(
        """PROJECT[R_A <= R_A, R_B <= R_B, R_C <= R_C, C <= S_C, D <= S_D, N <= {{test_0[A, B]}}](
            SELECT[R_C = S_C](JOIN(R, S))
        )"""
      )
    }
    "handle full-nondeterministic joins" in {
      percolate(
        """SELECT[R_C = S_C](
            JOIN(
              PROJECT[A <= R_A, R_C <= R_C, N <= {{ test_0[A, B] }}](R), 
              PROJECT[S_C <= S_C, D <= S_D, M <= {{ test_1 [B, C]}}](S)
            )
          )
        """
      ) must be equalTo oper(
        """PROJECT[
              A <= R_A, 
              R_C <= R_C, 
              N <= {{test_0[A, B]}},
              S_C <= S_C, 
              D <= S_D, 
              M <= {{test_1[B, C]}}
            ](
              SELECT[R_C = S_C](JOIN(R, S))
            )"""
      )
    }
    "handle full-nondeterministic join conflicts" in {
      percolate(
        """SELECT[A1 = A2](
            JOIN(
              PROJECT[A1 <= R_A, B1 <= R_B, N <= {{ test_0[R_A, R_B] }}](R(R_A:int, R_B:int, R_C:int // ROWID:rowid)), 
              PROJECT[A2 <= R_A, B2 <= R_B, M <= {{ test_1[R_A, R_B] }}](R(R_A:int, R_B:int, R_C:int // ROWID:rowid))
            )
          )
        """
      ) must be equalTo oper(
        """PROJECT[
              A1 <= R_A_1, 
              B1 <= R_B_1, 
              N <= {{test_0[R_A_1, R_B_1]}},
              A2 <= R_A_2, 
              B2 <= R_B_2, 
              M <= {{test_1[R_A_2, R_B_2]}}
          ](
            SELECT[R_A_1 = R_A_2](
              JOIN(
                PROJECT[R_A_1 <= R_A, R_B_1 <= R_B, 
                        R_C_1 <= R_C, ROWID_1 <= ROWID](R(R_A:int, R_B:int, R_C:int // ROWID:rowid)),
                PROJECT[R_A_2 <= R_A, R_B_2 <= R_B, 
                        R_C_2 <= R_C, ROWID_2 <= ROWID](R(R_A:int, R_B:int, R_C:int // ROWID:rowid))
              )
            )
          )"""
      )
    }
    "handle row-ids correctly" in {
      percolate(
        """SELECT[C = S_C](
            JOIN(
              PROJECT[A <= R_A, C <= R_C, N <= {{ test_0[ROWID_MIMIR, R_A] }}](R(R_A:int, R_B:int, R_C:int // ROWID_MIMIR:rowid)),
              S
            )
          )
        """
      ) must be equalTo oper(
        """PROJECT[A <= R_A, C <= R_C, N <= {{ test_0[ROWID_MIMIR_1, R_A] }}, 
                   S_C <= S_C, S_D <= S_D](
            SELECT[R_C = S_C](
              JOIN(
                PROJECT[R_A <= R_A, R_B <= R_B, R_C <= R_C, 
                        ROWID_MIMIR_1 <= ROWID_MIMIR](R(R_A:int, R_B:int, R_C:int // ROWID_MIMIR:rowid)), 
                S
              )
            )
          )"""
      )
    }
    "properly propagate rowids" in {
      InlineProjections.optimize(CTPercolator.propagateRowIDs(oper(
        """
        PROJECT[ROWID <= ROWID_MIMIR](
          JOIN(
            R(A_A:int, A_B:int, A_C:int),
            R(B_A:int, B_B:int, B_C:int)
          )
        )
        """
      ))) must be equalTo oper(
        """
          PROJECT[ROWID <= JOIN_ROWIDS(LEFT_ROWID, RIGHT_ROWID)](
            JOIN(
              PROJECT[LEFT_ROWID <= ROWID_MIMIR, A_A <= A_A, A_B <= A_B, A_C <= A_C](
                R(A_A:int, A_B:int, A_C:int // ROWID_MIMIR:rowid)
              ),
              PROJECT[RIGHT_ROWID <= ROWID_MIMIR, B_A <= B_A, B_B <= B_B, B_C <= B_C](
                R(B_A:int, B_B:int, B_C:int // ROWID_MIMIR:rowid)
              )
            )
          )
        """
      )    
    }
    "properly process join rowids" in {
      CTPercolator.percolate(
        project(List(("X", "ROWID_MIMIR")),
          Join(
            oper("R(A_A:int, A_B:int, A_C:int)"),
            oper("R(B_A:int, B_B:int, B_C:int)")
          )
        )
      ) must be equalTo 
        project(
          List(
            ("X", "JOIN_ROWIDS(ROWID_MIMIR_1, ROWID_MIMIR_2)")
          ),
          Join(
            project(
              List(
                ("A_A", "A_A"),("A_B", "A_B"),("A_C", "A_C"),
                ("ROWID_MIMIR_1", "ROWID_MIMIR")
              ),
              oper("R(A_A:int, A_B:int, A_C:int // ROWID_MIMIR:rowid)")
            ),
            project(
              List(
                ("B_A", "B_A"),("B_B", "B_B"),("B_C", "B_C"),
                ("ROWID_MIMIR_2", "ROWID_MIMIR")
              ),
              oper("R(B_A:int, B_B:int, B_C:int // ROWID_MIMIR:rowid)")
            )
          ) 
        )   
    }

    "Properly process 3-way joins" in {
      CTPercolator.percolate(
        project(List(("X", "ROWID_MIMIR")),
          Join(
            Join(
              oper("R(A_A:int, A_B:int, A_C:int)"),
              oper("R(B_A:int, B_B:int, B_C:int)")
            ),
            oper("R(C_A:int, C_B:int, C_C:int)")
          )
        )
      ) must be equalTo 
        project(
          List(("X", "JOIN_ROWIDS(JOIN_ROWIDS(ROWID_MIMIR_1, ROWID_MIMIR_2), ROWID_MIMIR_4)")),
          Join(
            Join(
              oper("""
                PROJECT[A_A <= A_A, A_B <= A_B, A_C <= A_C, ROWID_MIMIR_1 <= ROWID_MIMIR](
                  R(A_A:int, A_B:int, A_C:int // ROWID_MIMIR:rowid))
              """),
              oper("""
                PROJECT[B_A <= B_A, B_B <= B_B, B_C <= B_C, ROWID_MIMIR_2 <= ROWID_MIMIR](
                  R(B_A:int, B_B:int, B_C:int // ROWID_MIMIR:rowid))
              """)
            ),
            oper("""
              PROJECT[C_A <= C_A, C_B <= C_B, C_C <= C_C, ROWID_MIMIR_4 <= ROWID_MIMIR](
                R(C_A:int, C_B:int, C_C:int // ROWID_MIMIR:rowid))
            """)
          )
        )
      }

    "Properly process 4-way joins" in {
      CTPercolator.percolate(
        project(List(("X", "ROWID_MIMIR")),
          Join(
            Join(
              Join(
                oper("R(A_A:int, A_B:int, A_C:int)"),
                oper("R(B_A:int, B_B:int, B_C:int)")
              ),
              oper("R(C_A:int, C_B:int, C_C:int)")
            ),
            oper("R(D_A:int, D_B:int, D_C:int)")
          )
        )
      ) must be equalTo 
        project(
          List(
            ("X", "JOIN_ROWIDS(JOIN_ROWIDS(JOIN_ROWIDS(ROWID_MIMIR_1, ROWID_MIMIR_2), ROWID_MIMIR_4), ROWID_MIMIR_5)")
          ),
          Join(
            Join(
              Join(
                oper("""
                  PROJECT[A_A <= A_A, A_B <= A_B, A_C <= A_C, ROWID_MIMIR_1 <= ROWID_MIMIR](
                    R(A_A:int, A_B:int, A_C:int // ROWID_MIMIR:rowid))
                """),
                oper("""
                  PROJECT[B_A <= B_A, B_B <= B_B, B_C <= B_C, ROWID_MIMIR_2 <= ROWID_MIMIR](
                    R(B_A:int, B_B:int, B_C:int // ROWID_MIMIR:rowid))
                """)
              ),
              oper("""
                PROJECT[C_A <= C_A, C_B <= C_B, C_C <= C_C, ROWID_MIMIR_4 <= ROWID_MIMIR](
                  R(C_A:int, C_B:int, C_C:int // ROWID_MIMIR:rowid))
              """)
            ),
            oper("""
              PROJECT[D_A <= D_A, D_B <= D_B, D_C <= D_C, ROWID_MIMIR_5 <= ROWID_MIMIR](
                R(D_A:int, D_B:int, D_C:int // ROWID_MIMIR:rowid))
            """)
          )
        )
    }
  }

  "The Optimizer Should" should {
    "Inline functions correctly" in {
      InlineProjections.optimize(oper("""
        PROJECT[Q <= A](
          PROJECT[A <= JOIN_ROWIDS(A, B)](R(A,B)))
      """)) must be equalTo oper("""
        PROJECT[Q <= JOIN_ROWIDS(A,B)](R(A,B))
      """)
    }

    "Inline Join ROWIDs correctly" in {
      InlineProjections.optimize(CTPercolator.propagateRowIDs(oper("""
        PROJECT[Q <= ROWID_MIMIR](
          JOIN(R, S)
        )
      """))) must be equalTo oper("""
        PROJECT[Q <= JOIN_ROWIDS(LEFT_ROWID, RIGHT_ROWID)](
          JOIN(
            PROJECT[LEFT_ROWID <= ROWID_MIMIR, R_A <= R_A, R_B <= R_B, R_C <= R_C](
              R(R_A:int, R_B:int, R_C:int // ROWID_MIMIR:rowid)
            ),
            PROJECT[RIGHT_ROWID <= ROWID_MIMIR, S_C <= S_C, S_D <= S_D](
              S(S_C:int, S_D:decimal // ROWID_MIMIR:rowid)
            )
          )
        )
      """)
    }
  }


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
      percolite("R(A, B // ROWID:rowid)") must be equalTo ((
        oper("R(A, B // ROWID:rowid)"),
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
      percolite("PROJECT[A <= A, B <= {{X_1[ROWID_MIMIR]}}](R(A, B))") must be equalTo ((
        oper("PROJECT[A <= A, B <= {{X_1[ROWID_MIMIR]}}](R(A, B // ROWID_MIMIR:rowid))"),
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
                B <= IF B IS NULL THEN {{X_1[ROWID_MIMIR]}} ELSE B END
               ](R(A, B))""") must be equalTo ((
        oper("""
          PROJECT[A <= A, 
                  B <= IF B IS NULL THEN {{X_1[ROWID_MIMIR]}} ELSE B END, 
                  MIMIR_COL_DET_B <= 
                       IF B IS NULL THEN FALSE ELSE TRUE END
                ](R(A, B // ROWID_MIMIR:rowid))"""),
        Map( 
          ("A", expr("true")),
          ("B", expr("MIMIR_COL_DET_B"))
        ),
        expr("true")
      ))
    }
    "Handle Data-Independent Non-Deterministic Inline Selection" in {
      percolite("SELECT[{{X_1[ROWID_MIMIR]}} = 3](R(A, B))") must be equalTo ((
        oper("SELECT[{{X_1[ROWID_MIMIR]}} = 3](R(A, B // ROWID_MIMIR:rowid))"),
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
                  B <= IF B IS NULL THEN {{X_1[ROWID_MIMIR]}} ELSE B END
                 ](R(A, B)))""") must be equalTo ((
        oper("""
        PROJECT[A <= A, B <= B, MIMIR_COL_DET_B <= MIMIR_COL_DET_B, MIMIR_ROW_DET <= MIMIR_COL_DET_B](
          SELECT[B = 3](
            PROJECT[A <= A, 
                    B <= IF B IS NULL THEN {{X_1[ROWID_MIMIR]}} ELSE B END, 
                    MIMIR_COL_DET_B <= 
                         IF B IS NULL THEN FALSE ELSE TRUE END
                  ](R(A, B // ROWID_MIMIR:rowid))))"""),
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
          PROJECT[A <= {{X_1[ROWID_MIMIR,A]}}](R(A,B)), 
          S(C,D)
        )
      """) must be equalTo ((
        oper("""
          JOIN(
            PROJECT[A <= {{X_1[ROWID_MIMIR,A]}}](R(A,B//ROWID_MIMIR:rowid)), 
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