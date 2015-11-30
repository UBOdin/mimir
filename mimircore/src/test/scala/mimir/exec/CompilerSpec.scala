package mimir.exec;

import java.io.{StringReader,FileReader}

import mimir.parser.{MimirJSqlParser}
import org.specs2.mutable._

import mimir._
import mimir.ctables._
import mimir.parser._
import mimir.algebra._
import mimir.sql._
import mimir.optimizer._

object CompilerSpec extends Specification {
  
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
  def percolate(s: String) = CTPercolator.percolate(oper(s))
  def analyze(s: String) = new Compiler(null).compileAnalysis(oper(s))

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
            SELECT[R_B = S_B](
              JOIN(R, S)
          ))"""
      ) must be equalTo oper(
        """PROJECT[A <= R_A, D <= S_D](
            SELECT[R_B = S_B](
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
        "PROJECT[A <= R_A, B <= R_B, D <= {{ test_0 }}](R)"
      ) must be equalTo
        oper("PROJECT[A <= R_A, B <= R_B, D <= {{ test_0 }}](R)")
    }
    "work on nondeterministic SP queries" in {
      percolate(
        """PROJECT[A <= R_A, B <= R_B, D <= {{ test_0 }}](
            SELECT[R_A = R_B](R)
        )"""
      ) must be equalTo oper(
        """PROJECT[A <= R_A, B <= R_B, D <= {{ test_0 }}](
            SELECT[R_A = R_B](R)
        )""")
    }
    "work on nondeterministic PSP queries" in {
      percolate(
        """PROJECT[A <= A, E <= D](
            SELECT[A = B](
              PROJECT[A <= R_A, B <= R_B, D <= {{ test_0 }}](R)
            )
          )"""
      ) must be equalTo oper(
        """PROJECT[A <= R_A, E <= {{ test_0 }}](
            SELECT[R_A = R_B](R)
          )"""
      )
    }
    "handle nondeterministic selections" in {
      percolate(
        """PROJECT[A <= A, E <= D](
            SELECT[A = D](
              PROJECT[A <= R_A, B <= R_B, D <= {{ test_0 }}](R)
            )
          )"""
      ) must be equalTo oper(
        """PROJECT[A <= R_A, E <= {{ test_0 }}, 
                   __MIMIR_CONDITION <= R_A = {{ test_0 }}](R)"""
      )
    }
    "handle selections that are both deterministic and nondeterministic" in {
      percolate(
        """PROJECT[A <= A, E <= D](
            SELECT[(A = D) & (A = B)](
              PROJECT[A <= R_A, B <= R_B, D <= {{ test_0 }}](R)
            )
          )"""
      ) must be equalTo oper(
        """PROJECT[A <= R_A, E <= {{ test_0 }}, 
                   __MIMIR_CONDITION <= R_A = {{ test_0 }}](
            SELECT[R_A = R_B](R)
        )"""
      )
    }
    "handle left-nondeterministic joins" in {
      percolate(
        """SELECT[C = S_C](
              JOIN(
                PROJECT[A <= R_A, C <= R_C, N <= {{ test_0 }}](R),
                S
              )
            )
        """
      ) must be equalTo oper(
        """PROJECT[A <= R_A, C <= R_C, N <= {{test_0}}, 
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
              PROJECT[C <= S_C, D <= S_D, N <= {{ test_0 }}](S)
            )
          )
        """
      ) must be equalTo oper(
        """PROJECT[R_A <= R_A, R_B <= R_B, R_C <= R_C, C <= S_C, D <= S_D, N <= {{test_0}}](
            SELECT[R_C = S_C](JOIN(R, S))
        )"""
      )
    }
    "handle full-nondeterministic joins" in {
      percolate(
        """SELECT[R_C = S_C](
            JOIN(
              PROJECT[A <= R_A, R_C <= R_C, N <= {{ test_0 }}](R), 
              PROJECT[S_C <= S_C, D <= S_D, M <= {{ test_1 }}](S)
            )
          )
        """
      ) must be equalTo oper(
        """PROJECT[
              A <= R_A, 
              R_C <= R_C, 
              N <= {{test_0}},
              S_C <= S_C, 
              D <= S_D, 
              M <= {{test_1}}
            ](
              SELECT[R_C = S_C](JOIN(R, S))
            )"""
      )
    }
    "handle full-nondeterministic join conflicts" in {
      percolate(
        """SELECT[A1 = A2](
            JOIN(
              PROJECT[A1 <= R_A, B1 <= R_B, N <= {{ test_0 }}](R(R_A:int, R_B:int, R_C:int // ROWID:rowid)), 
              PROJECT[A2 <= R_A, B2 <= R_B, M <= {{ test_1 }}](R(R_A:int, R_B:int, R_C:int // ROWID:rowid))
            )
          )
        """
      ) must be equalTo oper(
        """PROJECT[
              A1 <= R_A_1, 
              B1 <= R_B_1, 
              N <= {{test_0}},
              A2 <= R_A_2, 
              B2 <= R_B_2, 
              M <= {{test_1}}
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
              PROJECT[A <= R_A, C <= R_C, N <= {{ test_0[ROWID, R_A] }}](R(R_A:int, R_B:int, R_C:int // ROWID:rowid)),
              S
            )
          )
        """
      ) must be equalTo oper(
        """PROJECT[A <= R_A, C <= R_C, N <= {{ test_0[ROWID_1, R_A] }}, 
                   S_C <= S_C, S_D <= S_D](
            SELECT[R_C = S_C](
              JOIN(
                PROJECT[R_A <= R_A, R_B <= R_B, R_C <= R_C, 
                        ROWID_1 <= ROWID](R(R_A:int, R_B:int, R_C:int // ROWID:rowid)), 
                S
              )
            )
          )"""
      )
    }
    "properly propagate rowids" in {
      InlineProjections.optimize(CTPercolator.propagateRowIDs(oper(
        """
        PROJECT[ROWID <= ROWID](
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
              PROJECT[LEFT_ROWID <= ROWID, A_A <= A_A, A_B <= A_B, A_C <= A_C](
                R(A_A:int, A_B:int, A_C:int // ROWID:rowid)
              ),
              PROJECT[RIGHT_ROWID <= ROWID, B_A <= B_A, B_B <= B_B, B_C <= B_C](
                R(B_A:int, B_B:int, B_C:int // ROWID:rowid)
              )
            )
          )
        """
      )    
    }
    "properly process join rowids" in {
      CTPercolator.percolate(oper(
        """
        PROJECT[X <= ROWID](
          JOIN(
            R(A_A:int, A_B:int, A_C:int),
            R(B_A:int, B_B:int, B_C:int)
          )
        )
        """
      )) must be equalTo oper(
        """
          PROJECT[X <= JOIN_ROWIDS(ROWID_1, ROWID_2)](
            JOIN(
              PROJECT[A_A <= A_A, A_B <= A_B, A_C <= A_C, ROWID_1 <= ROWID](
                R(A_A:int, A_B:int, A_C:int // ROWID:rowid)
              ),
              PROJECT[B_A <= B_A, B_B <= B_B, B_C <= B_C, ROWID_2 <= ROWID](
                R(B_A:int, B_B:int, B_C:int // ROWID:rowid)
              )
            )
          )
        """
      )    
    }
    "Properly process 3-way joins" in {
      CTPercolator.percolate(oper("""
        PROJECT[X <= ROWID](
          JOIN(
            JOIN(
              R(A_A:int, A_B:int, A_C:int),
              R(B_A:int, B_B:int, B_C:int)
            ),
            R(C_A:int, C_B:int, C_C:int)
          )
        )
      """)) must be equalTo oper("""
        PROJECT[X <= JOIN_ROWIDS(JOIN_ROWIDS(ROWID_1, ROWID_2), ROWID_4)](
          JOIN(
            JOIN(
              PROJECT[A_A <= A_A, A_B <= A_B, A_C <= A_C, ROWID_1 <= ROWID](
                R(A_A:int, A_B:int, A_C:int // ROWID:rowid)),
              PROJECT[B_A <= B_A, B_B <= B_B, B_C <= B_C, ROWID_2 <= ROWID](
                R(B_A:int, B_B:int, B_C:int // ROWID:rowid))
            ),
            PROJECT[C_A <= C_A, C_B <= C_B, C_C <= C_C, ROWID_4 <= ROWID](
              R(C_A:int, C_B:int, C_C:int // ROWID:rowid))
          )
        )
      """)

    }
    "Properly process 4-way joins" in {
      CTPercolator.percolate(oper("""
        PROJECT[X <= ROWID](
          JOIN(
            JOIN(
              JOIN(
                R(A_A:int, A_B:int, A_C:int),
                R(B_A:int, B_B:int, B_C:int)
              ),
              R(C_A:int, C_B:int, C_C:int)
            ),
            R(D_A:int, D_B:int, D_C:int)
          )
        )
      """)) must be equalTo oper("""
        PROJECT[X <= JOIN_ROWIDS(JOIN_ROWIDS(JOIN_ROWIDS(ROWID_1, ROWID_2), ROWID_4), ROWID_5)](
          JOIN(
            JOIN(
              JOIN(
                PROJECT[A_A <= A_A, A_B <= A_B, A_C <= A_C, ROWID_1 <= ROWID](
                  R(A_A:int, A_B:int, A_C:int // ROWID:rowid)),
                PROJECT[B_A <= B_A, B_B <= B_B, B_C <= B_C, ROWID_2 <= ROWID](
                  R(B_A:int, B_B:int, B_C:int // ROWID:rowid))
              ),
              PROJECT[C_A <= C_A, C_B <= C_B, C_C <= C_C, ROWID_4 <= ROWID](
                R(C_A:int, C_B:int, C_C:int // ROWID:rowid))
            ),
            PROJECT[D_A <= D_A, D_B <= D_B, D_C <= D_C, ROWID_5 <= ROWID](
              R(D_A:int, D_B:int, D_C:int // ROWID:rowid))
          )
        )
      """)

    }
  }

  "The Analysis Compiler" should {

    "Compile bounds analyses correctly for deterministic queries" in {
      // Remember, test_0 is a uniform distribution
      analyze("""
        PROJECT[A <= BOUNDS(R_A)](R)
      """) must be equalTo oper("""
        PROJECT[A_MIN <= R_A, A_MAX <= R_A](R)
      """)
    }
    "Compile bounds analyses correctly for non-deterministic queries" in {
      // Remember, test_0 is a uniform distribution
      analyze("""
        PROJECT[A <= BOUNDS({{ test_0[R_A, R_B] }})](R)
      """) must be equalTo oper("""
        PROJECT[A_MIN <= R_A, A_MAX <= R_B](R)
      """)
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
        PROJECT[Q <= ROWID](
          JOIN(R, S)
        )
      """))) must be equalTo oper("""
        PROJECT[Q <= JOIN_ROWIDS(LEFT_ROWID, RIGHT_ROWID)](
          JOIN(
            PROJECT[LEFT_ROWID <= ROWID, R_A <= R_A, R_B <= R_B, R_C <= R_C](
              R(R_A:int, R_B:int, R_C:int // ROWID:rowid)
            ),
            PROJECT[RIGHT_ROWID <= ROWID, S_C <= S_C, S_D <= S_D](
              S(S_C:int, S_D:decimal // ROWID:rowid)
            )
          )
        )
      """)
    }

  }
}