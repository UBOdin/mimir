package mimir.exec;

import java.io.{StringReader,FileReader}

import mimir.parser.{MimirJSqlParser}
import org.specs2.mutable._

import mimir._
import mimir.ctables._
import mimir.parser._
import mimir.algebra._
import mimir.sql._

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
    schema.get(_).get.toList
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
        """PROJECT[A <= R_A, E <= D](
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
        """PROJECT[A <= R_A, E <= D](
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
                PROJECT[S_C <= S_C, S_D <= S_D](S)
              )
            )
          )"""
      )
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
}