package mimir.ctables

import java.io._
import org.specs2.mutable._
import mimir.test._
import optimus.optimization._

object CTPrioritizerSpec extends Specification {


/* val test = CTPrioritizer.optimize
*
*"test" should {
*	"be null" >> {
*	test should beNull
*	}
*}
* 
* def is = s2"""
*
* This is my first specification
*   it is working                 $ok
*   really working!               $ok
*                                 """
*/


implicit val problem = LQProblem(SolverLib.ojalgo)

      val x = MPFloatVar("x", 100, 200)
      val y = MPFloatVar("y", 80, 170)

      maximize(-2 * x + 5 * y)
      add(y >:= -x + 200)
      start()
"test" should {
	"pass" >> {
      x.value should beEqualTo(Some(100))
      y.value should beEqualTo(Some(170))
      objectiveValue should beEqualTo(650)
      checkConstraints() should beTrue
      status should beEqualTo(ProblemStatus.OPTIMAL)
	}
}
      release()
}
