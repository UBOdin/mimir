package mimir.ctables

import java.io._
import org.specs2.mutable._
import mimir.test._
import optimus.optimization._
import optimus.algebra._

object CTPrioritizerSpec extends Specification {


  //val test = CTPrioritizer.optimize


  implicit val problem = LQProblem(SolverLib.ojalgo)

  // Data (filled randomly)
  val numI = 5
  val I = 0 until numI
  val numJ = 5
  val J = 0 until numJ
  var q = Array.tabulate(numI)(_*0.2)
  var w = Array.tabulate(numI)(_*0.1)
  var to = Array.tabulate(numI,numJ)((x,y)=>x+y)
  var r = Array.tabulate(numI,numJ)((x,y)=>0.1*x+0.2*y)
  var c = Array.tabulate(numI,numJ)((x,y)=>x+2*y)
  var B = 50
  var T = Array.tabulate(numJ)(x => (x+2)*3)
  var M = 1000

  //Variables
  val x = Array.tabulate(numI,numJ)((i,j) => MPIntVar(s"x($i,$j)", 0 to 1))
  val y = Array.tabulate(numI)(i=>MPFloatVar(s"y$i"))
  val z = Array.tabulate(numI)(i=>MPIntVar(s"z$i", 0 to 1))

  // Optimal Function
  maximize(sum(I,J){ (i,j) => w(i)*y(i) })

  // Constraints
  for ( j <- J ) {
    add(sum(I)(i => to(i)(j)*x(i)(j))<:=T(j))
  }
  for ( i <- I ) {
    add(y(i) <:= q(i) + M*z(i))
    add(y(i) <:= sum(J)(j => c(i)(j)*x(i)(j)) + M*(1-z(i)))
    add(sum(J)(j => x(i)(j)) <:= z(i))
  }
  add(sum(I,J){(i,j) => c(i)(j)*x(i)(j)} <:= B)

  start()
  println("objective: " + objectiveValue)
  for ( i <- I ) {
    for ( j <- J ) {
      //if ( x(i)(j).value > 0 )
       println ("Oracle "+(j+1)+" is assigned to UDO "+(i+1)+" "+x(i)(j).value)
    }
  }

  for (i <- I) {
    println ("Credibility of UDO "+(i+1)+" after curation: "+y(i).value)
    //if ( z(i).value > 0 )
    println ("UDO "+(i+1)+" was repaired by a Repairing Oracle "+z(i).value)
  }
  release()

  /*
  "test" should {
  	"be null" >> {
  	test should beNull
  	}
  }

   def is = s2"""

   This is my first specification
     it is working                 $ok
     really working!               $ok
                                   """
  */


  /*implicit val problem = LQProblem(SolverLib.ojalgo)

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
  */


  "test" should {
  	"pass" >> {
        z.foreach(_.isBinary should beTrue)
        checkConstraints() should beTrue
        status should beEqualTo(ProblemStatus.OPTIMAL)
  	}
  }


}
