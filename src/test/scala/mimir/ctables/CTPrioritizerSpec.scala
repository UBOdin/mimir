package mimir.ctables

import java.io._
import org.specs2.mutable._
import mimir.test._
import optimus.optimization._
import optimus.algebra._
import optimus.algebra.AlgebraOps._
import optimus.optimization.enums._
import optimus.optimization.model._

object CTPrioritizerSpec extends Specification {


  //val test = CTPrioritizer.optimize

  "test" should {
  	"pass" >> {
      implicit val problem = MPModel(SolverLib.oJSolver)

      // Data (filled randomly)
      val numI = 5
      val I = 0 until numI
      val numJ = 5
      val J = 0 until numJ
      var q = Array.tabulate(numI)(x=>(x+1)*0.2)
      var w = Array.tabulate(numI)(_=>1)
      var to = Array.tabulate(numI,numJ)((x,y)=>x+y)
      var r = Array.tabulate(numI,numJ)((x,y)=>0.1*x+0.2*y)
      var c = Array.tabulate(numI,numJ)((x,y)=>x+1+2*y)
      var B = 50
      var T = Array.tabulate(numJ)(x => (x+2)*3)
      var M = 1000

      //Variables
      val x = Array.tabulate(numI,numJ)((i,j) => MPIntVar( 0 to 1))
      val y = Array.tabulate(numI)(i=>MPFloatVar(s"y$i"))
      val z = Array.tabulate(numI)(i=>MPIntVar(s"z$i", 0 to 1))

      // Objective Function
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
          x(i)(j).value match {
            case Some(value) => if(value==1) println ("Oracle "+(j+1)+" is assigned to UDO "+(i+1))
                                //else println((i,j)+" "+value)
            case None => None
          }
           //println ("Oracle "+(j+1)+" is assigned to UDO "+(i+1)+" "+x(i)(j).value)
        }
      }

      for (i <- I) {
        y(i).value match {
          case Some(value) => println ("Credibility of UDO "+(i+1)+" after curation: "+value)
          case None => None
        }
        z(i).value match {
          case Some(value) => if(value==1) println ("UDO "+(i+1)+" was repaired by a Repairing Oracle ")
          case None => None
        }
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


      /*implicit val problem = LQProblem(SolverLib.oJSolver)

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




      x.foreach(_.foreach(_.isBinary should beTrue))
      z.foreach(_.isBinary should beTrue)
      checkConstraints() should beTrue
      status should beEqualTo(SolutionStatus.OPTIMAL)
  	}
  	/*"pass this also" >> {

    implicit val lp = MPModel(SolverLib.oJSolver)

    val x = Array.tabulate(6)(j => MPIntVar(s"x$j", 0 to 1))
    val z = 3 * x(0) + 5 * x(1) + 6 * x(2) + 9 * x(3) + 10 * x(4) + 10 * x(5)
    minimize(z)
    add(-2 * x(0) + 6 * x(1) - 3 * x(2) + 4 * x(3) + x(4) - 2 * x(5) >:= 2)
    add(-5 * x(0) - 3 * x(1) + x(2) + 3 * x(3) - 2 * x(4) + x(5) >:= -2)
    add(5 * x(0) - x(1) + 4 * x(2) -2 * x(3) + 2 * x(4) - x(5) >:= 3)

    x.foreach(_.isBinary should beTrue)

    start()

    release()

    for ( a<-x){
      println(a.value)
    }
    //x.foreach(println(_.value))

    status should beEqualTo(ProblemStatus.OPTIMAL)
    lp.objectiveValue should beEqualTo(11.0)
  }*/
  }


}
