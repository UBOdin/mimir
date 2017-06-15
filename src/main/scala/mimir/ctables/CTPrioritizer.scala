package mimir.ctables;

import optimus.optimization._
import optimus.algebra._
import mimir.ctables._

object CTPrioritizer {
	def prioritize(reasons : Iterable[Reason]) = {
		implicit val problem = MIProblem(SolverLib.ojalgo)



		// Data (filled randomly)
		val numI = reasons.size /* 5 */
		val I = 0 until numI
		val numJ = 5
		val J = 0 until numJ
		var q = Array.tabulate(numI)(_*0.2)
		var w = Array.tabulate(numI)(_=>1)
		var t = Array.tabulate(numI,numJ)((x,y)=>x+y)
		var r = Array.tabulate(numI,numJ)((x,y)=>0.1*x+0.2*y)
		var c = Array.tabulate(numI,numJ)((x,y)=>x+2*y)
		var B = 50
		var T = Array.tabulate(numJ)(x => (x+2)*3)
		var M = 1000

		//Variables
		val x = Array.tabulate(numI,numJ)((i,j) => MPIntVar(s"x($i,$j)", 0 to 1))
		val y = Array.tabulate(numI)(i=>MPFloatVar(s"y$i"))
		val z = Array.tabulate(numI)(i=>MPIntVar(s"z$i", 0 to 1))

		// Objective Function
		maximize(sum(I,J){ (i,j) => w(i)*y(i) })

		// Constraints
		for ( j <- J ) {
			add(sum(I)(i => t(i)(j)*x(i)(j))<:=T(j))
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
				x(i)(j).value match {
					case Some(value) => if(value==1) println ("Oracle "+(j+1)+" is assigned to UDO "+(i+1))
					case None => None
				}
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

	}
}
