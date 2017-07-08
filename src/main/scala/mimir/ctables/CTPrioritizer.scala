package mimir.ctables;

import optimus.optimization._
import optimus.algebra._
import mimir.ctables._
import mimir.models._
import mimir.algebra._

class Oracle(time : Double, trcmap : collection.mutable.LinkedHashMap[(Model,Int,Seq[PrimitiveValue]),(Double,Double,Double)]) {
	var T: Double = time
	var trc: collection.mutable.LinkedHashMap[(Model,Int,Seq[PrimitiveValue]),(Double,Double,Double)] = trcmap
}

object CTPrioritizer {

	def prioritize(reasons : Iterable[Reason]) = {

		implicit val problem = MIProblem(SolverLib.ojalgo)

		if(reasons.size != 0) {
			val trcmap = collection.mutable.LinkedHashMap[(Model,Int,Seq[PrimitiveValue]),(Double,Double,Double)]()
			var i = 0
			var j = 0.0
			var k = 0.0

			val numI = reasons.size // Number of UDOs
			val I = 0 until numI
			val numJ = 1 // Number of Oracles
			val J = 0 until numJ
			var q = new Array[Double](5) // Reliability of UDO prior to curation
			for (reason <- reasons) {
				i += 1
				j += 0.5
				k += 2
				//trcmap += ("{{"+reason.model+";"+reason.idx+"["+reason.args.mkString(", ")+"]}}" -> (i,j,k))
				trcmap += ((reason.model,reason.idx,reason.args) -> (i,j,k))
				q(i-1) = reason.model.confidence(reason.idx,reason.args,reason.hints)
			}
			val oracle = new Oracle(10,trcmap)
			// println(reasons)
			// Data (filled randomly)

			// var q = Array.tabulate(numI)(_*0.2) // Reliability of UDO prior to curation
			var w = Array.tabulate(numI)(_=>1) // Temporarily set to 1
			//var t = Array.tabulate(numI,numJ)((x,y)=>x+y) // Time
			//var r = Array.tabulate(numI,numJ)((x,y)=>0.1*x+0.2*y) // Reliability
			//var c = Array.tabulate(numI,numJ)((x,y)=>x+2*y) // Cost of Repair
			//var T = Array.tabulate(numJ)(x => (x+2)*3) // Time Limit per Oracle
			var t = Array.ofDim[Double](numI,numJ)
			var r = Array.ofDim[Double](numI,numJ)
			var c = Array.ofDim[Double](numI,numJ)
			var T = Array.ofDim[Double](numJ)
			for ( j <- J ) {
				val trcIter = oracle.trc.iterator
				var i = 0
				T(j) = oracle.T
				while (trcIter.hasNext) {
					val triple = trcIter.next._2
					t(i)(j) = triple._1
					r(i)(j) = triple._2
					c(i)(j) = triple._3
					i += 1
				}
			}
			var B = 50	// Budget
			var M = 1000 // Large Number

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
}
