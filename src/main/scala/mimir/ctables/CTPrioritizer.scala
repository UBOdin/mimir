package mimir.ctables;

import optimus.optimization._
import optimus.algebra._

object CTPrioritizer {
	def optimize = {
	implicit val problem = MIProblem(SolverLib.ojalgo)

	// Data (filled randomly)
	// val I = 5
	val numI = 5
	val I = 0 until numI
	// val J = 5
	val numJ = 5
	val J = 0 until numJ
	var q = Array.tabulate(numI)(_*0.2)
	var w = Array.tabulate(numI)(_*0.1)
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

	// Optimal Function
	/*var f = J*w(0)*y(0)+J*w(1)*y(1)
	*for ( i <- 2 to I-1 ) {
	*	for ( j <- 0 to J-1 ) {
	*		f+=(w(i)*y(i))
	*	}
	*}
	*maximize(f)
	*/

	maximize(sum(I,J){ (i,j) => w(i)*y(i) })


	//var temp1 = new Array[optimus.algebra.Expression](J)
	//var temp1 = t(0)(0)*x(0)(0)+t(1)(0)*x(1)(0)
	//var temp2 = new Array[optimus.algebra.Expression](I)
	//var temp3 = new Array[optimus.algebra.Expression](I)
	// Constraints
	for ( j <- J ) {
		/*for ( i <- 0 to I-1 ) {
		*	//if( !(j==0 && i<2) ) {
		*	temp1(j)+=t(i)(j)*x(i)(j)
		*	//}
		*}
		*/
		//add(temp1(j) <:= T(j))
		add(sum(I)(i => t(i)(j)*x(i)(j))<:=T(j))

	}
	for ( i <- I ) {
		add(y(i) <:= q(i) + M*z(i))
		/*for ( j <- 0 to J-1 ) {
		*	temp2(i)+=c(i)(j)*x(i)(j)
		*	temp3(i)+=x(i)(j)
		*}
		*/
		//add(y(i) <:= temp2(i) + M*(1-z(i)))
		add(y(i) <:= sum(J)(j => c(i)(j)*x(i)(j)) + M*(1-z(i)))
		//add(temp3(i) <:= z(i))
		add(sum(J)(j => x(i)(j)) <:= z(i))
	}
	//var temp4 = c(0)(0)*x(0)(0) + c(0)(1)*x(0)(1)
	/*for ( i <- 0 to I-1 ) {
	*	for ( j <- 0 to J-1 ) {
	*		if ( !(i==0  && j<2)) {
	*			temp4+=c(i)(j)*x(i)(j)
	*		}
	*	}
	*}
	*/
	//add(temp4 <:= B)
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
	//println("x = " + x.value + "y = " + y.value + "z = " + z.value)

	release()

	}
}
