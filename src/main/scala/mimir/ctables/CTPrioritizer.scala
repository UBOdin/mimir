package mimir.ctables;

import optimus.optimization._

object CTPrioritizer {
	def optimize = {
	implicit val problem = LQProblem(SolverLib.ojalgo)

	val x = MPFloatVar("x", 100, 200)
	val y = MPFloatVar("y", 80, 170)

	maximize(-2 * x + 5 * y)
	add(y >:= -x + 200)

	start()
	println("objective: " + objectiveValue)
	println("x = " + x.value + "y = " + y.value)

	release()

	}
}
