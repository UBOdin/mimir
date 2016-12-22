package mimir.sql.sqlite

import java.sql.SQLException
import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir.algebra._
import mimir.ctables._
import mimir.Database

class BestGuessVGTerm(db:Database)
  extends MimirFunction
  with LazyLogging
{
  override def xFunc(): Unit = 
  {
    try {
      val modelName = value_text(0).toUpperCase
      val idx = value_int(1)

      val model = db.models.getModel(modelName)

      val argList =
        model.argTypes(idx).
          zipWithIndex.
          map( arg => value_mimir(arg._2+2, arg._1) )

      val guess = model.bestGuess(idx, argList)

      logger.trace(s"$modelName;$idx: $argList -> $guess")

      return_mimir(guess)
    } catch {
      case e:Throwable => {
        println(e)
        e.printStackTrace
        throw new SQLException("ERROR IN BEST_GUESS_VGTERM()", e)
      }
    }
  }

}

object VGTermFunctions 
{

  def bestGuessVGTermFn = "BEST_GUESS_VGTERM"

  def register(db: Database, conn: java.sql.Connection): Unit =
  {
    org.sqlite.Function.create(conn, bestGuessVGTermFn, new BestGuessVGTerm(db))
    FunctionRegistry.register(
      bestGuessVGTermFn, 
      (args) => { throw new SQLException("Mimir Cannot Execute VGTerm Functions Internally") },
      (_) => TAny()
    )
  }

  def specialize(e: Expression): Expression = {
    e match {
      case VGTerm(model, idx, args) => 
        Function(
          bestGuessVGTermFn, 
          List(StringPrimitive(model.name), IntPrimitive(idx))++
            args.map(specialize(_))
        )
      case _ => e.recur(specialize(_))
    }
  }

  def specialize(o: Operator): Operator =
    o.recur(specialize(_)).recurExpressions(specialize(_))
}