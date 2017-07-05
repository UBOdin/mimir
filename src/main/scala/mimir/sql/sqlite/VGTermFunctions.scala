package mimir.sql.sqlite

import java.sql.SQLException
import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir.algebra._
import mimir.ctables.vgterm._
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

      val model = db.models.get(modelName)

      val argList =
        model.argTypes(idx).
          zipWithIndex.
          map( arg => value_mimir(arg._2+2, arg._1) )
      val hintList = 
        model.hintTypes(idx).
          zipWithIndex.
          map( arg => value_mimir(arg._2+argList.length+2, arg._1) )

      val guess = model.bestGuess(idx, argList, hintList)

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

class SampleVGTerm(db:Database)
  extends MimirFunction
  with LazyLogging
{
  override def xFunc(): Unit = 
  {
    try {
      val modelName = value_text(0).toUpperCase
      val idx = value_int(1)
      val seed = value_int(2)

      val model = db.models.get(modelName)

      val argList =
        model.argTypes(idx).
          zipWithIndex.
          map( arg => value_mimir(arg._2+3, arg._1) )
      val hintList = 
        model.hintTypes(idx).
          zipWithIndex.
          map( arg => value_mimir(arg._2+argList.length+3, arg._1) )

      val sample = model.sample(idx, seed, argList, hintList)

      logger.trace(s"$modelName;$idx: $argList -> $sample")

      return_mimir(sample)
    } catch {
      case e:Throwable => {
        println(e)
        e.printStackTrace
        throw new SQLException("ERROR IN BEST_GUESS_VGTERM()", e)
      }
    }
  }
}

class AcknowledgedVGTerm(db:Database)
  extends MimirFunction
  with LazyLogging
{
  override def xFunc(): Unit = 
  {
    try {
      val modelName = value_text(0).toUpperCase
      val idx = value_int(1)

      val model = db.models.get(modelName)

      val argList =
        model.argTypes(idx).
          zipWithIndex.
          map( arg => value_mimir(arg._2+2, arg._1) )

      result(if(model.isAcknowledged(idx, argList)) { 1 } else { 0 })
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
  extends LazyLogging
{

  def bestGuessVGTermFn = "BEST_GUESS_VGTERM"
  def sampleVGTermFn = "SAMPLE_VGTERM"
  def acknowledgedVGTermFn = "ACKNOWLEDGED_VGTERM"

  def register(db: Database, conn: java.sql.Connection): Unit =
  {
    org.sqlite.Function.create(conn, bestGuessVGTermFn, new BestGuessVGTerm(db))
    db.functions.register(
      bestGuessVGTermFn, 
      (args) => { throw new RAException(s"Mimir Cannot Execute VGTerm Functions Internally: $bestGuessVGTermFn:$args") },
      (_) => TAny()
    )
    org.sqlite.Function.create(conn, sampleVGTermFn, new SampleVGTerm(db))
    db.functions.register(
      sampleVGTermFn, 
      (args) => { throw new RAException(s"Mimir Cannot Execute VGTerm Functions Internally: $sampleVGTermFn:$args") },
      (_) => TAny()
    )
    org.sqlite.Function.create(conn, acknowledgedVGTermFn, new AcknowledgedVGTerm(db))
    db.functions.register(
      acknowledgedVGTermFn, 
      (args) => { throw new RAException(s"Mimir Cannot Execute VGTerm Functions Internally: $acknowledgedVGTermFn:$args") },
      (_) => TBool()
    )
  }

  def specialize(e: Expression): Expression = {
    e match {
      case BestGuess(model, idx, args, hints) => 
      logger.debug(s"Specializing: $model;$idx[${args.mkString(",")}][${hints.mkString(",")}]")
        Function(
          bestGuessVGTermFn, 
          Seq(StringPrimitive(model.name), IntPrimitive(idx))++
            args.map(specialize(_))++
            hints.map(specialize(_))
        )
      case Sampler(model, idx, args, hints, seed) => 
        Function(
          sampleVGTermFn,
          Seq(
              StringPrimitive(model.name), 
              IntPrimitive(idx), 
              specialize(seed)
            )++
            args.map(specialize(_))++
            hints.map(specialize(_))
        )
      case IsAcknowledged(model, idx, args) => 
        Function(
          acknowledgedVGTermFn,
          Seq(
              StringPrimitive(model.name), 
              IntPrimitive(idx)
            )++
            args.map(specialize(_))
        )

      case _ => e.recur(specialize(_))
    }
  }

  def specialize(o: Operator): Operator =
    o.recur(specialize(_)).recurExpressions(specialize(_))
}