package mimir.backend.sqlite

import java.sql.SQLException
import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir.algebra._
import mimir.ctables.vgterm._
import mimir.Database
import mimir.ctables.RepairFromList
import mimir.models.Model
import mimir.models.FiniteDiscreteDomain
import mimir.ctables.RepairByType

class BestGuessVGTerm(db:Database)
  extends MimirFunction
  with LazyLogging
{
  override def xFunc(): Unit = 
  {
    try {
      // Going to assume that this function is only ever created
      // by Mimir itself, so we can treat the argument as case sensitive
      val modelName = ID(value_text(0))
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
        var paramsStr: String = "";
        try{
          // Going to assume that this function is only ever created
          // by Mimir itself, so we can treat the argument as case sensitive
          val modelName = ID(value_text(0))
          paramsStr += modelName
          val idx = value_int(1)
          paramsStr += ", " + idx 
          val model = db.models.get(modelName)

          val argTyps = model.argTypes(idx)
          
          paramsStr += "/*args*/" 
          argTyps.
              zipWithIndex.
              map( arg => paramsStr +=  ", " + value_mimir(arg._2+2, arg._1) )
          
          paramsStr += "/*hints*/" 
          model.hintTypes(idx).
              zipWithIndex.
              map( arg => paramsStr += ", " + value_mimir(arg._2+argTyps.length+2, arg._1) )
        }catch {
          case e:Throwable => {
            paramsStr += ", ...error"
          }
        }
        throw new SQLException(s"ERROR IN BEST_GUESS_VGTERM($paramsStr) ", e)
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
      // Going to assume that this function is only ever created
      // by Mimir itself, so we can treat the argument as case sensitive
      val modelName = ID(value_text(0))
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
      // Going to assume that this function is only ever created
      // by Mimir itself, so we can treat the argument as case sensitive
      val modelName = ID(value_text(0))
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

class DomainVGTerm(db:Database)
  extends MimirFunction
  with LazyLogging
{
  override def xFunc(): Unit = 
  {
    try {
      // Going to assume that this function is only ever created
      // by Mimir itself, so we can treat the argument as case sensitive
      val modelName = ID(value_text(0))
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

      val domain = model match {
        case finite:( Model with FiniteDiscreteDomain ) =>
          RepairFromList(finite.getDomain(idx, argList, hintList))
        case _ => 
          RepairByType(model.varType(idx, argList.map(_.getType)))
      }

      logger.trace(s"$modelName;$idx: $argList -> $domain")

      return_mimir(StringPrimitive(domain.toJSON))
    } catch {
      case e:Throwable => {
        println(e)
        e.printStackTrace
        throw new SQLException("ERROR IN DOMAIN_VGTERM()", e)
      }
    }
  }
}

object VGTermFunctions 
  extends LazyLogging
{

  def bestGuessVGTermFn    = ID("best_guess_vgterm")
  def sampleVGTermFn       = ID("sample_vgterm")
  def acknowledgedVGTermFn = ID("acknowledged_vgterm")
  def domainVGTermFn       = ID("domain_vgterm")

  def register(db: Database, conn: java.sql.Connection): Unit =
  {
    org.sqlite.Function.create(conn, bestGuessVGTermFn.id, new BestGuessVGTerm(db))
    db.functions.register(
      bestGuessVGTermFn, 
      (args) => { throw new RAException(s"Mimir Cannot Execute VGTerm Functions Internally: $bestGuessVGTermFn:$args") },
      (_) => TAny()
    )
    org.sqlite.Function.create(conn, sampleVGTermFn.id, new SampleVGTerm(db))
    db.functions.register(
      sampleVGTermFn, 
      (args) => { throw new RAException(s"Mimir Cannot Execute VGTerm Functions Internally: $sampleVGTermFn:$args") },
      (_) => TAny()
    )
    org.sqlite.Function.create(conn, acknowledgedVGTermFn.id, new AcknowledgedVGTerm(db))
    db.functions.register(
      acknowledgedVGTermFn, 
      (args) => { throw new RAException(s"Mimir Cannot Execute VGTerm Functions Internally: $acknowledgedVGTermFn:$args") },
      (_) => TBool()
    )
    org.sqlite.Function.create(conn, domainVGTermFn.id, new DomainVGTerm(db))
    db.functions.register(
      domainVGTermFn, 
      (args) => { throw new RAException(s"Mimir Cannot Execute VGTerm Functions Internally: $domainVGTermFn:$args") },
      (_) => TAny()
    )
  }

  def specialize(e: Expression): Expression = {
    e match {
      case BestGuess(model, idx, args, hints) => 
      logger.debug(s"Specializing: $model;$idx[${args.mkString(",")}][${hints.mkString(",")}]")
        Function(
          bestGuessVGTermFn, 
          Seq(StringPrimitive(model.name.id), IntPrimitive(idx))++
            args.map(specialize(_))++
            hints.map(specialize(_))
        )
      case Sampler(model, idx, args, hints, seed) => 
        Function(
          sampleVGTermFn,
          Seq(
              StringPrimitive(model.name.id), 
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
              StringPrimitive(model.name.id), 
              IntPrimitive(idx)
            )++
            args.map(specialize(_))
        )
      case DomainDumper(model, idx, args, hints) => 
        Function(
          domainVGTermFn,
          Seq(StringPrimitive(model.name.id), IntPrimitive(idx))++
            args.map(specialize(_))++
            hints.map(specialize(_))
        )
      case _ => e.recur(specialize(_))
    }
  }

  def specialize(o: Operator): Operator =
    o.recur(specialize(_)).recurExpressions(specialize(_))
}