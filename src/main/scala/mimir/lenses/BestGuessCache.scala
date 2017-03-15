package mimir.lenses;

import java.sql.SQLException
import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir._
import mimir.algebra._
import mimir.ctables._
import mimir.provenance._
import mimir.optimizer._
import mimir.models._

class BestGuessCache(db: Database) extends LazyLogging {

  val dataColumn = "MIMIR_DATA"
  def keyColumn(idx: Int) = "MIMIR_KEY_"+idx

  def joinDataColumn(termId: Int) = "MIMIR_BEST_GUESS_"+termId
  def joinKeyColumn(idx: Int, termId: Int) = "MIMIR_ARG_"+termId+"_KEY_"+idx


  def cacheTableForModel(model: Model, varIdx: Int): String =
    "MIMIR_"+model.name.replaceAll(":","_") + "_CACHE_" + varIdx
  def cacheTableForTerm(term: VGTerm): String =
    cacheTableForModel(term.model, term.idx)
  def cacheTableDefinition(model: Model, varIdx: Int, termId: Int): Table = {
    val tableName = cacheTableForModel(model, varIdx)
    val sch = db.getTableSchema(tableName).get;
    
    val keyCols = (0 to (sch.length - 2)).map( joinKeyColumn(_, termId) ).toList
    val dataCols = List(joinDataColumn(termId))

    Table(
      tableName,
      tableName,
      (keyCols++dataCols).zip(sch.map(_._2)),
      List()
    )
  }

  private def swapVGTerms(expr: Expression, termMap: List[(VGTerm, Int)]): Expression =
    expr match {
      case v:VGTerm => 
        termMap.find( _._1.equals(v) ) match {
          case Some((_, idx)) => Var(joinDataColumn(idx))
          case None => 
            throw new Exception("Assert Failed: Expecting to have a replacement for all VG terms")
        }
      case _ => expr.recur(swapVGTerms(_, termMap))
  }


  private def buildOuterJoins(expressions: Seq[Expression], src: Operator): 
    (Seq[Expression], Operator) =
  {
    assert(CTables.isDeterministic(src))
    val vgTerms = expressions.
      flatMap( CTables.getVGTerms(_) ).
      toSet.toList.
      zipWithIndex
    val typechecker = new ExpressionChecker(db.bestGuessSchema(src).toMap)
    val newSrc = 
      vgTerms.foldLeft(src)({
        case (oldSrc, (v @ VGTerm(model, idx, args, hints), termId)) =>
          LeftOuterJoin(oldSrc,
            cacheTableDefinition(model, idx, termId),
            ExpressionUtils.makeAnd(
              args.zipWithIndex.map(
                arg => {
                  val keyBase = Var(joinKeyColumn(arg._2, termId))
                  val key = 
                    typechecker.typeOf(arg._1) match {
                      case TRowId() =>
                        // We materialize rowids as Strings in the backing store.  As
                        // a result, we need to convince the typechecker that we're
                        // being sane here.
                        Function("CAST", List[Expression](keyBase, TypePrimitive(TRowId())))
                      case _  =>
                        keyBase
                    }
                  Comparison(Cmp.Eq, key, swapVGTerms(arg._1, vgTerms))
                }
              )
            )
          )
      })
    val newExprs = 
      expressions.map( swapVGTerms(_, vgTerms) )

    logger.debug(s"REBUILT: Project[$newExprs](\n$newSrc\n)")
    assert(CTables.isDeterministic(newSrc))
    newExprs.foreach( e => assert(CTables.isDeterministic(e)) )

    ( newExprs, newSrc )
  }

  def rewriteToUseCache(oper: Operator): Operator = {
    logger.trace(s"Rewriting: $oper")
    oper match {
      case x if x.expressions.flatMap(CTables.getVGTerms(_)).isEmpty =>
        x.recur(rewriteToUseCache(_))

      case Project(cols, src) => {
        val (newCols, newSrc) = 
          buildOuterJoins(
            cols.map(_.expression), 
            rewriteToUseCache(src)
          )
        Project(
          cols.map(_.name).
               zip(newCols).
               map( col => ProjectArg(col._1, col._2)), 
          newSrc
        )
      }

      case Select(cond, src) => {
        val (newCond, newSrc) = 
          buildOuterJoins(
            List(cond), 
            rewriteToUseCache(src)
          )
        Project(
          src.schema.map(_._1).map( x => ProjectArg(x, Var(x))),
          Select(newCond.head, newSrc)
        )
      }

    }
  }

  def buildCache(view: Operator): Unit =
  {
    // Start with all the expressions in the current RA node
    view.expressions.
      // Pick out all of the VG terms, along with the conditions under which they 
      // contaminate the result
      flatMap(CTAnalyzer.compileCausality(_)).
      map( (x) => { logger.trace(s"Causality: $x"); x }).
      // We only need caches for data-dependent terms
      filter( _._2.args.exists(ExpressionUtils.isDataDependent(_)) ).
      map( (x) => { logger.trace(s"Surviving: $x"); x }).
      // Extract the relevant features of the VGTerm
      map({ case (cond, term) => ((term.model, term.idx, term.args, term.hints), cond) }).
      // Gather the conditions for each term
      groupBy( _._1 ).
      // The VGTerm applies if any of the gathered conditions are true
      map({ case (term, groups) => (term, ExpressionUtils.makeOr(groups.map(_._2))) }).
      // And build a cache for those terms where appropriate
      foreach({ case ((model, idx, args, hints), cond) =>
        if(view.children.length != 1){ 
          throw new SQLException("Assertion Failed: Expecting operator with expressions to have only one child")
        }
        buildCache(model, idx, args, hints, Select(cond, view.children.head))
      })
    // Finally recur to the child nodes
    view.children.foreach( buildCache(_) )
  }
  def buildCache(term: VGTerm, input: Operator): Unit =
    buildCache(term.model, term.idx, term.args, term.hints, input)
  def buildCache(model: Model, varIdx: Int, args: Seq[Expression], hints: Seq[Expression], input: Operator): Unit = {
    val cacheTable = cacheTableForModel(model, varIdx)

    // Use the best guess schema for the typechecker... we want just one instance
    val typechecker = new ExpressionChecker(db.bestGuessSchema(input).toMap)

    if(db.getTableSchema(cacheTable) != None) {
      dropCacheTable(cacheTable)
    }

    val argTypes = args.map( typechecker.typeOf(_) )
    createCacheTable(cacheTable, model.varType(varIdx, argTypes), argTypes)

    val modelName = model.name
    logger.debug(s"Building cache for $modelName-$varIdx[$args] with\n$input")
    
    val guesses = 
      db.query(input).mapRows(row => {
        val compiledArgs = args.map(Provenance.plugInToken(_, row.provenanceToken()))
        val compiledHints = hints.map(Provenance.plugInToken(_, row.provenanceToken()))
        val tuple = row.currentTuple()
        val dataArgs = compiledArgs.map(Eval.eval(_, tuple))
        val dataHints = compiledHints.map(Eval.eval(_, tuple))
        val guess = model.bestGuess(varIdx, dataArgs, dataHints)
        
        logger.trace(s"Registering $dataArgs -> $guess")
        
        guess :: dataArgs.toList
      })
      
    val uniqueMap = collection.mutable.Map[String, Boolean]()
    val uniqueKeysGuesses = guesses.map(row => {
        val keyVal = row.last.toString
        if(uniqueMap.contains(keyVal))
          None
        else{
          uniqueMap(keyVal) = true
          Some(row)
        }
      }
      ).flatten

    db.backend.fastUpdateBatch(
      s"""INSERT INTO $cacheTable(
        $dataColumn, 
        ${args.zipWithIndex.
          map( arg => keyColumn(arg._2) ).
          mkString(", ")}
      ) VALUES (?, ${args.map(_ => "?").mkString("?")})
      """,
      uniqueKeysGuesses
    )
  }
  
  def update(model: Model, idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue): Unit =
  {
    if(args.isEmpty){ return; }
    
    db.backend.update(
      s"""UPDATE ${cacheTableForModel(model, idx)} 
        SET $dataColumn = ?
        WHERE ${args.map(x => x+" = ?").mkString("?")}
      """,
      List(v) ++ args
    )

  }

  private def emptyCacheTable(cacheTable: String) =
    db.backend.update( "DELETE FROM "+cacheTable )

  private def dropCacheTable(cacheTable: String) =
    db.backend.update( "DROP TABLE "+cacheTable )

  private def createCacheTable(cacheTable: String, dataType: Type, cacheTypes: Seq[Type]) = {
    val keyCols =
      cacheTypes.zipWithIndex.map( 
        typeIndex => (keyColumn(typeIndex._2), typeIndex._1)
      )

    logger.debug(s"CREATING $cacheTable[$cacheTypes] -> $dataType")

    val dataCols = List( (dataColumn, dataType) )
    val tableDirectives = 
      (keyCols ++ dataCols).map( 
        col => { col._1+" "+col._2 }
      ) ++ List(
        "PRIMARY KEY ("+keyCols.map(_._1).mkString(", ")+")"
      )
    val sql =
      "CREATE TABLE "+cacheTable+"("+
        tableDirectives.map("\n"+_).mkString(",")+
      ")"

    db.backend.update( sql )
  }
}