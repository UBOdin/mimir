package mimir.lenses;

import java.sql.SQLException
import mimir._
import mimir.algebra._;
import mimir.ctables._;
import mimir.provenance._;

class BestGuessCache(db: Database) {

  val dataColumn = "MIMIR_DATA"
  def keyColumn(idx: Int) = "MIMIR_KEY_"+idx

  def joinDataColumn(termId: Int) = "MIMIR_BEST_GUESS_"+termId
  def joinKeyColumn(idx: Int, termId: Int) = "MIMIR_ARG_"+termId+"_KEY_"+idx


  def cacheTableForLens(lensName: String, varIdx: Int): String =
    lensName + "_CACHE_" + varIdx
  def cacheTableForTerm(term: VGTerm): String =
    cacheTableForLens(term.model._1, term.idx)

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

  private def buildOuterJoins(expressions: List[Expression], src: Operator): 
    (List[Expression], Operator) =
  {
    val vgTerms = expressions.
      flatMap( CTables.getVGTerms(_) ).
      toSet.toList.
      zipWithIndex
    val typechecker = new ExpressionChecker(Typechecker.schemaOf(src).toMap)
    val newSrc = 
      vgTerms.foldLeft(src)({
        case (oldSrc, (v @ VGTerm((model,_),idx,args), termId)) =>
          val argColumns = 
            args.
              zipWithIndex.
              map( arg => (joinKeyColumn(arg._2, termId), typechecker.typeOf(arg._1)) )
          val dataColumns = 
            List( (joinDataColumn(termId), typechecker.typeOf(v)) )
          LeftOuterJoin(oldSrc, 
            Table(cacheTableForLens(model, idx), argColumns++dataColumns, List()),
            ExpressionUtils.makeAnd(
              args.zipWithIndex.map(
                arg =>
                  Comparison(Cmp.Eq, Var(joinKeyColumn(arg._2, termId)), arg._1)
              )
            )
          )
      })
    (
      expressions.map(swapVGTerms(_, vgTerms)),
      newSrc
    )
  }

  def rewriteToUseCache(oper: Operator): Operator = 
    oper match {
      case x if x.expressions.flatMap(CTables.getVGTerms(_)).isEmpty =>
        x.recur(rewriteToUseCache(_))

      case Project(cols, src) => {
        val (newCols, newSrc) = 
          buildOuterJoins(
            cols.map(_.expression), 
            src.recur(rewriteToUseCache)
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
            src.recur(rewriteToUseCache)
          )
        Project(
          src.schema.map(_._1).map( x => ProjectArg(x, Var(x))),
          Select(newCond.head, newSrc)
        )
      }
    }

  def buildCache(lens: Lens) =
    recurCacheBuildThroughLensView(lens.view, lens.name)
  private def recurCacheBuildThroughLensView(view: Operator, lensName: String): Unit =
  {
    // Start with all the expressions in the current RA node
    view.expressions.
      // Find all of the VG terms in each expression
      flatMap(CTables.getVGTerms(_)).
      // Pick out those belonging to the current lens
      filter( _.model._1.equals(lensName) ).
      // We only need caches for data-dependent terms
      filter( _.args.exists(ExpressionUtils.isDataDependent(_)) ).
      // Extract the relevant attributes
      map( (x:VGTerm) => (x.model._2, x.idx, x.args) ).
      // Eliminate duplicates (in case the same VGTerm appears multiple times)
      toSet.
      // And build caches for each var
      foreach( (x:(Model, Int, List[Expression])) => {
        if(view.children.length != 1){ 
          throw new SQLException("Assertion Failed: Expecting operator with expressions to have only one child")
        }
        buildCache(lensName, x._1, x._2, x._3, view.children.head)
      })
    // Finally recur to the child nodes
    view.children.foreach( recurCacheBuildThroughLensView(_, lensName) )
  }
  def buildCache(term: VGTerm, input: Operator): Unit =
    buildCache(term.model._1, term.model._2, term.idx, term.args, input)
  def buildCache(lensName: String, model: Model, varIdx: Int, args: List[Expression], input: Operator): Unit = {
    val cacheTable = cacheTableForLens(lensName, varIdx)
    val typechecker = new ExpressionChecker(Typechecker.schemaOf(input).toMap)

    if(db.getTableSchema(cacheTable) != None) {
      dropCacheTable(cacheTable)
    }

    createCacheTable(cacheTable, model.varTypes(varIdx), args.map( typechecker.typeOf(_) ))

    db.query(input).foreachRow(row => {
      val compiledArgs = args.map(Provenance.plugInToken(_, row.provenanceToken()))
      val tuple = row.currentTuple()
      val dataArgs = compiledArgs.map(Eval.eval(_, tuple))
      val guess = model.bestGuess(varIdx, dataArgs)
      db.backend.update(
          "INSERT INTO "+cacheTable+"("+dataColumn+","+
            dataArgs.zipWithIndex.
              map( arg => (","+keyColumn(arg._2)) ).
              mkString("")+
          ") VALUES (?"+
            dataArgs.map(_ => ",?").mkString("")+
          ")",
          guess.asString :: dataArgs.map(_.asString)
        )
    })
  }

  private def emptyCacheTable(cacheTable: String) =
    db.backend.update( "DELETE FROM "+cacheTable )

  private def dropCacheTable(cacheTable: String) =
    db.backend.update( "DROP TABLE "+cacheTable )

  private def createCacheTable(cacheTable: String, dataType: Type.T, cacheTypes: List[Type.T]) = {
    val keyCols =
      cacheTypes.zipWithIndex.map( 
        typeIndex => (keyColumn(typeIndex._2), typeIndex._1)
      )
    val dataCols = List( (dataColumn, dataType) )
    val tableDirectives = 
      (keyCols ++ dataCols).map( 
        col => col._1+" "+Type.toString(col._2) 
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