package mimir.ctables;

import mimir._
import mimir.algebra._;

class LensCache(db: Database) {

  def cacheTableForLens(lensName: String, varIdx: Int): String =
    lensName + "_CACHE_" + varIdx
  def cacheTableForTerm(term: VGTerm): String =
    cacheTableForLens(term.model._1, term.idx)

  def buildCache(term: VGTerm, input: Operator): Unit = {
    buildCache(term.model._1, term.model._2, term.idx, term.args, input)
  }
  def buildCache(lensName: String, model: Model, varIdx: Int, args: List[Expression], input: Operator): Unit = {
    val cacheTable = cacheTableForTerm(term)

    lens.db.getTableSchema(cacheTable) match {
      case Some(_) => dropCacheTable()
    }
    createCacheTable(cacheTable, model.varTypes(varIdx), args, input)


  }

  private def emptyCacheTable(cacheTable: String) =
    lens.db.backend.update( "DELETE FROM "+cacheTable )

  private def dropCacheTable(cacheTable: String) =
    lens.db.backend.update( "DROP TABLE "+cacheTable )

  private def createCacheTable(cacheTable: String, dataType: Type.T, cacheArgs: List[Expression], input: Operator) = {
    val check = new ExpressionChecker(Typechecker.schemaOf(input))
    createCacheTable(cacheTable, dataType, cacheArgs.map(check.typeOf(_)))
  }

  private def createCacheTable(cacheTable: String, dataType: Type.T, cacheTypes: List[Type.T]) = {
    val keyCols =
      cacheTypes.zipWithIndex.map( 
        typeIndex => ("KEY_"+typeIndex._2, typeIndex._1)
      )
    val dataCols = List( ("DATA", dataType) )
    val tableDirectives = 
      (keyCols ++ dataCols).map( 
        col => col._1+" "+Types.stringOf(col._2) 
      ) ++ List(
        "PRIMARY KEY ("+keyCols.map(_._1).makeString(", ")+")"
      )
    val sql =
      "CREATE TABLE "+cacheTable+"("+
        tableDirectives.map("\n"+_).makeString(",")+
      ")"

    lens.db.backend.update( sql )
  }
}