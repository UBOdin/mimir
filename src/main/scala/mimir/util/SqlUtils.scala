package mimir.util;

import java.io.{FileReader, Reader, StringReader}
import java.sql._

import sparsity.Name
import sparsity.expression.{
  Expression => SparsityExpression,
  Function   => SparsityFunction,
  Column
}
import sparsity.select.{
  SelectTarget,
  SelectExpression,
  SelectTable,
  SelectAll,
  SelectBody,
  FromElement,
  FromSelect,
  FromTable,
  FromJoin
}

import scala.collection.JavaConversions._
import mimir.Database
import mimir.algebra.{Operator, ID}
import mimir.algebra.function.AggregateRegistry

object SqlUtils {

  def expressionContainsAggregate(tgt: SparsityExpression, aggs: AggregateRegistry): Boolean =
  {
    val allReferencedFunctions = 
      getFunctions(tgt)
        .map { _.name }
        .map { ID.lower(_) }
    allReferencedFunctions.exists { aggs.isAggregate(_) }
  }
  
  def getAlias(expr : SparsityExpression): Name = 
  {
    expr match {
      case c: Column           => c.column
      case f: SparsityFunction => f.name
      case _                   => Name("EXPR")
    }
  }
  
  def getAlias(item : SelectExpression): Name = 
  {
    item.alias match {
      case Some(alias) => alias
      case None => getAlias(item.expression)
    }
  }

  /**
   * Post processing step to rewrite a sequence of names to be unique
   * 
   * - Duplicate names have a _N appended to them, where N is an 
   *   integer chosen to make a unique name
   * - Names that are already unique are untouched (unless they overlap 
   *   with a newly created _N name)
   */
  def makeAliasesUnique(aliases: Seq[Name]): Seq[Name] =
  {
    val dupAliases = aliases.
      groupBy { x=>x }.          // Aggregate by name
      filter { _._2.size > 1 }.  // Find aliases that occur more than once
      map { _._1 }.              // Retain only the name
      toSet

    if(dupAliases.isEmpty){ aliases } 
    else {
      var usedNames = scala.collection.mutable.Set[Name]()
      aliases.map( alias => {
        val renamedAlias = 
          if(dupAliases.contains(alias) || usedNames.contains(alias)){
            // Somewhere in the list, there is a potential duplicate
            var ctr = 1
            // Loop until we have a name that (so far) is unique
            while(usedNames.contains(alias.withSuffix("_"+ctr))) { ctr += 1 }
            // Assemble the name
            alias.withSuffix("_" + ctr)

            // Otherwise... just return the name as written
          } else { alias }

        // Save the name so we avoid duplicating it
        usedNames.add(renamedAlias)

        // And return the renamed unique alias
        renamedAlias
      })
    }
  }
  
  def changeColumnSources(e: SparsityExpression, newSource: Name): SparsityExpression = 
  {
    e match {
      case Column(col, _) => Column(col, Some(newSource))
      case _ => e.rebuild( e.children.map { changeColumnSources(_, newSource) })
    }
  }
  
  def changeColumnSources(s: SelectExpression, newSource: Name): SelectExpression = 
    SelectExpression(changeColumnSources(s.expression, newSource), s.alias)

  def getSchema(source: SelectBody, db: Database): Seq[Name] =
  {
    val fromSchemas: Map[Name, Seq[Name]] = 
      source.from.flatMap { getSchemas(_, db) }.toMap
    
    makeAliasesUnique(source.target.flatMap { 
      case expr:SelectExpression => Seq(getAlias(expr))
      case SelectTable(table)    => fromSchemas(table)
      case SelectAll()           => fromSchemas.flatMap { _._2 }
    })
  } 

  def getFunctions(expr: SparsityExpression): Seq[SparsityFunction] =
    (expr match {
      case f:SparsityFunction => Seq(f)
      case _ => Seq()
    }) ++ expr.children.flatMap { getFunctions(_) }

  /**
   * Extract source schemas from a FromItem
   */
  def getSchemas(source: FromElement, db: Database, implicitCols: Seq[Name] = Seq(Name("ROWID", true))): Seq[(Name, Seq[Name])] =
  {
    source match {
      case FromSelect(query, alias) =>
        Seq( alias -> getSchema(query, db) )

      case FromTable(_, table, alias) =>
        Seq( alias.getOrElse(table) -> 
            ((db.metadataTableSchema(ID.upper(table)) match {
              case Some(tblSchmd) => tblSchmd
              case None => db.tableSchema(table) match {
                case Some(tblSch) => tblSch
                case None => throw new mimir.algebra.RAException(s"Table doesn't exist: ${table}")
              }
            }).map(_._1.quoted:Name).toSeq ++ implicitCols)
          )
      case join: FromJoin =>
        val subSchemas = getSchemas(join.lhs, db) ++
                            getSchemas(join.rhs, db)
        val mySchema = join.alias match { 
          case None => Seq()
          case Some(alias) =>
            Seq(alias -> subSchemas.flatMap { case (_, attrs) => attrs })
        }
        mySchema ++ subSchemas
    }
  }   
  
  
}