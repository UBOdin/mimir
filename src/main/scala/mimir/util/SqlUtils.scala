package mimir.util;

import java.sql._;

import net.sf.jsqlparser.statement.select._
import net.sf.jsqlparser.schema._
import net.sf.jsqlparser.expression._
import net.sf.jsqlparser.expression.operators.conditional._
import net.sf.jsqlparser.expression.operators.relational._

import scala.collection.JavaConversions._

import mimir.context._
import mimir.Database

object SqlUtils {
  
  def aggPartition(selectItems : List[SelectItem]) = 
  {
    var normal = List[SelectItem]();
    var agg = List[SelectItem]();
    
    selectItems foreach ((tgt) => {
        if(ScanForFunctions.hasAggregate(
          tgt.asInstanceOf[SelectExpressionItem].getExpression())
        ){
          agg = tgt :: agg;
        } else {
          normal = tgt :: normal;
        }
      }
    )
    
    (normal, agg)
  }
  
  def mkSelectItem(expr : Expression, alias: String): SelectExpressionItem = 
  {
    val item = new SelectExpressionItem();
    item.setExpression(expr);
    item.setAlias(alias);
    item;
  }
  
  def binOp(exp: BinaryExpression, l: Expression, r: Expression) = 
  {
    exp.setLeftExpression(l);
    exp.setRightExpression(r);
    exp
  }
  
  def getAlias(expr : Expression): String = 
  {
    expr match {
      case c: Column   => c.getColumnName.toUpperCase
      case f: Function => f.getName.toUpperCase
      case _           => "EXPR"
    }
  }
  
  def getAlias(item : SelectExpressionItem): String = 
  {
    if(item.getAlias() != null) { return item.getAlias() }
    getAlias(item.getExpression())
  }

  /**
   * Post processing step to rewrite a sequence of names to be unique
   * 
   * - Duplicate names have a _N appended to them, where N is an 
   *   integer chosen to make a unique name
   * - Names that are already unique are untouched (unless they overlap 
   *   with a newly created _N name)
   */
  def makeAliasesUnique(items: Seq[String]): Seq[String] =
  {
    val dupAliases = items.
      map(_.toUpperCase).     // Ensure Consistency
      groupBy(x=>x).          // Aggregate by name
      filter(_._2.size > 1).  // Find aliases that occur more than once
      map(_._1).              // Retain only the name
      toSet

    if(dupAliases.isEmpty){ items } 
    else {
      var usedNames = scala.collection.mutable.Set[String]()
      items.map( alias => {
        val renamedAlias = 
          if(dupAliases.contains(alias) || usedNames.contains(alias)){
            // Somewhere in the list, there is a potential duplicate
            var ctr = 1
            // Loop until we have a name that (so far) is unique
            while(usedNames.contains(alias+"_"+ctr)){ ctr += 1 }
            // Assemble the name
            alias + "_" + ctr

            // Otherwise... just return the name as written
          } else { alias }

        // Save the name so we avoid duplicating it
        usedNames.add(renamedAlias)

        // And return the renamed unique alias
        renamedAlias
      })
    }
  }
  
  def changeColumnSources(e: Expression, newSource: String): Expression = 
  {
    val ret = new ReSourceColumns(newSource);
    e.accept(ret);
    ret.get()
  }
  
  def changeColumnSources(s: SelectItem, newSource: String): SelectExpressionItem = 
  {
    mkSelectItem(
      changeColumnSources(s.asInstanceOf[SelectExpressionItem].getExpression(),
                          newSource),
      s.asInstanceOf[SelectExpressionItem].getAlias()
    )
  }
 
  /**
   * Extract source schemas from a FromItem
   */
  def getSchemas(source: FromItem, db: Database): List[(String, List[String])] =
  {
    source match {
      case subselect: SubSelect =>
        List((subselect.getAlias(), subselect.getSelectBody() match {
          case plainselect: PlainSelect => 
            plainselect.getSelectItems().flatMap({
              case sei:SelectExpressionItem =>
                List(sei.getAlias())
              case _:AllColumns => 
                getSchemas(plainselect.getFromItem, db).flatMap(_._2) ++
                  plainselect.getJoins match {
                    case null => None
                    case joins => 
                      joins.asInstanceOf[java.util.List[Join]].flatMap( (join:Join) => 
                        getSchemas(join.getRightItem(), db).flatMap(_._2)
                      )
                  }
            }).toList
          case union: net.sf.jsqlparser.statement.select.Union =>
            union.getPlainSelects().get(0).getSelectItems().map({
              case sei:SelectExpressionItem =>
                sei.getAlias()
            }).toList
        }))
      case table: net.sf.jsqlparser.schema.Table =>
        List(
          ( table.getAlias(), 
            db.getTableSchema(table.getName()).
              get.map(_._1).toList++List("ROWID")
          )
        )
      case join: SubJoin =>
        getSchemas(join.getLeft(), db) ++
          getSchemas(join.getJoin().getRightItem(), db)
    }
  }   
}

class ReSourceColumns(s: String) extends ExpressionRewrite {
  override def visit(col: Column): Unit = {
    ret(new Column(new Table(null, s), col.getColumnName()));
  }
}