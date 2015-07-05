package mimir.util;

import java.sql._;

import net.sf.jsqlparser.statement.select._
import net.sf.jsqlparser.schema._
import net.sf.jsqlparser.expression._
import net.sf.jsqlparser.expression.operators.conditional._
import net.sf.jsqlparser.expression.operators.relational._

import mimir.context._

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
  
  def mkSelectItem(expr : Expression, alias: String): SelectExpressionItem = {
    val item = new SelectExpressionItem();
    item.setExpression(expr);
    item.setAlias(alias);
    item;
  }
  
  def binOp(exp: BinaryExpression, l: Expression, r: Expression) = {
    exp.setLeftExpression(l);
    exp.setRightExpression(r);
    exp
  }
  
  def getAlias(expr : Expression): String = {
    if(expr.isInstanceOf[Column]){
      expr.asInstanceOf[Column].getColumnName();
    } else {
      null;
    }
  }
  
  def getAlias(item : SelectExpressionItem): String = {
    if(item.getAlias() == null) {
      getAlias(item.getExpression());
    } else {
      item.getAlias();
    }
  }
  def getAlias(item : SelectItem): String = { 
    getAlias(item.asInstanceOf[SelectExpressionItem])
  }
  
  def changeColumnSources(e: Expression, newSource: String): Expression = {
    val ret = new ReSourceColumns(newSource);
    e.accept(ret);
    ret.get()
  }
  
  def changeColumnSources(s: SelectItem, newSource: String): SelectExpressionItem = {
    mkSelectItem(
      changeColumnSources(s.asInstanceOf[SelectExpressionItem].getExpression(),
                          newSource),
      s.asInstanceOf[SelectExpressionItem].getAlias()
    )
  }
    
}

class ReSourceColumns(s: String) extends ExpressionRewrite {
  override def visit(col: Column): Unit = {
    ret(new Column(new Table(null, s), col.getColumnName()));
  }
}