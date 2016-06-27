/**
 * Parent class for many of the utility classes in this package.  Recursively 
 * scan through an expression object.  This class does oes nothing by itself, 
 * but subclasses   can overload individual visit methods rather than having to 
 * re-code all of the recursion for each utility class.
 */

package mimir.context;

import java.util.ArrayList;
import java.util.List;
import java.sql.*;

import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.statement.select.SubSelect;


public abstract class ExpressionScan implements ExpressionVisitor, ItemsListVisitor {
  boolean warnOnSubSelect;
  SQLException error = null;
  
  public void raise(SQLException error){ this.error = error; }
  public void reraise() throws SQLException { if(error != null) { throw error; } }

  public ExpressionScan(boolean warnOnSubSelect)
  {
    this.warnOnSubSelect = warnOnSubSelect;
  }

  public void visit(BinaryExpression be) { recur(be.getLeftExpression()); 
                                           recur(be.getRightExpression()); }
  public void visit(Addition a) { visit((BinaryExpression)a); }
  public void visit(AllComparisonExpression all) { visit(all.getSubSelect()); }
  public void visit(AndExpression a) { visit((BinaryExpression)a); }
  public void visit(AnyComparisonExpression any) { visit(any.getSubSelect()); }
  public void visit(Between between) { 
                                       recur(between.getLeftExpression());
                                       recur(between.getBetweenExpressionStart()); 
                                       recur(between.getBetweenExpressionEnd()); }
  public void visit(BitwiseXor a) { visit((BinaryExpression)a); }
  public void visit(BitwiseOr a) { visit((BinaryExpression)a); }
  public void visit(BitwiseAnd a) { visit((BinaryExpression)a); }
  public void visit(CaseExpression c) { if(c.getSwitchExpression() != null) 
                                          { recur(c.getSwitchExpression()); }
                                        for(Object w : c.getWhenClauses()) { 
                                          recur(((WhenClause)w));
                                        }
                                        recur(c.getElseExpression());
                                      }
  public void visit(Column tableColumn) { }
  public void visit(Concat a) { visit((BinaryExpression)a); }
  public void visit(DateValue dateValue) { }
  public void visit(BooleanValue boolValue) { }
  public void visit(Division a) { visit((BinaryExpression)a); }
  public void visit(DoubleValue doubleValue) { }
  public void visit(EqualsTo a) { visit((BinaryExpression)a); }
  public void visit(ExistsExpression exists) { recur(exists.getRightExpression()); }
  public void visit(Function function) { if(!function.isAllColumns()){
                                           function.getParameters().accept(this);
                                         } }
  public void visit(GreaterThan a) { visit((BinaryExpression)a); }
  public void visit(GreaterThanEquals a) { visit((BinaryExpression)a); }
  public void visit(InExpression in) { recur(in.getLeftExpression()); 
                                       in.getItemsList().accept(this); 
                                     }
  public void visit(InverseExpression inverse) { recur(inverse.getExpression()); }
  public void visit(IsNullExpression isNull) { recur(isNull.getLeftExpression()); }
  public void visit(JdbcParameter jdbcParameter) { }
  public void visit(LikeExpression like) { visit((BinaryExpression)like);
                                         }
  public void visit(LongValue longValue) { }
  public void visit(Matches matches) { visit((Matches)matches); }
  public void visit(MinorThan a) { visit((BinaryExpression)a); }
  public void visit(MinorThanEquals a) { visit((BinaryExpression)a); }
  public void visit(Multiplication a) { visit((BinaryExpression)a); }
  public void visit(NotEqualsTo a) { visit((BinaryExpression)a); }
  public void visit(NullValue nullValue) {}
  public void visit(OrExpression a) { visit((BinaryExpression)a); }
  public void visit(StringValue stringValue) { }
  public void visit(SubSelect subSelect) 
    { if(warnOnSubSelect) {System.err.println("WARNING: NOT RECURRING INTO Sub Select"); } }
  public void visit(Subtraction a) { visit((BinaryExpression)a); }
  public void visit(TimestampValue timestampValue) { }
  public void visit(TimeValue timeValue) { }
  public void visit(WhenClause whenClause) { recur(whenClause.getWhenExpression());
                                             recur(whenClause.getThenExpression()); }
  
  public void recur(Expression e)
  {
    if(error != null) { return; }
    else { e.accept(this); }
  }
  
  public void visit(ExpressionList el){
    for(Object e : el.getExpressions()) { recur((Expression)e); }
  }
}