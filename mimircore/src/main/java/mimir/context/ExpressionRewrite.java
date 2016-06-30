/**
 * A use of the visitor pattern to rewrite expressions.  Disassembles and 
 * re-assembles an arbitrary expression.  This class does nothing by itself, but
 * subclasses can overload individual methods to avoid having to re-code all of 
 * the recursion by themselves.  
 *
 * Because the native visitor pattern does not allow values to be returned, 
 * values are returned indirectly through the ret() and get() methods.  Each 
 * visitor method is expected to call ret() to return a value.
 */

package mimir.context;

import java.util.*;
import java.sql.*;

import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.statement.select.SubSelect;

public abstract class ExpressionRewrite implements ExpressionVisitor {
  Stack<Expression> result = new Stack<Expression>();
  SQLException error = null;

  public void raise(SQLException error){ this.error = error; }

  public void ret(Expression e)
  {
    result.push(e);
  }

  public Expression get()
  {
    return result.pop();
  }
  
  

  public void visit(BinaryExpression be) {
    try {
      BinaryExpression ret = be.getClass().newInstance();
      ret.setLeftExpression(internalRewrite(be.getLeftExpression()));
      ret.setRightExpression(internalRewrite(be.getRightExpression()));
      ret(ret);
    } catch(InstantiationException e){
      e.printStackTrace();
      System.exit(-1);
    } catch(IllegalAccessException e){
      e.printStackTrace();
      System.exit(-1);
    }
  }
    
    
  public void visit(Addition a) { visit((BinaryExpression)a); }
  public void visit(AllComparisonExpression all) { ret(internalRewrite((Expression)all.getSubSelect())); }
  public void visit(AndExpression a) 
    { ret(new AndExpression(internalRewrite(a.getLeftExpression()), 
                            internalRewrite(a.getRightExpression()))); }
  public void visit(AnyComparisonExpression any) { ret(internalRewrite((Expression)any.getSubSelect())); }
  public void visit(Between between) { 
    Between ret = new Between();
    ret.setNot(between.isNot());
    ret.setLeftExpression(internalRewrite(between.getLeftExpression()));
    ret.setBetweenExpressionStart(internalRewrite(between.getBetweenExpressionStart()));
    ret.setBetweenExpressionEnd(internalRewrite(between.getBetweenExpressionEnd()));
    ret(ret);
  }
  public void visit(BitwiseXor a) { visit((BinaryExpression)a); }
  public void visit(BitwiseOr a) { visit((BinaryExpression)a); }
  public void visit(BitwiseAnd a) { visit((BinaryExpression)a); }
  public void visit(CaseExpression c) { 
    CaseExpression ret = new CaseExpression();
    ret.setSwitchExpression(internalRewrite(c.getSwitchExpression()));
    ret.setElseExpression(internalRewrite(c.getElseExpression()));
    ArrayList<WhenClause> whenClauses = new ArrayList<WhenClause>();
    for(Object w : c.getWhenClauses()) { 
      whenClauses.add((WhenClause)internalRewrite((WhenClause)w));
    }
    ret.setWhenClauses(whenClauses);
    ret(ret);
  }
  public void visit(Column tableColumn) { ret(tableColumn); }
  public void visit(Concat a) { visit((BinaryExpression)a); }
  public void visit(DateValue dateValue) { ret(dateValue); }
  public void visit(BooleanValue boolValue) { ret(boolValue); }
  public void visit(Division a) { visit((BinaryExpression)a); }
  public void visit(DoubleValue doubleValue) { ret(doubleValue); }
  public void visit(EqualsTo a) { visit((BinaryExpression)a); }
  public void visit(ExistsExpression exists) { 
    ExistsExpression ret = new ExistsExpression();
    ret.setNot(exists.isNot());
    ret.setRightExpression(internalRewrite(exists.getRightExpression()));
    ret(ret);
  }
  public void visit(Function function) {
    Function ret = new Function();
    ret.setName(function.getName());
    ret.setEscaped(function.isEscaped());
    ret.setAllColumns(function.isAllColumns());
    ret.setParameters(rewrite(function.getParameters()));
    ret(ret);
  }
  public void visit(GreaterThan a) { visit((BinaryExpression)a); }
  public void visit(GreaterThanEquals a) { visit((BinaryExpression)a); }
  public void visit(InExpression in) { 
    InExpression ret = new InExpression();
    ret.setNot(in.isNot());
    ret.setLeftExpression(internalRewrite(in.getLeftExpression()));
    ret.setItemsList(rewrite(in.getItemsList()));
    ret(ret);
  }
  public void visit(InverseExpression inverse) { 
    ret(new InverseExpression(internalRewrite(inverse.getExpression())));
  }
  public void visit(IsNullExpression isNull) { 
    IsNullExpression ret = new IsNullExpression();
    ret.setNot(isNull.isNot());
    ret.setLeftExpression(internalRewrite(isNull.getLeftExpression()));
    ret(ret);
  }
  public void visit(JdbcParameter jdbcParameter) { ret(jdbcParameter); }
  public void visit(LikeExpression like) { visit((BinaryExpression)like); }
  public void visit(LongValue longValue) { ret(longValue); }
  public void visit(Matches matches) { visit((Matches)matches); }
  public void visit(MinorThan a) { visit((BinaryExpression)a); }
  public void visit(MinorThanEquals a) { visit((BinaryExpression)a); }
  public void visit(Multiplication a) { visit((BinaryExpression)a); }
  public void visit(NotEqualsTo a) { visit((BinaryExpression)a); }
  public void visit(NullValue nullValue) { ret(nullValue); }
  public void visit(OrExpression a)
    { ret(new OrExpression(internalRewrite(a.getLeftExpression()), 
      internalRewrite(a.getRightExpression()))); }
  public void visit(StringValue stringValue) { ret(stringValue); }
  public void visit(SubSelect subSelect) { ret(subSelect); }
  public void visit(Subtraction a) { visit((BinaryExpression)a); }
  public void visit(TimestampValue timestampValue) { ret(timestampValue); }
  public void visit(TimeValue timeValue) { ret(timeValue); }
  public void visit(WhenClause whenClause) { 
    WhenClause ret = new WhenClause();
    ret.setWhenExpression(internalRewrite(whenClause.getWhenExpression()));
    ret.setThenExpression(internalRewrite(whenClause.getThenExpression()));
    ret(ret);
  }
  
  public Expression internalRewrite(Expression e)
  {
    if(error != null) { return e; }
    try { return rewrite(e); }
    catch(SQLException err) { raise(err); return e; }
  }
  
  public Expression rewrite(Expression e)
    throws SQLException
  {
    if(error != null){ throw error; }
    else { e.accept(this); }
    return get();
  }
  
  public ExpressionList rewrite(ExpressionList l){
    if(l == null) { return null; }
    
    ArrayList<Expression> newList = new ArrayList<Expression>();
    for(Object e : l.getExpressions()){
      newList.add(internalRewrite((Expression)e));
    }
    return new ExpressionList(newList);
  }
  
  public ItemsList rewrite(ItemsList l){
    if(l instanceof ExpressionList){ return rewrite((ExpressionList)l); }
    if(l instanceof SubSelect) { return (SubSelect)internalRewrite((Expression)((SubSelect)l)); }
    return null;
  }
}