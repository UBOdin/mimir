package mimir.sql;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.expression.Expression;

public class Pragma implements Statement{
  private Expression expression;

  public Pragma(Expression expression)
  {
    this.expression = expression;
  }

  public Expression getExpression() 
  {
    return this.expression;
  }

  public void setExpression(Expression expression) {
    this.expression = expression;
  }
  
  @Override
  public void accept(StatementVisitor statementVisitor) {
    // no equivalent
  }
  
}
