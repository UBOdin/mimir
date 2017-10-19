package mimir.sql;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.expression.Expression;

public class DirectQuery implements Statement {
  private Statement stmt;

  public DirectQuery(Statement stmt)
  {
    this.stmt = stmt;
  }

  public Statement getStatement() 
  {
    return this.stmt;
  }

  public void setStatement(Statement stmt) {
    this.stmt = stmt;
  }
  
  @Override
  public void accept(StatementVisitor statementVisitor) {
    // no equivalent
  }
  
}
