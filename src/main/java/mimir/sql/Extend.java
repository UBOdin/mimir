package mimir.sql;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;

public class Extend implements Statement{
  public static enum Op { 

    ADD_COLUMN,
    ALTER_COLUMN, 
    RENAME_COLUMN

  }; 

  private Op op;
  private String target;
  private Expression value;

  public Extend(Op op, String target){ this(op, target, null); }
  public Extend(Op op, String target, Expression value)
  { 
    this.op = op;
    this.target = target;
    this.value = value;
  }

  public Op getOp() { return this.op; }
  public void setOp(Op op) { this.op = op; }

  public String getTarget() { return this.target; }
  public void setTarget(String target) { this.target = target; }

  public Expression getValue() { return this.value; }
  public void setValue(Expression value) { this.value = value; }

  @Override
  public void accept(StatementVisitor statementVisitor) 
  { /* ignore */ }

}