package mimir.sql;

import java.util.List;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;

/**
 * 
 * FEEDBACK model id ([arg1[, arg2[, ...]]]) IS prim
 * 
 */

public class Feedback implements Statement {
  String model;
  int idx;
  List<PrimitiveValue> args;
  PrimitiveValue value;

  public Feedback(String model, int idx, List<PrimitiveValue> args, PrimitiveValue value)
  {
    this.model = model;
    this.idx = idx;
    this.args = args;
    this.value = value;
  }

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder(
        "FEEDBACK "+model+" "+idx+" ("
      );
    String sep = "";
    for(PrimitiveValue arg : args){ sb.append(sep+arg); sep = ", "; }
    sb.append(") IS "+value);
    return sb.toString();
  }

  @Override
  public void accept(StatementVisitor statementVisitor) {
    throw new RuntimeException("Unhandled");
  }


  public String getModel() { return model; }
  public void setModel(String model) { this.model = model; }

  public int getIdx() { return idx; }
  public void setIdx(int idx) { this.idx = idx; }

  public List<PrimitiveValue> getArgs() { return args; }
  public void setArgs(List<PrimitiveValue> args) { this.args = args; }

  public PrimitiveValue getValue() { return value; }
  public void setVale(PrimitiveValue value) { this.value = value; }
}