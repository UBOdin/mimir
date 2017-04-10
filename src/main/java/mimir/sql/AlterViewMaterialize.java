package mimir.sql;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;

public class AlterViewMaterialize implements Statement
{
  String target;
  boolean drop;

  public AlterViewMaterialize(String target, boolean drop)
  {
    this.target = target;
    this.drop = drop;
  }

  public String getTarget(){ return target; }
  public void setTarget(){ this.target = target; }
  public boolean getDrop(){ return drop; }
  public void setDrop(){ this.drop = drop; }

  public void accept(StatementVisitor sv)
  {
    throw new RuntimeException("ERROR");
  }
}