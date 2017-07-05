package mimir.sql;


import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.*;
import java.util.*;

/**
 * PLOT plot_source 
 *      [WITH config[, config[, ...]]]
 *      [USING linespec[, linespec[, ...]]]
 *
 * config :=
 *    | XLABEL = string
 *    | YLABEL = string
 *    | XMIN = number
 *    | XMAX = number
 *    | YMIN = number
 *    | YMAX = number
 * plot_source := table | (select)
 * linespec := (xfn, yfn) WITH args
 * args := 
 *     | TITLE string
 */

public class DrawPlot implements Statement {

  Map<String, PrimitiveValue> config = new HashMap<>();
  FromItem source;
  List<LineSpec> lines = new ArrayList<>();

  public DrawPlot(FromItem source)
  {
    this.source = source;
  }

  public void addConfig(String param, PrimitiveValue value)
  {
    config.put(param.toUpperCase(), value);
  }

  public Map<String, PrimitiveValue> getConfig()
  {
    return config;
  }

  public PrimitiveValue getConfig(String param)
  {
    return config.get(param.toUpperCase());
  }

  public FromItem getSource()
  {
    return source;
  }

  public void addLine(LineSpec spec)
  {
    lines.add(spec);
  }

  public List<LineSpec> getLines()
  {
    return lines;
  }

  public void accept(StatementVisitor vis)
  {
    throw new RuntimeException("Visitor Unsupported!");
  }

  public static class LineSpec {
    Expression x, y;
    Map<String, PrimitiveValue> config = new HashMap<>();
    public LineSpec(Expression x, Expression y) { this.x = x; this.y = y; }
    public void addConfig(String param, PrimitiveValue value) { config.put(param.toUpperCase(), value); }
    public Expression getX(){ return x; }
    public Expression getY(){ return y; }
    public Map<String,PrimitiveValue> getConfig(){ return config; }
    public PrimitiveValue getConfig(String param){ return config.get(param.toUpperCase()); }
  }
}