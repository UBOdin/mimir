/**
 * Identify the name of all functions invoked within a given Expression
 *
 * Descend through the expression, identify every instance of a Function
 * expression, and record the name of every function encountered.
 *
 * General usage is of the form
 * ScanForFunctions.scan(myExpression)
 */
package mimir.context;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.*;

public class ScanForFunctions extends ExpressionScan {
  ArrayList<String> functions = new ArrayList<String>();
  
  public ScanForFunctions(){ super(false); }
  
  public void visit(Function fn) {
    functions.add(fn.getName());
    super.visit(fn);
  }
  
  public List<String> getFunctions(){ return functions; }
  
  /**
   * Compute a list of all function names in the given expression
   * 
   * @param e  An arbitrary expression
   * @returns A list of all function names in e
   */
  public static List<String> scan(Expression e){
    ScanForFunctions scan = new ScanForFunctions();
    e.accept(scan);
    return scan.getFunctions();
  }
 
  /**
   * Determine if a given expression contains any aggregate function calls
   * 
   * @param e  An arbitrary expression
   * @returns true if e contains any aggregate functions as determined by 
   *          the isAggregate method.
   */
  public static boolean hasAggregate(Expression e){
    for(String fn : scan(e)){
      if(isAggregate(fn)){ return true; }
    }
    return false;
  }
  
  /**
   * Determine if the given function name corresponds to a standard aggregate
   * function.
   * 
   * @param fn  The name of a function
   * @returns true if fn corresponds to the name of an aggregate function.
   */
  public static boolean isAggregate(String fn)
  {
    fn = fn.toUpperCase();
    return  "SUM".equals(fn) 
         || "COUNT".equals(fn)
         || "AVG".equals(fn)
         || "STDDEV".equals(fn)
         || "MAX".equals(fn)
         || "MIN".equals(fn);
  }
}