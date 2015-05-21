package mimir.exec;

import java.sql._;
import mimir.sql._;
import mimir.algebra._;
import mimir.ctables._;
import mimir.Database;

class Compiler(db: Database) {

  /**
   * Perform a full end-end compilation pass.  Return an iterator over
   * the result set.  
   */
  def compile(oper: Operator): ResultIterator =
  {
    val optimizedOper = 
      List[Operator => Operator](
        CTPercolator.percolate _, // Partition Det & Nondet query fragments
        optimize _,               // Basic Optimizations
        compileAnalysis _         // Transform BOUNDS(), CONF(), etc... into 
                                  // actual expressions that SQL can understand
      ).foldLeft(oper)( (o, fn) => fn(o) )
    // Finally build the iterator
    return buildIterator(optimizedOper);
  }

  /**
   * Apply any local optimization rewrites.  Currently a placeholder.
   */
  def optimize(oper: Operator): Operator = oper
  
  /**
   * Build an iterator out of the specified operator.  If the operator
   * is probabilistic, we'll strip the Project() operator off the top
   * (which, given a pass with CTPercolator.percolate(), will contain
   * all of the nondeterminism in the query), and build a 
   * ProjectionResultIterator over it.  Deterministic queries are 
   * translated back into SQL and evaluated directly over the host 
   * database.
   *
   * Preconditions: 
   *   - If the operator is probabilistic, it must have already been 
   *     passed through CTPercolator.percolate().  
   *   - If the operator contains analysis terms, it must have already
   *     been passed through compileAnalysis().
   *
   * Basically... there's relatively little reason that you'd want to
   * call this method directly.  If you don't know what you're doing, 
   * use compile() instead.
   */
  def buildIterator(oper: Operator): ResultIterator =
  {
    if(CTables.isProbabilistic(oper)){
      oper match { 
        case Project(cols, src) =>
          val inputIterator = buildIterator(src);
          // println("Compiled ["+inputIterator.schema+"]: \n"+src)
          new ProjectionResultIterator(
            db,
            inputIterator,
            cols.map( (x) => (x.column, x.input) )
          );
          
        case _ =>
          throw new SQLException("Called buildIterator without calling percolate\n"+oper);
      }
      
      
    } else {
      db.query(db.convert(oper));
    }
  }

  /**
   * This method implements the current ghetto-ish UI for analyzing 
   * uncertainty in query results.  The UI works by providing top-level
   * aggregate-style methods for doing analysis:
   *   - BOUNDS(A): Hard upper- and lower-bounds for 'A'
   *   - CONF():    The confidence of the specified row.
   *
   * Each of these analysis functions can be expanded out into an equivalent
   * Expression (or multiple Expressions).  This function does a quick pass
   * over the top-level Project (if one exists), and replaces any top-level
   * calls to any of the above functions with their flattened equivalents.
   *
   * Preconditions:
   *   - oper must have been passed through CTPercolator.percolate
   */
  def compileAnalysis(oper: Operator): Operator =
  {
    oper match {
      case p @ Project(cols, src) => {
          return Project(
            cols.map( _ match { case ProjectArg(name,expr) => 
              expr match {
                case Function("BOUNDS", subexp) => {
                  if(subexp.length != 1){
                    throw new SQLException(
                      "BOUNDS() expects 1 argument, got "+subexp.length);
                  }
                  val bounds = 
                    CTBounds.compile(subexp(0))
                  List(
                    ProjectArg(name+"_MIN", bounds._1), 
                    ProjectArg(name+"_MAX", bounds._2)
                  )
                }

                // case Function("CONF", _) => {
                //   CTAnalyzer.compileConfidence(p.get(CTables.confidenceColumn))
                // }

                case _ => List(ProjectArg(name, expr))
              }
            }).flatten,
            src
          )
        }

      case _ =>
        if(CTables.isProbabilistic(oper)){
          throw new SQLException("Called compileAnalysis without calling percolate\n"+oper);
        } else { oper }
    }
  }
}