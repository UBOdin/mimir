package mimir.exec

;

import java.sql._

import mimir.Database
import mimir.algebra._
import mimir.ctables._;

class Compiler(db: Database) {

  val standardOptimizations =
    List[Operator => Operator](
      CTPercolator.percolate _, // Partition Det & Nondet query fragments
      optimize _,               // Basic Optimizations
      compileAnalysis _         // Transform BOUNDS(), CONF(), etc... into actual expressions that SQL can understand
    )

  /**
   * Perform a full end-end compilation pass.  Return an iterator over
   * the result set.  
   */
  def compile(oper: Operator): ResultIterator =
    compile(oper, standardOptimizations)

  /**
   * Perform a full end-end compilation pass.  Return an iterator over
   * the result set.  Use only the specified list of optimizations.
   */
  def compile(oper: Operator, opts: List[Operator => Operator]):
  ResultIterator = {
    val optimizedOper = opts.foldLeft(oper)((o, fn) => fn(o))
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
   * - If the operator is probabilistic, it must have already been
   * passed through CTPercolator.percolate().
   * - If the operator contains analysis terms, it must have already
   * been passed through compileAnalysis().
   *
   * Basically... there's relatively little reason that you'd want to
   * call this method directly.  If you don't know what you're doing, 
   * use compile() instead.
   */
  def buildIterator(oper: Operator): ResultIterator = {
    if (CTables.isProbabilistic(oper)) {
      oper match {
        case Project(cols, src) =>
          val inputIterator = buildIterator(src);
          // println("Compiled ["+inputIterator.schema+"]: \n"+src)
          new ProjectionResultIterator(
            db,
            inputIterator,
            cols.map((x) => (x.column, x.input))
          );

        case _ =>
          throw new SQLException("Called buildIterator without calling percolate\n" + oper);
      }


    } else {
      db.query(db.convert(oper));
    }
  }

  /**
   * This method implements the current ghetto-ish UI for analyzing 
   * uncertainty in query results.  The UI works by providing top-level
   * aggregate-style methods for doing analysis:
   * - BOUNDS(A): Hard upper- and lower-bounds for 'A'
   * - CONF(A): The confidence interval for 'A'
   * - VAR(A): Variance of 'A'
   * - SAMPLE(A): Produces a sample from one possible world of evaluating 'A'
   *
   * Each of these analysis functions can be expanded out into an equivalent
   * Expression (or multiple Expressions).  This function does a quick pass
   * over the top-level Project (if one exists), and replaces any top-level
   * calls to any of the above functions with their flattened equivalents.
   *
   * Preconditions:
   * - oper must have been passed through CTPercolator.percolate
   */
  def compileAnalysis(oper: Operator): Operator = {
    oper match {
      case p@Project(cols, src) => {
        Project(
          cols.flatMap {
            case ProjectArg(name, expr) =>
              expr match {
                case Function("BOUNDS", subexp) => {
                  if (subexp.length != 1)
                    throw new SQLException("BOUNDS() expects 1 argument, got " + subexp.length)
                  val bounds = CTBounds.compile(subexp(0))
                  List(
                    ProjectArg(name + "_MIN", bounds._1),
                    ProjectArg(name + "_MAX", bounds._2)
                  )
                }

                case Function("CONF", subexp) => {
                  if (subexp.length != 1)
                    throw new SQLException("CONF() expects 1 argument, got " + subexp.length)
                  val ex = CTAnalyzer.compileConfidence(subexp(0))
                  List(ProjectArg(name + "_CONF_MIN", ex._1),
                    ProjectArg(name + "_CONF_MAX", ex._2))
                }

                case Function("VAR", subexp) => {
                  if (subexp.length != 1)
                    throw new SQLException("VAR() expects 1 argument, got " + subexp.length)
                  val ex = CTAnalyzer.compileVariance(subexp(0))
                  List(ProjectArg(name + "_VAR", ex))
                }

                case Function("SAMPLE", subexp) => {
                  if (subexp.length != 1)
                    throw new SQLException("SAMPLE() expects 1 argument, got " + subexp.length)
                  val ex = CTAnalyzer.compileRowConfidence(subexp(0))
                  List(ProjectArg(name + "_SAMPLE", ex))
                }

                case Function("CONFIDENCE", subexp) => {

                  List(ProjectArg(name, expr))
                }

                case _ => List(ProjectArg(name, expr))
              }
          },
          src
        )
      }

      case _ =>
        if (CTables.isProbabilistic(oper)) {
          throw new SQLException("Called compileAnalysis without calling percolate\n" + oper);
        } else {
          oper
        }
    }
  }
}