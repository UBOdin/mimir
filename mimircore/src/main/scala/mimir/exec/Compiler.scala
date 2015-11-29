package mimir.exec

;

import java.sql._
import java.util

import mimir.Database
import mimir.algebra.Union
import mimir.algebra._
import mimir.ctables._
import mimir.lenses.TypeCastModel
import net.sf.jsqlparser.statement.select._
;

class Compiler(db: Database) {

  val standardOptimizations =
    List[Operator => Operator](
      CTPercolator.percolate _, // Partition Det & Nondet query fragments
      // CTPercolator.partitionConstraints _, // Partition constraint col according to data
//      optimize _,               // Basic Optimizations
      CTPartition.partition _,
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
    buildIterator(optimizedOper)
  }

  /**
   * Apply any local optimization rewrites.  Currently a placeholder.
   */
  def optimize(oper: Operator): Operator = oper match {
    /*
     Rewrite TypeInference VGTerms to SQL equivalent cast expression
     */
    case Project(cols, src) =>
      Project(
        cols.zipWithIndex.map{(coli) =>
          coli._1.input match {
            case VGTerm((_, model: TypeCastModel), idx, args) =>
              ProjectArg(coli._1.column, Function("CAST", List(args(1), args(2))))
            case _ =>
              coli._1
          }
        },
        src
     )

    case _ => oper
  }

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

        case mimir.algebra.Union(true, lhs, rhs) =>
          new BagUnionResultIterator(
            buildIterator(lhs),
            buildIterator(rhs)
          );

        case mimir.algebra.Union(false, _, _) =>
          throw new UnsupportedOperationException("UNION DISTINCT unimplemented")

        case _ =>
          throw new SQLException("Called buildIterator without calling percolate\n" + oper);
      }


    } else {
      db.query(db.convert(oper))
    }
  }

  /**
   * This method implements the current ghetto-ish UI for analyzing 
   * uncertainty in query results.  The UI works by providing top-level
   * aggregate-style methods for doing analysis:
   * - BOUNDS(A): Hard upper- and lower-bounds for 'A'
   * - CONFIDENCE(A, PERCENTILE): The confidence interval for 'A'
   * - VAR(A): Variance of 'A'
   * - SAMPLE(A, SEED): Produces a sample from one possible world of evaluating 'A'
   * - PROB(): Gives the probability of a row being in the result set
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

                case Function(CTables.CONFIDENCE, subexp) => {
                  if (subexp.isEmpty)
                    throw new SQLException(CTables.CONFIDENCE + "() expects at 2 arguments" +
                      "(expression, percentile), got " + subexp.length)
                  val percentile = {
                    if(subexp.length == 1)
                      FloatPrimitive(50)
                    else subexp(1)
                  }
                  val ex = CTAnalyzer.compileSample(subexp(0), Var(CTables.SEED_EXP))
                  List(ProjectArg(name + "_CONF", Function(CTables.CONFIDENCE, List(ex, percentile))))
                }

                case Function(CTables.VARIANCE, subexp) => {
                  if (subexp.length != 1)
                    throw new SQLException(CTables.VARIANCE + "() expects 1 argument, got " + subexp.length)
                  val ex = CTAnalyzer.compileSample(subexp(0), Var(CTables.SEED_EXP))
                  List(ProjectArg(name + "_VAR", Function(CTables.VARIANCE, List(ex))))
                }

                case Function("SAMPLE", subexp) => {
                  if (subexp.isEmpty)
                    throw new SQLException("SAMPLE() expects at least 1 argument " +
                      "(expression, seed[optional]), got " + subexp.length)
                  val ex = {
                    if(subexp.length == 1) CTAnalyzer.compileSample(subexp(0))
                    else CTAnalyzer.compileSample(subexp(0), subexp(1))
                  }
                  List(ProjectArg(name + "_SAMPLE", ex))
                }

                case Function(CTables.ROW_PROBABILITY, subexp) => {
                  val exp = p.get(CTables.conditionColumn)
                  exp match {
                    case None => List(ProjectArg(name + "_" + CTables.ROW_PROBABILITY, FloatPrimitive(1)))
                    case Some(cond) => {
                      val func = Function(CTables.ROW_PROBABILITY, List(CTAnalyzer.compileSample(cond, Var(CTables.SEED_EXP))))
                      List(ProjectArg(name + "_" + CTables.ROW_PROBABILITY, func))
                    }
                  }
                }

                case _ => List(ProjectArg(name, expr))
              }
          },
          src
        )
      }

      case u@mimir.algebra.Union(bool, lhs, rhs) =>
        mimir.algebra.Union(bool, compileAnalysis(lhs), compileAnalysis(rhs))

      case _ =>
        if (CTables.isProbabilistic(oper)) {
          throw new SQLException("Called compileAnalysis without calling percolate\n" + oper);
        } else {
          oper
        }
    }
  }
}