package mimir.exec

;

import java.sql._
import java.util

import mimir.sql.IsNullChecker
import mimir.Database
import mimir.algebra.Union
import mimir.algebra._
import mimir.ctables._
import mimir.optimizer._
import mimir.provenance._
import mimir.lenses.TypeCastModel
import net.sf.jsqlparser.statement.select._

class Compiler(db: Database) {

  def standardOptimizations: List[Operator => Operator] = List(
    InlineProjections.optimize _
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
  def compile(oper: Operator, opts: List[Operator => Operator]): ResultIterator = 
  {
    // We'll need the pristine pre-manipulation schema down the line
    // As a side effect, this also forces the typechecker to run, 
    // acting as a sanity check on the query before we do any serious
    // work.
    val outputSchema = oper.schema;

    // The names that the provenance compilation step assigns will
    // be different depending on the structure of the query.  As a 
    // result it is **critical** that this be the first step in 
    // compilation.  
    val (provenanceAwareOper, provenanceCols) =
      Provenance.compile(oper)

    // Tag rows/columns with provenance metadata
    val (taggedOper, colDeterminism, rowDeterminism) =
      CTPercolator.percolateLite(provenanceAwareOper)

    // The deterministic result set iterator should strip off the 
    // provenance columns.  Figure out which columns need to be
    // kept.  Note that the order here actually matters.
    val tagPlusOutputSchemaNames = 
      outputSchema.map(_._1).toList ++
        colDeterminism.toList.flatMap( x => ExpressionUtils.getColumns(x._2)) ++ 
        ExpressionUtils.getColumns(rowDeterminism)

    // Clean things up a little... make the query prettier.
    val optimizedOper = 
      optimize(taggedOper, opts)

    // Remove any VG Terms for which static best-guesses are possible
    // In other words, best guesses that don't depend on which row we're
    // looking at (like the Type Inference or Schema Matching lenses)
    val mostlyDeterministicOper =
      InlineVGTerms.optimize(optimizedOper)

    // Since this operator gets used a few times below, we rename it in
    // case we need to add more stages.  Scalac should be smart enough
    // to optimize the double-ref away.
    val finalOper =
      mostlyDeterministicOper

    // We'll need it a few times, so cache the final operator's schema.
    // This also forces the typechecker to run, so we get a final sanity
    // check on the output of the rewrite rules.
    val finalSchema = 
      finalOper.schema

    // We'll need to line the attributes in the output up with
    // the order in which the user expects to see them.  Build
    // a lookup table with name + position in the query being execed.
    val finalSchemaOrderLookup = 
      finalSchema.map(_._1).zipWithIndex.toMap

    // Generate the SQL
    val sql = 
      db.convert(finalOper)

    // Deploy to the backend
    val results = 
      db.backend.execute(sql)

    // And wrap the results.
    new NonDetIterator(
      new ResultSetIterator(results, 
        finalSchema.toMap,
        tagPlusOutputSchemaNames.map(finalSchemaOrderLookup(_)), 
        provenanceCols.map(finalSchemaOrderLookup(_))
      ),
      outputSchema,
      outputSchema.map(_._1).map(colDeterminism(_)), 
      rowDeterminism
    )
  }

  /**
   * Optimize the query
   */
  def optimize(oper: Operator): Operator = 
    optimize(oper, standardOptimizations)

  /**
   * Optimize the query
   */
  def optimize(oper: Operator, opts: List[Operator => Operator]): Operator =
    opts.foldLeft(oper)((o, fn) => fn(o))

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

              // This is a gnarly hack that needs to be completely rewritten.  Excising for now
              expr match {
                case Function("BOUNDS", subexp) => {
                  List(
                    ProjectArg(name + "_MIN", NullPrimitive()),
                    ProjectArg(name + "_MAX", NullPrimitive())
                  )
                  // if (subexp.length != 1)
                  //   throw new SQLException("BOUNDS() expects 1 argument, got " + subexp.length)
                  // try {
                  //   val bounds = CTBounds.compile(subexp(0))
                  //   List(
                  //     ProjectArg(name + "_MIN", bounds._1),
                  //     ProjectArg(name + "_MAX", bounds._2)
                  //   )
                  // } catch {
                  //   case BoundsUnsupportedException(_,_) =>
                  //     List(
                  //       ProjectArg(name + "_MIN", StringPrimitive("Unknown")),
                  //       ProjectArg(name + "_MAX", StringPrimitive("Unknown"))
                  //     )
                  // }
                }

                case Function(CTables.CONFIDENCE, subexp) => {
                  List(
                    ProjectArg(name + "_CONF", NullPrimitive())
                  )
                  // if (subexp.isEmpty)
                  //   throw new SQLException(CTables.CONFIDENCE + "() expects at 2 arguments" +
                  //     "(expression, percentile), got " + subexp.length)
                  // val percentile = {
                  //   if(subexp.length == 1)
                  //     FloatPrimitive(50)
                  //   else subexp(1)
                  // }
                  // val ex = CTAnalyzer.compileSample(subexp(0), Var(CTables.SEED_EXP))
                  // List(ProjectArg(name + "_CONF", Function(CTables.CONFIDENCE, List(ex, percentile))))
                }

                case Function(CTables.VARIANCE, subexp) => {
                  List(
                    ProjectArg(name + "_VAR", NullPrimitive())
                  )
                  // if (subexp.length != 1)
                  //   throw new SQLException(CTables.VARIANCE + "() expects 1 argument, got " + subexp.length)
                  // val ex = CTAnalyzer.compileSample(subexp(0), Var(CTables.SEED_EXP))
                  // List(ProjectArg(name + "_VAR", Function(CTables.VARIANCE, List(ex))))
                }

                case Function("SAMPLE", subexp) => {
                  List(
                    ProjectArg(name + "_SAMPLE", NullPrimitive())
                  )
                  // if (subexp.isEmpty)
                  //   throw new SQLException("SAMPLE() expects at least 1 argument " +
                  //     "(expression, seed[optional]), got " + subexp.length)
                  // val ex = {
                  //   if(subexp.length == 1) CTAnalyzer.compileSample(subexp(0))
                  //   else CTAnalyzer.compileSample(subexp(0), subexp(1))
                  // }
                  // List(ProjectArg(name + "_SAMPLE", ex))
                }

                case Function(CTables.ROW_PROBABILITY, subexp) => {
                  List(
                    ProjectArg(name + CTables.ROW_PROBABILITY, NullPrimitive())
                  )
                  // val exp = p.get(CTables.conditionColumn)
                  // exp match {
                  //   case None => List(ProjectArg(name + "_" + CTables.ROW_PROBABILITY, FloatPrimitive(1)))
                  //   case Some(cond) => {
                  //     val func = Function(CTables.ROW_PROBABILITY, List(CTAnalyzer.compileSample(cond, Var(CTables.SEED_EXP))))
                  //     List(ProjectArg(name + "_" + CTables.ROW_PROBABILITY, func))
                  //   }
                  // }
                }

                case _ => List(ProjectArg(name, expr))
              }
          },
          src
        )
      }

      case u@mimir.algebra.Union(lhs, rhs) =>
        mimir.algebra.Union(compileAnalysis(lhs), compileAnalysis(rhs))

      case _ =>
        if (CTables.isProbabilistic(oper)) {
          throw new SQLException("Called compileAnalysis without calling percolate\n" + oper);
        } else {
          oper
        }
    }
  }
}