package mimir.exec

;

import java.sql._
import java.util

import mimir.Database
import mimir.algebra.Union
import mimir.algebra._
import mimir.ctables._
import mimir.optimizer._
import mimir.lenses.TypeCastModel
import net.sf.jsqlparser.statement.select._
;

class Compiler(db: Database) {

  val ndBuildOpts = Map[NonDeterminism.Strat, (List[Operator => Operator])](
    (NonDeterminism.Classic, List[Operator => Operator](
      CTPercolator.percolate _
    )),
    (NonDeterminism.Partition, List[Operator => Operator](
      CTPercolator.percolate _,
      CTPartition.partition _
    )),
    (NonDeterminism.Inline, List[Operator => Operator](
      CTPercolator.percolate _, // Partition Det & Nondet query fragments
      PushdownSelections.optimize _
    )),
    (NonDeterminism.Hybrid, List[Operator => Operator](
      CTPercolator.percolate _,
      CTPartition.partition _,
      PushdownSelections.optimize _
    ))
  )

  val standardPostOptimizations = List[Operator => Operator](
    InlineProjections.optimize _,
    compileAnalysis _         // Transform BOUNDS(), CONF(), etc... into actual expressions that SQL can understand
  )

  def standardOptimizations: List[Operator => Operator] = {
    ndBuildOpts(db.nonDeterminismStrategy) ++ 
      standardPostOptimizations
  }

  /**
   * Perform a full end-end compilation pass.  Return an iterator over
   * the result set.
   */
  def compile(oper: Operator): ResultIterator =
    compile(oper, standardOptimizations)

  /**
   * Perform a full end-end compilation pass.  Return an iterator over
   * the result set.  Use the specified non-determinism strategy
   */
  def compile(oper: Operator, ndStrat: NonDeterminism.Strat): ResultIterator =
    compile(oper, ndBuildOpts(ndStrat)++standardPostOptimizations)

  /**
   * Perform a full end-end compilation pass.  Return an iterator over
   * the result set.  Use only the specified list of optimizations.
   */
  def compile(oper: Operator, opts: List[Operator => Operator]): ResultIterator = 
  {
    val optimizedOper = optimize(oper, opts)
    // println(optimizedOper)
    // Finally build the iterator
    buildIterator(optimizedOper)
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
          // println("Compiled ["+inputIterator.schema+"]: \n"+oper)
          new ProjectionResultIterator(
            db,
            inputIterator,
            cols.map((x) => (x.column, x.input))
          );

        case mimir.algebra.Union(lhs, rhs) =>
          new BagUnionResultIterator(
            buildIterator(lhs),
            buildIterator(rhs)
          );

        case _ => buildInlinedIterator(oper)
      }


    } else {
      val operWithProvenance = CTPercolator.propagateRowIDs(oper, true);
      val results = db.backend.execute(
        db.convert(operWithProvenance)
      )
      val schemaList = operWithProvenance.schema
      val schema = schemaList.
                      map( _._1 ).
                      zipWithIndex.
                      toMap

      if(!schema.contains(CTPercolator.ROWID_KEY)){
        throw new SQLException("ERROR: No "+CTPercolator.ROWID_KEY+" in "+schema+"\n"+operWithProvenance);
      }
      new ResultSetIterator(
        results, 
        schemaList.toMap,
        oper.schema.map( _._1 ).map( schema(_) ),
        List(schema(CTPercolator.ROWID_KEY))
      )
    }
  }

  def buildInlinedIterator(oper: Operator): ResultIterator =
  {
    // println("BASE: "+oper)
    val operWithRowIDs = 
      CTPercolator.propagateRowIDs(oper, true)
    // println("ROWIDs: "+operWithRowIDs)
    val (operatorWithDeterminism, columnDetExprs, rowDetExpr) =
      CTPercolator.percolateLite(operWithRowIDs)
    // println("Determinism: "+operatorWithDeterminism)
    val inlinedOperator =
      InlineVGTerms.optimize(operatorWithDeterminism)
    // println("Inlined: "+inlinedOperator)
    val schema = oper.schema.filter(_._1 != CTPercolator.ROWID_KEY);

    new NDInlineResultIterator(
      db.query(db.convert(inlinedOperator)), 
      schema,
      schema.map(_._1).map(columnDetExprs(_)), 
      rowDetExpr,
      Var(CTPercolator.ROWID_KEY)
    )

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

object NonDeterminism extends Enumeration {
  type Strat = Value
  val Classic, Partition, Inline, Hybrid = Value
}