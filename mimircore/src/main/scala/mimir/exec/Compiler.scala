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
    InlineProjections.optimize _,
    PushdownSelections.optimize _
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

}