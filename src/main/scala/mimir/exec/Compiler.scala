package mimir.exec


import java.sql._
import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir.Database
import mimir.algebra.Union
import mimir.algebra._
import mimir.ctables._
import mimir.optimizer._
import mimir.provenance._
import net.sf.jsqlparser.statement.select._

class Compiler(db: Database) extends LazyLogging {

  def standardOptimizations: List[Operator => Operator] = List(
    ProjectRedundantColumns(_),
    InlineProjections(_),
    PushdownSelections(_),
    PropagateEmptyViews(_)
  )

  /**
   * Perform a full end-end compilation pass.  Return an iterator over
   * the result set.  
   */
  def compile(rawOper: Operator, opts: List[Operator => Operator] = standardOptimizations): ResultIterator = 
  {
    // Recursively expand all view tables using mimir.optimizer.ResolveViews
    var oper = ResolveViews(db, rawOper)
    
    logger.debug(s"RAW: $oper")
    
    val compiled = compileInline(oper, opts)
    oper               = compiled._1
    val outputSchema   = compiled._2
    val colDeterminism = compiled._3
    val rowDeterminism = compiled._4
    val provenanceCols = compiled._5

    // The deterministic result set iterator should strip off the 
    // provenance columns.  Figure out which columns need to be
    // kept.  Note that the order here actually matters.
    val tagPlusOutputSchemaNames = 
      outputSchema.map(_._1).toList ++
        colDeterminism.toList.flatMap( x => ExpressionUtils.getColumns(x._2)) ++ 
        ExpressionUtils.getColumns(rowDeterminism)

    // We'll need it a few times, so cache the final operator's schema.
    // This also forces the typechecker to run, so we get a final sanity
    // check on the output of the rewrite rules.
    val finalSchema = oper.schema

    // We'll need to line the attributes in the output up with
    // the order in which the user expects to see them.  Build
    // a lookup table with name + position in the query being execed.
    val finalSchemaOrderLookup = 
      finalSchema.map(_._1).zipWithIndex.toMap
   
    logger.debug(s"SCHEMA: $finalSchema")

    val sql = sqlForBackend(oper, opts)

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

  def compileInline(operRaw: Operator, opts: List[Operator => Operator] = standardOptimizations): (
    Operator,                   // The compiled query
    Seq[(String, Type)],        // The base schema
    Map[String, Expression],    // Column taint annotations
    Expression,                 // Row taint annotation
    Seq[String]                 // Provenance columns
  ) =
  {
    var oper = operRaw

    // We'll need the pristine pre-manipulation schema down the line
    // As a side effect, this also forces the typechecker to run, 
    // acting as a sanity check on the query before we do any serious
    // work.
    val outputSchema = oper.schema;
      
    // The names that the provenance compilation step assigns will
    // be different depending on the structure of the query.  As a 
    // result it is **critical** that this be the first step in 
    // compilation.  
    val provenance = Provenance.compile(oper)
    oper               = provenance._1
    val provenanceCols = provenance._2

    // Tag rows/columns with provenance metadata
    val tagging = CTPercolator.percolateLite(oper, Some(db))
    oper               = tagging._1
    val colDeterminism = tagging._2
    val rowDeterminism = tagging._3

    logger.debug(s"PRE-OPTIMIZED: $oper")

    // Clean things up a little... make the query prettier, tighter, and 
    // faster
    oper = optimize(oper, opts)

    logger.debug(s"OPTIMIZED: $oper")

    // Replace VG-Terms with their "Best Guess values"
    oper = bestGuessQuery(oper)

    logger.debug(s"GUESSED: $oper")

    return (
      oper,
      outputSchema,
      colDeterminism,
      rowDeterminism,
      provenanceCols
    )
  }

  def sqlForBackend(oper: Operator, opts: List[Operator => Operator] = standardOptimizations): SelectBody =
  {
    val optimized = optimize(oper)

    // The final stage is to apply any database-specific rewrites to adapt
    // the query to the quirks of each specific target database.  Each
    // backend defines a specializeQuery method that handles this
    val specialized = db.backend.specializeQuery(optimized)

    logger.debug(s"SPECIALIZED: $specialized")
    
    // Generate the SQL
    val sql = db.ra.convert(oper)

    logger.debug(s"SQL: $sql")

    return sql
  }

  /**
   * Remove all VGTerms in the query and replace them with the 
   * equivalent best guess values
   */
  def bestGuessQuery(oper: Operator): Operator =
  {
    // Remove any VG Terms for which static best-guesses are possible
    // In other words, best guesses that don't depend on which row we're
    // looking at (like the Type Inference or Schema Matching lenses)
    val mostlyDeterministicOper =
      InlineVGTerms(oper)

    // Deal with the remaining VG-Terms.  
    if(db.backend.canHandleVGTerms()){
      // The best way to do this would be a database-specific "BestGuess" 
      // UDF if it's available.
      return mostlyDeterministicOper
    } else {
      // Unfortunately, this UDF may not always be available, so if needed
      // we fall back to the Guess Cache
      val fullyDeterministicOper =
        db.bestGuessCache.rewriteToUseCache(mostlyDeterministicOper)

      return fullyDeterministicOper
    }
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