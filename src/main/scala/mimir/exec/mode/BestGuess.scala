package mimir.exec.mode

import java.sql._
import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir.Database
import mimir.algebra._
import mimir.ctables._
import mimir.provenance._
import mimir.optimizer.operator._
import mimir.exec._
import mimir.exec.result._
import mimir.util.ExperimentalOptions

object BestGuess
  extends CompileMode[ResultIterator]
  with LazyLogging
{
  type MetadataT = (
    Seq[String]                 // Provenance columns
  )

  /**
   * Compile the query for best-guess-style evalaution.
   *
   * Includes:
   *  * Provenance Annotations
   *  * Taint Annotations
   *  * One result from the "Best Guess" world.
   */ 
  def rewriteRaw(db: Database, operRaw: Operator): (
    Operator,                   // The compiled query
    Seq[(String, Type)],        // The base schema
    Map[String, Expression],    // Column taint annotations
    Expression,                 // Row taint annotation
    Seq[String]                 // Provenance columns
  ) =
  {
    var oper = operRaw
    val rawColumns = operRaw.columnNames.toSet

    // We'll need the pristine pre-manipulation schema down the line
    // As a side effect, this also forces the typechecker to run, 
    // acting as a sanity check on the query before we do any serious
    // work.
    val outputSchema = db.bestGuessSchema(oper)
      
    // The names that the provenance compilation step assigns will
    // be different depending on the structure of the query.  As a 
    // result it is **critical** that this be the first step in 
    // compilation.  
    val provenance = 
    if(ExperimentalOptions.isEnabled("GPROM-PROVENANCE")
        && db.backend.isInstanceOf[mimir.sql.GProMBackend])
      { Provenance.compileGProM(oper) }
      else { Provenance.compile(oper) }

    oper               = provenance._1
    val provenanceCols = provenance._2

    logger.debug(s"WITH-PROVENANCE (${provenanceCols.mkString(", ")}): $oper")


    // Tag rows/columns with provenance metadata
    val tagging = CTPercolator.percolateLite(oper, db.models.get(_))
    oper               = tagging._1
    val colDeterminism = tagging._2.filter( col => rawColumns(col._1) )
    val rowDeterminism = tagging._3

    logger.debug(s"PERCOLATED: $oper")

    // It's a bit of a hack for now, but provenance
    // adds determinism columns for provenance metadata, since
    // we have no way to explicitly track what's an annotation
    // and what's "real".  Remove this metadata now...
    val minimalSchema: Set[String] = 
      operRaw.columnNames.toSet ++ 
      provenanceCols.toSet ++
      (colDeterminism.map(_._2) ++ Seq(rowDeterminism)).flatMap( ExpressionUtils.getColumns(_) ).toSet


    oper = ProjectRedundantColumns(oper, minimalSchema)

    logger.debug(s"PRE-OPTIMIZED: $oper")

    oper = db.views.resolve(oper)

    logger.debug(s"INLINED: $oper")

    // Clean things up a little... make the query prettier, tighter, and 
    // faster
    oper = db.compiler.optimize(oper)

    logger.debug(s"OPTIMIZED: $oper")

    // Replace VG-Terms with their "Best Guess values"
    oper = bestGuessQuery(db, oper)

    logger.debug(s"GUESSED: $oper")

    return (
      oper, 
      outputSchema,
      colDeterminism,
      rowDeterminism,
      provenanceCols
    )
  }

  def rewrite(db: Database, operRaw: Operator): (Operator, Seq[String], MetadataT) =
  {
    val (oper, outputSchema, colDeterminism, rowDeterminism, provenanceCols) =
      rewriteRaw(db, operRaw)

    // Finally, fold the annotations back in
    val completeOper =
      Project(
        operRaw.columnNames.map { name => ProjectArg(name, Var(name)) } ++
        colDeterminism.map { case (name, expression) => ProjectArg(CTPercolator.mimirColDeterministicColumnPrefix + name, expression) } ++
        Seq(
          ProjectArg(CTPercolator.mimirRowDeterministicColumnName, rowDeterminism),
          ProjectArg(Provenance.rowidColnameBase, Function(Provenance.mergeRowIdFunction, provenanceCols.map( Var(_) ) ))
        ),// ++ provenanceCols.map(pc => ProjectArg(pc,Var(pc))),
        oper
      )

    return (
      completeOper, 
      operRaw.columnNames,
      provenanceCols
    )
  }

  /**
   * Remove all VGTerms in the query and replace them with the 
   * equivalent best guess values
   */
  def bestGuessQuery(db: Database, oper: Operator): Operator =
  {
    // Remove any VG Terms for which static best-guesses are possible
    // In other words, best guesses that don't depend on which row we're
    // looking at (like the Type Inference or Schema Matching lenses)
    val mostlyDeterministicOper =
      InlineVGTerms(oper, db)

    // Deal with the remaining VG-Terms.  
    if(db.backend.canHandleVGTerms){
      // The best way to do this would be a database-specific "BestGuess" 
      // UDF if it's available.
      return mostlyDeterministicOper
    } else {
      throw new RAException("Error, Best Guess Cache Doesn't Work")
    }
  }

  def wrap(db: Database, results: ResultIterator, query: Operator, meta: MetadataT): ResultIterator =
    results
}