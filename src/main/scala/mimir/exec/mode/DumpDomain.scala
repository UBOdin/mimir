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
import mimir.ctables.vgterm.DomainDumper
import mimir.models.Model
import mimir.models.FiniteDiscreteDomain

object DumpDomain
  extends CompileMode[ResultIterator]
  with LazyLogging
{
  type MetadataT = (
    Seq[ID]                 // Provenance columns
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
    Seq[(ID, Type)],        // The base schema
    Map[ID, Expression],    // Column taint annotations
    Expression,                 // Row taint annotation
    Seq[ID]                 // Provenance columns
  ) =
  {
    var oper = operRaw
    val rawColumns = operRaw.columnNames.toSet

    // We'll need the pristine pre-manipulation schema down the line
    // As a side effect, this also forces the typechecker to run, 
    // acting as a sanity check on the query before we do any serious
    // work.
    val outputSchema = db.typechecker.schemaOf(oper)
      
    // The names that the provenance compilation step assigns will
    // be different depending on the structure of the query.  As a 
    // result it is **critical** that this be the first step in 
    // compilation.  
    val provenance = Provenance.compile(oper)

    oper               = provenance._1
    val provenanceCols = provenance._2

    logger.debug(s"WITH-PROVENANCE (${provenanceCols.mkString(", ")}): $oper")


    // Tag rows/columns with provenance metadata
    oper = OperatorDeterminism.compile(oper, db.models.get(_)) 
    val colDeterminism = rawColumns.toSeq.map { OperatorDeterminism.mimirColDeterministicColumn(_) }

    logger.debug(s"PERCOLATED: $oper")

    // It's a bit of a hack for now, but provenance
    // adds determinism columns for provenance metadata, since
    // we have no way to explicitly track what's an annotation
    // and what's "real".  Remove this metadata now...
    val minimalSchema: Set[ID] = 
      operRaw.columnNames.toSet ++ 
      provenanceCols.toSet ++
      ( colDeterminism :+ 
        OperatorDeterminism.mimirRowDeterministicColumnName
      ).toSet


    oper = ProjectRedundantColumns(oper, minimalSchema)

    logger.debug(s"PRE-OPTIMIZED: $oper")

    oper = db.views.resolve(oper)

    logger.debug(s"INLINED: $oper")


    // Replace VG-Terms with their "Best Guess values"
    oper = DomainInlineVGTerms(oper, db)

    logger.debug(s"GUESSED: $oper")

    // Clean things up a little... make the query prettier, tighter, and 
    // faster
    oper = db.compiler.optimize(oper)

    logger.debug(s"OPTIMIZED: $oper")

    return (
      oper, 
      outputSchema,
      rawColumns.toSeq.map { col => col -> Var(OperatorDeterminism.mimirColDeterministicColumn(col)) }.toMap,
      Var(OperatorDeterminism.mimirRowDeterministicColumnName),
      provenanceCols
    )
  }

  def rewrite(db: Database, operRaw: Operator): (Operator, Seq[ID], MetadataT) =
  {
    val (oper, outputSchema, colDeterminism, rowDeterminism, provenanceCols) =
      rewriteRaw(db, operRaw)

    // Finally, fold the annotations back in
    val completeOper =
      Project(
        operRaw.columnNames.map { name => ProjectArg(name, Var(name)) } ++
        colDeterminism.map { case (name, expression) => 
          ProjectArg(ID(OperatorDeterminism.mimirColDeterministicColumnPrefix, name), expression) 
        } ++ Seq(
          ProjectArg(OperatorDeterminism.mimirRowDeterministicColumnName, rowDeterminism),
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

  def wrap(db: Database, results: ResultIterator, query: Operator, meta: MetadataT): ResultIterator =
    results
}

object DomainInlineVGTerms {

	def inline(e: Expression, db: Database): Expression =
	{

		e match {
			case v @ VGTerm(model, idx, args, hints) => {
				val simplifiedChildren = v.children.map(apply(_, db))
				val ret = v.rebuild(simplifiedChildren)

				if(
					ret.args.forall(_.isInstanceOf[PrimitiveValue])
					&& ret.hints.forall(_.isInstanceOf[PrimitiveValue])
				) { 
					val model = db.models.get(v.name)
					val args = ret.args.map { _.asInstanceOf[PrimitiveValue] }
					val hints = ret.args.map { _.asInstanceOf[PrimitiveValue] }

					val domain = model match {
            case finite:( Model with FiniteDiscreteDomain ) =>
              RepairFromList(finite.getDomain(idx, args, hints))
            case _ => 
              RepairByType(model.varType(idx, args.map(_.getType)))
          }
          StringPrimitive(domain.toJSON)
				} else { 
					DomainDumper(
						db.models.get(ret.name),
						ret.idx,
						ret.args,
						ret.hints
					)
				}
			}

			case _ => e.recur(inline(_, db))
		}
	}

	def apply(e: Expression, db: Database): Expression =
	{
		inline(e, db)
	}

	def apply(o: Operator, db: Database): Operator = 
	{
		o.recurExpressions(apply(_, db)).recur(apply(_, db))
	}

}