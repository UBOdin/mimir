package mimir.exec.shortcut

/**
 * Compiler for shortcutting Spark execution. 
 * 
 * Spark has a really high start-up cost.  This is acceptable in its target use case (big data), 
 * but makes it painfully slow on small datasets.  The shortcut compiler is a first-hack attempt at
 * working around this limitation.  When a query is deemed to be "small" (more on this shortly), 
 * the shortcut compiler assembles an iterator chain that can evaluate the entire query internally,
 * (mostly) within Mimir.  
 * 
 * Spark is still responsible for data loading (for now), but this is usually cheap once the table
 * load query is cached.  Everything else is evaluated by an iterator chain (see 
 * mimir.exec.result.ResultIterator).
 * 
 * Limitations of ShortcutCompiler (including TODOs):
 *   - ShortcutCompiler targets purely in-memory queries and is only intended for use on "small" 
 *     datsets.  It is not meant to be a full relational database implementation, just a way to get
 *     quick responses on datasets where Spark's startup costs are too expensive.
 *   - ShortcutCompiler, as of the time of writing this comment is NOT meant to produce anytihng 
 *     remotely resembling an optimal excution plan.  Since it is (presently) only targetted at 
 *     small datasets, a precise query optimizer is pure overhead / complexity.  
 *   - ShortcutCompiler does not presently have a very rigorous definition of "small".  It makes a
 *     (VERY) pessimistic assumption that all operators have a selectivity of 1 (pass all tuples) 
 *     and uses a threshold size to decide whether the final result is too big.  Eventually we 
 *     will probably want a proper cardinality/cost estimator to replace it (TODO), but this is a 
 *     huge can of worms that we should think very very very hard about opening.
 *   - SortcutCompiler is NOT a hybrid database engine.  Either we execute the query entirely in
 *     Spark, or entirely with Shortcut.  This is not a strict requirement.  It would be nice to
 *     (a) allow Mimir to seamlessly take over from Spark once Spark produces a sufficiently small 
 *     Dataframe (TODO), or (b) allow Mimir to fail over to Spark if cardinality estimation 
 *     under-estimates (TODO).  However, both of these are likely to require a significant 
 *     engineering effort and produce a significant increase in code complexity that, at the time
 *     of writing this comment, we don't have a sufficient need to justify.
 */

import mimir.Database
import mimir.algebra._
import mimir.data._
import mimir.exec._
import mimir.exec.result._
import mimir.exec.spark.GetSparkStatistics
import mimir.util.ExperimentalOptions

object ShortcutCompiler
{
  val DEFAULT_THRESHOLD:Long = 100*1024*1024 // 100 MB
  
  /**
   * A simple heuristic analysis of 
   */
  def shouldUseShortcut(db: Database, query: Operator): Boolean = 
  {
    if(ExperimentalOptions.isEnabled("ALLOW-SPARK-SHORTCUT")){
      val thresholdInBytes = 
        Option(
          System.getenv("MIMIR_SHORTCUT_THRESHOLD")
        ).map { _.toLong }
         .getOrElse(DEFAULT_THRESHOLD)

      return thresholdInBytes >= GetSparkStatistics(db, query).sizeInBytes;
    } else {
      false
    }
  }

  def apply(db: Database, query: Operator): ResultIterator =
    compile(
      db, 
      db.compiler.optimize(query), 
      None,
      Seq()
    )

  def compile(
    db: Database,
    query: Operator,
    projection: Option[Seq[(ID, Expression)]],
    selection: Seq[Expression]
  ): ResultIterator =
  {
    def assembleFlatMap(ret: ResultIterator): ResultIterator = {
      val eval = assembleEval(db, ret)

      val (
        projectionGetter: (Row => Seq[Row]),
        projectionSchema: Seq[(ID, Type)]
      ) = 
        projection match {
          case Some(projectionFields) => {
            val tupleGetters = 
              projectionFields.map { case (_, expr) => eval.compile(expr) }
            val sourceSchema = ret.schema.toMap
            val tupleSchema = 
              projectionFields.map { case (col, expr) => col -> db.typechecker.typeOf(expr, sourceSchema(_)) }

            ({ (row: Row) => 
              Seq(LazyRow(row, tupleGetters, Seq(), tupleSchema, Map()))
            }, tupleSchema)
          }
          case None => 
            (
              { (row: Row) => Seq(row) },
              ret.tupleSchema
            )
        }

      val selectionGetter: (Row => Seq[Row]) = 
        if(selection.isEmpty) { projectionGetter } 
        else {
          val selectionGetter = eval.compileForBool(ExpressionUtils.makeAnd(selection))
          ((row: Row) => (if(selectionGetter(row)) { projectionGetter(row) } else { Seq() }))
        }

      new FlatMapIterator(projectionSchema, selectionGetter, ret)
    }

    query match {
      case p@Project(cols, source) => {
        val bindings = p.bindings
        val newProjection = 
          projection match {
            case None => cols.map { col => (col.name, col.expression) }
            case Some(fields) => 
              fields.map { 
                case (name, expr) => (name, Eval.inline(expr, bindings))
              }
          }
        val newSelection = selection.map { Eval.inline(_, bindings) }
        compile(db, source, Some(newProjection), newSelection)
      }

      case Select(condition, source) => {
        compile(
          db,
          source,
          projection,
          selection ++ ExpressionUtils.getConjuncts(condition)
        )
      }

      case u@Union(_, _) => {
        val sourceIterators =
          OperatorUtils.extractUnionClauses(u)
                       .map { compile(db, _, projection, selection) }
        new UnionResultIterator(sourceIterators.iterator)
      }

      case Limit(offset, Some(limit), Sort(order, source)) => {
        val sourceIterator = apply(db, source)
        var topK:Seq[Seq[PrimitiveValue]] = ComputeTopK(
          limit+offset, 
          SortOrdering(db, order, sourceIterator), 
          sourceIterator
        )
        assert(offset == offset.toInt)
        assembleFlatMap(
          new HardTableIterator(
            db.typechecker.schemaOf(source),
            topK.drop(offset.toInt)
          )
        )
      }

      case Limit(offset, limitMaybe, source) => {
        val sourceIterator = apply(db, source)
        assert(offset == offset.toInt)
        if(offset > 0){ sourceIterator.drop(offset.toInt) }
        assembleFlatMap(
          limitMaybe match {
            case None => sourceIterator
            case Some(limit) => new LimitIterator(limit, sourceIterator)
          }
        )
      }
      case Sort(order, source) => {
        val sourceIterator = apply(db, source)
        assembleFlatMap(
          HeapSortIterator(SortOrdering(db, order, sourceIterator), sourceIterator)
        )
      }
      
      case t@Table(providerId, tableId, _, _) => {
        db.catalog.getSchemaProvider(providerId) match {
          case view:ViewSchemaProvider => compile(db, view.view(tableId), projection, selection)
          case plan:LogicalPlanSchemaProvider => db.compiler.sparkBackendRootIterator(t)._2
        }
      }
      case HardTable(schema, table) => new HardTableIterator(schema, table)
      case View(_, source, _) => compile(db, source, projection, selection)
      case AdaptiveView(_, _, source, _) => compile(db, source, projection, selection)

      case Aggregate(_, _, _) => ???
      case Join(_, _) => ???
      case LeftOuterJoin(_, _, _) => ???

    }
  }

  def assembleEval(db: Database, source: ResultIterator): EvalInlined[Row] =
  {
    new EvalInlined(
      source.tupleSchema
            .zipWithIndex
            .map { case ((col, t), i) =>
              (col -> (t, (r:Row) => r(i)))
            }
            .toMap,
      db
    )
  }
}