package mimir.sql;

import java.sql.SQLException
import java.util

import mimir.Database
import mimir.algebra._
import mimir.provenance._
import mimir.optimizer.operator.{InlineProjections, PushdownSelections}
import mimir.util.SqlUtils

import com.typesafe.scalalogging.slf4j.LazyLogging

import sparsity.statement.Statement
import sparsity.Name

import scala.collection.JavaConversions._

import sparsity.select.SelectBody

sealed abstract class TargetClause
// case class AnnotateTarget(invisSch:Seq[(ProjectArg, (String,Type), String)]) extends TargetClause
case class ProjectTarget(cols:Seq[ProjectArg]) extends TargetClause
case class AggregateTarget(gbCols:Seq[Var], aggCols:Seq[AggFunction]) extends TargetClause
case class AllTarget() extends TargetClause


/**
 * Utility methods for converting from RA Operators back into JSqlParser's Select objects
 */
class RAToSql(db: Database) 
  extends LazyLogging 
{

  /**
   * An optimizing rewrite to align the expected and real schemas of table operators
   *
   * RA Table operators are allowed to define their own naming conventions.  This
   * forces us into an ugly hack where we need to wrap each table access in a nested 
   * select.  These naming rewrites can sometimes be pulled out into the parent object
   * by wrapping the table operator in a project that handles the renaming.  This rewrite 
   * does so.
   */
  def standardizeTables(oper: Operator): Operator = 
  {
    oper match {
      case Table(name, source, tgtSch, tgtMetadata) => {
        val realSch = db.catalog.tableSchema(source, name) match {
            case Some(realSch) => realSch
            case None => throw new SQLException(s"Unknown Table '$source.$name'");
          }
        val schMap = tgtSch.map(_._1).zip(realSch.map(_._1)).map ( 
          { case (tgt, real)  => ProjectArg(tgt, Var(real)) }
        )
        val metadata = tgtMetadata.map( { 
          case (out, Var(in), t) => ((in, Var(in), t), ProjectArg(out, Var(in))) 
          case (out, RowIdVar(), t) => ((ID("ROWID"), RowIdVar(), t), ProjectArg(out, Var(ID("ROWID")))) 
          case (o, i, t) => throw new SQLException(s"Unsupported Metadata: $o <- $i:$t")
        })
        Project(
          schMap ++ metadata.map(_._2),
          Table(name, source, realSch, metadata.map(_._1))
        )
      }
      case _ => oper.rebuild(oper.children.map(standardizeTables(_)))
    }
  }

  def apply(oper: Operator) = convert(oper)
  def apply(expr: Expression) = convert(expr)
  def apply(expr: Expression, sources: Seq[(Name,Seq[Name])]) = convert(expr, sources)

  /**
   * [use case] Convert an operator tree into JSQLParser's SelectBody type.
   */
  def convert(oper: Operator): SelectBody = 
  {
    // The actual recursive conversion is factored out into a separate fn
    // so that we can do a little preprocessing.
    logger.debug(s"PRE-CONVERT: $oper")

    // standardizeTables adds a new layer of projections that we may be
    // able to optimize away.
    val optimized = 
      InlineProjections(PushdownSelections(oper))

    // println("OPTIMIZED: "+optimized)

    // and then actually do the conversion
    makeSelect(optimized)
  }

  def replaceUnion(
    target:SelectBody, 
    union: (sparsity.select.Union.Type, SelectBody)
  ): SelectBody =
  {
    SelectBody(
      distinct = target.distinct,
      target   = target.target,
      from     = target.from,
      where    = target.where,
      groupBy  = target.groupBy,
      having   = target.having,
      orderBy  = target.orderBy,
      limit    = target.limit,
      offset   = target.offset,
      union    = Some(union)
    )
  }

  def assembleUnion(first: SelectBody, rest: Seq[SelectBody]): SelectBody =
  {
    if(rest.isEmpty){ return first }
    first.union match { 
      case Some((t, subq)) => 
        replaceUnion(subq, (t, assembleUnion(subq, rest)))
      case None => 
        replaceUnion(first, (sparsity.select.Union.All, assembleUnion(rest.head, rest.tail)))
    }
  }

  /**
   * Step 1: Strip UNIONs off the top of the operator stack
   * 
   * These get converted to JSqlParser UNIONs.  Both branches invoke step 2
   */
  def makeSelect(oper:Operator): SelectBody =
  {
    logger.trace(s"makeSelect: \n$oper")
    val unionClauses = OperatorUtils.extractUnionClauses(oper)

    val unionSelects = unionClauses.map { makeSimpleSelect(_) }
    assembleUnion(unionSelects.head, unionSelects.tail)
  }

  /**
   * Step 2: Unwrap an operator stack into a PlainSelect
   *
   * The optimal case here is when operators are organized into
   * the form:
   *    Limit(Sort(Project/Aggregate(Select(Join(...)))))
   * If these are out of order, we'll need to wrap them in nested 
   * selects... but that's ok.  We pick off as many of them as we
   * can and then stick them into a plain select
   *
   * Note that operators are unwrapped outside in, so they need to be
   * applied in reverse order of how they are evaluated.
   */
  private def makeSimpleSelect(oper:Operator): SelectBody =
  {
    var head = oper
    logger.debug("Assembling Plain Select:\n"+oper)

    // Limit clause is the final processing step, so we handle
    // it first.
    val (limitClause, offsetClause):(Option[Long], Option[Long]) = 
      head match {
        case Limit(offset, limit, src) => {
          logger.debug("Assembling Plain Select: Including a LIMIT")
          ////// Remember to strip the limit operator off //////
          head = src
          (limit, if(offset > 0) { Some(offset) } else { None })
        }
        case _ => (None, None)
      }

    // Sort comes after limit, but before Project/Select.
    // We need to get the per-source column bindings before actually adding the
    // clause, so save the clause until we have those.
    //
    // There's also the oddity that ORDER BY behaves differently for aggregate
    // and non-aggregate queries.  That is, for aggregate queries, ORDER BY uses
    // the schema defined in the target clause.  For non-aggregate queries, 
    // ORDER BY is *supposed* to use the schema of the source columns (although
    // some databases allow you to use the target column names too).  Because this
    // means a different ordering Sort(Aggregate(...)) vs Project(Sort(...)), we
    // simply assume that the projection can be inlined.  
    val sortColumns: Seq[SortColumn] = 
      head match {
        case Sort(cols, src) => {
          head = src; 
          logger.debug("Assembling Plain Select: Will include a SORT: "+cols); 
          cols
        }
        case _ => Seq()
      }

    // Project/Aggregate is next... Don't actually convert them yet, but 
    // pull them off the stack and save the arguments
    //
    // We also save a set of bindings to rewrite the Sort clause if needed
    //
    val (preRenderTarget:TargetClause, sortBindings:Map[ID,Expression]) = 
      head match {
        case p@Project(cols, src)              => {
          logger.debug("Assembling Plain Select: Target is a flat projection")
          head = src; 
          (ProjectTarget(cols), p.bindings)
        }
        case Aggregate(gbCols, aggCols, src) => {
          logger.debug("Assembling Plain Select: Target is an aggregation")
          head = src; 
          (AggregateTarget(gbCols, aggCols), Map())
        }
        case _                               => {
          logger.debug("Assembling Plain Select: Target involves no computation")
          (AllTarget(), Map())
        }
      }

    // Strip off the sources, select condition(s) and so forth
    val (condition, froms) = extractSelectsAndJoins(head)

    // Extract the synthesized table names
    val schemas: Seq[(Name, Seq[Name])] = 
      froms.flatMap { SqlUtils.getSchemas(_, db) }

    // Sanity check...
    val extractedSchema = schemas.flatMap(_._2).toSet
    val expectedSchema:Set[Name] = preRenderTarget match { 
      //case AnnotateTarget(invisScm) => head.columnNames.union(invisScm.map(invisCol => ExpressionUtils.getColumns(invisCol._1.expression))).toSet
      case ProjectTarget(cols) => 
          cols.flatMap { col => ExpressionUtils.getColumns(col.expression) }
              .map { col => col.quoted }
              .toSet
      case AggregateTarget(gbCols, aggCols) => 
          gbCols.map { col => col.name.quoted }
                .toSet ++ 
          aggCols.flatMap { agg => agg.args
                                      .flatMap { arg => ExpressionUtils.getColumns(arg) } }
                                      .map { col => col.quoted }
                 .toSet
      case AllTarget() => head.columnNames
                              .map { col => col.quoted }
                              .toSet
    }
    if(!(expectedSchema -- extractedSchema).isEmpty){
      throw new SQLException(s"Error Extracting Joins!\nExpected: $expectedSchema\nGot: $extractedSchema\nMissing: ${expectedSchema -- extractedSchema}\n$head\n${froms.mkString("\n")}")
    }

    // Add the WHERE clause if needed
    val whereClause:Option[sparsity.expression.Expression] = condition match {
      case BoolPrimitive(true) => None
      case _ => {
        logger.debug(s"Assembling Plain Select: Target has a WHERE ($condition)")
        Some(convert(condition, schemas))
      }
    }

    // Apply the ORDER BY clause if we found one earlier
    // Remember that the clause may have been further transformed if we hit a 
    // projection instead of an aggregation.
    val sortOrder = sortColumns.map { col => 
      sparsity.select.OrderBy(
        convert(Eval.inline(col.expression, sortBindings), schemas),
        col.ascending
      )
    }
    logger.debug(s"Assembling Plain Select: ORDER BY: "+sortOrder)

    // Finally, generate the target clause
    val (target, groupBy): (
      Seq[sparsity.select.SelectTarget], 
      Option[Seq[sparsity.expression.Expression]]
    ) = preRenderTarget match { 

      case ProjectTarget(cols) => {
        (
          cols.map { col => 
            sparsity.select.SelectExpression(
              convert(col.expression, schemas), 
              Some(Name(col.name.id, true))
            ) 
          },
          None
        )
      }

      case AggregateTarget(gbCols, aggCols) => {
        val gbConverted = gbCols.map { convert(_, schemas) }
        val gbTargets = gbConverted.map { sparsity.select.SelectExpression(_) }
        val aggTargets = aggCols.map( agg => {
          sparsity.select.SelectExpression(
            sparsity.expression.Function(
              Name(agg.function.id, true),
              ( if(agg.function.id == "count") { None }
                else { Some(agg.args.map { convert(_, schemas) }) }
              ),
              agg.distinct
            ),
            Some(Name(agg.alias.id, true))
          )
        })

        (
          (gbTargets ++ aggTargets),
          (if(gbConverted.isEmpty) { None } else { Some(gbConverted) })
        )
      }

      case AllTarget() => {
        ( 
          Seq(sparsity.select.SelectAll()),
          None
        )
      }
    }

    return sparsity.select.SelectBody(
      target = target,
      limit = limitClause,
      offset = offsetClause,
      where = whereClause,
      orderBy = sortOrder,
      from = froms,
      groupBy = groupBy
    )
  }

  /**
   * Step 3: Build a FromItem Tree
   *
   * Selects, Joins, Tables, etc.. can be stacked into an odd tree 
   * structure.  This method simultaneously pulls up Selects, while
   * converting Joins, Tables, etc... into the corresponding 
   * JSqlParser FromItem tree.  
   * 
   * If we get something that doesn't map to a FromItem, the conversion
   * punts back up to step 1.
   */
  private def extractSelectsAndJoins(oper: Operator): 
    (Expression, Seq[sparsity.select.FromElement]) =
  {
    oper match {
      case Select(cond, source) =>
        val (childCond, froms) = 
          extractSelectsAndJoins(source)
        (
          ExpressionUtils.makeAnd(cond, childCond),
          froms
        )
        
      /*case Annotate(subj,invisScm) => {
          /*subj match {
            case Table(name, alias, sch, metadata) => {
              extractSelectsAndJoins(Table(name, alias, sch, metadata.union(invisScm.map(f => (f._2._1, f._1.expression, f._2._2)))))
            }
            case _ => extractSelectsAndJoins(subj)
          }*/
        extractSelectsAndJoins(Project(invisScm.map( _._1), subj))
      }*/

      case Join(lhs, rhs) =>
        val (lhsCond, lhsFroms) = extractSelectsAndJoins(lhs)
        val (rhsCond, rhsFroms) = extractSelectsAndJoins(rhs)
        (
          ExpressionUtils.makeAnd(lhsCond, rhsCond),
          lhsFroms ++ rhsFroms
        )

      case LeftOuterJoin(lhs, rhs, cond) => 
        val lhsFrom = makeSubSelect(lhs)
        val rhsFrom = makeSubSelect(rhs)
        val schemas = SqlUtils.getSchemas(lhsFrom, db)++
                      SqlUtils.getSchemas(rhsFrom, db)
        val condition = convert(cond, schemas)
        val joinItem = 
          sparsity.select.FromJoin(
            lhsFrom,
            rhsFrom,
            t = sparsity.select.Join.LeftOuter,
            on = condition
          )

        (
          BoolPrimitive(true),
          Seq(joinItem)
        )

      case Table(name, source, tgtSch, metadata) =>
        val realSch = db.catalog.tableSchema(source, name) match {
            case Some(realSch) => realSch
            case None => throw new SQLException(s"Unknown Table `$source`.`$name`");
          }
        // Since Mimir's RA tree structure has no real notion of aliasing,
        // it's only really safe to inline tables directly into a query
        // when tgtSch == realSch.  Eventually, we should add some sort of
        // rewrite that tacks on aliasing metadata... but for now let's see
        // how much milage we can get out of this simple check.
        if(realSch.map(_._1).             // Take names from the real schema
            zip(tgtSch.map(_._1)).        // Align with names from the target schema
            forall( { case (real,tgt) => real.equals(tgt) } )
                                          // Ensure that both are equivalent.
          && metadata.forall {            // And make sure only standardized metadata are preserved
                case (ID("ROWID"), RowIdVar(), _) => true
                case _ => false
              }
        ){ 
          // If they are equivalent, then...
          (
            BoolPrimitive(true), 
            Seq(new sparsity.select.FromTable(None, Name(name.id, true), None))
          )
        } else {
          // If they're not equivalent, revert to old behavior
          (
            BoolPrimitive(true), 
            Seq(makeSubSelect(standardizeTables(oper)))
          )
        }
        
      case HardTable(schema,data) => {
        val unionChain = data.foldRight(None:Option[sparsity.select.SelectBody]) { 
          case (row, nextSelectBody) => 
            Some(
              SelectBody(
                target = schema.zip(row).map { 
                  case ((col, _), v) =>
                    sparsity.select.SelectExpression(convert(v), Some(col.quoted))
                },
                union = nextSelectBody.map { (sparsity.select.Union.All, _) }
              )
            )
        }

        val query = unionChain match {
          case Some(query) => query
          case None => 
            SelectBody(
              target = schema.map { case (col, _) => 
                        sparsity.select.SelectExpression(sparsity.expression.NullPrimitive(), Some(col.quoted))
              },
              where = Some(sparsity.expression.Comparison(
                sparsity.expression.LongPrimitive(1),
                sparsity.expression.Comparison.Neq,
                sparsity.expression.LongPrimitive(1)
              ))
            )
        }

        // might need to do something to play nice with oracle here like setFromItem(new Table(null, "dual"))
        (BoolPrimitive(true), Seq(sparsity.select.FromSelect(query, Name("SINGLETON"))))
      }

      case View(_, query, _) => 
        logger.warn("Inlined view when constructing SQL: RAToSQL will not use materialized views")
        extractSelectsAndJoins(query)

      case LensView(_, _, query, _) => 
        logger.warn("Inlined view when constructing SQL: RAToSQL will not use materialized views")
        extractSelectsAndJoins(query)

      case _ => (BoolPrimitive(true), Seq(makeSubSelect(oper)))
    }
  }
  
  

  /**
   * Punt an Operator conversion back to step 1 and make a SubSelect
   *
   * If Step 3 hits something it can't convert directly to a FromItem, 
   * we restart the conversion process by going back to step 1 to wrap
   * the operator in a nested Select.  
   *
   * The nested select (SubSelect) needs to be assigned an alias, which
   * we assign using the (guaranteed to be unique) first element of the
   * schema.
   */
  def makeSubSelect(oper: Operator) =
    sparsity.select.FromSelect(
      makeSelect(oper),
      Name("SUBQ_"+oper.columnNames.head, true)
    )

  /**
   * Make sure that the schemas of union elements follow the same order
   */
  private def alignUnionOrders(clauses: Seq[Operator]): Seq[Operator] =
  {
    val targetSchema = clauses.head.columnNames
    clauses.map { clause =>
      if(clause.columnNames.equals(targetSchema)){
        clause
      } else {
        Project(
          targetSchema.map( col => ProjectArg(col, Var(col)) ),
          clause
        )
      }
    }
  }

  def convert(e: Expression): sparsity.expression.Expression = {
    convert(e, Seq())
  }

  def convert(
    e: Expression, 
    sources: Seq[(Name,Seq[Name])]
  ): sparsity.expression.Expression = {
    e match {
      case IntPrimitive(v)     =>  sparsity.expression.LongPrimitive(v)
      case StringPrimitive(v)  =>  sparsity.expression.StringPrimitive(v)
      case FloatPrimitive(v)   =>  sparsity.expression.DoublePrimitive(v)
      case RowIdPrimitive(v)   =>  sparsity.expression.StringPrimitive(v)
      case TypePrimitive(t)    =>  sparsity.expression.StringPrimitive(t.toString())
      case BoolPrimitive(true) =>  
        sparsity.expression.Comparison(
          sparsity.expression.LongPrimitive(1),
          sparsity.expression.Comparison.Eq,
          sparsity.expression.LongPrimitive(1)
        )
      case BoolPrimitive(false) =>
        sparsity.expression.Comparison(
          sparsity.expression.LongPrimitive(1),
          sparsity.expression.Comparison.Neq,
          sparsity.expression.LongPrimitive(1)
        )
      case NullPrimitive() => sparsity.expression.NullPrimitive()
      case DatePrimitive(y,m,d) => 
        sparsity.expression.Function(Name("DATE"),
          Some(Seq(sparsity.expression.StringPrimitive("%04d-%02d-%02d".format(y, m, d))))
        )
      case Comparison(op, l, r)  => 
        sparsity.expression.Comparison(convert(l, sources), op, convert(r, sources))
      case Arithmetic(op, l, r)  => 
        sparsity.expression.Arithmetic(convert(l, sources), op, convert(r, sources))
      case Var(n) => 
        convertColumn(n, sources)
      case JDBCVar(t) => 
        sparsity.expression.JDBCVar()
      case Conditional(_, _, _) => {
        val (whenClauses, elseClause) = ExpressionUtils.foldConditionalsToCase(e)
        sparsity.expression.CaseWhenElse(
          None,
          whenClauses.map { case (when, then) => (
            convert(when, sources), 
            convert(then, sources)
          )},
          convert(elseClause, sources)
        )
      }
      case IsNullExpression(subexp) => 
        sparsity.expression.IsNull(convert(subexp, sources))
      case Not(subexp) => 
        sparsity.expression.Not(convert(subexp, sources))
      case mimir.algebra.Function(ID("cast"), Seq(body_arg, TypePrimitive(t))) => 
        sparsity.expression.Cast(
          convert(body_arg, sources), 
          Name(t.toString, true)
        )
      case mimir.algebra.Function(ID("cast"), _) => 
        throw new SQLException("Invalid Cast: "+e)
      case mimir.algebra.Function(fname, fargs) => 
        sparsity.expression.Function(
          fname.quoted,
          Some(fargs.map { convert(_, sources) })
        )
    }
  }

  private def convertColumn(
    n:ID, 
    sources: Seq[(Name,Seq[Name])]
  ): sparsity.expression.Column =
  {
    val src = sources.find {
      case (_, vars) => vars.exists { n.equals(_) }
    }
    if(src.isEmpty)
      throw new SQLException("Could not find appropriate source for '"+n+"' in "+sources)
    sparsity.expression.Column(Name(n.id, true), Some(src.get._1))
  }

}
