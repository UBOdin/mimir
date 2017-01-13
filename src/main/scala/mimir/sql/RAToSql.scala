package mimir.sql;

import java.sql.SQLException
import java.util

import mimir.Database
import mimir.algebra._
import mimir.provenance._
import mimir.optimizer.{InlineProjections, PushdownSelections}
import mimir.util.SqlUtils

import com.typesafe.scalalogging.slf4j.LazyLogging

import net.sf.jsqlparser.expression.operators.arithmetic._
import net.sf.jsqlparser.expression.operators.conditional._
import net.sf.jsqlparser.expression.operators.relational._
import net.sf.jsqlparser.expression.{BinaryExpression, DoubleValue, Function, LongValue, NullValue, InverseExpression, StringValue, WhenClause, JdbcParameter}
import net.sf.jsqlparser.{schema, expression}
import net.sf.jsqlparser.schema.Column
import net.sf.jsqlparser.statement.select.{SelectBody, PlainSelect, SubSelect, SelectExpressionItem, FromItem, SelectItem, SubJoin}

import scala.collection.JavaConversions._

sealed abstract class TargetClause
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
      case Table(name, tgtSch, tgtMetadata) => {
        val realSch = db.getTableSchema(name) match {
          case Some(realSch) => realSch
          case None => throw new SQLException("Unknown Table '"+name+"'");
        }
        val schMap = tgtSch.map(_._1).zip(realSch.map(_._1)).map ( 
          { case (tgt, real)  => ProjectArg(tgt, Var(real)) }
        )
        val metadata = tgtMetadata.map( { 
          case (out, Var(in), t) => ((in, Var(in), t), ProjectArg(out, Var(in))) 
          case (o, i, t) => throw new SQLException("Unsupported Metadata: $o <- $i:$t")
        })
        Project(
          schMap ++ metadata.map(_._2),
          Table(name, realSch, metadata.map(_._1))
        )
      }
      case _ => oper.rebuild(oper.children.map(standardizeTables(_)))
    }
  }

  /**
   * [use case] Convert an operator tree into JSQLParser's SelectBody type.
   */
  def convert(oper: Operator): SelectBody = 
  {
    // The actual recursive conversion is factored out into a separate fn
    // so that we can do a little preprocessing.
    logger.debug(s"PRE-CONVERT: $oper")
    // Start by rewriting table schemas to make it easier to inline them.
    val standardized = standardizeTables(oper)

    // standardizeTables adds a new layer of projections that we may be
    // able to optimize away.
    val optimized = 
      InlineProjections(PushdownSelections(standardized))

    // println("OPTIMIZED: "+optimized)

    // and then actually do the conversion
    makeSelect(optimized)
  }

  /**
   * Step 1: Strip UNIONs off the top of the operator stack
   * 
   * These get converted to JSqlParser UNIONs.  Both branches invoke step 2
   */
  private def makeSelect(oper:Operator): SelectBody =
  {
    oper match {
      case u:Union => {
        var union = new net.sf.jsqlparser.statement.select.Union()
        union.setAll(true);
        union.setDistinct(false);
        union.setPlainSelects(
          OperatorUtils.extractUnionClauses(u).map(makePlainSelect(_))
        )
        return union
      }

      case _ => 
        return makePlainSelect(oper)
    }
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
  private def makePlainSelect(oper:Operator): PlainSelect =
  {
    var head = oper
    val select = new PlainSelect()

    logger.debug("Assembling Plain Select")

    // Limit clause is the final processing step, so we handle
    // it first.
    head match {
      case Limit(offset, maybeCount, src) => {
        logger.debug("Assembling Plain Select: Including a LIMIT")
        val limit = new net.sf.jsqlparser.statement.select.Limit()
        if(offset > 0){ limit.setOffset(offset) }
        maybeCount match {
          case None        => limit.setLimitAll(true)
          case Some(count) => limit.setRowCount(count)
        }
        select.setLimit(limit)

        ////// Remember to strip the limit operator off //////
        head = src
      }
      case _ => ()
    }

    // Sort comes after limit, but before Project/Select
    head match {
      case Sort(cols, src) => {
        logger.debug("Assembling Plain Select: Including a SORT")
        select.setOrderByElements(
          cols.map(col => {
            val ob = new net.sf.jsqlparser.statement.select.OrderByElement()
            ob.setExpression(new net.sf.jsqlparser.schema.Column(
              new net.sf.jsqlparser.schema.Table(null, null), 
              col.name
            ))
            ob.setAsc(col.ascending)
            ob
          })
        )
        ////// Remember to strip the sort operator off //////
        head = src

      }
      case _ => ()
    }

    // Project/Aggregate is next... Don't actually convert them yet, but 
    // pull them off the stack and save the arguments
    val target:TargetClause = 
      head match {
        case Project(cols, src)              => {
          logger.debug("Assembling Plain Select: Target is a flat projection")
          head = src; 
          ProjectTarget(cols)
        }
        case Aggregate(gbCols, aggCols, src) => {
          logger.debug("Assembling Plain Select: Target is an aggregation")
          head = src; 
          AggregateTarget(gbCols, aggCols)
        }
        case _                               => {
          logger.debug("Assembling Plain Select: Target involves no computation")
          AllTarget()
        }
      }

    // Strip off the sources, select condition(s) and so forth
    val (condition, from) = extractSelectsAndJoins(head)

    // Extract the synthesized table names
    val schemas = SqlUtils.getSchemas(from, db)

    // Add the WHERE clause if needed
    condition match {
      case BoolPrimitive(true) => ()
      case _ => {
        logger.debug(s"Assembling Plain Select: Target has a WHERE ($condition)")
        select.setWhere(convert(condition, schemas))
      }
    }

    // Add the FROM clause
    logger.debug(s"Assembling Plain Select: FROM ($from)")
    select.setFromItem(from)

    // Finally, generate the target clause
    target match {
      case ProjectTarget(cols) => {
        select.setSelectItems(
          cols.map( col => 
            makeSelectItem(convert(col.expression, schemas), col.name) )
        )
      }

      case AggregateTarget(gbCols, aggCols) => {
        val gbConverted = gbCols.map(convert(_, schemas).asInstanceOf[Column])
        val gbTargets = gbConverted.map( gb => makeSelectItem(gb, gb.getColumnName) )
        val aggTargets = aggCols.map( agg => {
          val func = new Function()
          func.setName(agg.function)
          func.setParameters(new ExpressionList(
            agg.args.map(convert(_, schemas))))
          func.setDistinct(agg.distinct)

          makeSelectItem(func, agg.alias)
        })
        select.setSelectItems(gbTargets ++ aggTargets)
        if(!gbConverted.isEmpty){ 
          select.setGroupByColumnReferences(gbConverted)
        }
      }

      case AllTarget() => {
        select.setSelectItems(List(new net.sf.jsqlparser.statement.select.AllColumns()))
      }
    }

    return select;
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
    (Expression, FromItem) =
  {
    oper match {
      case Select(cond, source) =>
        val (childCond, from) = 
          extractSelectsAndJoins(source)
        (
          ExpressionUtils.makeAnd(cond, childCond),
          from
        )

      case Join(lhs, rhs) =>
        val (lhsCond, lhsFrom) = extractSelectsAndJoins(lhs)
        val (rhsCond, rhsFrom) = extractSelectsAndJoins(rhs)
        (
          ExpressionUtils.makeAnd(lhsCond, rhsCond),
          makeJoin(lhsFrom, rhsFrom)
        )

      case LeftOuterJoin(lhs, rhs, cond) => 
        val (lhsCond, lhsFrom) = extractSelectsAndJoins(lhs)
        val rhsFrom = makeSubSelect(rhs)
        val joinItem = makeJoin(lhsFrom, rhsFrom)
        joinItem.getJoin().setSimple(false)
        joinItem.getJoin().setOuter(true)
        joinItem.getJoin().setLeft(true)
        joinItem.getJoin().setOnExpression(convert(cond, SqlUtils.getSchemas(lhsFrom, db)++SqlUtils.getSchemas(rhsFrom, db)))

        (
          lhsCond,
          joinItem
        )

      case Table(name, tgtSch, metadata) =>
        val realSch = db.getTableSchema(name) match {
          case Some(realSch) => realSch
          case None => throw new SQLException("Unknown Table '"+name+"'");
        }
        // Since Mimir's RA tree structure has no real notion of aliasing,
        // it's only really safe to inline tables directly into a query
        // when tgtSch == realSch.  Eventually, we should add some sort of
        // rewrite that tacks on aliasing metadata... but for now let's see
        // how much milage we can get out of this simple check.
        if(realSch.map(_._1).             // Take names from the real schema
            zip(tgtSch.map(_._1)).        // Align with names from the target schema
            forall( { case (real,tgt) => real.equalsIgnoreCase(tgt) } )
                                          // Ensure that both are equivalent.
          && metadata.map(_._1).          // And make sure only standardized metadata are preserved
                forall({
                  case "ROWID" => true
                  case _ => false
                })
        ){
          // If they are equivalent, then...
          val ret = new net.sf.jsqlparser.schema.Table(name);
          ret.setAlias(name);
          (BoolPrimitive(true), ret)
        } else {
          // If they're not equivalent, revert to old behavior
          (BoolPrimitive(true), makeSubSelect(oper))
        }

      case _ => (BoolPrimitive(true), makeSubSelect(oper))
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
  def makeSubSelect(oper: Operator): FromItem =
  {
    val subSelect = new SubSelect()
    subSelect.setSelectBody(makeSelect(oper))//doConvert returns a plain select
    subSelect.setAlias("SUBQ_"+oper.schema.head._1)
    subSelect
  }

  /**
   * Slightly more elegant join constructor.
   */
  private def makeJoin(lhs: FromItem, rhs: FromItem): SubJoin =
  {
    val rhsJoin = new net.sf.jsqlparser.statement.select.Join();
    rhsJoin.setRightItem(rhs)
    // rhsJoin.setSimple(true)
    val ret = new SubJoin()
    ret.setLeft(lhs)
    ret.setJoin(rhsJoin)
    return ret
  }

  private def makeSelectItem(expr: net.sf.jsqlparser.expression.Expression, alias: String): SelectExpressionItem =
  {
    val item = new SelectExpressionItem()
    item.setExpression(expr)
    item.setAlias(alias)
    return item
  }

  def bin(b: BinaryExpression, l: Expression, r: Expression): BinaryExpression = {
    bin(b, l, r, List())
  }

  /**
   * Binary expression constructor
   */
  def bin(b: BinaryExpression, l: Expression, r: Expression, sources: List[(String,List[String])]): BinaryExpression =
  {
    b.setLeftExpression(convert(l, sources))
    b.setRightExpression(convert(r, sources))
    b
  }

  def convert(e: Expression): net.sf.jsqlparser.expression.Expression = {
    convert(e, List())
  }

  def convert(e: Expression, sources: List[(String,List[String])]): net.sf.jsqlparser.expression.Expression = {
    e match {
      case IntPrimitive(v) => new LongValue(""+v)
      case StringPrimitive(v) => new StringValue(v)
      case FloatPrimitive(v) => new DoubleValue(""+v)
      case RowIdPrimitive(v) => new StringValue(v)
      case BoolPrimitive(true) =>
        bin(new EqualsTo(), IntPrimitive(1), IntPrimitive(1))
      case BoolPrimitive(false) =>
        bin(new NotEqualsTo(), IntPrimitive(1), IntPrimitive(1))
      case NullPrimitive() => new NullValue()
      case DatePrimitive(y,m,d) => {
        val f = new Function()
        if(db.backend.isInstanceOf[JDBCBackend]
          && db.backend.asInstanceOf[JDBCBackend].driver().equalsIgnoreCase("oracle")
        ) {
          f.setName("TO_DATE")
          f.setParameters(new ExpressionList(
            List[net.sf.jsqlparser.expression.Expression](
              new StringValue(""+y+"-%02d".format(m)+"-%02d".format(d)),
              new StringValue("YYYY-MM-DD")
            )
          ))
          f
        } else {
          f.setName("DATE")
          f.setParameters(new ExpressionList(
            List[net.sf.jsqlparser.expression.Expression](new StringValue(""+y+"-%02d".format(m)+"-%02d".format(d)))
          ))
          f
        }
      }
      case Comparison(Cmp.Eq, l, r)  => bin(new EqualsTo(), l, r, sources)
      case Comparison(Cmp.Neq, l, r) => bin(new NotEqualsTo(), l, r, sources)
      case Comparison(Cmp.Gt, l, r)  => bin(new GreaterThan(), l, r, sources)
      case Comparison(Cmp.Gte, l, r) => bin(new GreaterThanEquals(), l, r, sources)
      case Comparison(Cmp.Lt, l, r)  => bin(new MinorThan(), l, r, sources)
      case Comparison(Cmp.Lte, l, r) => bin(new MinorThanEquals(), l, r, sources)
      case Comparison(Cmp.Like, l, r) => bin(new LikeExpression(), l, r, sources)
      case Comparison(Cmp.NotLike, l, r) => val expr = bin(new LikeExpression(), l, r, sources).asInstanceOf[LikeExpression]; expr.setNot(true); expr
      case Arithmetic(Arith.Add, l, r)  => bin(new Addition(), l, r, sources)
      case Arithmetic(Arith.Sub, l, r)  => bin(new Subtraction(), l, r, sources)
      case Arithmetic(Arith.Mult, l, r) => bin(new Multiplication(), l, r, sources)
      case Arithmetic(Arith.Div, l, r)  => bin(new Division(), l, r, sources)
      case Arithmetic(Arith.And, l, r)  => new AndExpression(convert(l, sources), convert(r, sources))
      case Arithmetic(Arith.Or, l, r)   => new OrExpression(convert(l, sources), convert(r, sources))
      case Var(n) => {
        val src = sources.find( {
          case (_, vars) => vars.exists( _.equalsIgnoreCase(n) )
        })
        if(src.isEmpty)
          throw new SQLException("Could not find appropriate source for '"+n+"' in "+sources)
        new Column(new net.sf.jsqlparser.schema.Table(null, src.head._1), n)
      }
      case JDBCVar(t) => new JdbcParameter()
      case Conditional(_, _, _) => {
        val (whenClauses, elseClause) = ExpressionUtils.foldConditionalsToCase(e)
        val caseExpr = new net.sf.jsqlparser.expression.CaseExpression()
        caseExpr.setWhenClauses(new java.util.ArrayList(
          whenClauses.map( (clause) => {
            val whenThen = new WhenClause()
            whenThen.setWhenExpression(convert(clause._1, sources))
            whenThen.setThenExpression(convert(clause._2, sources))
            whenThen
          })
        ))
        caseExpr.setElseExpression(convert(elseClause, sources))
        caseExpr
      }
      case mimir.algebra.Not(mimir.algebra.IsNullExpression(subexp)) => {
        val isNull = new net.sf.jsqlparser.expression.operators.relational.IsNullExpression()
        isNull.setLeftExpression(convert(subexp, sources))
        isNull.setNot(true)
        isNull
      }
      case mimir.algebra.IsNullExpression(subexp) => {
        val isNull = new net.sf.jsqlparser.expression.operators.relational.IsNullExpression()
        isNull.setLeftExpression(convert(subexp, sources))
        isNull
      }
      case Not(subexp) => {
        new InverseExpression(convert(subexp, sources))
      }
      case mimir.algebra.Function(Provenance.mergeRowIdFunction, Nil) => {
          throw new SQLException("MIMIR_MAKE_ROWID with no arguments")
      }
      case mimir.algebra.Function(Provenance.mergeRowIdFunction, head :: rest) => {
          rest.map(convert(_, sources)).foldLeft(convert(head, sources))(concat(_,_,"|"))
      }
      case mimir.algebra.Function("CAST", body_arg :: TypePrimitive(t) :: Nil) => {
        return new CastOperation(convert(body_arg, sources), t.toString);
      }
      case mimir.algebra.Function("CAST", _) => {
        throw new SQLException("Invalid Cast: "+e)
      }
      case mimir.algebra.Function(fname, fargs) => {
        val func = new Function()
        func.setName(fname)
        func.setParameters(new ExpressionList(
          fargs.map(convert(_, sources))))
        return func
      }
      case _ =>
        throw new SQLException("Compiler Error: I don't know how to translate "+e+" into SQL")
    }
  }


  private def concat(lhs: net.sf.jsqlparser.expression.Expression,
                     rhs: net.sf.jsqlparser.expression.Expression,
                     sep: String): net.sf.jsqlparser.expression.Expression = {
    val e1 = new Concat()
    e1.setLeftExpression(lhs)
    e1.setRightExpression(new StringValue(sep))
    val e2 = new Concat()
    e2.setLeftExpression(e1)
    e2.setRightExpression(rhs)
    e2
  }

}