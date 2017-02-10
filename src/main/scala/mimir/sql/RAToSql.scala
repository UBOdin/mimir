package mimir.sql;

import java.sql.SQLException
import java.util

import mimir.Database
import mimir.algebra._
import mimir.provenance._
import mimir.optimizer.{InlineProjections, PushdownSelections}

import com.typesafe.scalalogging.slf4j.LazyLogging

import net.sf.jsqlparser.expression.operators.arithmetic._
import net.sf.jsqlparser.expression.operators.conditional._
import net.sf.jsqlparser.expression.operators.relational._
import net.sf.jsqlparser.expression.{BinaryExpression, DoubleValue, Function, LongValue, NullValue, InverseExpression, StringValue, WhenClause, JdbcParameter}
import net.sf.jsqlparser.{schema, expression}
import net.sf.jsqlparser.schema.Column
import net.sf.jsqlparser.statement.select.{SelectBody, PlainSelect, SubSelect, SelectExpressionItem, FromItem, SelectItem, SubJoin}
//import net.sf.jsqlparser.statement.provenance.ProvenanceSelect

import scala.collection.JavaConversions._

class RAToSql(db: Database) 
  extends LazyLogging 
{
  
  def standardizeTables(oper: Operator): Operator = 
  {
    oper match {
      case Table(name, alias, tgtSch, tgtMetadata) => {
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
          Table(name, alias, realSch, metadata.map(_._1))
        )
      }
      case _ => oper.rebuild(oper.children.map(standardizeTables(_)))
    }
  }
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
    doConvert(optimized)
  }

  def doConvert(oper: Operator): SelectBody = 
  {
    logger.debug(s"CONVERT: $oper")
    oper match {
      case Table(name, alias, sch, metadata) => {
        val body = new PlainSelect();
        val table = new net.sf.jsqlparser.schema.Table(null, name)
        val baseSch = db.getTableSchema(name).get
        body.setFromItem(table)
        body.setSelectItems(
          // new java.util.ArrayList(
            sch.map(_._1).zip(baseSch.map( _._1 )).map(_ match {
              case (external, internal) =>
                val item = new SelectExpressionItem()
                item.setAlias(external)
                item.setExpression(new Column(table, internal))
                item
            }) ++
            metadata.map( (element) => {
              val item = new SelectExpressionItem()
              item.setAlias(element._1)
              item.setExpression(convert(element._2, List( (name, List("ROWID")))))
              item
            })
          // )
        )
        body
      }
      case Union(lhs, rhs) => {
        val union = new net.sf.jsqlparser.statement.select.Union()
        val unionList: (SelectBody => List[PlainSelect]) = _ match {
          case s: PlainSelect => List(s)
          case u: net.sf.jsqlparser.statement.select.Union =>
            u.getPlainSelects().toList
        }
        union.setAll(true);
        union.setDistinct(false);
        union.setPlainSelects(
          unionList(doConvert(lhs)) ++ 
          unionList(doConvert(rhs))
        )
        union
      }
      case Annotate(subj,invisScm) => {
        subj match {
          case Table(name, alias, sch, metadata) => {
            metadata.addAll(invisScm.map(f => (f._2._1, null, f._2._2)))
            doConvert(new Table(name, alias, sch, metadata))
          }
        }
      }
      case Recover(subj,invisScm) => {
        val schemas = invisScm.groupBy(_._3).toList.map{ f => (f._1, f._2.map{ s => s._2._1 }.toList) }
        val pselBody = doConvert(subj).asInstanceOf[PlainSelect]
        pselBody.setSelectItems(pselBody.getSelectItems.union(
          new java.util.ArrayList(
            invisScm.map( (arg) => {
              val item = new SelectExpressionItem()
              item.setAlias(arg._1.name)
              item.setExpression(convert(arg._1.expression, schemas))
              item
            })
          )
        ))
        new ProvenanceSelect(pselBody)
      }
      case ProvenanceOf(psel) => {
        val pselBody = doConvert(psel).asInstanceOf[PlainSelect]
        new ProvenanceSelect(pselBody)
      }
      case Select(_,_) | Join(_,_) => {
        doConvert(Project(
          oper.schema.map(_._1).map( (x) => ProjectArg(x, Var(x))).toList, oper
        ))
      }
      case Aggregate(gbcols, aggregates, child) =>
        val body = new PlainSelect()
        val (cond, source) = extractSelectsAndJoins(child)
        val schemas = getSchemas(source)
        body.setFromItem(source)
        if(cond != BoolPrimitive(true)){
          // println("---- SCHEMAS OF:"+source);
          // println("---- ARE: "+schemas)
          body.setWhere(convert(cond, schemas))
        }

        body.setSelectItems(
          new java.util.ArrayList(
            gbcols.map( (col) => {
              val item = new SelectExpressionItem()
              val column =  convert(col, schemas)
              item.setAlias(column.asInstanceOf[Column].getColumnName())
              item.setExpression(column)
              item
            }) ++
            aggregates.map( (agg) => {
              val item = new SelectExpressionItem()
              item.setAlias(agg.alias)
              val func = new Function()
              func.setName(agg.function)
              func.setParameters(new ExpressionList(new java.util.ArrayList(
                agg.args.map(convert(_, schemas)))))
              func.setDistinct(agg.distinct)

              item.setExpression(func)
              item
            })
          )
        )
        body.setGroupByColumnReferences(new java.util.ArrayList(
          gbcols.map(convert(_, schemas).asInstanceOf[net.sf.jsqlparser.schema.Column])))

        body

      case Project(args, child) =>
        val body = new PlainSelect()
        val (cond, source) = extractSelectsAndJoins(child)
        val schemas = getSchemas(source)
        body.setFromItem(source)
        if(cond != BoolPrimitive(true)){
          // println("---- SCHEMAS OF:"+source);
          // println("---- ARE: "+schemas)
          body.setWhere(convert(cond, schemas))
        }
        body.setSelectItems(
          new java.util.ArrayList(
            args.map( (arg) => {
              val item = new SelectExpressionItem()
              item.setAlias(arg.name)
              item.setExpression(convert(arg.expression, schemas))
              item
            })
          )
        )
        body

    }
  }

  def getSchemas(source: FromItem): List[(String, List[String])] =
  {
    source match {
      case subselect: SubSelect =>
        List((subselect.getAlias(), subselect.getSelectBody() match {
          case plainselect: PlainSelect => 
            plainselect.getSelectItems().map({
              case sei:SelectExpressionItem =>
                sei.getAlias().toString
            }).toList
          case union: net.sf.jsqlparser.statement.select.Union =>
            union.getPlainSelects().get(0).getSelectItems().map({
              case sei:SelectExpressionItem =>
                sei.getAlias().toString
            }).toList
        }))
      case table: net.sf.jsqlparser.schema.Table =>
        List(
          ( table.getAlias(), 
            db.getTableSchema(table.getName()).
              get.map(_._1).toList++List("ROWID")
          )
        )
      case join: SubJoin =>
        getSchemas(join.getLeft()) ++
          getSchemas(join.getJoin().getRightItem())
    }
  }

  def makeSubSelect(oper: Operator): FromItem =
  {
    val subSelect = new SubSelect()
    subSelect.setSelectBody(doConvert(oper))//doConvert returns a plain select
    subSelect.setAlias("SUBQ_"+oper.schema.map(_._1).head)
    subSelect
  }

  def makeJoin(lhs: FromItem, rhs: FromItem): SubJoin =
  {
    val rhsJoin = new net.sf.jsqlparser.statement.select.Join();
    rhsJoin.setRightItem(rhs)
    // rhsJoin.setSimple(true)
    val ret = new SubJoin()
    ret.setLeft(lhs)
    ret.setJoin(rhsJoin)
    return ret
  }
  
  def extractSelectsAndJoins(oper: Operator): 
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
        joinItem.getJoin().setOnExpression(convert(cond, getSchemas(lhsFrom)++getSchemas(rhsFrom)))

        (
          lhsCond,
          joinItem
        )

      case Table(name, alias, tgtSch, metadata) =>
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

  def bin(b: BinaryExpression, l: Expression, r: Expression): BinaryExpression = {
    bin(b, l, r, List())
  }

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
          val explist = 
            new ExpressionList(
              new util.ArrayList[expression.Expression](fargs.map(convert(_, sources))))
          func.setParameters(explist)
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