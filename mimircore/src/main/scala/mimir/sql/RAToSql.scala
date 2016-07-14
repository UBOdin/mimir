package mimir.sql;

import java.sql.SQLException
import java.util

import mimir.Database
import mimir.algebra.Join
import mimir.algebra.Select
import mimir.algebra.Union
import mimir.algebra._
import mimir.ctables.{JointSingleVarModel, VGTerm}
import mimir.optimizer.{InlineProjections, PushdownSelections}
import mimir.lenses.{TypeInferenceModel, SchemaMatchingModel, MissingValueModel}
import mimir.util.TypeUtils
import net.sf.jsqlparser.expression.operators.arithmetic._
import net.sf.jsqlparser.expression.operators.conditional._
import net.sf.jsqlparser.expression.operators.relational._
import net.sf.jsqlparser.expression.{BinaryExpression, DoubleValue, Function, LongValue, NullValue, InverseExpression, StringValue, WhenClause}
import net.sf.jsqlparser.{schema, expression}
import net.sf.jsqlparser.schema.Column
import net.sf.jsqlparser.statement.select.{SelectBody, PlainSelect, SubSelect, SelectExpressionItem, FromItem, SelectItem}

import scala.collection.JavaConversions._

class RAToSql(db: Database) {
  
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
        })
        Project(
          schMap ++ metadata.map(_._2),
          Table(name, realSch, metadata.map(_._1))
        )
      }
      case _ => oper.rebuild(oper.children.map(standardizeTables(_)))
    }
  }
  def convert(oper: Operator): SelectBody = 
  {
    // The actual recursive conversion is factored out into a separate fn
    // so that we can do a little preprocessing.
    // println("CONVERT: "+oper)
    // Start by rewriting table schemas to make it easier to inline them.
    val standardized = standardizeTables(oper)

    // standardizeTables adds a new layer of projections that we may be
    // able to optimize away.
    val optimized = 
      InlineProjections.optimize(
        PushdownSelections.optimize(standardized))

    // println("OPTIMIZED: "+optimized)

    // and then actually do the conversion
    doConvert(optimized)
  }

  def doConvert(oper: Operator): SelectBody = 
  {
    oper match {
      case Table(name, sch, metadata) => {
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
      case Select(_,_) | Join(_,_) => {
        doConvert(Project(
          oper.schema.map(_._1).map( (x) => ProjectArg(x, Var(x))).toList, oper
        ))
      }
      case Project(args, src) =>
        val body = new PlainSelect()
        val (cond, sources) = extractSelectsAndJoins(src)
        body.setFromItem(sources.head)
        body.setJoins(new java.util.ArrayList(
          sources.tail.map( (s) => {
            val join = new net.sf.jsqlparser.statement.select.Join()
            join.setRightItem(s)
            join.setSimple(true)
            join
          })
        ))
        if(cond != BoolPrimitive(true)){
          // println("---- SCHEMAS OF:"+sources);
          // println("---- ARE: "+getSchemas(sources))
          body.setWhere(convert(cond, getSchemas(sources)))
        }
        body.setSelectItems(
          new java.util.ArrayList(
            args.map( (arg) => {
              val item = new SelectExpressionItem()
              item.setAlias(arg.name)
              item.setExpression(convert(arg.expression, getSchemas(sources)))
              item
            })
          )
        )
        body
    }
  }

  def getSchemas(sources: List[FromItem]): List[(String, List[String])] =
  {
    sources.map( {
      case subselect: SubSelect =>
        (subselect.getAlias(), subselect.getSelectBody() match {
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
        }) 
      case table: net.sf.jsqlparser.schema.Table =>
        (table.getAlias(), db.getTableSchema(table.getName()).get.map(_._1).toList++List("ROWID"))
    })
  }

  def makeSubSelect(oper: Operator): FromItem =
  {
    val subSelect = new SubSelect()
    subSelect.setSelectBody(doConvert(oper))
    subSelect.setAlias("SUBQ_"+oper.schema.map(_._1).head)
    subSelect
  }
  
  def extractSelectsAndJoins(oper: Operator): 
    (Expression, List[FromItem]) =
  {
    oper match {
      case Select(cond, child) =>
        val (childCond, childFroms) = extractSelectsAndJoins(child)
        ( Arith.makeAnd(cond, childCond), childFroms )
      case Join(lhs, rhs) =>
        val (lhsCond, lhsFroms) = extractSelectsAndJoins(lhs)
        val (rhsCond, rhsFroms) = extractSelectsAndJoins(rhs)
        ( Arith.makeAnd(lhsCond, rhsCond), 
          lhsFroms ++ rhsFroms
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
          (BoolPrimitive(true), List[FromItem](ret))
        } else {
          // If they're not equivalent, revert to old behavior
          (BoolPrimitive(true), List(makeSubSelect(oper)))
        }

      case _ => (BoolPrimitive(true), List(makeSubSelect(oper)))
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
      case mimir.algebra.Function("MIMIR_MAKE_ROWID", Nil) => {
          throw new SQLException("MIMIR_MAKE_ROWID with no arguments")
      }
      case mimir.algebra.Function("MIMIR_MAKE_ROWID", head :: rest) => {
          rest.map(convert(_, sources)).foldLeft(convert(head, sources))(concat(_,_,"||"))
      }
      case mimir.algebra.Function("CAST", body_arg :: body_type :: Nil) => {
        return new CastOperation(convert(body_arg, sources), body_type.toString);
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
      case VGTerm((_, model), idx, args) => {

        val plainSelect = new PlainSelect()

        /* FROM */
        val backingStore = new schema.Table(null, model.backingStore(idx))
        plainSelect.setFromItem(backingStore)

        /* WHERE */
        val expr = new EqualsTo()
        expr.setLeftExpression(new Column(backingStore, "EXP_LIST"))
        expr.setRightExpression(args.map(convert(_, sources)).reduceLeft(concat(_, _, "|")))
        plainSelect.setWhere(expr)

        /* PROJECT */
        val selItem = new SelectExpressionItem()
        val column = new Column()
        column.setTable(backingStore)
        column.setColumnName("DATA")
        selItem.setExpression(column)
        val selItemList = new util.ArrayList[SelectItem]()
        selItemList.add(selItem)
        plainSelect.setSelectItems(selItemList)


        val subSelect = new SubSelect()
        subSelect.setSelectBody(plainSelect)
        subSelect
      }
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