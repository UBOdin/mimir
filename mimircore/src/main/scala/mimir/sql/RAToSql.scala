package mimir.sql;

import java.sql.SQLException

import mimir.Database
import mimir.algebra._
import net.sf.jsqlparser.expression.operators.arithmetic._
import net.sf.jsqlparser.expression.operators.conditional._
import net.sf.jsqlparser.expression.operators.relational.{EqualsTo, ExpressionList, GreaterThan, GreaterThanEquals, MinorThan, MinorThanEquals, NotEqualsTo}
import net.sf.jsqlparser.expression.{BinaryExpression, DoubleValue, Function, LongValue, NullValue, Parenthesis, StringValue, WhenClause}
import net.sf.jsqlparser.schema.Column
import net.sf.jsqlparser.statement.select.{FromItem, PlainSelect, SelectBody, SelectExpressionItem, SubSelect}

import scala.collection.JavaConversions._;

class RAToSql(db: Database) {
  
  def convert(oper: Operator): SelectBody = 
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
            metadata.map(_._1).map( (key) => {
              val item = new SelectExpressionItem()
              item.setAlias(key)
              item.setExpression(new Column(table, key))
              item
            })
          // )
        )
        return body
      }
      case Union(lhs, rhs) => {
        val union = new net.sf.jsqlparser.statement.select.Union()
        val unionList: (SelectBody => List[PlainSelect]) = _ match {
          case s: PlainSelect => List(s)
          case u: net.sf.jsqlparser.statement.select.Union =>
            u.getPlainSelects().toList
        }
        union.setPlainSelects(
          unionList(convert(lhs)) ++ 
          unionList(convert(rhs))
        )
        return union
      }
      case Select(_,_) | Join(_,_) => {
        convert(Project(
          oper.schema.map(_._1).map( (x) => ProjectArg(x, Var(x))).toList, oper
        ))
      }
      case Project(args, src) =>
        val body = new PlainSelect();
        val (cond, sources) = extractSelectsAndJoins(src)
        body.setFromItem(sources.head)
        body.setJoins(new java.util.ArrayList(
          sources.tail.map( (s) => {
            val join = new net.sf.jsqlparser.statement.select.Join();
            join.setRightItem(s);
            join.setSimple(true);
            join
          })
        ))
        if(cond != BoolPrimitive(true)){
          body.setWhere(convert(cond))
        }
        body.setSelectItems(
          new java.util.ArrayList(
            args.map( (arg) => {
              val item = new SelectExpressionItem()
              item.setAlias(arg.column)
              item.setExpression(convert(arg.input))
              item
            })
          )
        )
        return body;
    }
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
      case _ => 
        val subSelect = new SubSelect()
        subSelect.setSelectBody(convert(oper))
        subSelect.setAlias("SUBQ_"+oper.schema.map(_._1).head)
        (BoolPrimitive(true), List[FromItem](subSelect))
    }
  }
  
  def bin(b: BinaryExpression, l: Expression, r: Expression): BinaryExpression =
  {
    b.setLeftExpression(convert(l))
    b.setRightExpression(convert(r))
    b
  }
  
  def convert(e: Expression): net.sf.jsqlparser.expression.Expression = {
    e match {
      case IntPrimitive(v) => new LongValue(""+v)
      case StringPrimitive(v) => new StringValue("\'"+v+"\'")
      case FloatPrimitive(v) => new DoubleValue(""+v)
      case RowIdPrimitive(v) => new StringValue("\'"+v+"\'")
      case BoolPrimitive(true) => 
        bin(new EqualsTo(), IntPrimitive(1), IntPrimitive(1))
      case BoolPrimitive(false) => 
        bin(new NotEqualsTo(), IntPrimitive(1), IntPrimitive(1))
      case NullPrimitive() => new NullValue()
      case DatePrimitive(y,m,d) => {
        val f = new Function();
        f.setName("DATE")
        f.setParameters(new ExpressionList(
          List[net.sf.jsqlparser.expression.Expression](new StringValue(""+y+"-"+m+"-"+d))
        ))
        f
      }
      case Not(subexp) => {
          val parens = new Parenthesis(convert(subexp))
          parens.setNot();
          parens
        }
      case Comparison(Cmp.Eq, l, r)  => bin(new EqualsTo(), l, r)
      case Comparison(Cmp.Neq, l, r) => bin(new NotEqualsTo(), l, r)
      case Comparison(Cmp.Gt, l, r)  => bin(new GreaterThan(), l, r)
      case Comparison(Cmp.Gte, l, r) => bin(new GreaterThanEquals(), l, r)
      case Comparison(Cmp.Lt, l, r)  => bin(new MinorThan(), l, r)
      case Comparison(Cmp.Lte, l, r) => bin(new MinorThanEquals(), l, r)
      case Arithmetic(Arith.Add, l, r)  => bin(new Addition(), l, r)
      case Arithmetic(Arith.Sub, l, r)  => bin(new Subtraction(), l, r)
      case Arithmetic(Arith.Mult, l, r) => bin(new Multiplication(), l, r)
      case Arithmetic(Arith.Div, l, r)  => bin(new Division(), l, r)
      case Arithmetic(Arith.And, l, r)  => new AndExpression(convert(l), convert(r))
      case Arithmetic(Arith.Or, l, r)   => new OrExpression(convert(l), convert(r))
      case Var(n) => new Column(new net.sf.jsqlparser.schema.Table(null, null), n)
      case CaseExpression(whenClauses, elseClause) => {
        val caseExpr = new net.sf.jsqlparser.expression.CaseExpression();
        caseExpr.setWhenClauses(new java.util.ArrayList(
          whenClauses.map( (clause) => {
            val whenThen = new WhenClause()
            whenThen.setWhenExpression(convert(clause.when))
            whenThen.setThenExpression(convert(clause.then))
            whenThen
          })
        ))
        caseExpr.setElseExpression(convert(elseClause))
        caseExpr
      }
      case IsNullExpression(subexp, neg) => {
        val isNull = new net.sf.jsqlparser.expression.operators.relational.IsNullExpression()
        isNull.setLeftExpression(convert(subexp))
        isNull.setNot(neg)
        isNull;
      }
      case mimir.algebra.Function(name, subexp) => {
        if(subexp.length > 1)
          throw new SQLException("Function " + name + " SQL conversion error")
        convert(subexp(0))
      }
    }
  }
  
  
}