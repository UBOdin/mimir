package mimir.sql;

import java.sql._

import mimir.Database
import mimir.algebra._
import mimir.util._
import net.sf.jsqlparser.expression.operators.arithmetic._
import net.sf.jsqlparser.expression.operators.conditional._
import net.sf.jsqlparser.expression.operators.relational._
import net.sf.jsqlparser.expression.{BinaryExpression, DateValue, DoubleValue, Function, LongValue, NullValue, Parenthesis, StringValue, WhenClause}
import net.sf.jsqlparser.schema.Column
import net.sf.jsqlparser.statement.create.table._
import net.sf.jsqlparser.statement.select.{AllColumns, AllTableColumns, FromItem, PlainSelect, SelectBody, SelectExpressionItem, SubJoin, SubSelect}
import org.joda.time.LocalDate

import scala.collection.JavaConversions._;

class SqlToRA(db: Database) 
{
  def unhandled(feature : String) = {
    println("ERROR: Unhandled Feature: " + feature)
    throw new SQLException("Unhandled Feature: "+feature)
  }
  
  def convert(t : ColDataType): Type.T = {
    t.getDataType.toUpperCase match { 
      case "INT" => Type.TInt
      case "NUMBER" => Type.TInt
      case "CHAR" => Type.TString
    }
  }
  
  def convert(s : net.sf.jsqlparser.statement.select.Select) : Operator = convert(s, null)._1
  def convert(s : net.sf.jsqlparser.statement.select.Select, alias: String) : (Operator, Map[String, String]) = {
    convert(s.getSelectBody(), alias)
  }
  
  def convert(sb : SelectBody) : Operator = 
    convert(sb, null)._1;
  
  def convert(sb : SelectBody, tableAlias: String) : (Operator, Map[String, String]) = {
    if(sb.isInstanceOf[PlainSelect]){
      val ps = sb.asInstanceOf[PlainSelect]
      
      val bindings = scala.collection.mutable.Map[String, String]()
      val reverseBindings = scala.collection.mutable.Map[String, String]()
      val sources = scala.collection.mutable.Map[String, List[String]]()
      
      var (ret, currBindings, sourceAlias) = convert(ps.getFromItem)
      sources.put(sourceAlias, currBindings.values.toList);
      bindings.putAll(currBindings)
      reverseBindings.putAll(
        currBindings.map( _ match { case (x,y) => (y,x) } )
      )

      var joinCond = null;
      
      if(ps.getJoins() != null) { 
        ret = ps.getJoins().foldLeft(ret)(
          (a, j) => {
            val (source, currBindings, sourceAlias) = convert(j.getRightItem())
            sources.put(sourceAlias, currBindings.values.toList);
            bindings.putAll(currBindings)
            reverseBindings.putAll(
              currBindings.map( _ match { case (x,y) => (y,x) } )
            )
            var r: Operator = Join(a, source)
            if(j.getOnExpression() != null) { 
              r = Select(convert(j.getOnExpression, bindings.toMap), r)
            }
            r
          }
        );
      }

      if(ps.getWhere != null) {
        ret = Select(convert(ps.getWhere, bindings.toMap), ret)
      }

      var exprId = 0;
      var includeAll = false;
      var needProject = (tableAlias != null);
      
      val defaultTargetsForTable = (name: String) => {
        sources.get(name).get.map(
          (x) => {
            val baseX = reverseBindings.get(x).get
            ( baseX, 
              ProjectArg(
                if(tableAlias != null){ tableAlias+"_"+baseX }
                  else { baseX },
                Var(x)
              ) 
            )
          }).filter( _._1 != "ROWID" )
      }
      
      val target: List[(String, ProjectArg)] = 
        ps.getSelectItems().map( (si) => {
          if(si.isInstanceOf[SelectExpressionItem]) {
            val se = si.asInstanceOf[SelectExpressionItem]
            val originalAlias = SqlUtils.getAlias(se);
            var alias = originalAlias;
            if(alias == null){
              exprId += 1
              alias = "EXPR_"+exprId
            } else {
              alias = alias.toUpperCase
            }
            if(tableAlias != null){
              alias = tableAlias + "_" + alias;
            }
            needProject = true;
            List( (
              originalAlias,
              ProjectArg( 
                    alias, 
                    convert(se.getExpression(), bindings.toMap) 
              ) )
            )
          } else if(si.isInstanceOf[AllColumns]) {
            includeAll = true;
            sources.keys.map( defaultTargetsForTable(_) ).flatten.toList
          } else if(si.isInstanceOf[AllTableColumns]) {
            needProject = true;
            defaultTargetsForTable( 
              si.asInstanceOf[AllTableColumns].getTable.getName.toUpperCase
            )
          } else {
            unhandled("SelectItem[Unknown]")
          }
        }).flatten.toList
      
      // if(needProject){
        ret = Project(target.map( _._2 ), ret);
      // }

  //     
  //     var optionalClauses = List[OptionalSelectClause]();
  //     
      if(ps.getGroupByColumnReferences != null) {
        unhandled("GROUP BY")
  //       optionalClauses ++= List(
  //         GroupByClause(ps.getGroupByColumnReferences.map( 
  //           (gb: Expression) => convert(gb) 
  //         ).toList)
  //       )
      }
      if(ps.getOrderByElements != null) {
        unhandled("ORDER BY")
  //       optionalClauses ++= List( 
  //         OrderByClause(
  //           ps.getOrderByElements.map( (o : OrderByElement) => {
  //             ( if(o.isAsc) { OrderBy.Asc } else { OrderBy.Desc }, 
  //               convert(o.getExpression)
  //             )
  //           }).toList
  //         )
  //       )
      }
      if(ps.getDistinct != null){
        unhandled("DISTINCT")
  //       optionalClauses ++= List(DistinctClause())
      }
      
      if(ps.getHaving != null)  { unhandled("HAVING") }
      if(ps.getLimit != null)   { unhandled("LIMIT") }
      
      return (ret, 
        target.map(
          (x) => (x._1, x._2.column)
        ).toMap
      );
    } else if(sb.isInstanceOf[net.sf.jsqlparser.statement.select.Union]) {
      val union = sb.asInstanceOf[net.sf.jsqlparser.statement.select.Union];
      val isAll = (union.isAll() || !union.isDistinct());
      return union.
        getPlainSelects().
        map( convert(_, tableAlias) ).
        reduce( (a,b) => (Union(isAll,a._1,b._1), a._2) )
    } else {
      unhandled("SelectBody[Unknown]")
    }
    return null;
  }
  // 
  def convert(fi : FromItem) : (Operator, Map[String, String], String) = {
    if(fi.isInstanceOf[SubJoin]){
      unhandled("FromItem[SubJoin]")
    }
    if(fi.isInstanceOf[SubSelect]){
      val (ret, bindings) = convert(
        fi.asInstanceOf[SubSelect].getSelectBody,
        fi.asInstanceOf[SubSelect].getAlias
      );
      return (ret, bindings, fi.asInstanceOf[SubSelect].getAlias.toUpperCase)
    }
    if(fi.isInstanceOf[net.sf.jsqlparser.schema.Table]){
      val name =
        fi.asInstanceOf[net.sf.jsqlparser.schema.Table].
          getName.toUpperCase
      var alias =
         fi.asInstanceOf[net.sf.jsqlparser.schema.Table].
          getAlias
      if(alias == null){ alias = name }
      else { alias = alias.toUpperCase }
      
      // Bind the table to a source: 
      db.getView(name) match {
        case None =>
          val sch = db.getTableSchema(name) match {
            case Some(sch) => sch
            case None => throw new SQLException("Unknown table or view: "+name);
          }
          val newBindings = sch.map(
              (x) => (x._1, alias+"_"+x._1)
            ).toMap[String, String]
          return (
            Table(name, 
              sch.map(
                _ match { case (v, t) => (alias+"_"+v, t)}
              ),
              List[(String,Type.T)]()
            ), 
            newBindings, 
            alias
          )
        case Some(view) =>
          val sch = view.schema.map(_._1)
          val newBindings = sch.map(
            (x) => (x, alias+"_"+x)
          ).toMap[String,String]
          return (
            Project(
              sch.map( (x) => 
                ProjectArg(alias+"_"+x, Var(x))
              ),
              view
            ), 
            newBindings,
            alias
          )
      }
      
    }
    unhandled("FromItem["+fi.getClass.toString+"]")
  }
  // 
  def convert(e : net.sf.jsqlparser.expression.Expression) : Expression = 
    convert(e, Map[String,String]())
  def convert(e : net.sf.jsqlparser.expression.Expression, bindings: Map[String, String]) : Expression = {
    if(e.isInstanceOf[DateValue]){ 
      val d = new LocalDate(e.asInstanceOf[DateValue].getValue())
      return DatePrimitive(d.getYear(), d.getMonthOfYear(), d.getDayOfMonth())
    }
    if(e.isInstanceOf[LongValue]){ 
      return IntPrimitive(e.asInstanceOf[LongValue].getValue()) 
    }
    if(e.isInstanceOf[DoubleValue]){ 
      return FloatPrimitive(e.asInstanceOf[DoubleValue].getValue()) 
    }
    if(e.isInstanceOf[StringValue]){
      return StringPrimitive(e.asInstanceOf[StringValue].getValue())
    }
    if(e.isInstanceOf[Parenthesis]){
      return convert(e.asInstanceOf[Parenthesis].getExpression, bindings)
    }
    if(e.isInstanceOf[BinaryExpression]){
      val lhs = convert(e.asInstanceOf[BinaryExpression].getLeftExpression(), bindings)
      val rhs = convert(e.asInstanceOf[BinaryExpression].getRightExpression(), bindings)
  
      if(e.isInstanceOf[Addition]){ return Arithmetic(Arith.Add, lhs, rhs); }
      if(e.isInstanceOf[Subtraction]){ return Arithmetic(Arith.Sub, lhs, rhs); }
      if(e.isInstanceOf[Multiplication]){ return Arithmetic(Arith.Mult, lhs, rhs); }
      if(e.isInstanceOf[Division]){ return Arithmetic(Arith.Div, lhs, rhs); }
      if(e.isInstanceOf[AndExpression]){ return Arithmetic(Arith.And, lhs, rhs); }
      if(e.isInstanceOf[OrExpression]){ return Arithmetic(Arith.Or, lhs, rhs); }
      
      if(e.isInstanceOf[EqualsTo]){ return Comparison(Cmp.Eq, lhs, rhs); }
      if(e.isInstanceOf[NotEqualsTo]){ return Comparison(Cmp.Neq, lhs, rhs); }
      if(e.isInstanceOf[GreaterThan]){ return Comparison(Cmp.Gt, lhs, rhs); }
      if(e.isInstanceOf[MinorThan]){ return Comparison(Cmp.Lt, lhs, rhs); }
      if(e.isInstanceOf[GreaterThanEquals]){ return Comparison(Cmp.Gte, lhs, rhs); }
      if(e.isInstanceOf[MinorThanEquals]){ return Comparison(Cmp.Lte, lhs, rhs); }
      if(e.isInstanceOf[LikeExpression]){ 
        return Comparison(if(e.asInstanceOf[LikeExpression].isNot()) 
                            { Cmp.NotLike } else { Cmp.Like }, lhs, rhs); 
      }
      unhandled("Expression[BinaryExpression]: "+e)
      return null;
    }
    if(e.isInstanceOf[Column]){
      val c = e.asInstanceOf[Column]
      val table = c.getTable.getName match {
        case null => null
        case x => x.toUpperCase
      }
      val name = c.getColumnName.toUpperCase
      if(table == null){
        val binding = bindings.get(name);
        if(binding.isEmpty){
          if(name.equalsIgnoreCase("ROWID")) return Var("ROWID")
          else throw new SQLException("Unknown Variable: "+name+" in "+bindings.toString)
        }
        return Var(binding.get)
      } else {
        return Var(table + "_" + name);
      }
    }
    if(e.isInstanceOf[Function]){
      val f = e.asInstanceOf[Function]
      val name = f.getName.toUpperCase
      val parameters : List[Expression] = 
        if(f.getParameters == null) { List[Expression]() }
        else {
          f.getParameters.
            getExpressions.
            map( (o : Any) => convert(o.asInstanceOf[net.sf.jsqlparser.expression.Expression], bindings) ).
            toList
        }
      return mimir.algebra.Function(name, parameters)
      // unhandled("Expression[Function:"+name+"]")
    }
    if(e.isInstanceOf[net.sf.jsqlparser.expression.CaseExpression]){
      val c = e.asInstanceOf[net.sf.jsqlparser.expression.CaseExpression]
      val inlineSwitch: Expression => Expression = 
        if(c.getSwitchExpression() == null){
          (x) => x
        } else {
          val switch = convert(c.getSwitchExpression(), bindings)
          (x) => Comparison(Cmp.Eq, switch, x)
        }
      return CaseExpression(
        c.getWhenClauses().map ( (w: WhenClause) => {
          WhenThenClause(
            inlineSwitch(convert(w.getWhenExpression(), bindings)),
            convert(w.getThenExpression(), bindings)
          )
        }).toList, 
        convert(c.getElseExpression(), bindings)
      )
    }
    if(e.isInstanceOf[NullValue]) { return NullPrimitive() }
    unhandled("Expression[Unknown]: " + e)
  }
}