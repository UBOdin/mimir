package mimir.sql;

import java.sql._

import mimir.Database
import mimir.algebra._
import mimir.ctables.CTPercolator
import mimir.util._
import net.sf.jsqlparser.expression
import net.sf.jsqlparser.expression.operators.arithmetic._
import net.sf.jsqlparser.expression.operators.conditional._
import net.sf.jsqlparser.expression.operators.relational._
import net.sf.jsqlparser.expression.{BinaryExpression, DateValue, DoubleValue, Function, LongValue, NullValue, InverseExpression, StringValue, WhenClause}
import net.sf.jsqlparser.schema.Column
import net.sf.jsqlparser.statement.create.table._
import net.sf.jsqlparser.statement.select.{AllColumns, AllTableColumns, FromItem, PlainSelect, SelectBody, SelectExpressionItem, SubJoin, SubSelect}
import org.joda.time.LocalDate

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
;

class SqlToRA(db: Database) 
{
  /*List of aggregate function names */
  val aggFuncNames = List("SUM", "AVG", "MAX", "MIN", "COUNT")

  def unhandled(feature : String) = {
    println("ERROR: Unhandled Feature: " + feature)
    throw new SQLException("Unhandled Feature: "+feature)
  }
  
  def convert(t : ColDataType): Type = {
    t.getDataType.toUpperCase match { 
      case "INT" => TInt()
      case "NUMBER" => TInt()
      case "CHAR" => TString()
    }
  }
/* Tests for unsupported aggregate query of the form "Select 1 + SUM(B) from R".
* Returns false if such a query form is detected */
  def isLegalAggQuery(expr: mimir.algebra.Expression): Boolean = {
    expr match {
      case Arithmetic(_, _, _) => expr.children.forall(x => isLegalAggQuery(x))
      case Var(_) => true
      case mimir.algebra.Function(op, _) => if(aggFuncNames.contains(op)){ false } else { true }
      case _ => true
    }
  }

  def convert(s : net.sf.jsqlparser.statement.select.Select) : Operator = convert(s, null)._1
  def convert(s : net.sf.jsqlparser.statement.select.Select, alias: String) : (Operator, Map[String, String]) = {
    convert(s.getSelectBody(), alias)
  }
  
  def convert(sb : SelectBody) : Operator = 
    convert(sb, null)._1;
  
  /**
   * Convert a SelectBody into an Operator + A projection map.
   */
  def convert(sb : SelectBody, tableAlias: String) : (Operator, Map[String, String]) = 
  {
    sb match {
      case ps: net.sf.jsqlparser.statement.select.PlainSelect => convert(ps, tableAlias)
      case u: net.sf.jsqlparser.statement.select.Union => convert(u, tableAlias)
    }
  }

  /**
   * Convert a PlainSelect into an Operator + A projection map.
   */
  def convert(ps: PlainSelect, tableAlias: String) : (Operator, Map[String, String]) =
  {
    // Unlike SQL, Mimir's relational algebra does not use range variables.  Rather,
    // variables are renamed into the form TABLENAME_VAR to prevent name conflicts.
    // Because SQL allows variables without a matching range variable, we need to keep
    // track of the variable's "base" name (without alias) and a mapping back to the
    // variable's real name.
    //
    // The following variables facilitate this conversion
    // 
    // Bindings: A map from the base name to the extended name (or an arbitrary
    //           name, if there are multiple source tables with the same variable)
    val bindings = scala.collection.mutable.Map[String, String]()
    // ReverseBindings: A map from the extended name back to the base name of the
    //                  variable (useful for inferring aliases)
    val reverseBindings = scala.collection.mutable.Map[String, String]()
    // Sources: A map from table name to the matching set of *extended* variable names
    val sources = scala.collection.mutable.Map[String, List[String]]()
    
    //////////////////////// CONVERT FROM CLAUSE /////////////////////////////

    // JSqlParser makes a distinction between the first FromItem, and items 
    // subsequently joined to it.  Start by extracting the first FromItem
    var (ret, currBindings, sourceAlias) = convert(ps.getFromItem)
    sources.put(sourceAlias, currBindings.values.toList);
    bindings.putAll(currBindings)
    reverseBindings.putAll(
      currBindings.map( _ match { case (x,y) => (y,x) } )
    )

    var joinCond = null;

    // Joins are optional, so make sure there is a join to begin with
    if(ps.getJoins() != null) { 

      // And then flatten out the join tree.
      for(j <- ps.getJoins()){
        val (source, currBindings, sourceAlias) = convert(j.getRightItem())
        sources.put(sourceAlias, currBindings.values.toList);
        bindings.putAll(currBindings)
        reverseBindings.putAll(
          currBindings.map( _ match { case (x,y) => (y,x) } )
        )
        ret = Join(ret, source)
        if(j.getOnExpression() != null) { 
          ret = Select(convert(j.getOnExpression, bindings.toMap), ret)
        }
      }
    }
    //////////////////////// CONVERT WHERE CLAUSE /////////////////////////////

    // This one's super simple.  The where clause just becomes a Select on top
    // of the return value.
    if(ps.getWhere != null) {
      ret = Select(convert(ps.getWhere, bindings.toMap), ret)
    }

    //////////////////////// CONVERT SELECT TARGETS /////////////////////////////

    // Utility function to compute expansions for table wildcard 
    // targets (i.e., table.*).  
    // Returns a 3-tuple: 
    //   t._1 : The base name of the variable in the output.
    //   t._2 : The extended name of the variable in the output.
    //   t._3 : The expression of the variable in the input.
    // Some examples:
    //   SELECT A FROM R
    //     -> ("A", "A", Var("A"))
    //   SELECT A AS B FROM R
    //     -> ("B", "B", Var("A"))
    //   (SELECT A FROM R) AS S
    //     -> ("A", "S_A", Var("A"))
    val defaultTargetsForTable:(String => Seq[(String, String, Expression)]) = 
      (name: String) => {
        sources(name).map(
          (x) => {
            val baseOutputX = reverseBindings(x)
            val extendedOutputX = 
              if(tableAlias != null){ tableAlias.toUpperCase+"_"+baseOutputX }
              else { baseOutputX }
            (baseOutputX, extendedOutputX, Var(x))
        })
      }

    // Start by converting to Mimir expressions and expanding clauses
    // Follows the same pattern as the utility function above
    val selectItems: Seq[(net.sf.jsqlparser.statement.select.SelectItem, Int)] = 
      ps.getSelectItems().zipWithIndex

    val targets: Seq[(String, String, Expression)] = 
      selectItems.flatMap({
        case (se:SelectExpressionItem, idx) => {
          val baseExpr: Expression = convert(se.getExpression, bindings.toMap)

          // Come up with a name for the expression
          val baseAlias: String = SqlUtils.getAlias(se, idx).toUpperCase;
          val extendedAlias: String = 
            if(tableAlias == null){ baseAlias }
            else { s"${tableAlias}_$baseAlias" }

          // Some expressions need to be special-cased
          val extendedExpr = baseExpr match {
            case Var("ROWID") => RowIdVar()
            case x => x
          }

          Some( (baseAlias, extendedAlias, extendedExpr) )
        }

        case (_:AllColumns, _) => 
          sources.keys.flatMap( defaultTargetsForTable(_) )

        case (tc:AllTableColumns, _) =>
          defaultTargetsForTable(tc.getTable.getName.toUpperCase)
      })

    // Check if this is an Aggregate Select or Flat Select
    // This is an Aggregate Select if ...
    //   ... any target column references an aggregate function
    //   ... there is a group by clause
    //   ... there is a having clause

    val hasGroupByRefs = 
      ((ps.getGroupByColumnReferences() != null)
        && (!ps.getGroupByColumnReferences().isEmpty))

    val hasHavingClause =
      (ps.getHaving() != null)

    val allReferencedFunctions =
      targets.flatMap( tgt => ExpressionUtils.getFunctions(tgt._3) )

    val isAggSelect =
      hasGroupByRefs || hasHavingClause || 
      allReferencedFunctions.exists( AggregateRegistry.isAggregate(_) )

    if(!isAggSelect){
      // NOT an aggregate select.  We just need to create a simple
      // projection around the return value.
      ret = 
        Project(
          targets.map({
            case (baseName, extendedName, inputExpr) =>
              ProjectArg(extendedName, inputExpr)
          }),
          ret
        )
    } else {
      // This is an aggregate select.  
      
      // It's legitimate SQL to write an aggregate query with a 
      // post-processing projection.  For example:
      // SELECT A + SUM(B) FROM R GROUP BY A
      // is entirely acceptable.  
      // 
      // Below, we've defined a function that segments this larger
      // expression, extracting the post-processing step: 
      //   (A + TMP)
      // And returning the set of actual aggregates that we need
      // to compute: 
      //   TMP -> SUM(B)

      val fragmentedTargets = 
        targets.map( tgt => fragmentAggregateExpression(tgt._3, "MIMIR_AGG_"+tgt._2 ) )

      val (targetPostProcExprs, perTargetGBVars, perTargetAggExprs) =
        fragmentedTargets.unzip3

      // The full set of referenced group by variables 
      val referencedGBVars: Set[String] = perTargetGBVars.flatten.toSet

      // The full list of aggregate expressions we need to compute
      val allAggFunctions = 
        perTargetAggExprs.flatten.
          map({ case (aggName, distinct, args, alias) =>
            AggFunction(aggName, distinct, args, alias)
          })

      // And pull out the list of group by variables that the user has declared for
      // this expression
      val declaredGBVars: Seq[Var] = 
        if(ps.getGroupByColumnReferences == null) { List[Var]()}
        else { 
          ps.getGroupByColumnReferences.
            map({ case c:Column => c }).
            map(convertColumn(_, bindings.toMap))
        }

      // Sanity Check: We should not be referencing a variable that's not in the GB list.
      val referencedNonGBVars = referencedGBVars -- declaredGBVars.map(_.name)
      if(!referencedNonGBVars.isEmpty){
        throw new SQLException(s"Variables $referencedNonGBVars not in group by list")
      }

      // Assemble the Aggregate
      ret = Aggregate(declaredGBVars, allAggFunctions, ret)

      // Generate the post-processing projection targets
      val targetNames = targets.map( _._2 )
      val postProcTargets = 
        targetNames.zip(targetPostProcExprs).
          map( tgt => ProjectArg(tgt._1, tgt._2) )

      // Assemble the post-processing Project
      ret = Project(postProcTargets, ret)

      // Check for a having clause
      if(ps.getHaving() != null){
        // The having clause, as a post-processing step, uses a different set of
        // bindings.  These include:
        //  - The group by variable bindings
        //  - The newly defined aggregate value bindings

        val havingBindings =
          targetNames.map( tgt => (tgt, tgt) ) ++
          declaredGBVars.map( v => (v.name, bindings(v.name)) )

        val havingExpr =
          convert(ps.getHaving, havingBindings.toMap)

        ret = Select(havingExpr, ret)
      }
    }

    // Sanity check unimplemented features
    if(ps.getOrderByElements != null){ unhandled("ORDER BY") }
    if(ps.getLimit != null){ unhandled("LIMIT") }
    if(ps.getDistinct != null){ unhandled("DISTINCT") }

    // We're responsible for returning bindings for this specific
    // query, so extract those from the target expressions we
    // produced earlier
    val returnedBindings =
      targets.map( tgt => (tgt._1, tgt._2) ).toMap

    // The operator should now be fully assembled.  Return it and
    // its bindings
    return (ret, returnedBindings)
  }

  def convert(union: net.sf.jsqlparser.statement.select.Union, alias: String): (Operator, Map[String,String]) =
  {
    val isAll = (union.isAll() || !union.isDistinct());
    if(!isAll){ unhandled("UNION DISTINCT") }
    if(union.getOrderByElements != null){ unhandled("UNION ORDER BY") }
    if(union.getLimit != null){ unhandled("UNION LIMIT") }

    return union.
      getPlainSelects().
      map( convert(_, alias) ).
      reduce( (a,b) => (Union(a._1,b._1), a._2) )
  }

  def convert(fi : FromItem) : (Operator, Map[String, String], String) = {
    if(fi.isInstanceOf[SubJoin]){
      unhandled("FromItem[SubJoin]")
    }
    if(fi.isInstanceOf[SubSelect]){
      if(fi.asInstanceOf[SubSelect].getAlias == null){
        throw new SQLException("Invalid Sub-Select (Needs Alias): "+fi);
      }
      val (ret, bindings) = convert(
        fi.asInstanceOf[SubSelect].getSelectBody,
        fi.asInstanceOf[SubSelect].getAlias.toUpperCase
      );

      // Used by the isNull check
      if(IsNullChecker.lookingForFrom()){
        IsNullChecker.setFrom("("+(fi.asInstanceOf[SubSelect].getSelectBody).toString() + ") as " + fi.asInstanceOf[SubSelect].getAlias);
      }

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


      // Used by the isNull check
      if(IsNullChecker.lookingForFrom()){
        IsNullChecker.setFrom(name);
      }

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
          List[(String,Expression,Type)]()
        ), 
        newBindings, 
        alias
      )
      
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
    if(e.isInstanceOf[InverseExpression]){
      return Not(convert(e.asInstanceOf[InverseExpression].getExpression, bindings))
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
    if(e.isInstanceOf[Column]){ return convertColumn(e.asInstanceOf[Column], bindings) }
    if(e.isInstanceOf[net.sf.jsqlparser.expression.Function]){
      val f = e.asInstanceOf[net.sf.jsqlparser.expression.Function]
      val name = f.getName.toUpperCase
      val parameters : List[Expression] = 
        if(f.getParameters == null) { List[Expression]() }
        else {
          f.getParameters.
            getExpressions.
            map( (o : Any) => convert(o.asInstanceOf[net.sf.jsqlparser.expression.Expression], bindings) ).
            toList
        }
      return (name, parameters) match {
        case ("ROWID", List()) => RowIdVar()
        case ("ROWID", List(x: RowIdPrimitive)) => x
        case ("ROWID", List(x: PrimitiveValue)) => RowIdPrimitive(x.payload.toString)
        case _ if f.isDistinct() => mimir.algebra.Function("DISTINCT_"+name, parameters)
        case _ => mimir.algebra.Function(name, parameters)
      }
      
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
      return ExpressionUtils.makeCaseExpression(
        c.getWhenClauses().map ( (w: WhenClause) => {
          (
            inlineSwitch(convert(w.getWhenExpression(), bindings)),
            convert(w.getThenExpression(), bindings)
          )
        }).toList, 
        convert(c.getElseExpression(), bindings)
      )
    }
    if(e.isInstanceOf[NullValue]) { return NullPrimitive() }
    if(e.isInstanceOf[net.sf.jsqlparser.expression.operators.relational.IsNullExpression]) {

      // IS NULL check
      IsNullChecker.setIsNull(true); // need to set for check later
      IsNullChecker.setIsNullExpression(e.asInstanceOf[net.sf.jsqlparser.expression.operators.relational.IsNullExpression]) // set the null expression for later
      IsNullChecker.setLookingForFrom(false);

      val isNullExpression = e.asInstanceOf[net.sf.jsqlparser.expression.operators.relational.IsNullExpression]
      val ret = mimir.algebra.IsNullExpression(
        convert(isNullExpression.getLeftExpression, bindings)
      )
      if(isNullExpression.isNot){
        return mimir.algebra.Not(ret)
      } else {
        return ret
      }
    }
    unhandled("Expression["+e.getClass+"]: " + e)
  }

  def convertColumn(c: Column, bindings: Map[String, String]): Var =
  {
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

  /**
   * Split an aggregate target expression into its component parts.
   * 
   * This is needed to handle complex aggregate expressions.
   * For example (if X is a group-by var):
   * > X + SUM(Y) / COUNT(*) AS FOO
   * This expression will return:
   * > X + (FOO_1_0 / FOO_1_1)
   * > [X]
   * > [(SUM, [Y], FOO_1_0), (COUNT, [], FOO_1_1)]
   *
   * In short, this function descends through the expression tree
   * and picks out all aggregates and var leaves that it hits.
   *
   * Aggregate expressions are removed from the nested expression
   * and replaced by unique placeholder variables (as long as 
   * alias is a unique prefix), and the entire expression is 
   * reassembled.  
   * 
   * - Raw Variables are returned in the second tuple element
   * - Aggregates are returned in the third tuple element
   * 
   * Unique placeholder variables are assigned unique names based
   * on the path through the operator tree.
   */
  def fragmentAggregateExpression(expr: Expression, alias: String): (
    Expression,                                   // The wrapper expression
    Set[String],                                  // Referenced Group-By Variables
    Seq[(String,Boolean,Seq[Expression],String)]  // Referenced Expressions (fn, args, alias)
  ) =
  {
    expr match {
      case Var(x) => (Var(x), Set(x), List())

      case mimir.algebra.Function(fn, args) if AggregateRegistry.isAggregate(fn) => 
        (Var(alias), Set(), List( (fn, false, args, alias) ))

      case mimir.algebra.Function(fn, args) if (
          fn.startsWith("DISTINCT_") 
          && AggregateRegistry.isAggregate(fn.substring("DISTINCT_".length))
        ) => 
        (Var(alias), Set(), List( (fn, true, args, alias) ))

      case _ => {
        val fragmentedChildren = 
          expr.children.zipWithIndex.
            map( child => fragmentAggregateExpression(child._1, alias+"_"+child._2) )

        val (childExprs, childGBVars, childAggs) =
          fragmentedChildren.unzip3

        (
          expr.rebuild(childExprs), 
          childGBVars.flatten.toSet,
          childAggs.flatten
        )
      }
    }
  }

}