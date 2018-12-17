package mimir.sql;

import java.sql._
import java.util

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
//import net.sf.jsqlparser.statement.provenance.ProvenanceStatement
import org.joda.time.LocalDate
import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.collection.JavaConversions._
import scala.collection.{immutable, mutable}
import scala.collection.mutable.ListBuffer
import mimir.provenance.Provenance

;

class SqlToRA(db: Database) 
  extends LazyLogging
{
  private val vizierNameMap = scala.collection.mutable.Map[String, String]()
  
  def registerVizierNameMapping(vizierName:String,mimirName:String) : Unit = {
    vizierNameMap.put(vizierName, mimirName)
  }
  
  def getVizierNameMapping(vizierName:String) : Option[String] =  vizierNameMap.get(vizierName)

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

  def convert(s : ProvenanceStatement) : Operator  = {
    val psel = new ProvenanceOf(convert(s.getSelect()));
    psel
  }
  
  
  def convert(s : net.sf.jsqlparser.statement.select.Select) : Operator = convert(s, null)._1
  def convert(s : net.sf.jsqlparser.statement.select.Select, alias: String) : (Operator, Seq[(String, String)]) = {
    convert(s.getSelectBody(), alias)
  }
  
  def convert(sb : SelectBody) : Operator = 
    convert(sb, null)._1;
  
  /**
   * Convert a SelectBody into an Operator + A projection map.
   */
  def convert(sb : SelectBody, tableAlias: String) : (Operator, Seq[(String, String)]) = 
  {
    sb match {
      case ps: net.sf.jsqlparser.statement.select.PlainSelect => convert(ps, tableAlias)
      case u: net.sf.jsqlparser.statement.select.Union => convert(u, tableAlias)
    }
  }

  /**
   * Convert a PlainSelect into an Operator + A projection map.
   */
  def convert(ps: PlainSelect, tableAlias: String) : (Operator, Seq[(String, String)]) =
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
    val sources = scala.collection.mutable.MutableList[(String, List[String])]()
    
    //////////////////////// CONVERT FROM CLAUSE /////////////////////////////

    // JSqlParser makes a distinction between the first FromItem, and items 
    // subsequently joined to it.  Start by extracting the first FromItem
    var (ret, currBindings, sourceAlias) = 
      if(ps.getFromItem() == null){
        (HardTable(Seq(),Seq()), Seq(), "SINGLETON");
      } else {
        convert(ps.getFromItem)
      }
    sources += ( (sourceAlias, currBindings.map(_._2).toList) )
    bindings.putAll(currBindings.toMap[String,String])
    reverseBindings.putAll(
      currBindings.map( _ match { case (x,y) => (y,x) } ).toMap[String,String]
    )

    var joinCond = null;

    // Joins are optional, so make sure there is a join to begin with
    if(ps.getJoins() != null) { 

      // And then flatten out the join tree.
      for(j <- ps.getJoins()){
        val (source, currBindings, sourceAlias) = convert(j.getRightItem())
        sources += ( (sourceAlias, currBindings.map(_._2).toList) )
        bindings.putAll(currBindings.toMap[String,String])
        reverseBindings.putAll(
          currBindings.map( _ match { case (x,y) => (y,x) } ).toMap[String,String]
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

    //////////////////// CONVERT SORT AND LIMIT CLAUSES ///////////////////////

    // Sort and limit are an odd and really really really annoying case, because of
    // how they interact with aggregate and non-aggregate queries.  
    // 
    // - For non-aggregate queries, SORT and LIMIT get applied BEFORE projection.
    //     - This is actually not entirely true... some SQL variants allow you to
    //       use names in both the target and source columns...  eeeew
    // - For aggregate quereis, SORT and LIMIT get applied AFTER aggregation.
    //
    // Because they could get applied in either of two places, we don't actually
    // do the conversion here.  Instead we define the entire conversion in one
    // place (here), wrapped in a function that gets called at the right place 
    // below, once we figure out whether we're dealing with an aggregate or 
    // flat query.
    //
    val applySortAndLimit = 
    () => {
      if(ps.getOrderByElements != null){ 
        val sortDirectives = 
          ps.getOrderByElements.map(ob => {
            val column = 
              ob.getExpression match {
                case col:Column => convertColumn(col, bindings.toMap)
                case _ => unhandled("ORDER BY on complex expression") 
              }
            SortColumn(column, ob.isAsc)
          })
        ret = Sort(sortDirectives, ret)
      }
      if(ps.getLimit != null){ 
        val limit = ps.getLimit
        if(limit.isLimitAll || (limit.getRowCount <= 0)){
          if(limit.getOffset > 0){
            ret = Limit(limit.getOffset, None, ret)
          }
        } else {
          ret = Limit(
                  math.max(0,limit.getOffset), 
                  Some(limit.getRowCount), 
                  ret
                )
        }
      }
    }

    //////////////////////// CONVERT SELECT TARGETS /////////////////////////////

    // Utility function to compute expansions for table wildcard 
    // targets (i.e., table.*).  
    // Returns a 2-tuple: 
    //   t._1 : The base name of the variable in the output.
    //   t._2 : The expression of the variable in the input.
    // Some examples:
    //   SELECT A FROM R
    //     -> ("A", Var("A"))
    //   SELECT A AS B FROM R
    //     -> ("B", Var("A"))
    val defaultTargetsForTable:(String => Seq[(String, Expression)]) = 
      (name: String) => {
        sources.find(_._1.equals(name)).get._2.map(
          (x) => (reverseBindings(x), Var(x))
        )
      }

    // Start by converting to Mimir expressions and expanding clauses
    // Follows the same pattern as the utility function above
    val selectItems: Seq[(net.sf.jsqlparser.statement.select.SelectItem, Int)] = 
      ps.getSelectItems().zipWithIndex

    val baseTargets: Seq[(String, Expression)] = 
      selectItems.flatMap({
        case (se:SelectExpressionItem, idx) => {
          val baseExpr: Expression = convert(se.getExpression, bindings.toMap)

          // Come up with a name for the expression
          // Note, this doesn't need to be unique (yet).  We assign unique
          // names in a post-processing step
          val alias: String = SqlUtils.getAlias(se).toUpperCase;

          // Some expressions need to be special-cased
          val extendedExpr = baseExpr match {
            case Var("ROWID") => RowIdVar()
            case x => x
          }

          Some( (alias, extendedExpr) )
        }

        case (_:AllColumns, _) => 
          sources.map(_._1).flatMap( defaultTargetsForTable(_) )

        case (tc:AllTableColumns, _) =>
          defaultTargetsForTable(tc.getTable.getName.toUpperCase)
      })

    // After wildcard expansion, do some post-processing on the targets.
    // First, it's possible that the aliases assigned here might contain 
    // duplicates.  Make sure that all of the aliases have unique names
    val uniqueAliases = 
      SqlUtils.makeAliasesUnique(baseTargets.map(_._1))

    val targetsWithUniqueAliases =
      uniqueAliases.zip(baseTargets.map(_._2))

    // We're also responsible for assigning a globally visible name based
    // on the table alias (if it's present), and retaining a mapping back
    // to the original name.  To do this, we need to create the global
    // name by prepending the table alias we've been given.  The globally
    // visible schema is what the ProjectArgs should use.
    val targets: Seq[(String, String, Expression)] =
      if(tableAlias == null){
        // No table alias given.  The globally visible alias is just the one we picked
        targetsWithUniqueAliases.map({ case (alias, expr) => 
                                          (alias, alias, expr) })
      } else {
        // Table alias exists.  The globally visible alias needs the table name prepended
        targetsWithUniqueAliases.map({ case (alias, expr) => 
                                          (alias, s"${tableAlias}_${alias}", expr) })
      }


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

    val isAggSelect =
      hasGroupByRefs || hasHavingClause || 
      targets.map { _._3 }.exists { expressionContainsAggregate(_) }

    if(!isAggSelect){
      // NOT an aggregate select.  

      // Apply the sort and limit clauses before the projection if necessary
      applySortAndLimit()

      // Create a simple projection around the return value.
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
      var referencedGBVars: Set[String] = perTargetGBVars.flatten.toSet

      // The full list of aggregate expressions we need to compute
      var allAggFunctions = 
        perTargetAggExprs.flatten
          .map({ case (aggName, distinct, args, alias) =>
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

      // Column names in the output schema
      val targetNames = targets.map( _._2 )

      // The having clause.  "Some" if there is one, "None" if not.
      // The Boolean is TRUE if the having expression is applied to the
      // post-aggregate schema and FALSE if applied to the pre-aggregate schema
      val havingExprAndIsPostAgg: Option[(Expression, Boolean)] = 
        if(ps.getHaving != null){
          // SQL is Ugh...
          // HAVING can be interpreted in one of two ways: Either it applies
          // to the output of the SELECT (i.e., referencing variables in the
          // output schema of the aggregate), or it can define new aggregate
          // values.  Worse, the signaling about which is which is fairly weak
          //
          // We adopt a pragmatic approach: If you use an aggregate function
          // in the HAVING clause, we assume it uses the pre-aggregate schema.
          // Otherwise it's the post-aggregate schema.

          val postAggregateBindings =
            targetNames.map( tgt => (tgt, tgt) ) ++
            declaredGBVars.map { v => (
              reverseBindings(v.name),
              v.name
            )}

          // Our first attempt at conversion: A post-aggregate
          val postAggregateHavingExpr: Option[Expression] =
            // Notably... the conversion could also fail due to missing
            // bindings.  This is our backup signal to fail over.
            ///
            // TODO: It might be useful if we had a function to do the 
            // AggFunction test prior to conversion.  That is, if we
            // had a version of expressionContainsAggregate that worked
            // on JSQLParser Expressions.
            try {
              val firstConversionAttempt = convert(ps.getHaving, postAggregateBindings.toMap)
              if(expressionContainsAggregate(firstConversionAttempt)){ None }
              else { Some(firstConversionAttempt) }
            } catch { case e: SQLException => None }

          // At this point, if `postAggregateHavingExpr` has something, then
          // it's safe to convert it directly.  Otherwise, we need to do some 
          // finagling to create temporary attributes in the aggregate.
          postAggregateHavingExpr match {
            case Some(havingExpr) => Some( (havingExpr, true) )

            case None => {
              // If we're here, it means that we're dealing with a pre-aggregate
              // HAVING expression.  Use the original bindings to convert the
              // expression.  If this fails, the SQL actually does have an error.
              val secondConversionAttempt = convert(ps.getHaving, bindings)

              val (havingPostExpression, havingGBVars, havingAggExprs) =
                fragmentAggregateExpression(secondConversionAttempt, "MIMIR_HAVING")

              // Tack on the newly generated aggregate expressions
              referencedGBVars = referencedGBVars ++ havingGBVars
              allAggFunctions = allAggFunctions ++ 
                havingAggExprs.map({ case (aggName, distinct, args, alias) =>
                  AggFunction(aggName, distinct, args, alias)
                })

              // And then the PostExpression is what we want to use in the
              // Select() that gets attached to the query.
              /* return */ Some( (havingPostExpression, false) )
            }
          }


        } else { None }

      // Sanity Check: We should not be referencing a variable that's not in the GB list.
      val referencedNonGBVars = referencedGBVars -- declaredGBVars.map(_.name)
      if(!referencedNonGBVars.isEmpty){
        throw new SQLException(s"Variables $referencedNonGBVars not in group by list")
      }

      // Assemble the Aggregate
      ret = Aggregate(declaredGBVars, allAggFunctions, ret)

      // Generate the post-processing projection targets
      val postProcTargets = 
        targetNames.zip(targetPostProcExprs).
          map( tgt => ProjectArg(tgt._1, tgt._2) )

      // If the having clause is applied to the pre-aggregate schema,
      // then we need to apply it before projecting down to the post-aggregate
      // schema.
      havingExprAndIsPostAgg match {
        case Some( (havingExpr, false) ) => {
          ret = Select(havingExpr, ret)
        }
        case _ => ()
      }

      // Assemble the post-processing Project
      ret = Project(postProcTargets, ret)

      // If the having clause is applied to the post-aggregate schema,
      // then we need to apply it after projecting down to the post-aggregate
      // schema.
      havingExprAndIsPostAgg match {
        case Some( (havingExpr, true) ) => {
          ret = Select(havingExpr, ret)
        }
        case _ => ()
      }

      // Apply sort and limit if necessary
      applySortAndLimit()
    }

    // Sanity check unimplemented features
    if(ps.getDistinct != null){ unhandled("DISTINCT") }

    // We're responsible for returning bindings for this specific
    // query, so extract those from the target expressions we
    // produced earlier
    val returnedBindings =
      targets.map( tgt => (tgt._1, tgt._2) )

    // The operator should now be fully assembled.  Return it and
    // its bindings
    return (ret, returnedBindings)
  }

  def expressionContainsAggregate(tgt: Expression): Boolean =
  {
    val allReferencedFunctions =
        ExpressionUtils.getFunctions(tgt)
          .map( tgt => if(tgt.startsWith("DISTINCT_")) {tgt.substring("DISTINCT_".length)} 
                       else { tgt })
    allReferencedFunctions.exists( db.aggregates.isAggregate(_) )
  }

  def convert(union: net.sf.jsqlparser.statement.select.Union, alias: String): (Operator, Seq[(String,String)]) =
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

  def convert(fi : FromItem) : (Operator, Seq[(String, String)], String) = {
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
      return (ret, bindings, fi.asInstanceOf[SubSelect].getAlias.toUpperCase)
    }
    if(fi.isInstanceOf[net.sf.jsqlparser.schema.Table]){
      var name =
        fi.asInstanceOf[net.sf.jsqlparser.schema.Table].
          getName.toUpperCase
      var alias =
         fi.asInstanceOf[net.sf.jsqlparser.schema.Table].
          getAlias
      if(alias == null){ alias = name }
      else { alias = alias.toUpperCase }

      if(fi.asInstanceOf[net.sf.jsqlparser.schema.Table].getSchemaName == null){
        val tableOp = if(db.metadataTables.contains(name) || name.startsWith("MIMIR_DA_FDG_") || name.startsWith("MIMIR_DA_SCH_")) 
          db.metadataTable(name, alias) else {
            vizierNameMap.get(name) match {
              case Some(mimirName) if name.equals(alias) => db.table(mimirName, mimirName)
              case Some(mimirName) => db.table(mimirName, alias)
              case None => db.table(name, alias)
            }
          }
        val newBindings = tableOp.columnNames.map { x => (x, alias+"_"+x) }
        return (
          Project(
            newBindings.map { x => ProjectArg(x._2, Var(x._1)) },
            tableOp
          ),
          newBindings, 
          alias
        )
      } else {
        val multilens =
          fi.asInstanceOf[net.sf.jsqlparser.schema.Table].getSchemaName.toUpperCase

        val viewQuery = 
          db.adaptiveSchemas.viewFor(multilens, name) match {
            case Some(query) => query
            case None => throw new SQLException("Unknown adaptive schema view: "+multilens+"."+name);
          }
        val newBindings = viewQuery.columnNames.map { x => (x, alias+"_"+x) }
        return ( 
          Project(
            newBindings.map( x => ProjectArg(x._2, Var(x._1)) ),
            viewQuery
          ),
          newBindings,
          alias
        )
      }
      
    }
    unhandled("FromItem["+fi.getClass.toString+"]")
  }
  // 
  def convert(e : net.sf.jsqlparser.expression.PrimitiveValue) : PrimitiveValue =
  {
    e match { 
      case i: LongValue   => return IntPrimitive(i.getValue())
      case f: DoubleValue => return FloatPrimitive(f.getValue())
      case s: StringValue => return StringPrimitive(s.getValue())
      case _: NullValue   => return NullPrimitive()
      case d: DateValue   => {
        val d2 = new LocalDate(e.asInstanceOf[DateValue].getValue())
        return DatePrimitive(d2.getYear(), d2.getMonthOfYear(), d2.getDayOfMonth())
      }
    }    
  }

  def convert(e : net.sf.jsqlparser.expression.Expression) : Expression = 
    convert(e, Map[String,String]())
  def convert(e : net.sf.jsqlparser.expression.Expression, bindings: String => String) : Expression = {
    e match {
      case prim: net.sf.jsqlparser.expression.PrimitiveValue => convert(prim)
      case inv: InverseExpression => 
        return Not(convert(inv.getExpression, bindings))
      case bin: BinaryExpression => {
        val lhs = convert(bin.getLeftExpression(), bindings)
        val rhs = convert(bin.getRightExpression(), bindings)
        bin match {  
          case _:Addition       => return Arithmetic(Arith.Add, lhs, rhs)
          case _:Subtraction    => return Arithmetic(Arith.Sub, lhs, rhs)
          case _:Multiplication => return Arithmetic(Arith.Mult, lhs, rhs)
          case _:Division       => return Arithmetic(Arith.Div, lhs, rhs)
          case _:AndExpression  => return Arithmetic(Arith.And, lhs, rhs)
          case _:OrExpression   => return Arithmetic(Arith.Or, lhs, rhs)
      
          case _:EqualsTo          => return Comparison(Cmp.Eq, lhs, rhs)
          case _:NotEqualsTo       => return Comparison(Cmp.Neq, lhs, rhs)
          case _:GreaterThan       => return Comparison(Cmp.Gt, lhs, rhs)
          case _:MinorThan         => return Comparison(Cmp.Lt, lhs, rhs)
          case _:GreaterThanEquals => return Comparison(Cmp.Gte, lhs, rhs)
          case _:MinorThanEquals   => return Comparison(Cmp.Lte, lhs, rhs)

          case like: LikeExpression =>
            return Comparison(if(like.isNot()){ Cmp.NotLike } else { Cmp.Like }, lhs, rhs)


        }
      }
      
      case col:Column => return convertColumn(col, bindings)

      case cast:Function if cast.getName.toUpperCase.equals("CAST") => {
        val params = cast.getParameters.getExpressions
        if(params.size() != 2){
          throw new SQLException(s"Invalid CAST: $cast")
        }
        val target = convert(params(0))
        val t = (params(1) match {
            case s: StringValue => BaseType.fromString(s.toRawString)
            case c: Column      => BaseType.fromString(c.getColumnName)
            case _              => None
          }).getOrElse { 
            throw new SQLException(s"Invalid CAST Type: $cast")
          }

        return mimir.algebra.Function("CAST", Seq(target, TypePrimitive(t)))
      }

      case f:Function => {
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
      }

      case b:net.sf.jsqlparser.expression.operators.relational.Between => {
        val lhs = convert(b.getLeftExpression(), bindings)
        val start = convert(b.getBetweenExpressionStart(), bindings)
        val end = convert(b.getBetweenExpressionEnd(), bindings)
        Arithmetic(
          Arith.And,
          Comparison(Cmp.Lte, start, lhs),
          Comparison(Cmp.Lte, lhs, end)
        )
      }

      case i:net.sf.jsqlparser.expression.operators.relational.InExpression => {
        val lhs = convert(i.getLeftExpression)
        i.getItemsList match {
          case e: net.sf.jsqlparser.expression.operators.relational.ExpressionList => {
            val comparables = e.getExpressions.map { convert(_) }

            val baseTest = ExpressionUtils.makeAnd(
                comparables.map { Comparison(Cmp.Eq, lhs, _) }
              )

            if(i.isNot){
              ExpressionUtils.makeNot(baseTest)
            } else { baseTest }
          }
        }

      }

      case c:net.sf.jsqlparser.expression.CaseExpression => {
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

      case isnull: net.sf.jsqlparser.expression.operators.relational.IsNullExpression => {
        val base = mimir.algebra.IsNullExpression(
          convert(isnull.getLeftExpression, bindings)
        )
        if(isnull.isNot){ return Not(base) }
        else { return base; }
      }
    }
  }

  def convertColumn(c: Column, bindings: String => String): Var =
  {
    var name = SqlUtils.canonicalizeIdentifier(c.getColumnName)

    c.getTable.getName match {
      case null => 
        val binding = 
          try {
            bindings(name);
          } catch {
            case _:NoSuchElementException => 
              //TODO: this is a temporary hack for spark data source loads that have 
              //  lower case cols.  we need to fix it at the source.
              try {
                bindings(name.toLowerCase());
              } catch {
                case _:NoSuchElementException => 
                  throw new SQLException(s"Unknown Variable: $name in $bindings")
              }
          }
        return Var(binding)
      case table => 
        return Var(table.toUpperCase + "_" + name);
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
    logger.debug(s"Fragmenting: $alias <- $expr")
    val recur = () => {
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

    expr match {
      case Var(x) => (Var(x), Set(x), List())

      case mimir.algebra.Function(fnBase, args) => {
        val fnUpper = fnBase.toUpperCase
        val fnIsDistinct = (fnUpper.startsWith("DISTINCT_"))
        val fn = 
          if(fnIsDistinct){ fnUpper.substring("DISTINCT_".length) }
          else { fnUpper }

        if(db.aggregates.isAggregate(fn)){
          (Var(alias), Set(), List( (fn, fnIsDistinct, args, alias) ))
        } else {
          recur()
        }
      }

      case _ => recur()
    }
  }

}
