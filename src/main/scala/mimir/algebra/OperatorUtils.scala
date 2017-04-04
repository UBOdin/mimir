package mimir.algebra;

import java.sql._

object OperatorUtils {
    
  /** 
   * Strip the expression required to compute a single column out
   * of the provided operator tree.  
   * Essentially a shortcut for an optimized form of 
   *
   * Project( [Var(col)], oper )
   * 
   * is equivalent to (and will often be slower than): 
   *
   * Project(ret(0)._1, ret(0)._2) UNION 
   *     Project(ret(1)._1, ret(1)._2) UNION
   *     ...
   *     Project(ret(N)._1, ret(N)._2) UNION
   */
  def columnExprForOperator(col: String, oper: Operator): 
    Seq[(Expression, Operator)] =
  {
    oper match {
      case p @ Project(_, src) => 
        List[(Expression,Operator)]((p.bindings.get(col).get, src))
      case Union(lhs, rhs) =>
        columnExprForOperator(col, lhs) ++ 
        	columnExprForOperator(col, rhs)
      case _ => 
        List[(Expression,Operator)]((Var(col), oper))
    }
  }

  def extractUnionClauses(o: Operator): Seq[Operator] =
  {
    o match { 
      case Union(lhs, rhs) => extractUnionClauses(lhs) ++ extractUnionClauses(rhs)
      case _ => List(o)
    }
  }

  def makeUnion(terms: Seq[Operator]): Operator = 
  {
    if(terms.isEmpty){ throw new SQLException("Union of Empty List") }
    val head = terms.head 
    val tail = terms.tail
    if(tail.isEmpty){ return head }
    else { return Union(head, makeUnion(tail)) }
  }

  def makeDistinct(oper: Operator): Operator = 
  {
    Aggregate(
      oper.schema.map(_._1).map(Var(_)),
      Seq(),
      oper
    )
  }

  def extractProjections(oper: Operator): (Seq[ProjectArg], Operator) =
  {
    oper match {
      case Project(cols, src) => (cols.map(col => ProjectArg(col.name, col.expression)), src)
      case _ => (oper.schema.map(col => ProjectArg(col._1, Var(col._1))), oper)
    }
  }

  def projectDownToColumns(columns: Seq[String], oper: Operator): Operator =
  {
    Project( columns.map( x => ProjectArg(x, Var(x)) ), oper)
  }

  def projectAwayColumn(target: String, oper: Operator): Operator =
  {
    val (cols, src) = extractProjections(oper)
    Project(
      cols.filter( !_.name.equalsIgnoreCase(target) ),
      src
    )
  }

  def projectAwayColumns(targets: Set[String], oper: Operator): Operator =
  {
    val targetsUpperCase = targets.map(_.toUpperCase)
    val (cols, src) = extractProjections(oper)
    Project(
      cols.filter { col => !targetsUpperCase(col.name.toUpperCase) },
      src
    )
  }

  def projectInColumn(target: String, value: Expression, oper: Operator): Operator =
  {
    val (cols, src) = extractProjections(oper)
    val bindings = cols.map(_.toBinding).toMap
    Project(
      cols ++ Some(ProjectArg(target, Eval.inline(value, bindings))), 
      src
    )
  }

  def replaceColumn(target: String, replacement: Expression, oper: Operator) =
  {
    val (cols, src) = extractProjections(oper)
    val bindings = cols.map(_.toBinding).toMap
    Project(
      cols.map( col => 
        if(col.name.equalsIgnoreCase(target)){
          ProjectArg(target, Eval.inline(replacement, bindings))  
        } else { col }
      ),
      src
    )
  }

  def renameColumn(target: String, replacement: String, oper: Operator) =
  {
    val (cols, src) = extractProjections(oper)
    Project(
      cols.map( col => 
        if(col.name.equalsIgnoreCase(target)){
          ProjectArg(replacement, col.expression)
        } else { col }
      ),
      src
    )
  }


  def applyFilter(condition: List[Expression], oper: Operator): Operator =
    applyFilter(condition.fold(BoolPrimitive(true))(ExpressionUtils.makeAnd(_,_)), oper)

  def applyFilter(condition: Expression, oper: Operator): Operator =
    condition match {
      case BoolPrimitive(true) => oper
      case _ => Select(condition, oper)
    }

  def projectColumns(cols: Seq[String], oper: Operator) =
  {
    Project(
      cols.map( (col) => ProjectArg(col, Var(col)) ),
      oper
    )
  }

  def joinMergingColumns(cols: List[(String, (Expression,Expression) => Expression)], lhs: Operator, rhs: Operator) =
  {
    val allCols = lhs.schema.map(_._1).toSet ++ rhs.schema.map(_._1).toSet
    val affectedCols = cols.map(_._1).toSet
    val wrappedLHS = 
      Project(
        lhs.schema.map(_._1).map( x => 
          ProjectArg(if(affectedCols.contains(x)) { "__MIMIR_LJ_"+x } else { x }, 
                     Var(x))),
        lhs
      )
    val wrappedRHS = 
      Project(
        rhs.schema.map(_._1).map( x => 
          ProjectArg(if(affectedCols.contains(x)) { "__MIMIR_RJ_"+x } else { x }, 
                     Var(x))),
        rhs
      )
    Project(
      ((allCols -- affectedCols).map( (x) => ProjectArg(x, Var(x)) )).toList ++
      cols.map({
        case (name, op) =>
          ProjectArg(name, op(Var("__MIMIR_LJ_"+name), Var("__MIMIR_RJ_"+name)))

        }),
      Join(wrappedLHS, wrappedRHS)
    )
  }
}