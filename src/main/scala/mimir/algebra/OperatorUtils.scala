package mimir.algebra;

import java.sql._

import mimir.util.RandUtils
import com.typesafe.scalalogging.slf4j.LazyLogging

object OperatorUtils extends LazyLogging {
    
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
      case _ => Seq(o)
    }
  }

  def makeUnion(terms: Seq[Operator]): Operator = 
  {
    if(terms.isEmpty){  }
    terms match {
      case Seq() => throw new SQLException("Union of Empty List")
      case Seq(singleton) => singleton
      case _ => {
        val (head, tail) = terms.splitAt(terms.size / 2)
        Union(makeUnion(head), makeUnion(tail))
      }
    }
  }

  def extractProjections(oper: Operator): (Seq[ProjectArg], Operator) =
  {
    oper match {
      case Project(cols, src) => (cols.map(col => ProjectArg(col.name, col.expression)), src)
      case _ => (oper.columnNames.map(col => ProjectArg(col, Var(col))), oper)
    }
  }

  def mergeWithColumn(target: String, default: Expression, oper: Operator)(merge: Expression => Expression): Operator =
  {
    if(oper.columnNames.contains(target)){
      replaceColumn(target, merge(Var(target)), oper)
    } else {
      oper.addColumn(target -> merge(default))
    }
  }

  def shallowRename(mapping: Map[String, String], oper: Operator): Operator =
  {
    // Shortcut if the mapping is a no-op
    if(!mapping.exists { 
      case (original, replacement) => !original.equals(replacement) 
    }) { return oper; }

    // Strip off any existing projections:
    val (baseProjections, input) = extractProjections(oper)

    // Then rename and reapply them
    Project(
      baseProjections.map { case ProjectArg(name, expr) => 
        ProjectArg(mapping.getOrElse(name, name), expr)
      }, 
      input
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

  def applyFilter(condition: List[Expression], oper: Operator): Operator =
    applyFilter(condition.fold(BoolPrimitive(true))(ExpressionUtils.makeAnd(_,_)), oper)

  def applyFilter(condition: Expression, oper: Operator): Operator =
    condition match {
      case BoolPrimitive(true) => oper
      case _ => 
        oper match {
          case Select(otherCond, src) =>
            Select(ExpressionUtils.makeAnd(condition, otherCond), src)
          case _ => 
            Select(condition, oper)
        }
    }

  def projectColumns(cols: Seq[String], oper: Operator) =
  {
    Project(
      cols.map( (col) => ProjectArg(col, Var(col)) ),
      oper
    )
  }

  def joinMergingColumns(cols: Seq[(String, (Expression,Expression) => Expression)], lhs: Operator, rhs: Operator) =
  {
    val allCols = lhs.columnNames.toSet ++ rhs.columnNames.toSet
    val affectedCols = cols.map(_._1).toSet & lhs.columnNames.toSet & rhs.columnNames.toSet
    val wrappedLHS = 
      Project(
        lhs.columnNames.map( x => 
          ProjectArg(if(affectedCols.contains(x)) { "__MIMIR_LJ_"+x } else { x }, 
                     Var(x))),
        lhs
      )
    val wrappedRHS = 
      Project(
        rhs.columnNames.map( x => 
          ProjectArg(if(affectedCols.contains(x)) { "__MIMIR_RJ_"+x } else { x }, 
                     Var(x))),
        rhs
      )
    Project(
      ((allCols -- cols.map(_._1).toSet).map( (x) => ProjectArg(x, Var(x)) )).toList ++
      cols.flatMap({
        case (name, op) =>
          if(affectedCols(name)){
            Some(ProjectArg(name, op(Var("__MIMIR_LJ_"+name), Var("__MIMIR_RJ_"+name))))
          } else {
            if(allCols(name)){
              Some(ProjectArg(name, Var(name)))
            } else { 
              None
            }
          }
        }),
      Join(wrappedLHS, wrappedRHS)
    )
  }

  /**
   * Safely join two columns together, even if there's some possibility that the two
   * joins have non-overlapping columns.
   * Columns on the right-hand-side of the join will be assigned new names.
   * @param lhs     The left hand side of the join to create
   * @param rhs     The right hand side of the join to create
   * @return        A conflict-free join, and a list of renamings for the right-hand-side columns.
   */
  def makeSafeJoin(lhs: Operator, rhs: Operator): (Operator, Map[String,String]) = 
  {
    def lhsCols = lhs.columnNames.toSet
    def rhsCols = rhs.columnNames.toSet
    def conflicts = lhsCols & rhsCols
    logger.trace(s"Make Safe Join: $lhsCols & $rhsCols = $conflicts => \n${Join(lhs, rhs)}")
    if(conflicts.isEmpty){
      (Join(lhs, rhs), Map())
    } else {
      var currRhs = rhs
      var rewrites = List[(String, String)]()
      for( conflict <- conflicts ){
        val (newConflictName, newRhs) = makeColumnNameUnique(conflict, lhsCols ++ rhsCols, currRhs)
        rewrites = (conflict, newConflictName) :: rewrites
        currRhs = newRhs
      }
      logger.debug(s"   RHS Rewritten $rewrites\n$currRhs")
      (Join(lhs, currRhs), rewrites.toMap)
    }

  }

  /**
   * Alpha-rename the specified column in the specified tree.  That is, take the specified
   * operator tree and modify it so that the column `name` is a name that is not part of
   * `conflicts`.  
   * @param name        The name of the column to rewrite.
   * @param conflicts   Names to avoid renaming `name` to.
   * @param oper        The operator tree to rewrite
   * @return            A 2-tuple: The new name of the renamed column, and the new operator tree
   */
  def makeColumnNameUnique(name: String, conflicts: Set[String], oper: Operator): (String, Operator) =
  {
    if(!conflicts(name)){ return (name, oper); }
    if(!oper.columnNames.exists { _.equals(name) }){ 
      throw new RAException(s"Error in makeColumnNameUnique: Wanting to rename $name in \n$oper")
    }
    val allConflicts = conflicts ++ findRenamingConflicts(name, oper)
    val newName = RandUtils.uniqueName(name, allConflicts)
    (newName, deepRenameColumn(name, newName, oper))
  }

  private def findRenamingConflicts(name: String, oper: Operator): Set[String] =
  {
    oper match {
      case Select(cond, src) => 
        findRenamingConflicts(name, src)
      case Project(cols, _) => 
        cols.map(_.name).toSet
      case Aggregate(gb, agg, src) => 
        gb.map(_.name).toSet ++ agg.map(_.alias).toSet ++ (
          if(gb.exists( _.name.equals(name) )){
            findRenamingConflicts(name, src)
          } else { Set() }
        )
      case Union(lhs, rhs) => 
        findRenamingConflicts(name, lhs) ++ findRenamingConflicts(name, rhs)
      case Join(lhs, rhs) => 
        findRenamingConflicts(name, lhs) ++ findRenamingConflicts(name, rhs)
      case EmptyTable(_) | Table(_,_,_,_) | View(_,_,_) | SingletonTable(_,_) => 
        oper.columnNames.toSet
      case Sort(_, src) =>
        findRenamingConflicts(name, src)
      case Limit(_, _, src) =>
        findRenamingConflicts(name, src)
      case LeftOuterJoin(lhs, rhs, cond) =>
        findRenamingConflicts(name, lhs) ++ findRenamingConflicts(name, rhs)
      case Annotate(src, _) => 
        findRenamingConflicts(name, src)
      case ProvenanceOf(src) =>
        findRenamingConflicts(name, src)
      case Recover(src, cols) =>
        // Check to see if the column is a recovered annotation... if that's the case,
        // we can apply the renaming here and this operator acts like a Project.
        // Otherwise, we flow-through.
        if(cols.exists { _._2.name.equals(name) }){
          src.columnNames.toSet ++ cols.map { _._2.name }.toSet
        } else {
          findRenamingConflicts(name, src)
        }
    }
  }

  private def deepRenameColumn(target: String, replacement: String, oper: Operator): Operator =
  {
    val rewrite = (e:Expression) => Eval.inline(e, Map(target -> Var(replacement)))
    oper match {
      case Project(cols, src) => {
        Project(
          cols.map { col => 
            if(col.name.equals(target)) { ProjectArg(replacement, col.expression)}
            else { col }
          }, src)
      }
      case Aggregate(gb, aggs, src) => {
        if(gb.exists( _.name.equals(target) )){
          Aggregate(
            gb.map { col => if(col.name.equals(target)){ Var(replacement) } else { col } },
            aggs.map { agg => 
              AggFunction(
                agg.function,
                agg.distinct,
                agg.args.map(rewrite(_)),
                agg.alias
              )
            },
            deepRenameColumn(target, replacement, src)
          )
        } else {
          Aggregate(gb, 
            aggs.map { agg => 
              if(agg.alias.equals(target)){
                AggFunction(
                  agg.function,
                  agg.distinct,
                  agg.args,
                  replacement
                )
              } else { agg }
            },
            src
          )
        }
      }
      case Join(lhs, rhs) => {
        if(lhs.columnNames.exists( _.equals(target) )){
          Join(deepRenameColumn(target, replacement, lhs), rhs)
        } else {
          Join(lhs, deepRenameColumn(target, replacement, rhs))
        }
      }
      case Union(lhs, rhs) => {
        Union(
          deepRenameColumn(target, replacement, lhs),
          deepRenameColumn(target, replacement, rhs)
        )
      }
      case LeftOuterJoin(lhs, rhs, cond) => {
        if(lhs.columnNames.exists( _.equals(target) )){
          LeftOuterJoin(deepRenameColumn(target, replacement, lhs), rhs, rewrite(cond))
        } else {
          LeftOuterJoin(lhs, deepRenameColumn(target, replacement, rhs), rewrite(cond))
        }
      }
      case Table(name, alias, sch, meta) => {
        Table(name, alias, 
          sch.map { col => if(col._1.equals(target)) { (replacement, col._2) } else { col } },
          meta.map { col => if(col._1.equals(target)) { (replacement, col._2, col._3) } else { col } }
        )
      }
      case EmptyTable(sch) => {
        EmptyTable(
          sch.map { col => if(col._1.equals(target)) { (replacement, col._2) } else { col } }
        )
      }
      case SingletonTable(sch, data) => {
        SingletonTable(
          sch.map { col => if(col._1.equals(target)) { (replacement, col._2) } else { col } },
          data
        )
      }

      case View(_, _, _) => {
        Project(
          oper.columnNames.map { col =>
            if(col.equals(target)){ ProjectArg(replacement, Var(target)) }
            else { ProjectArg(col, Var(col)) }
          },
          oper
        )
      }
      case Sort(_, _) | Select(_, _) | Limit(_, _, _) | Annotate(_, _) | ProvenanceOf(_) => 
        oper.
          recurExpressions(rewrite(_)).
          recur(deepRenameColumn(target, replacement, _))

      case Recover(src, cols) =>
        // Check to see if the column is a recovered annotation... if that's the case,
        // we can apply the renaming here and this operator acts like a Project.
        // Otherwise, we flow-through.
        if(cols.exists { _._2.name.equals(target) }){
          Recover(src, 
            cols.map { case old @ (name, AnnotateArg(at, col, t, expr)) =>
              if(col.equals(target)){
                (replacement, AnnotateArg(at, col, t, expr))
              } else {
                old
              }
            }
          )
        } else {
          oper.
            recurExpressions(rewrite(_)).
            recur(deepRenameColumn(target, replacement, _))
        }
    }
  }
}
