package mimir.ctables

import java.sql.SQLException

import mimir.algebra._
import mimir.util._

object CTPercolator {

  /**
   * Transform an operator tree into a union of
   * union-free, percolated subtrees.
   * Postconditions:   
   *   The root of the tree is a hierarchy of Unions
   *   The leaves of the Union hierarchy are...
   *     ... if the leaf is nondeterministic,
   *       then a Project node that may be uncertain
   *       but who's subtree is deterministic
   *     ... if the leaf has no uncertainty, 
   *       then an arbitrary deterministic subtree
   */
  def percolate(oper: Operator): Operator = {
    // println("Percolate: "+o)
    OperatorUtils.extractUnions(
      propagateRowIDs(oper)
    ).map( percolateOne(_) ).reduceLeft( Union(true,_,_) )
  }
  
  /*
   * Normalize a union-free operator tree by percolating 
   * uncertainty-creating projections up through the
   * operator tree.  Selection predicates based on 
   * probabilistic predicates are converted into 
   * constraint columns.  If necessary uncertainty-
   * creating projections are converted into non-
   * uncertainty-creating projections to allow 
   * the uncertain attributes to percolate up.
   */
  def percolateOne(o: Operator): Operator = 
  {
    // println("percolateOne: "+o)
    val extractProject:
        Operator => ( List[(String,Expression)], Operator ) =
      (e: Operator) =>
        e match { 
          case Project(cols, rest) => (
              cols.map( (x) => (x.column, x.input) ),
              rest
            )
          case _ => (
              e.schema.map(_._1).map( (x) => (x, Var(x)) ).toList,
              e
            )
        }
    val addConstraintCol = (e: Operator) => {
      if(e.schema.map(_._1).toSet contains CTables.conditionColumn) {
        List(ProjectArg(CTables.conditionColumn, Var(CTables.conditionColumn)))
      } else {
        List[ProjectArg]()
        // List(ProjectArg(CTables.conditionColumn, BoolPrimitive(true)))
      }
    }
    val afterDescent = 
      o.rebuild( 
        o.children.map( percolateOne(_) ) 
        // post-condition: Only the immediate child
        // of o is uncertainty-generating.
      ) 
    // println("---\nbefore\n"+o+"\nafter\n"+afterDescent+"\n")
    afterDescent match {
      case t: Table => t
      case Project(cols, p2 @ Project(_, source)) => 
        val bindings = p2.bindings
        // println("---\nrebuilding\n"+o)
        // println("mapping with bindings " + bindings.toString)
        val ret = Project(
          (cols ++ addConstraintCol(p2)).map( 
            (x) => ProjectArg( 
              x.column, 
              Eval.inline(x.input, bindings) 
          )),
          source
        )
        // println("---\nrebuilt\n"+ret)
        return ret
      case Project(cols, source) =>
        return Project(cols ++ addConstraintCol(source), source)
      case s @ Select(cond1, Select(cond2, source)) => 
        return Select(Arithmetic(Arith.And,cond1,cond2), source)
      case s @ Select(cond, p @ Project(cols, source)) =>
        // Percolate the projection up through the 
        // selection
        if(!CTables.isProbabilistic(p)){
          return Project(cols, 
            percolateOne(Select(Eval.inline(cond, p.bindings),source))
          )
        } else {
          val newCond = Eval.inline(cond, p.bindings)
          CTables.extractProbabilisticClauses(newCond) match {
            case (BoolPrimitive(true), BoolPrimitive(true)) => 
              // Select clause is irrelevant
              return p
            case (BoolPrimitive(true), c:Expression) => 
              // Select clause is deterministic
              return Project(cols, Select(c, source))
            case (u, c) => 
              // Select clause is at least partly 
              // nondeterministic
              val newSelect = 
                if(c == BoolPrimitive(true)){ source }
                else { percolateOne(Select(c, source)) }
              val inputCondition = 
                cols.find( _.column == CTables.conditionColumn )
              if(inputCondition.isEmpty){
                return Project(cols ++ List(
                          ProjectArg(CTables.conditionColumn, u)
                        ), 
                        newSelect
                )
              } else {
                return Project(cols.map(
                    (x) => if(x.column == CTables.conditionColumn){
                      ProjectArg(CTables.conditionColumn, 
                        Arith.makeAnd(x.input, u)
                      )
                    } else { x }
                ), newSelect)
              }
          }
        }
      case s: Select => return s
      case Join(lhs, rhs) => {
        
        // println("Percolating Join: \n" + o)
        
        val rename = (name:String, x:String) => 
                ("__"+name+"_"+x)
        val (lhsCols, lhsChild) = extractProject(percolate(lhs))
        val (rhsCols, rhsChild) = extractProject(percolate(rhs))
        
//         println("Percolated LHS: "+lhsCols+"\n" + lhsChild)
//         println("Percolated RHS: "+rhsCols+"\n" + rhsChild)
        
        // Pulling projections up through a join may require
        // renaming columns under the join if the same column
        // name appears on both sides of the source
        val lhsColNames = lhsChild.schema.map(_._1).toSet
        val rhsColNames = rhsChild.schema.map(_._1).toSet
        
        val conflicts = (
          (lhsColNames & rhsColNames)
          | (Set[String]("ROWID") & lhsColNames)
          | (Set[String]("ROWID") & rhsColNames)
        ) 
//        println("CONFLICTS: "+conflicts+"in: "+lhsColNames+", "+rhsColNames+"; for \n"+afterDescent);
          
        val newJoin = 
          if(conflicts.isEmpty) {
            Join(lhsChild, rhsChild)
          } else {
            val fullMapping = (name:String, x:String) => {
              ( if(conflicts contains x){ rename(name, x) }
                else { x }, 
                Var(x)
              )
            }
            // Create a projection that remaps the names of
            // all the variables to the appropriate unqiue
            // name.
            val rewrite = (name:String, child:Operator) => {
              Project(
                child.schema.map(_._1).
                  map( fullMapping(name, _) ).
                  map( (x) => ProjectArg(x._1, x._2)).toList,
                child
              )
            }
            Join(
              rewrite("LHS", lhsChild),
              rewrite("RHS", rhsChild)
            )
          }
        val remap = (name: String, 
                     cols: List[(String,Expression)]) => 
        {
          val mapping =
            conflicts.map( 
              (x) => (x, Var(rename(name, x))) 
            ).toMap[String, Expression]
          cols.filter( _._1 != CTables.conditionColumn ).
            map( _ match { case (name, expr) =>
              (name, Eval.inline(expr, mapping))
            })
        }
        var cols = remap("LHS", lhsCols) ++
                   remap("RHS", rhsCols)
        val lhsHasCondition = 
          lhsCols.exists( _._1 == CTables.conditionColumn)
        val rhsHasCondition = 
          rhsCols.exists( _._1 == CTables.conditionColumn)
        if(lhsHasCondition || rhsHasCondition) {
          if(conflicts contains CTables.conditionColumn){
            cols = cols ++ List(
              ( CTables.conditionColumn, 
                Arithmetic(Arith.And, 
                  Var(rename("LHS", CTables.conditionColumn)),
                  Var(rename("RHS", CTables.conditionColumn))
              )))
          } else {
            cols = cols ++ List(
              (CTables.conditionColumn, Var(CTables.conditionColumn))
            )
          }
        }
        // println(cols.toString);
        val ret = {
          if(cols.exists(
              _ match {
                case (colName, Var(varName)) => 
                        (colName != varName)
                case _ => true
              }) 
            )
          {
            Project( cols.map(
              _ match { case (name, colExpr) => 
                ProjectArg(name, colExpr)
              }),
              newJoin
            )
          } else {
            newJoin
          }
        }
        return ret
      }
    }
  }

  def percolateNoJoin(oper: Operator): Operator = {
    OperatorUtils.extractUnions(
      propagateRowIDs(oper)
    ).map( percolateOneNoJoin(_) ).reduceLeft( Union(true,_,_) )
  }

  def percolateOneNoJoin(o: Operator): Operator =
  {
    // println("percolateOne: "+o)
    val extractProject:
    Operator => ( List[(String,Expression)], Operator ) =
      (e: Operator) =>
        e match {
          case Project(cols, rest) => (
            cols.map( (x) => (x.column, x.input) ),
            rest
            )
          case _ => (
            e.schema.map(_._1).map( (x) => (x, Var(x)) ).toList,
            e
            )
        }
    val afterDescent =
      o.rebuild(
        o.children.map( percolateOne(_) )
        // post-condition: Only the immediate child
        // of o is uncertainty-generating.
      )
    // println("---\nbefore\n"+o+"\nafter\n"+afterDescent+"\n")
    afterDescent match {
      case t: Table => t
      case Project(cols, p2 @ Project(_, source)) =>
        val bindings = p2.bindings
        // println("---\nrebuilding\n"+o)
        // println("mapping with bindings " + bindings.toString)
        val ret = Project(
          (cols).map(
            (x) => ProjectArg(
              x.column,
              Eval.inline(x.input, bindings)
            )),
          source
        )
        // println("---\nrebuilt\n"+ret)
        ret
      case Project(cols, source) =>
        Project(cols, source)
      case s @ Select(cond1, Select(cond2, source)) =>
        Select(Arithmetic(Arith.And,cond1,cond2), source)
      case s @ Select(cond, p @ Project(cols, source)) =>
        // Percolate the projection up through the
        // selection
        Project(cols,
            percolateOne(Select(Eval.inline(cond, p.bindings),source))
          )

      case s: Select => s
      case Join(lhs, rhs) => {

        // println("Percolating Join: \n" + o)

        val rename = (name:String, x:String) =>
          ("__"+name+"_"+x)
        val (lhsCols, lhsChild) = extractProject(percolate(lhs))
        val (rhsCols, rhsChild) = extractProject(percolate(rhs))

        //         println("Percolated LHS: "+lhsCols+"\n" + lhsChild)
        //         println("Percolated RHS: "+rhsCols+"\n" + rhsChild)

        // Pulling projections up through a join may require
        // renaming columns under the join if the same column
        // name appears on both sides of the source
        val lhsColNames = lhsChild.schema.map(_._1).toSet
        val rhsColNames = rhsChild.schema.map(_._1).toSet

        val conflicts = (
          (lhsColNames & rhsColNames)
            | (Set[String]("ROWID") & lhsColNames)
            | (Set[String]("ROWID") & rhsColNames)
          )
        //        println("CONFLICTS: "+conflicts+"in: "+lhsColNames+", "+rhsColNames+"; for \n"+afterDescent);

        val newJoin =
          if(conflicts.isEmpty) {
            Join(lhsChild, rhsChild)
          } else {
            val fullMapping = (name:String, x:String) => {
              ( if(conflicts contains x){ rename(name, x) }
              else { x },
                Var(x)
                )
            }
            // Create a projection that remaps the names of
            // all the variables to the appropriate unqiue
            // name.
            val rewrite = (name:String, child:Operator) => {
              Project(
                child.schema.map(_._1).
                  map( fullMapping(name, _) ).
                  map( (x) => ProjectArg(x._1, x._2)).toList,
                child
              )
            }
            Join(
              rewrite("LHS", lhsChild),
              rewrite("RHS", rhsChild)
            )
          }
        val remap = (name: String,
                     cols: List[(String,Expression)]) =>
        {
          val mapping =
            conflicts.map(
              (x) => (x, Var(rename(name, x)))
            ).toMap[String, Expression]
          cols.map( _ match { case (name, expr) =>
              (name, Eval.inline(expr, mapping))
            })
        }
        var cols = remap("LHS", lhsCols) ++
          remap("RHS", rhsCols)
        // println(cols.toString);
        val ret = {
          if(cols.exists(
            _ match {
              case (colName, Var(varName)) =>
                (colName != varName)
              case _ => true
            })
          )
          {
            Project( cols.map(
              _ match { case (name, colExpr) =>
                ProjectArg(name, colExpr)
              }),
              newJoin
            )
          } else {
            newJoin
          }
        }
        return ret
      }
    }
  }

  def expandProbabilisticCases(expr: Expression): 
    List[(Expression, Expression)] = 
  {
    expr match { 
      case CaseExpression(whenClauses, elseClause) =>
        val whenTerms = // List[condition, result]
          whenClauses.flatMap( (clause) => 
            expandProbabilisticCases(clause.when).flatMap( _ match {
              case (wCondition, wClause) =>
                expandProbabilisticCases(clause.then).map( _ match {
                  case (tCondition, tClause) =>
                    ( Arith.makeAnd(wCondition,
                        Arith.makeAnd(wClause,tCondition)), 
                      tClause
                    )
                })
            })
          ).toList
        val whenSatisfiedIf =
          whenTerms.map( _._1 ).reduce(Arith.makeOr(_,_))
        val elseTriggeredIf = Arith.makeNot(whenSatisfiedIf)
  
        whenTerms ++ 
        expandProbabilisticCases(elseClause).map( _ match {
          case (eCondition, eClause) =>
            ( Arith.makeAnd(eCondition, elseTriggeredIf),
              eClause
            )
        })
  
      case _ => 
        if(CTables.isProbabilistic(expr)){
          ListUtils.powerList[(Expression,Expression)](
            expr.children.map(expandProbabilisticCases(_))
          ).map( (conditionAndChildren) =>
            ( conditionAndChildren.
                map(_._1).foldLeft(
                  BoolPrimitive(true): Expression
                )( 
                  Arith.makeAnd(_,_) 
                ),
              expr.rebuild(conditionAndChildren.map(_._2))
            )
          )
        } else { List((BoolPrimitive(true), expr)) }
    }
  }

  /**
   * Consider the expression: $\pi_{case when A = 1 then B else c}(R)$
   * 
   * It may be beneficial to rewrite the CASE into a union two expressions, as
   * the latter gives the database optimizer a little more leeway in terms of
   * optimization, and also allows us to more efficiently compute deterministic
   * and non-deterministic fragments.  
   * $|pi_{B}(\sigma_{A=1}(R)) \cup \pi_{C}(\sigma_{A\neq 1}(R))$
   * 
   * This function applies this rewrite using extractProbabilisticCases for
   * Expression objects, defined above.
   * 
   * At present, this optimization is not being used.
   */
  def expandProbabilisticCases(oper: Operator): Operator = {
    // println("Expand: " + oper)
    oper match {
      case Project(args, child) =>
        ListUtils.powerList[(Expression,ProjectArg)](
          args.map( (arg:ProjectArg) =>
            if(!CTables.isProbabilistic(arg.input)){
              // println("Skipping '"+arg+"', not probabilistic")
              List((BoolPrimitive(true), arg))
            } else {
              expandProbabilisticCases(arg.input).
                map( _ match {
                  case (cond, expr) =>
                    (cond, ProjectArg(arg.column, expr))
                })
            }
          )
        ).map( (condsAndArgs) => {
          val conds = condsAndArgs.map(_._1).
                        reduce(Arith.makeAnd)
          val args = condsAndArgs.map(_._2)
          var ret = expandProbabilisticCases(child)
          if(conds != BoolPrimitive(true)){
            ret = Select(conds, ret)
          }
          if(args.exists( (arg) => arg.input != Var(arg.column) )){
            ret = Project(args, ret)
          }
          ret
        }).reduce[Operator]( Union(true, _, _) )
  
      case _ =>
        oper.rebuild(oper.children.map( expandProbabilisticCases(_) ))
    }
  }
  
  def requiresRowID(expr: Expression): Boolean = {
    expr match {
      case Var("ROWID") => true;
      case _ => expr.children.exists( requiresRowID(_) )
    }
  }
  
  def propagateRowIDs(oper: Operator): Operator = 
    propagateRowIDs(oper, false)

  def propagateRowIDs(oper: Operator, force: Boolean): Operator = 
  {
    // println("Propagate["+(if(force){"F"}else{"NF"})+"]:\n" + oper);
    oper match {
      case Project(args, child) =>
        var newArgs = args;
        if(force) {
          newArgs = 
            (new ProjectArg("ROWID", Var("ROWID"))) :: 
              newArgs
        }
        Project(newArgs, 
          propagateRowIDs(child, 
            newArgs.exists((x)=>requiresRowID( x.input ))
        ))
        
      case Select(cond, child) =>
        Select(cond, propagateRowIDs(child,
          force || requiresRowID(cond)))
          
      case Join(left, right) =>
        if(force){
          Project(
            ProjectArg("ROWID",
              Function("JOIN_ROWIDS", List[Expression](Var("LEFT_ROWID"), Var("RIGHT_ROWID")))) ::
            (left.schema ++ right.schema).map(_._1).map(
              (x) => ProjectArg(x, Var(x)) 
            ).toList,
            Join(
              Project(
                ProjectArg("LEFT_ROWID", Var("ROWID")) ::
                left.schema.map(_._1).map(
                  (x) => ProjectArg(x, Var(x)) 
                ).toList,
                propagateRowIDs(left, true)),
              Project(
                ProjectArg("RIGHT_ROWID", Var("ROWID")) ::
                right.schema.map(_._1).map(
                  (x) => ProjectArg(x, Var(x)) 
                ).toList,
                propagateRowIDs(right, true))
            )
          )
        } else {
          Join(
            propagateRowIDs(left, false),
            propagateRowIDs(right, false)
          )
        }
        
        case Union(true, left, right) =>
          if(force){
            Union(true, 
              Project(
                ProjectArg("ROWID", 
                  Function("__LEFT_UNION_ROWID",
                    List[Expression](Var("ROWID")))) ::
                left.schema.map(_._1).map(
                  (x) => ProjectArg(x, Var(x)) 
                ).toList,
                propagateRowIDs(left, true)),
              Project(
                ProjectArg("ROWID", 
                  Function("__RIGHT_UNION_ROWID",
                    List[Expression](Var("ROWID")))) ::
                right.schema.map(_._1).map(
                  (x) => ProjectArg(x, Var(x)) 
                ).toList,
                propagateRowIDs(right, true))
            )
          } else {
            Union(true, 
              propagateRowIDs(left, false),
              propagateRowIDs(right, false)
            )
          }
        
        case Table(name, sch, metadata) =>
          if(force && !metadata.exists( _._1 == "ROWID" )){
            Table(name, sch, metadata ++ Map(("ROWID", Type.TRowId)))
          } else {
            Table(name, sch, metadata)
          }
      
    }
  }




  private def extractMissingValueVarPVar(expr: Expression): (Option[Var], Option[VGTerm]) = {
    expr match {
      case CaseExpression(List(WhenThenClause(IsNullExpression(var1: Var, false), vg: VGTerm)), var2: Var) =>
        if(var1 == var2) (Some(var1), Some(vg)) else (None, None)

      case va: Var => (Some(va), None)

      case _ => (None, None)
    }
  }

  private def extractMissingValueMCClause(expr: Expression):
    ( (Option[Var], Option[VGTerm]), (Option[Var], Option[VGTerm]) ) = {

    expr match {
      case Comparison(_, lhs, rhs) =>
        (extractMissingValueVarPVar(lhs), extractMissingValueVarPVar(rhs))

    }
  }

  def splitArith(expr: Expression): List[Expression] = {
    expr match {
      case Arithmetic(op, lhs, rhs) => splitArith(lhs) ++ splitArith(rhs)
      case x => List(x)
    }
  }

  val removeConstraintColumn = (oper: Operator) => {
    oper match {
      case Project(cols, src) =>
        Project(cols.filterNot((p) => p.column.equalsIgnoreCase(CTables.conditionColumn)), src)
      case _ =>
        oper
    }
  }

  def partition(oper: Project): Operator = {
    val cond = oper.columns.find(p => p.column.equalsIgnoreCase(CTables.conditionColumn)).head.input
    var detOp: Operator = null
    var nondetOp: Operator = null

    val (nondetExpr, detExpr) =
      splitArith(cond).partition { (p) =>
        val (lhs, rhs) = extractMissingValueMCClause(p)
        (lhs._1.isDefined && lhs._2.isDefined) || (rhs._1.isDefined && rhs._2.isDefined)
      }

    if(nondetExpr.nonEmpty)
      nondetOp = nondetExpr.map { (e) =>
        val (lhs, rhs) = extractMissingValueMCClause(e)
        (lhs._1, lhs._2, rhs._1, rhs._2) match {
          case (None, None, None, None) => throw new SQLException("Invalid values in partition")

          case (Some(_), Some(_), None, None) => oper

          case (None, None, Some(_), Some(_)) => oper

          case (Some(va1), Some(vg1), Some(va2), None) =>
            Union(
              true,
              removeConstraintColumn(oper).rebuild( List(Select(Comparison(Cmp.Eq, va1, va2), oper.children().head)) ),
              oper.rebuild( List(Select(IsNullExpression(va1), oper.children().head)) )
            )

          case (Some(va1), None, Some(va2), Some(vg2)) =>
            Union(
              true,
              removeConstraintColumn(oper).rebuild( List(Select(Comparison(Cmp.Eq, va1, va2), oper.children().head)) ),
              oper.rebuild( List(Select(IsNullExpression(va2), oper.children().head)) )
            )

          case (Some(va1), Some(vg1), Some(va2), Some(vg2)) =>
            Union(
              true,
              removeConstraintColumn(oper).rebuild( List(Select(Comparison(Cmp.Eq, va1, va2), oper.children().head))),
              oper.rebuild(
                List(
                  Select(
                    Arithmetic(
                      Arith.Or,
                      IsNullExpression(va1),
                      IsNullExpression(va2)
                    ), oper.children().head
                  )
                )
              )
            )
        }
      }.reduce(Union(false, _, _))

    if(detExpr.nonEmpty)
      detOp = Project(
        oper.columns.filterNot( (p) => p.column.equalsIgnoreCase(CTables.conditionColumn))
          ++ List(ProjectArg(CTables.conditionColumn, detExpr.reduce(Arith.makeAnd(_, _)))),
        oper.source
      )

    (detOp, nondetOp) match {
      case (null, null) => throw new SQLException("Both partitions null")

      case (null, y) => y

      case (x, null) => x

      case (x, y) => Union(true, x, y)
    }
  }

  /**
   * Break up the conditions in the constraint column
   * into deterministic and non-deterministic fragments
   * ACCORDING to the data
   */
  def partitionConstraints(oper: Operator): Operator = {
    oper match {
      case Project(cols, src) =>
        cols.find(p => p.column.equalsIgnoreCase(CTables.conditionColumn)) match {
          case Some(arg) =>
            partition(oper.asInstanceOf[Project])

          case None =>
            oper
        }

      case _ =>
        oper
    }
  }
}