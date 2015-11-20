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

  val mimirRowDeterministicColumnName = "MIMIR_ROW_DET"
  val mimirColDeterministicColumnPrefix = "MIMIR_COL_DET_"

  /**
   * Rewrite the input operator to evaluate a 'provenance lite'
   * 
   * The operator's output will be extended with three types of columns:
   *   - a binary 'Row Deterministic' column that stores whether the row 
   *     is non-deterministically in the result set (true = deterministic).
   *   - a set of binary 'Column Deterministic' columns that store whether
   *     the value of the column is deterministic or not.
   *
   * The return value is a triple: The rewritten operator, an expression
   * for computing the non determinism of all columns, and an expression for
   * computing the non-determinism of all rows.
   */
  def percolateLite(oper: Operator): (Operator, Map[String,Expression], Expression) =
  {
    val schema = oper.schema;

    oper match {
      case Project(columns, src) => {
        val (rewrittenSrc, colDeterminism, rowDeterminism) = percolateLite(src);

        // We can use CTAnalyzer.compileDeterministic to construct an expression
        // that evaluates whether the given expression is non-deterministic.  
        // However, we need to 'trick it' into using the right value.
        // 
        // The NonDeterministicOrigin expression type allows us to define an 
        // expression to be directly inlined into the deterministic clause.
        val sourceMappings = colDeterminism.mapValues( NonDeterministicOrigin(_) )

        // Compute the determinism of each column.
        val newColDeterminismBase = columns.map( case ProjectArg(col, expr) => {
          val isDeterministic = 
            CTAnalyzer.compileDeterministic(
              Eval.inline(expr, sourceMappings)
            );
          
          (col, isDeterministic)
        })

        // Determine which of them are deterministic.
        val computedDeterminismCols = 
           newColDeterminismBase.filterNot( 
            // Retain only columns where the isDeterministic expression
            // is a constant (i.e., no Column references)
            ExpressionUtils.getColumns(_._2).isEmpty
          ).map( 
            // Then just translate to a list of ProjectArgs
            case (col, isDeterministic) => ProjectArg(col, isDeterministic) 
          )

        // Rewrite these expressions so that the computed expressions use the
        // computed version from the source data.
        val newColDeterminism =
          newColDeterminismBase.map( case (col, isDeterministic) =>
            if(ExpressionUtils.getColumns(_._2).isEmpty) {
              (col, isDeterministic)
            } else {
              (col, Var(mimirColDeterministicColumnPrefix+col))
            }
          )

        val retProject = Project(
            columns ++ computedDeterminismCols,
            rewrittenSrc
          )

        return (retProject, newColDeterminism, rowDeterminism)
      }
      case Select(cond, src) => {
        val (rewrittenSrc, colDeterminism, rowDeterminism) = percolateLite(src);

        // As above, we need to inline computed input determinisms
        // into `cond` so that we can use CTAnalyzer on it correctly.
        val sourceMappings = colDeterminism.mapValues( NonDeterministicOrigin(_) )

        // Compute the determinism of the selection predicate
        val condDeterminism = 
          CTAnalyzer.compileDeterministic(
            Eval.inline(cond, sourceMappings)
          )

        // Combine the determinism with the computed determinism from the child...
        val newRowDeterminism = 
          Arith.makeOr(condDeterminism, rowDeterminism)

        if( ExpressionUtils.getColumns(newRowDeterminism).isEmpty
            || condDeterminism.equals(BoolPrimitive(true))
          ){
          // If the predicate's determinism is data-independent... OR if the
          // predicate is deterministic already, then we don't need to add anything.
          // Just return what we already have!
          return (Select(cond, src), colDeterminism, newRowDeterminism)
        } else {
          // Otherwise, we need to tack on a new projection operator to compute
          // the new non-determinism

          val projectArgs = 
            // remap the existing schema
            rewrittenSrc.schema.
              map(_._1).
              // drop any existing row non-determinsm column
              filterNot( _.equals(mimirRowDeterministicColumnName) ).
              // pass the rest through unchanged
              map( (x) => ProjectArg(x, Var(x))) ++
            List(ProjectArg(
              mimirRowDeterministicColumnName,
              newRowDeterminism
            ))

          val newOperator = 
            Project(projectArgs, Select(cond, rewrittenSrc) )

          return (newOperator, colDeterminism, Var(mimirRowDeterministicColumnName))
        }
      }
      case Union(left, right) => 
      {
        val (rewrittenLeft, colDetLeft, rowDetLeft) = percolateLite(left);
        val (rewrittenRight, colDetRight, rowDetRight) = percolateLite(right);

        // We need to make the schemas line up: the schemas of the left and right
        // need to have all of the relevant columns
        // Going to cheat a bit here and just force the projection on.

        val mergeNonDeterminism = (col, detCol, colLeft, colRight) => {
          // It's only safe to skip the non-determinism column if both
          // LHS and RHS have exactly the same condition AND that condition
          // is data-independent.
          if(  ExprUtils.getColumns(colLeft).isEmpty
            && ExprUtils.getColumns(colRight).isEmpty
            && colLeft.equals(colRight)
          ){
            // Skip the column and return the data-independent condition
            (List[(ProjectArg, ProjectArg)](), (col, colLeft))
          } else {
            // Otherwise, materialize the condition, and return a reference
            // to the new condition.
            val detCol = ;
            ( List( (ProjectArg(detCol, colLeft), 
                     ProjectArg(detCol, colRight) )), 
              (col, Var(detCol))
            )
          }
        }

        val (colProjectArgs, colDeterminism) =
          (col.detLeft.keys.asSet ++ col.detRight.keys.asSet).map(
            (col) => 
              mergeNonDeterminism(
                col, mimirColDeterministicColumnPrefix+col, 
                colDetLeft, colDetRight
              )
          ).unzip

        val (condProjectArgs, condDeterminism) =
          mergeNonDeterminism(
            null, mimirRowDeterministicColumnName,
            rowDetLeft, rowDetRight
          )
        
        val (projectArgsLeft, projectArgsRight) = 
          (colProjectArgs.flatten ++ condProjectArgs).unzip
        val projectArgsBase = cols.map( (col) => ProjectArg(col, Var(col)) )

        val newOperator = 
          Union(
              Project(projectArgsBase ++ projectArgsLeft, rewrittenLeft),
              Project(projectArgsBase ++ projectArgsRight, rewrittenRight)
            )

        return (newOperator, colDeterminism.toMap, condDeterminism._2)

      }
      case Join(left, right) =>
      {
        val (rewrittenLeft, colDetLeft, rowDetLeft) = percolateLite(left);
        val (rewrittenRight, colDetRight, rowDetRight) = percolateLite(right);

        return (
          Join(rewrittenLeft, rewrittenRight),
          colDetLeft ++ colDetRight,
          Arith.makeAnd(rowDetLeft, rowDetRight)
        )
      }
      case _:Table => {
        return (oper, 
          // All columns are deterministic
          oper.schema.map( (_._1, BoolPrimitive(true)) ),
          // All rows are deterministic
          BoolPrimitive(true)
        )
      }
    }
  }

  val mimirOriginColumnName = "MIMIR_ORIGIN"
  /**
   * Rewrite the input operator into a two-component split:
   *   - A CTProvenance object describing the uncertainty provenance of all 
   *     attributes in the schema of the output
   *   - An operator rewritten to track inputs that are relevant to the CTProvenance object
   */
  def percolateProvenance(oper: Operator): (Operator, CTProvenance) = 
  {
    oper match {
      case Project(columns, src) => {
        val (srcRewrite, srcProvenance) = percolateProvenance(src)

        // We have three considerations in rewriting provenance.
        //  - `srcProvenance`'s column expressions need to be updated to correspond
        //    to the expressions in `columns`
        //  - `columns` may need to be extended with attributes required by the 
        //    provenance computation.
        //  - This process may introduce naming conflicts, so both `columns` and 
        //    `srcProvenance` need to be updated accordingly.

        // New names for each input column
        var renamedColumns: mutable.Map[String, Expression] = null;
        // Names of existing columns -- do not re-use for new columns
        var reservedNames = columns.map( _.getColumnName() ).asSet;
        // Columns that need to get added
        var addedColumns = List[ProjectArg]();

        // Start by figuring out which columns already exist in the output
        // and what their names are.
        columns.foreach( case ProjectArg(colName, expr) =>
          expr match {
            case Var(varName) => renamedColumns.put(varName, Var(colName)) 
          }
        )

        // Preserve the mimir origin metadata
        if(!(reservedNames contains mimirOriginColumnName)){
          renamedColumns.put(mimirOriginColumnName, Var(mimirOriginColumnName))
          addedColumns = 
            ProjectArg(mimirOriginColumnName, Var(mimirOriginColumnName)) ::
              addedColumns
        }

        // Utility Function to ensure that all dependent columns in `e` are 
        // retained.  
        // Optimization question: We don't actually need to keep *all* of the
        // columns.  Which do we really want to keep?
        //  Possibility 1: Nothing
        //     - We can enumerate which VGTerms *could* affect the result
        //  Possibility 2: Control flow vars
        //     - We can enumerate which VGTerms *do* affect the result
        //     - We can determine whether the output is deterministic
        //  Possibility 3: Everything
        //     - We can do everything in P2
        //     - We can also compute all stddev/etc... values
        // 
        // We can potentially optimize possibility 3 a bit.  For example,
        // let's say we have A*B+VGTerm(C).  Rather than tracking all
        // values, we could force tracking of <X:A*B, C:C>
        //
        // For now, let's do this the simple, dumb way.
        val rebuildExpressionForNewSchema = (e:Expression) => {

          // Start by creating mappings for all of the variables in the 
          // expression.

          ExpressionUtils.getColumns(e).foreach( (varName: String) => {
            // If we already have a new name for the expression, great!
            if(!renamedColumns.hasKey(varName)){
              // if not...
              // find a new name that doesn't conflict with an existing
              // name.
              if(reservedNames contains varName){
                // append _1, _2, _3, ... until we find one that
                // doesn't conflict.
                var i = 1;
                while(reservedNames contains (varName+"_"+i)){ i++; }
                renamedColumns.put(varName, Var(varName+"_"+i))
                reservedNames.add(varName+"_"+i)
                addedColumns = ProjectArg(varName+"_"+i, Var(varName)) :: addedColumns
              } else {
                // Trivial case: varName already doesn't conflict
                renamedColumns.put(varName, Var(varName))
                reservedNames.add(varName)
                addedColumns = ProjectArg(varName, Var(varName)) :: addedColumns
              }

            }
          })
          
          // As of right now, all variables in the provenance are represented 
          // in renamedColumns.  Return the expression with the mappings inlined.
          Eval.inline(e, renamedColumns)
        }

        val outputProvenance =
          srcProvenance.map( case (inputColProvenance, inputCondProvenance) =>
            {
              val colProvenance =
                columns.flatMap( case ProjectArg(col, expr) => 

                  // Define the provenance of 'col' in the project argument output
                  val outputColProvenance = Eval.inline(expr, inputColProvenance)

                  // Trivial possibility: outputColProvenance is deterministic.
                  if(!CTables.isProbabilistic(outputColProvenance)){
                    // If so, great, we don't care about the provenance of this
                    // attribute.  Dump it.
                    List[(String, Expression)]()
                  } else {

                    // Ok.  The `col` is nondeterministic.  We need some provenance.
                    // Apply the utility function to remap the schema
                    val newSchOutputColProvenance =
                      rebuildExpressionForNewSchema(outputColProvenance)

                    // And wrap the provenance in a list for the flatMap.
                    List( (col, newSchOutputColProvenance) )

                  }
                ).asMap
              // We don't really interact with the output condition provenance, 
              // but we do need to remap its schema.  Use the utility function!
              val condProvenance = 
                rebuildExpressionForNewSchema(srcProvenance);

              // ... and return the pair
              (colProvenance, condProvenance)
            }
          )
        
        // The final return value extends the projection with the
        // newly required columns, and uses the newly constructed output 
        // provenance
        ( 
          Project(columns ++ addedColumns, srcRewrite), 
          outputProvenance
        )

      }
      case Select(condition, src) => {
        val (srcRewrite, srcProvenance) = percolateProvenance(src)
        val (extracted, remaining) = CTables.extractProbabilisticClauses(condition)

        val outputProvenance =
          srcProvenance.map( case (inputColProvenance, inputCondProvenance) => {
            (
              inputColProvenance, 
              Arith.makeAnd(inputCondProvenance, extracted)
            )
          })

        remaining match {
          // If the entire condition is non-deterministic, drop the select 
          // operator.
          case BoolPrimitive(true) => (srcRewrite, outputProvenance)
          case _ => (Select(remaining, srcRewrite), outputProvenance)
        }
      }
      case Join(left, right) => {

      }
      case Union(isAll, left, right) => {

      }
      case Table(name, sch, metadata) => {

      }
    }
  }
}

/**
 * Utility class for use with CTAnalyzer's compileDeterministic
 * method.  Allows non-deterministic origin computations to be 
 * in-lined.
 */
case class NonDeterministicOrigin(origin: Expression) extends Expression {
  def exprType(bindings: Map[String,Type.T]) = Type.TBool
  def children: List(origin)
  def rebuild(c: List[Expression]): NonDeterministicOrigin(c(0))
}
