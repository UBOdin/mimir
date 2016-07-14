package mimir.ctables

import java.sql.SQLException

import mimir.algebra._
import mimir.util._
import mimir.optimizer._

object CTPercolator {

  val ROWID_KEY = "ROWID_MIMIR"

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
    InlineProjections.optimize(
      OperatorUtils.extractUnions(
        oper
      ).map( percolateOne(_) ).reduceLeft( Union(_,_) )
    )
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
              cols.map( (x) => (x.name, x.expression) ),
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
              x.name,
              Eval.inline(x.expression, bindings)
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
                cols.find( _.name == CTables.conditionColumn )
              if(inputCondition.isEmpty){
                return Project(cols ++ List(
                          ProjectArg(CTables.conditionColumn, u)
                        ),
                        newSelect
                )
              } else {
                return Project(cols.map(
                    (x) => if(x.name == CTables.conditionColumn){
                      ProjectArg(CTables.conditionColumn,
                        Arith.makeAnd(x.expression, u)
                      )
                    } else { x }
                ), newSelect)
              }
          }
        }
      case s: Select => return s
      case Join(lhs, rhs) => {

        // println("Percolating Join: \n" + o)
        val makeName = (name:String, x:Integer) =>
          (name+"_"+x)
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
          | (Set[String](ROWID_KEY) & lhsColNames)
          | (Set[String](ROWID_KEY) & rhsColNames)
        )
        var allNames = lhsColNames ++ rhsColNames

        val nameMap = conflicts.map( (col) => {
          var lhsSuffix = 1
          while(allNames.contains(makeName(col, lhsSuffix))){ lhsSuffix += 1; }
          var rhsSuffix = lhsSuffix + 1
          while(allNames.contains(makeName(col, rhsSuffix))){ rhsSuffix += 1; }
          (col, (lhsSuffix, rhsSuffix))
        }).toMap
        val renameLHS = (name:String) =>
          makeName(name, nameMap(name)._1)
        val renameRHS = (name:String) =>
          makeName(name, nameMap(name)._2)

//        println("CONFLICTS: "+conflicts+"in: "+lhsColNames+", "+rhsColNames+"; for \n"+afterDescent);

        val newJoin =
          if(conflicts.isEmpty) {
            Join(lhsChild, rhsChild)
          } else {
            val fullMapping = (name:String, rename:String => String) => {
              ( if(conflicts contains name){ rename(name) }
                else { name },
                Var(name)
              )
            }
            // Create a projection that remaps the names of
            // all the variables to the appropriate unqiue
            // name.
            val rewrite = (child:Operator, rename:String => String) => {
              Project(
                child.schema.map(_._1).
                  map( fullMapping(_, rename) ).
                  map( (x) => ProjectArg(x._1, x._2)).toList,
                child
              )
            }
            Join(
              rewrite(lhsChild, renameLHS),
              rewrite(rhsChild, renameRHS)
            )
          }
        val remap = (cols: List[(String,Expression)], 
                     rename:String => String) =>
        {
          val mapping =
            conflicts.map(
              (x) => (x, Var(rename(x)))
            ).toMap[String, Expression]
          cols.filter( _._1 != CTables.conditionColumn ).
            map( _ match { case (name, expr) =>
              (name, Eval.inline(expr, mapping))
            })
        }
        var cols = remap(lhsCols, renameLHS) ++
                   remap(rhsCols, renameRHS)
        val lhsHasCondition =
          lhsCols.exists( _._1 == CTables.conditionColumn)
        val rhsHasCondition =
          rhsCols.exists( _._1 == CTables.conditionColumn)
        if(lhsHasCondition || rhsHasCondition) {
          if(conflicts contains CTables.conditionColumn){
            cols = cols ++ List(
              ( CTables.conditionColumn,
                Arithmetic(Arith.And,
                  Var(renameLHS(CTables.conditionColumn)),
                  Var(renameRHS(CTables.conditionColumn))
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

  private def extractMissingValueVar(expr: Expression): Var = {
    expr match {
      case Conditional(IsNullExpression(v1: Var), vg: VGTerm, v2: Var) =>
        if(v1 == v2) v1 else throw new SQLException("Unexpected clause to extractMisingValueVar")

      case _ => throw new SQLException("Unexpected clause to extractMisingValueVar")
    }
  }

  private def isMissingValueExpression(expr: Expression): Boolean = {
    expr match {
      case Conditional(IsNullExpression(var1: Var), vg: VGTerm, var2: Var) =>
        var1 == var2
      case _ => false
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
        Project(cols.filterNot((p) => p.name.equalsIgnoreCase(CTables.conditionColumn)), src)
      case _ =>
        oper
    }
  }

  def partition(oper: Project): Operator = {
    val cond = oper.columns.find(p => p.name.equalsIgnoreCase(CTables.conditionColumn)).head.expression
    var otherClausesOp: Operator = null
    var missingValueClausesOp: Operator = null
    var detExpr: List[Expression] = List()
    var nonDeterExpr: List[Expression] = List()


    val (missingValueClauses, otherClauses) =
      splitArith(cond).partition { (p) =>
        p match {
          case Comparison(_, lhs, rhs) => isMissingValueExpression(lhs) || isMissingValueExpression(rhs)
          case _ => false
        }
      }

    missingValueClauses.foreach{ (e) =>
      e match {
        case Comparison(op, lhs, rhs) =>
          var lhsExpr = lhs
          var rhsExpr = rhs

          if (isMissingValueExpression(lhs)) {
            val lhsVar = extractMissingValueVar(lhs)
            lhsExpr = lhsVar
            detExpr ++= List(Not(IsNullExpression(lhsVar)))
            nonDeterExpr ++= List(IsNullExpression(lhsVar))
          }
          if (isMissingValueExpression(rhs)) {
            val rhsVar = extractMissingValueVar(rhs)
            rhsExpr = rhsVar
            detExpr ++= List(Not(IsNullExpression(rhsVar)))
            nonDeterExpr ++= List(IsNullExpression(rhsVar))
          }

          detExpr ++= List(Comparison(op, lhsExpr, rhsExpr))

        case _ => throw new SQLException("Missing Value Clauses must be Comparison expressions")
      }
    }

    missingValueClausesOp = Union(
      removeConstraintColumn(oper).rebuild(List(Select(detExpr.distinct.reduce(Arith.makeAnd(_, _)), oper.children().head))),
      oper.rebuild(List(Select(nonDeterExpr.distinct.reduce(Arith.makeOr(_, _)), oper.children().head)))
    )

    if(otherClauses.nonEmpty)
      otherClausesOp = Project(
        oper.columns.filterNot( (p) => p.name.equalsIgnoreCase(CTables.conditionColumn))
          ++ List(ProjectArg(CTables.conditionColumn, otherClauses.reduce(Arith.makeAnd(_, _)))),
        oper.source
      )

    (otherClausesOp, missingValueClausesOp) match {
      case (null, null) => throw new SQLException("Both partitions null")

      case (null, y) => y

      case (x, null) => x

      case (x, y) => Union(x, y)
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
        cols.find(p => p.name.equalsIgnoreCase(CTables.conditionColumn)) match {
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

        // Compute the determinism of each column.
        val newColDeterminismBase = 
          columns.map( _ match { case ProjectArg(col, expr) => {
            val isDeterministic = 
              CTAnalyzer.compileDeterministic(expr, colDeterminism)
            
            (col, isDeterministic)
          }})

        // Determine which of them are deterministic.
        val computedDeterminismCols = 
          newColDeterminismBase.filterNot( 
            // Retain only columns where the isDeterministic expression
            // is a constant (i.e., no Column references)
            _ match { case (col, expr) => {
              ExpressionUtils.getColumns(expr).isEmpty
            }}
          ).map( 
            // Then just translate to a list of ProjectArgs
            _ match { case (col, isDeterministic) => 
              ProjectArg(mimirColDeterministicColumnPrefix+col, isDeterministic) 
            }
         )

        // Rewrite these expressions so that the computed expressions use the
        // computed version from the source data.
        val newColDeterminism =
          newColDeterminismBase.map( _ match { case (col, isDeterministic) =>
              if(ExpressionUtils.getColumns(isDeterministic).isEmpty) {
                (col, isDeterministic)
              } else {
                (col, Var(mimirColDeterministicColumnPrefix+col))
              }
            }
          )

        val (newRowDeterminism, rowDeterminismCols) = 
          if(ExpressionUtils.getColumns(rowDeterminism).isEmpty){
            (rowDeterminism, List())
          } else {
            (Var(mimirRowDeterministicColumnName), 
              List(ProjectArg(
                mimirRowDeterministicColumnName, 
                rowDeterminism
            )))
          }

        val retProject = Project(
            columns ++ computedDeterminismCols ++ rowDeterminismCols,
            rewrittenSrc
          )

        return (retProject, newColDeterminism.toMap, newRowDeterminism)
      }
      case Select(cond, src) => {
        val (rewrittenSrc, colDeterminism, rowDeterminism) = percolateLite(src);

        // Compute the determinism of the selection predicate
        val condDeterminism = 
          CTAnalyzer.compileDeterministic(cond, colDeterminism)

        // Combine the determinism with the computed determinism from the child...
        val newRowDeterminism = 
          Arith.makeAnd(condDeterminism, rowDeterminism)
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
        val columnNames = colDetLeft.keys.toSet ++ colDetRight.keys.toSet
        // We need to make the schemas line up: the schemas of the left and right
        // need to have all of the relevant columns
        // Going to cheat a bit here and just force the projection on.

        val mergeNonDeterminism = 
          (col:String, detCol:String, colLeft:Expression, colRight:Expression) => {
          // It's only safe to skip the non-determinism column if both
          // LHS and RHS have exactly the same condition AND that condition
          // is data-independent.
          if(  ExpressionUtils.getColumns(colLeft).isEmpty
            && ExpressionUtils.getColumns(colRight).isEmpty
            && colLeft.equals(colRight)
          ){
            // Skip the column and return the data-independent condition
            (List[(ProjectArg, ProjectArg)](), (col, colLeft))
          } else {
            // Otherwise, materialize the condition, and return a reference
            // to the new condition.
            ( List( (ProjectArg(detCol, colLeft), 
                     ProjectArg(detCol, colRight) )), 
              (col, Var(detCol))
            )
          }
        }

        val (colProjectArgs, colDeterminism) =
          columnNames.map(
            (col) => 
              mergeNonDeterminism(
                col, mimirColDeterministicColumnPrefix+col, 
                colDetLeft(col), colDetRight(col)
              )
          ).unzip

        val (condProjectArgs, condDeterminism) =
          mergeNonDeterminism(
            null, mimirRowDeterministicColumnName,
            rowDetLeft, rowDetRight
          )
        
        val (projectArgsLeft, projectArgsRight) = 
          (colProjectArgs.flatten ++ condProjectArgs).unzip
        val projectArgsBase = 
          columnNames.map( 
            (col) => ProjectArg(col, Var(col)) 
          ).toList

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

        // if left and right have no schema overlap, then the only
        // possible overlap in rewrittenLeft and rewrittenRight is
        // the row determinism column.  Start by detecting whether
        // this column is present in both inputs:
        val (schemaLeft,detColumnLeft) = 
          rewrittenLeft.schema.map(_._1).
            partition( _ != mimirRowDeterministicColumnName )
        val (schemaRight,detColumnRight) = 
          rewrittenRight.schema.map(_._1).
            partition( _ != mimirRowDeterministicColumnName )

        // If either left or right side lacks a determinism column,
        // we're safe.  Fast-path return a simple join.
        if(detColumnLeft.isEmpty || detColumnRight.isEmpty){
          return (
            Join(rewrittenLeft, rewrittenRight),
            colDetLeft ++ colDetRight,
            Arith.makeAnd(rowDetLeft, rowDetRight)
          )          
        }

        // if both left and right have a row determinism column,
        // then we need to rewrite them to prevent a namespace
        // collision.

        // Generate a schema mapping that leaves normal columns
        // intact.
        val schemaMappingLeft = 
          schemaLeft.map( (x) => ProjectArg(x, Var(x))) ++ 
          (detColumnLeft.map( 
            (_) => ProjectArg(
                mimirRowDeterministicColumnName+"_LEFT",
                Var(mimirRowDeterministicColumnName)
              ))
          )
        val schemaMappingRight = 
          schemaRight.map( (x) => ProjectArg(x, Var(x))) ++ 
          (detColumnRight.map( 
            (_) => ProjectArg(
                mimirRowDeterministicColumnName+"_RIGHT",
                Var(mimirRowDeterministicColumnName)
              ))
          )

        // Map the variables in the determinism columns...
        val mappedRowDetLeft = Eval.inline(rowDetLeft, 
            Map((mimirRowDeterministicColumnName, 
                 Var(mimirRowDeterministicColumnName+"_LEFT"))))
        val mappedRowDetRight = Eval.inline(rowDetRight, 
            Map((mimirRowDeterministicColumnName, 
                 Var(mimirRowDeterministicColumnName+"_RIGHT"))))

        // And return it.
        return (
          Join(
            Project(schemaMappingLeft, rewrittenLeft), 
            Project(schemaMappingRight, rewrittenRight)
          ),
          colDetLeft ++ colDetRight,
          Arith.makeAnd(mappedRowDetLeft, mappedRowDetRight)
        )
      }
      case Table(name, cols, metadata) => {
        return (oper, 
          // All columns are deterministic
          cols.map(_._1).map((_, BoolPrimitive(true)) ).toMap,
          // All rows are deterministic
          BoolPrimitive(true)
        )
      }
    }
  }
}
