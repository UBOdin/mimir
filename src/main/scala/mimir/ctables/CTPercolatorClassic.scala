package mimir.ctables

import java.sql.SQLException

import mimir.algebra._
import mimir.util._
import mimir.optimizer.operator._

object CTPercolatorClassic {

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
    InlineProjections(
      OperatorUtils.extractUnionClauses(
        propagateRowIDs(oper)
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
    if(CTables.isDeterministic(o)){ return o }
    // println("percolateOne: "+o)
    val extractProject:
        Operator => ( Seq[(String,Expression)], Operator ) =
      (e: Operator) =>
        e match {
          case Project(cols, rest) => (
              cols.map( (x) => (x.name, x.expression) ),
              rest
            )
          case _ => (
              e.columnNames.map( (x) => (x, Var(x)) ).toSeq,
              e
            )
        }
    val addConstraintCol = (e: Operator) => {
      if(e.columnNames.toSet contains CTables.conditionColumn) {
        Seq(ProjectArg(CTables.conditionColumn, Var(CTables.conditionColumn)))
      } else {
        Seq[ProjectArg]()
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
                return Project(cols ++ Seq(
                          ProjectArg(CTables.conditionColumn, u)
                        ),
                        newSelect
                )
              } else {
                return Project(cols.map(
                    (x) => if(x.name == CTables.conditionColumn){
                      ProjectArg(CTables.conditionColumn,
                        ExpressionUtils.makeAnd(x.expression, u)
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
        val lhsColNames = lhsChild.columnNames.toSet
        val rhsColNames = rhsChild.columnNames.toSet

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
                child.columnNames.
                  map( fullMapping(_, rename) ).
                  map( (x) => ProjectArg(x._1, x._2)).toSeq,
                child
              )
            }
            Join(
              rewrite(lhsChild, renameLHS),
              rewrite(rhsChild, renameRHS)
            )
          }
        val remap = (cols: Seq[(String,Expression)], 
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
            cols = cols ++ Seq(
              ( CTables.conditionColumn,
                Arithmetic(Arith.And,
                  Var(renameLHS(CTables.conditionColumn)),
                  Var(renameRHS(CTables.conditionColumn))
              )))
          } else {
            cols = cols ++ Seq(
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
      case Annotate(subj,invisScm) => {
        percolateOne(subj)
      }
      
			case Recover(subj,invisScm) => {
        percolateOne(subj)
      }
      
      case ProvenanceOf(psel) => {
        percolateOne(psel)
      }

      case _ => {
        throw new Exception("CTPercolator classic is only here for testing and comparison and should be phased out.  Many query types are unsupported.")
      }
    }
  }
  
  def requiresRowID(expr: Expression): Boolean = 
  {
    expr match {
      case Var(ROWID_KEY) => true;
      case _ => expr.children.exists( requiresRowID(_) )
    }
  }

  def hasRowID(oper: Operator): Boolean = 
  {
    oper match {
      case Project(cols, _) => cols.contains( (_:ProjectArg).name.equals(ROWID_KEY))
      case Select(_, src) => hasRowID(src)
      case Table(_,_,_,meta) => meta.contains( (_:(String,Type))._1.equals(ROWID_KEY))
      case Union(_,_) => false
      case Join(_,_) => false
      case Aggregate(_,_,_) => false
      case _ => ???
    }
  }
  
  def propagateRowIDs(oper: Operator): Operator = 
    propagateRowIDs(oper, false)

  def propagateRowIDs(oper: Operator, force: Boolean): Operator = 
  {
    // println("Propagate["+(if(force){"F"}else{"NF"})+"]:\n" + oper);
    if(hasRowID(oper)){ return oper; }
    oper match {
      case p @ Project(args, child) =>
        var newArgs = args;
        if(force && p.get(ROWID_KEY).isEmpty) {
          newArgs = 
            Seq(new ProjectArg(ROWID_KEY, Var(ROWID_KEY))) ++
              newArgs
        }
        Project(newArgs, 
          propagateRowIDs(child, 
            newArgs.exists((x)=>requiresRowID( x.expression ))
        ))
        
      case Select(cond, child) =>
        Select(cond, propagateRowIDs(child,
          force || requiresRowID(cond)))
          
      case Join(left, right) =>
        if(force){
          Project(
            Seq(ProjectArg(ROWID_KEY,
              Function("JOIN_ROWIDS", Seq[Expression](Var("LEFT_ROWID"), Var("RIGHT_ROWID"))))) ++
            (left.columnNames ++ right.columnNames).map(
              (x) => ProjectArg(x, Var(x)) 
            ).toSeq,
            Join(
              Project(
                Seq(ProjectArg("LEFT_ROWID", Var(ROWID_KEY))) ++
                left.columnNames.map(
                  (x) => ProjectArg(x, Var(x)) 
                ).toSeq,
                propagateRowIDs(left, true)),
              Project(
                Seq(ProjectArg("RIGHT_ROWID", Var(ROWID_KEY))) ++
                right.columnNames.map(
                  (x) => ProjectArg(x, Var(x)) 
                ).toSeq,
                propagateRowIDs(right, true))
            )
          )
        } else {
          Join(
            propagateRowIDs(left, false),
            propagateRowIDs(right, false)
          )
        }

        case agg@Aggregate(gbArgs, _, _) =>
          if(force){
            agg.addColumn(ROWID_KEY -> Function("JOIN_ROWIDS", gbArgs))
          } else { agg }
        
        
        case Union(left, right) =>
          if(force){
            Union( 
              Project(
                Seq(ProjectArg(ROWID_KEY,
                  Function("__LEFT_UNION_ROWID",
                    Seq[Expression](Var(ROWID_KEY))))) ++
                left.columnNames.map(
                  (x) => ProjectArg(x, Var(x)) 
                ).toSeq,
                propagateRowIDs(left, true)),
              Project(
                Seq(ProjectArg(ROWID_KEY,
                  Function("__RIGHT_UNION_ROWID",
                    Seq[Expression](Var(ROWID_KEY))))) ++
                right.columnNames.map(
                  (x) => ProjectArg(x, Var(x)) 
                ).toSeq,
                propagateRowIDs(right, true))
            )
          } else {
            Union( 
              propagateRowIDs(left, false),
              propagateRowIDs(right, false)
            )
          }
        
        case Table(name, alias, sch, metadata) =>
          if(force && !metadata.exists( _._1 == ROWID_KEY )){
            Table(name, alias, sch, metadata ++ Seq((ROWID_KEY, Var("ROWID"), TRowId())))
          } else {
            Table(name, alias, sch, metadata)
          }
          
        case _ => ???
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

  def splitArith(expr: Expression): Seq[Expression] = {
    expr match {
      case Arithmetic(op, lhs, rhs) => splitArith(lhs) ++ splitArith(rhs)
      case x => Seq(x)
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
    var detExpr: Seq[Expression] = Seq()
    var nonDeterExpr: Seq[Expression] = Seq()


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
            detExpr ++= Seq(Not(IsNullExpression(lhsVar)))
            nonDeterExpr ++= Seq(IsNullExpression(lhsVar))
          }
          if (isMissingValueExpression(rhs)) {
            val rhsVar = extractMissingValueVar(rhs)
            rhsExpr = rhsVar
            detExpr ++= Seq(Not(IsNullExpression(rhsVar)))
            nonDeterExpr ++= Seq(IsNullExpression(rhsVar))
          }

          detExpr ++= Seq(Comparison(op, lhsExpr, rhsExpr))

        case _ => throw new SQLException("Missing Value Clauses must be Comparison expressions")
      }
    }

    missingValueClausesOp = Union(
      removeConstraintColumn(oper).rebuild(Seq(Select(detExpr.distinct.reduce(ExpressionUtils.makeAnd(_, _)), oper.children().head))),
      oper.rebuild(Seq(Select(nonDeterExpr.distinct.reduce(ExpressionUtils.makeOr(_, _)), oper.children().head)))
    )

    if(otherClauses.nonEmpty)
      otherClausesOp = Project(
        oper.columns.filterNot( (p) => p.name.equalsIgnoreCase(CTables.conditionColumn))
          ++ Seq(ProjectArg(CTables.conditionColumn, otherClauses.reduce(ExpressionUtils.makeAnd(_, _)))),
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


}