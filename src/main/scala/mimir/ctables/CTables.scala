package mimir.ctables;

import mimir.algebra._;
import mimir.algebra.Type._;
import mimir.util.ListUtils;

case class PVar(
  iview: String, 
  module: Int,
  variable: Int,
  params: List[Expression]
) extends Expression {
  override def toString() = "{{ "+iview+"_"+module+"_"+variable+"["+params.mkString(", ")+"] }}"
  def exprType(bindings: Map[String, Type.T]):Type.T = Type.TFloat
  def children: List[Expression] = params
  def rebuild(x: List[Expression]) = PVar(iview, module, variable, x)
}

object CTables 
{
  
  def extractUncertainClauses(e: Expression): 
    (Expression, Expression) =
  {
    e match {
      case Arithmetic(Arith.And, lhs, rhs) =>
        val (lhsExtracted, lhsRemaining) = 
          extractUncertainClauses(lhs)
        val (rhsExtracted, rhsRemaining) = 
          extractUncertainClauses(rhs)
        ( Arith.makeAnd(lhsExtracted, rhsExtracted),
          Arith.makeAnd(lhsRemaining, rhsRemaining)
        )
      case _ => 
        if(CTAnalysis.isProbabilistic(e)){
          (e, BoolPrimitive(true))
        } else {
          (BoolPrimitive(true), e)
        }
    }
  }
  
  def conditionColumn = "__MIMIR_CONDITION"
  
  // Normalize an operator tree by distributing operators
  // over union terms.
  def extractUnions(o: Operator): List[Operator] =
  {
    // println("Extract: " + o)
    o match {
      case Union(lhs, rhs) => 
        extractUnions(lhs) ++ extractUnions(rhs)
      case Project(args, c) =>
        extractUnions(c).map ( Project(args, _) )
      case Select(expr, c) =>
        extractUnions(c).map ( Select(expr, _) )
      case t : Table => List[Operator](t)
      case Join(lhs, rhs) =>
        extractUnions(lhs).flatMap (
          (lhsTerm: Operator) =>
            extractUnions(rhs).map( 
              Join(lhsTerm, _)
            )
        )
    }
  }
  
  // Transform an operator tree into a union of
  // union-free, percolated subtrees.
  // Postconditions:   
  //   The root of the tree is a hierarchy of Unions
  //   The leaves of the Union hierarchy are...
  //     ... if the leaf is nondeterministic,
  //       then a Project node that may be uncertain
  //       but who's subtree is deterministic
  //     ... if the leaf has no uncertainty, 
  //       then an arbitrary deterministic subtree
  def percolate(oper: Operator): Operator = {
    // println("Percolate: "+o)
    extractUnions(
      propagateRowIDs(oper)
    ).map( percolateOne(_) ).reduceLeft( Union(_,_) )
  }
  
  // Normalize a union-free operator tree by percolating 
  // uncertainty-creating projections up through the
  // operator tree.  Selection predicates based on 
  // probabilistic predicates are converted into 
  // constraint columns.  If necessary uncertainty-
  // creating projections are converted into non-
  // uncertainty-creating projections to allow 
  // the uncertain attributes to percolate up.
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
              e.schema.keys.map( (x) => (x, Var(x)) ).toList,
              e
            )
        }
    val addConstraintCol = (e: Operator) => {
      if(e.schema.keys.toSet contains conditionColumn) {
        List(ProjectArg(conditionColumn, Var(conditionColumn)))
      } else {
        List[ProjectArg]()
        // List(ProjectArg(conditionColumn, BoolPrimitive(true)))
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
        if(!CTAnalysis.isProbabilistic(p)){
          return Project(cols, 
            percolateOne(Select(Eval.inline(cond, p.bindings),source))
          )
        } else {
          val newCond = Eval.inline(cond, p.bindings)
          extractUncertainClauses(newCond) match {
            case (BoolPrimitive(true), BoolPrimitive(true)) => 
              // Select clause is irrelevant
              return p
            case (BoolPrimitive(true), c) => 
              // Select clause is deterministic
              return Project(cols, Select(c, source))
            case (u, c) => 
              // Select clause is at least partly 
              // nondeterministic
              val newSelect = 
                if(c == BoolPrimitive(true)){ source }
                else { percolateOne(Select(c, source)) }
              val inputCondition = 
                cols.find( _.column == conditionColumn )
              if(inputCondition.isEmpty){
                return Project(cols ++ List(
                          ProjectArg(conditionColumn, u)
                        ), 
                        newSelect
                )
              } else {
                return Project(cols.map(
                    (x) => if(x.column == conditionColumn){
                      ProjectArg(conditionColumn, 
                        Arith.makeAnd(x.input, u)
                      )
                    } else { x }
                ), newSelect)
              }
          }
        }
      case s: Select => return s
      case Join(lhs, rhs) => {
        val rename = (name:String, x:String) => 
                ("__"+name+"_"+x)
        val (lhsCols, lhsChild) = extractProject(lhs)
        val (rhsCols, rhsChild) = extractProject(rhs)
        // Pulling projections up through a join may require
        // renaming columns under the join if the same column
        // name appears on both sides of the source
        val conflicts = lhsChild.schema.keys.toSet & 
                        rhsChild.schema.keys.toSet
        val newJoin = 
          if(conflicts.isEmpty){
            return Join(lhsChild, rhsChild)
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
                child.schema.keys.
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
          cols.filter( _._1 != conditionColumn ).
            map( _ match { case (name, expr) =>
              (name, Eval.inline(expr, mapping))
            })
        }
        var cols = remap("LHS", lhsCols) ++
                   remap("RHS", rhsCols)
        val lhsHasCondition = 
          lhsCols.exists( _._1 == conditionColumn)
        val rhsHasCondition = 
          rhsCols.exists( _._1 == conditionColumn)
        if(lhsHasCondition || rhsHasCondition) {
          if(conflicts contains conditionColumn){
            cols = cols ++ List(
              ( conditionColumn, 
                Arithmetic(Arith.And, 
                  Var(rename("LHS", conditionColumn)),
                  Var(rename("RHS", conditionColumn))
              )))
          } else {
            cols = cols ++ List(
              (conditionColumn, Var(conditionColumn))
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
        if(CTAnalysis.isProbabilistic(expr)){
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
  def expandProbabilisticCases(oper: Operator): Operator = {
    // println("Expand: " + oper)
    oper match {
      case Project(args, child) =>
        ListUtils.powerList[(Expression,ProjectArg)](
          args.map( (arg:ProjectArg) =>
            if(!CTAnalysis.isProbabilistic(arg.input)){
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
        }).reduce[Operator]( Union(_, _) )
  
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
    propagateRowIDs(oper, false);
  
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
            (left.schema ++ right.schema).keys.map(
              (x) => ProjectArg(x, Var(x)) 
            ).toList,
            Join(
              Project(
                ProjectArg("LEFT_ROWID", Var("ROWID")) ::
                left.schema.keys.map(
                  (x) => ProjectArg(x, Var(x)) 
                ).toList,
                propagateRowIDs(left, true)),
              Project(
                ProjectArg("RIGHT_ROWID", Var("ROWID")) ::
                right.schema.keys.map(
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
        
        case Union(left, right) =>
          if(force){
            Union(
              Project(
                ProjectArg("ROWID", 
                  Function("LEFT_UNION_ROWID",
                    List[Expression](Var("ROWID")))) ::
                left.schema.keys.map(
                  (x) => ProjectArg(x, Var(x)) 
                ).toList,
                propagateRowIDs(left, true)),
              Project(
                ProjectArg("ROWID", 
                  Function("RIGHT_UNION_ROWID",
                    List[Expression](Var("ROWID")))) ::
                right.schema.keys.map(
                  (x) => ProjectArg(x, Var(x)) 
                ).toList,
                propagateRowIDs(right, true))
            )
          } else {
            Union(
              propagateRowIDs(left, false),
              propagateRowIDs(right, false)
            )
          }
        
        case Table(name, sch, metadata) =>
          if(force){
            Table(name, sch, metadata ++ Map(("ROWID", Type.TInt)))
          } else {
            Table(name, sch, metadata)
          }
      
    }
  }
}