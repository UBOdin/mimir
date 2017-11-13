package mimir.optimizer.operator

import java.sql._

import mimir.algebra._
import mimir.ctables._
import mimir.optimizer.OperatorOptimization

object ProjectRedundantColumns extends OperatorOptimization {

  private def projectIfNeeded(oper: Operator, dependencies: Set[String]) = 
  {
    val existingColumns = oper.columnNames
    if(existingColumns.forall( dependencies contains _ )){ oper }
    else {
      OperatorUtils.projectColumns(dependencies.toList, oper)
    }
  }

  def apply(o: Operator): Operator = 
    apply(o, o.columnNames.toSet)

  def apply(o: Operator, dependencies: Set[String]): Operator =
  {
    o match {
      case Project(args, source) => {
        val newArgs = 
          args.filter( arg => dependencies contains arg.name )

        val childDependencies = 
          newArgs.map( _.expression ).
            flatMap( ExpressionUtils.getColumns(_) ).
            toSet

        Project( newArgs, apply(source, childDependencies) )
      }
      
      case Annotate(subj,invisScm) => {
        Annotate(apply(subj),invisScm)
      }
      
			case Recover(subj,invisScm) => {
        val provSelApl = apply(subj)
        new Recover(provSelApl, invisScm)
      }
      
      
       case ProvenanceOf(psel) => {
        val provSelApl = apply(psel)
        val provApl = new ProvenanceOf(provSelApl)
        provApl
      }
      
      case Select(condition, source) => {
        val childDependencies = 
          ExpressionUtils.getColumns( condition ) ++ dependencies

        Select(condition, apply(source, childDependencies))
      }

      case Sort(cols, source) => {
        val childDependencies = 
          cols.map(_.expression).flatMap(ExpressionUtils.getColumns(_)) ++ dependencies

        Sort(cols, apply(source, childDependencies.toSet))
      }

      case l:Limit =>
        l.recur(apply(_, dependencies))

      case Union(lhs, rhs) => {
        // Naively, we could just push down the dependencies, but
        // in some cases we'll get back a superset of the desired 
        // schema, which would result in an invalid union
        // For example Union(Table(...), Project[...](Table(...)))
        //
        // We could introduce spurious projections around tables if
        // ABSOLUTELY needed... but let's instead see if it's really
        // critical

        val newLhs = apply(lhs, dependencies)
        val newRhs = apply(rhs, dependencies)
        val newLhsSchema = lhs.columnNames.toSet
        val newRhsSchema = rhs.columnNames.toSet

        if(   (!(dependencies -- newLhsSchema).isEmpty)
            ||(!(dependencies -- newRhsSchema).isEmpty))
        {
          if(!(newLhsSchema).equals(newRhsSchema)){
            Union(
              projectIfNeeded(newLhs, dependencies),
              projectIfNeeded(newRhs, dependencies)
            )
          } else {
            Union(newLhs,newRhs)
          }
        } else {
          Union(newLhs,newRhs)
        }
      }

      case Join(lhs, rhs) => {
        val lhsDeps = lhs.columnNames.toSet & dependencies
        val rhsDeps = rhs.columnNames.toSet & dependencies

        Join(apply(lhs, lhsDeps), apply(rhs, rhsDeps))
      }

      case Aggregate(groupby, computed, source) => 
      {
        val computedDependencies = 
          computed.flatMap( _.args ).
            flatMap( ExpressionUtils.getColumns(_) ).
            toSet

        val groupbyDependencies = 
          groupby.flatMap( ExpressionUtils.getColumns(_) ).
            toSet

        val aggregate = 
          Aggregate(
            groupby,
            computed.filter( (column) => dependencies contains column.alias ),
            apply(source, computedDependencies ++ groupbyDependencies)
          )

        projectIfNeeded(aggregate, dependencies)
      }

      case view: View => view
      case view: AdaptiveView => view
      case table: Table => table
      case table@ HardTable(_,Seq()) => table //EmptyTable
      case HardTable(sch,data) => 
        HardTable(sch.filter { case (name, _) => dependencies contains name }, data)

      case LeftOuterJoin(lhs, rhs, condition) => {
        val childDependencies = 
          ExpressionUtils.getColumns( condition ) ++ dependencies

        val lhsDeps = lhs.columnNames.toSet & dependencies
        val rhsDeps = rhs.columnNames.toSet & dependencies

        LeftOuterJoin(
          apply(lhs, lhsDeps),
          apply(rhs, rhsDeps),
          condition
        )
      }

    }

  }
}
