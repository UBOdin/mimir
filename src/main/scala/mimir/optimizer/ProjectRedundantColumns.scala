package mimir.optimizer;

import java.sql._;

import mimir.algebra._;
import mimir.ctables._;

object ProjectRedundantColumns {

  private def projectIfNeeded(oper: Operator, dependencies: Set[String]) = 
  {
    val existingColumns = oper.schema.map(_._1)
    if(existingColumns.forall( dependencies contains _ )){ oper }
    else {
      OperatorUtils.projectColumns(dependencies.toList, oper)
    }
  }

  def apply(o: Operator): Operator = 
    apply(o, o.schema.map(_._1).toSet)

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

      case Select(condition, source) => {
        val childDependencies = 
          ExpressionUtils.getColumns( condition ) ++ dependencies

        Select(condition, apply(source, childDependencies))
      }

      case Union(lhs, rhs) => {
        Union(apply(lhs, dependencies), apply(rhs, dependencies))
      }

      case Join(lhs, rhs) => {
        val lhsDeps = lhs.schema.map(_._1).toSet & dependencies
        val rhsDeps = rhs.schema.map(_._1).toSet & dependencies

        Join(apply(lhs, lhsDeps), apply(rhs, rhsDeps))
      }

      case Aggregate(computed, groupby, source) => 
      {
        val computedDependencies = 
          computed.flatMap( _.columns ).
            flatMap( ExpressionUtils.getColumns(_) ).
            toSet

        val groupbyDependencies = 
          groupby.flatMap( ExpressionUtils.getColumns(_) ).
            toSet

        val aggregate = 
          Aggregate(
            computed.filter( (column) => dependencies contains column.alias ),
            groupby,
            apply(source, computedDependencies ++ groupbyDependencies)
          )

        projectIfNeeded(aggregate, dependencies)
      }

      case table: Table => table
    }

  }
}