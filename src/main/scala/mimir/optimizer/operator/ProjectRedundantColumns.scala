package mimir.optimizer.operator

import java.sql._

import mimir.algebra._
import mimir.ctables._
import mimir.optimizer.OperatorOptimization

object ProjectRedundantColumns extends OperatorOptimization {

  private def projectIfNeeded(oper: Operator, dependencies: Set[ID]) = 
  {
    val existingColumns = oper.columnNames
    if(existingColumns.forall( dependencies contains _ )){ oper }
    else {
      oper.projectByID( dependencies.toSeq:_* )
    }
  }

  def apply(o: Operator): Operator = 
    apply(o, o.columnNames.toSet)

  def apply(o: Operator, dependencies: Set[ID]): Operator =
  {
    o match {
      case proj@Project(args, Select(condition, source)) => {
        val newArgs = 
          args.filter( arg => dependencies contains arg.name )

        val childDependencies = 
          newArgs.map( _.expression ).
            flatMap( ExpressionUtils.getColumns(_) ).
            toSet
        
        val nextChildDependencies = 
          ExpressionUtils.getColumns( condition ) ++ childDependencies
            
        val projRedunDependencies = source.columnNames.toSet
       
        if(childDependencies.subsetOf(projRedunDependencies))
          proj    
        else
          Project( newArgs, Select(condition, apply(source, nextChildDependencies)) )
      }
      
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

        val allDownwardDependencies = computedDependencies ++ groupbyDependencies

        // It is possible for an Aggregate column to want to project away ALL columns
        // While this isn't a problem for us, it makes it VERY difficult to convert the resulting
        // expression to SQL.  Preseve a single (arbitrary) column for safety in this case.
        val safeDownwardDependencies =
          if(allDownwardDependencies.isEmpty){
            Set(source.columnNames.head)
          } else {
            allDownwardDependencies
          }

        val aggregate = 
          Aggregate(
            groupby,
            computed.filter( (column) => dependencies contains column.alias ),
            apply(source, safeDownwardDependencies)
          )

        projectIfNeeded(aggregate, dependencies)
      }

      case view: View => view
      case view: LensView => view
      case table: Table => table
      case HardTable(sch,data) => 
      {
        val colNamesWithIndices = 
          sch.map { _._1 }.zipWithIndex
        val columnIndicesToKeep = 
          colNamesWithIndices
            .filter { case (col, _) => dependencies contains col }
            .map { _._2 }

        HardTable(
          columnIndicesToKeep.map { sch(_) },
          data.map { row => columnIndicesToKeep.map { row(_) } }
        )
      }

      case LeftOuterJoin(lhs, rhs, condition) => {
        val childDependencies = 
          ExpressionUtils.getColumns( condition ) ++ dependencies

        val lhsDeps = lhs.columnNames.toSet & childDependencies
        val rhsDeps = rhs.columnNames.toSet & childDependencies

        LeftOuterJoin(
          apply(lhs, lhsDeps),
          apply(rhs, rhsDeps),
          condition
        )
      }

      case d@DrawSamples(mode, source, seed) => {
        val localDeps = mode.expressions
                       .flatMap { ExpressionUtils.getColumns(_) }
                       .toSet
        d.recur { apply(_, dependencies ++ localDeps) }
      }

    }

  }
}
