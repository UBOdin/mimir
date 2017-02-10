package mimir.optimizer

import mimir._
import mimir.algebra._

class ResolveViews(db: Database) {
  def apply(oper: Operator): Operator = {
    oper match {
      case Table(name, alias, desiredSchema, meta) =>
        db.getView(name) match {
          case Some(viewQuery) => {
            val desiredSchemaNames = desiredSchema.map(_._1)
            val existingSchemaNames = viewQuery.schema.map(_._1)
            val schemaMappings =
              desiredSchemaNames.
                zip(existingSchemaNames).
                map(x => ProjectArg(x._1, Var(x._2)))
            val metaMappings =
              meta.map( x => ProjectArg(x._1, x._2) )

            Project(
              schemaMappings++metaMappings,
              apply(viewQuery)
            )
          }
          case None =>
            oper
        }

      case _ => oper.recur(apply(_))
    }
  }
}

object ResolveViews {
  def apply(db: Database, oper: Operator) = 
    new ResolveViews(db)(oper)

}