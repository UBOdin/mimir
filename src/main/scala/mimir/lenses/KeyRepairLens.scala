package mimir.lenses;

import java.sql._

import mimir.Database
import mimir.models._
import mimir.algebra._
import mimir.ctables._

object KeyRepairLens {
  def create(
    db: Database, 
    name: String, 
    query: Operator, 
    args:Seq[Expression]
  ): (Operator, Seq[Model]) =
  {

    val schema = query.schema
    val schemaMap = schema.toMap
    val keys: Seq[String] = args.map {
      case Var(col) => {
        if(schemaMap contains col){ col }
        else {
          throw new SQLException(s"Invalid column: $col in KeyRepairLens $name")
        }
      }
      case somethingElse => throw new SQLException(s"Invalid argument ($somethingElse) for KeyRepairLens $name")
    }
    val values: Seq[(String, Model)] = 
      schema.map(_._1).filterNot( keys contains _ ).
      map { col => 
        val model =
          new KeyRepairModel(
            s"$name:$col", 
            name, 
            query, 
            keys.map { k => (k, schemaMap(k)) }, 
            col
          )
        model.reconnectToDatabase(db)
        ( col, model )
      }

    (
      Project(
        keys.map { col => ProjectArg(col, Var(col))} ++
        values.map { case (col, model) => 
          ProjectArg(col, 
            Conditional(
              Comparison(Cmp.Lte, Var(s"MIMIR_KR_COUNT_$col"), IntPrimitive(1)),
              Var(col),
              VGTerm(model, 0, keys.map(Var(_)), Seq())
            )
          )
        },
        Aggregate(
          keys.map(Var(_)),
          values.flatMap { case (col, _) => 
            List(
              AggFunction("FIRST", false, List(Var(col)), col),
              AggFunction("COUNT", true, List(Var(col)), s"MIMIR_KR_COUNT_$col")
            )
          },
          query
        )
      ),
      values.map(_._2)
    )

  }
}
