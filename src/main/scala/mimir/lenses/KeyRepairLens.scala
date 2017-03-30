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

    var schema = db.bestGuessSchema(query)
    val schemaMap = schema.toMap
    var scoreCol:Option[String] = None
    var fastPath:Option[String] = None

    //////////  Parse Arguments  //////////
    val keys: Seq[String] = args.flatMap {
      case Var(col) => {
        if(schemaMap contains col){ Some(col) }
        else {
          throw new SQLException(s"Invalid column: $col in KeyRepairLens $name")
        }
      }
      case Function("SCORE_BY", Seq(Var(col))) => {
        scoreCol = Some(col.toUpperCase)
        None
      }
      case Function("FASTPATH", Seq()) => {
        fastPath = Some("MIMIR_FASTPATH_"+name)
        None
      }
      case somethingElse => throw new SQLException(s"Invalid argument ($somethingElse) for KeyRepairLens $name")
    }

    //////////  Assemble Models  //////////
    val values: Seq[(String, Model)] = 
      schema.
      filterNot( (keys ++ scoreCol) contains _._1 ).
      map { case (col, t) => 
        val model =
          new KeyRepairModel(
            s"$name:$col", 
            name, 
            query, 
            keys.map { k => (k, schemaMap(k)) }, 
            col, t,
            scoreCol
          )
        model.reconnectToDatabase(db)
        ( col, model )
      }

    //////////  Build Fastpath Lookup Cache If Needed  //////////
    fastPath.foreach { fastPathTable => 
      buildFastPathCache(
        db, 
        query, 
        keys.map( k => (k, schemaMap(k))), 
        values.map(_._1), 
        fastPathTable
      )
    }

    //////////  Assemble the query  //////////
    val lensQuery = 
      fastPath match {
        case None => 
          assemble(query, keys, values, scoreCol)
        case Some(fastPathTable) => 
          assembleFastPath(db, query, keys, values, scoreCol, fastPathTable)
      }

    ( lensQuery, values.map(_._2) )
  }

  def buildFastPathCache(
    db: Database,
    query: Operator,
    keys: Seq[(String, Type)], 
    values: Seq[String], 
    table: String
  ): Unit =
  {
    val tableElements = 
      keys.map { k => k._1 + " " + k._2 }.mkString(", ") ++
      values.map { v => v + " int"} +
      s"PRIMARY KEY (${keys.map(_._1).mkString(",")})"

    db.backend.update(s"CREATE TABLE $table (${ tableElements.mkString(", ") });")

    db.selectInto(table, 
      Aggregate(
        keys.map( k => Var(k._1)),
        values.map { col => 
          AggFunction("COUNT", true, List(Var(col)), col)
        },
        query
      )
    )
  }

  def assemble(
    query: Operator,
    keys: Seq[String],
    values: Seq[(String, Model)],
    scoreCol: Option[String]
  ): Operator =
  {
    Project(
      keys.map { col => ProjectArg(col, Var(col))} ++
      values.map { case (col, model) => 
        ProjectArg(col, 
          Conditional(
            Comparison(Cmp.Lte, Var(s"MIMIR_KR_COUNT_$col"), IntPrimitive(1)),
            Var(col),
            VGTerm(model, 0, keys.map(Var(_)), 
              Seq(
                Var(s"MIMIR_KR_HINT_COL_$col"),
                scoreCol.
                  map { _ => Var("MIMIR_KR_HINT_SCORE") }.
                  getOrElse( NullPrimitive() )
              )
            )
          )
        )
      },
      Aggregate(
        keys.map(Var(_)),
        values.flatMap { case (col, _) => 
          List(
            AggFunction("FIRST", false, List(Var(col)), col),
            AggFunction("COUNT", true, List(Var(col)), s"MIMIR_KR_COUNT_$col"),
            AggFunction("JSON_GROUP_ARRAY", false, List(Var(col)), s"MIMIR_KR_HINT_COL_$col")
          )
        } ++ scoreCol.map { col => 
            AggFunction("JSON_GROUP_ARRAY", false, List(Var(col)), "MIMIR_KR_HINT_SCORE")
          },
        query
      )
    )
  }

  def assembleFastPath(
    db: Database,
    query: Operator,
    keys: Seq[String],
    values: Seq[(String, Model)],
    scoreCol: Option[String], 
    fastPathTable: String
  ): Operator =
  {
    val rowsWhere = (cond:Expression) => {
      Select(
        ExpressionUtils.makeAnd(keys.map { k => 
          Comparison(Cmp.Eq, Var(s"MIMIR_KR_$k"), Var(k))
        }),
        Join(
          Project(
            keys.map { k => ProjectArg(s"MIMIR_KR_$k", Var(k)) },
            Select(cond, db.getTableOperator(fastPathTable))
          ),
          query
        )
      )}
    val rowsWithDuplicates =
      rowsWhere(
        ExpressionUtils.makeOr(
          values.map { v => Comparison(Cmp.Neq, Var(v._1), IntPrimitive(1)) }
        )
      )
    val rowsWithoutDuplicates =
      rowsWhere(
        ExpressionUtils.makeAnd(
          values.map { v => Comparison(Cmp.Eq, Var(v._1), IntPrimitive(1)) }
        )
      )
    Union(
      rowsWithoutDuplicates,
      assemble(
        rowsWithDuplicates,
        keys,
        values,
        scoreCol
      )
    )
  }
}
