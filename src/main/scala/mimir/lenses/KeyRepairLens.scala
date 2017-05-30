package mimir.lenses;

import java.sql._

import mimir.Database
import mimir.models._
import mimir.algebra._
import mimir.ctables._
import com.typesafe.scalalogging.slf4j.LazyLogging

object KeyRepairLens extends LazyLogging {
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
      case Function("ENABLE", opts) => {
        opts.foreach { 
          case Var("FAST_PATH") => 
            fastPath = Some("MIMIR_FASTPATH_"+name)
          case x => 
            throw new SQLException(s"Invalid Key-Repair option: $x")
        }
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
    fastPath match { 
      case None => ()
      case Some(fastPathTable) => {
        buildFastPathCache(
          db, 
          query, 
          keys.map( k => (k, schemaMap(k))), 
          values.map(_._1), 
          fastPathTable
        )
      }
    }

    //////////  Assemble the query  //////////
    val lensQuery = 
      fastPath match {
        case None => 
          assembleView(db, query, keys, values, scoreCol, "MIMIR_KR_SOURCE_"+name)
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
    db.selectInto(table, 
      Aggregate(
        keys.map( k => Var(k._1)),
        Seq(AggFunction("COUNT", false, Seq(), "NUM_INSTANCES")),
        query
      )
    )
  }

  def assemble(
    query: Operator,
    keys: Seq[String],
    values: Seq[(String, Model)],
    scoreCol: Option[String],
    forceGuess: Boolean = false
  ): Operator =
  {
    logger.debug(s"Assembling KR Lens RAW")

    Project(
      keys.map { col => ProjectArg(col, Var(col))} ++
      values.map { case (col, model) => 
        val vgTerm = 
          VGTerm(model, 0, keys.map(Var(_)), 
            Seq(
              Var(s"MIMIR_KR_HINT_COL_$col"),
              scoreCol.
                map { _ => Var("MIMIR_KR_HINT_SCORE") }.
                getOrElse( NullPrimitive() )
            )
          )

        ProjectArg(col, 
          if(forceGuess){ vgTerm } 
          else {
            Conditional(
              Comparison(Cmp.Lte, Var(s"MIMIR_KR_COUNT_$col"), IntPrimitive(1)),
              Var(col), vgTerm
            )
          }
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

  def assembleView(
    db: Database,
    query: Operator,
    keys: Seq[String],
    values: Seq[(String, Model)],
    scoreCol: Option[String],
    view: String,
    forceGuess: Boolean = false
  ): Operator =
  {
    logger.debug(s"Assembling KR Lens with View $view")
    db.views.create(
      view,
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

    Project(
      keys.map { col => ProjectArg(col, Var(col))} ++
      values.map { case (col, model) => 
        val vgTerm = 
          VGTerm(model, 0, keys.map(Var(_)), 
            Seq(
              Var(s"MIMIR_KR_HINT_COL_$col"),
              scoreCol.
                map { _ => Var("MIMIR_KR_HINT_SCORE") }.
                getOrElse( NullPrimitive() )
            )
          )

        ProjectArg(col, 
          if(forceGuess){ vgTerm } 
          else {
            Conditional(
              Comparison(Cmp.Lte, Var(s"MIMIR_KR_COUNT_$col"), IntPrimitive(1)),
              Var(col), vgTerm
            )
          }
        )
      },
      db.views.get(view).get.operator
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
    logger.debug(s"Assembling KR Lens FastPath $fastPathTable")
    val rowsWhere = (cond:Expression) => {
      OperatorUtils.projectColumns(
        keys ++ values.map(_._1),
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
        )
      )}
    val rowsWithDuplicates =
      rowsWhere(
        Comparison(Cmp.Neq, Var("NUM_INSTANCES"), IntPrimitive(1))
      )
    val rowsWithoutDuplicates =
      rowsWhere(
        Comparison(Cmp.Eq, Var("NUM_INSTANCES"), IntPrimitive(1))
      )
    Union(
      rowsWithoutDuplicates,
      assemble(
        rowsWithDuplicates,
        keys,
        values,
        scoreCol,
        forceGuess = true
      )
    )
  }
}
