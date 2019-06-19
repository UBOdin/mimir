package mimir.lenses;

import java.sql._

import mimir.Database
import mimir.models._
import mimir.algebra._
import mimir.ctables._
import com.typesafe.scalalogging.slf4j.LazyLogging

object RepairKeyLens extends LazyLogging {
  def create(
    db: Database, 
    name: ID, 
    query: Operator, 
    args:Seq[Expression]
  ): (Operator, Seq[Model]) =
  {

    var schema = db.typechecker.schemaOf(query)
    val schemaMap = schema.toMap
    var scoreCol:Option[ID] = None
    var fastPath:Option[ID] = None

    //////////  Parse Arguments  //////////
    val keys: Seq[ID] = args.flatMap {
      case Var(col) => {
        if(schemaMap contains col){ Some(col) }
        else {
          throw new SQLException(s"Invalid column: $col in RepairKeyLens $name")
        }
      }
      case Function(ID("score_by"), Seq(Var(col))) => {
        scoreCol = Some(col)
        None
      }
      case Function(ID("enable"), opts) => {
        opts.foreach { 
          case Var(ID("FAST_PATH")) => 
            fastPath = Some(ID("MIMIR_FASTPATH_", name))
          case x => 
            throw new SQLException(s"Invalid Key-Repair option: $x")
        }
        None
      }
      case somethingElse => throw new SQLException(s"Invalid argument ($somethingElse) for RepairKeyLens $name")
    }

    //////////  Assemble Models  //////////
    val values: Seq[(ID, Model)] = 
      schema.
      filterNot( (keys ++ scoreCol) contains _._1 ).
      map { case (col, t) => 
        val model =
          new RepairKeyModel(
            ID(name,":", col), 
            name.id, 
            query, 
            keys.map { k => (k, schemaMap(k)) }, 
            col, t,
            scoreCol
          )
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
          assembleView(db, query, keys, values, scoreCol, ID("MIMIR_KR_SOURCE_",name))
        case Some(fastPathTable) => 
          assembleFastPath(db, query, keys, values, scoreCol, fastPathTable)
      }

    ( lensQuery, values.map(_._2) )
  }

  def buildFastPathCache(
    db: Database,
    query: Operator,
    keys: Seq[(ID, Type)], 
    values: Seq[ID], 
    table: ID
  ): Unit =
  {
    db.selectInto(table, 
      Aggregate(
        keys.map( k => Var(k._1)),
        Seq(AggFunction(ID("count"), false, Seq(), ID("NUM_INSTANCES"))),
        query
      )
    )
  }

  def assemble(
    query: Operator,
    keys: Seq[ID],
    values: Seq[(ID, Model)],
    scoreCol: Option[ID],
    forceGuess: Boolean = false
  ): Operator =
  {
    logger.debug(s"Assembling KR Lens RAW")

    Project(
      keys.map { col => ProjectArg(col, Var(col))} ++
      values.map { case (col, model) => 
        val vgTerm = 
          VGTerm(model.name, 0, keys.map(Var(_)), 
            Seq(
              Var(ID("MIMIR_KR_HINT_COL_",col)),
              scoreCol.
                map { _ => Var(ID("MIMIR_KR_HINT_SCORE")) }.
                getOrElse( NullPrimitive() )
            )
          )

        ProjectArg(col, 
          if(forceGuess){ vgTerm } 
          else {
            Conditional(
              Comparison(Cmp.Lte, Var(ID("MIMIR_KR_COUNT_",col)), IntPrimitive(1)),
              Var(col), vgTerm
            )
          }
        )
      },
      Aggregate(
        keys.map(Var(_)),
        values.flatMap { case (col, _) => 
          List(
            AggFunction(ID("first"), false, List(Var(col)), col),
            AggFunction(ID("count"), true, List(Var(col)), ID("MIMIR_KR_COUNT_",col)),
            AggFunction(ID("json_group_array"), false, List(Var(col)), ID("MIMIR_KR_HINT_COL_",col))
          )
        } ++ scoreCol.map { col => 
            AggFunction(ID("json_group_array"), false, List(Var(col)), ID("MIMIR_KR_HINT_SCORE"))
          },
        query
      )
    )
  }

  def assembleView(
    db: Database,
    query: Operator,
    keys: Seq[ID],
    values: Seq[(ID, Model)],
    scoreCol: Option[ID],
    view: ID,
    forceGuess: Boolean = false,
    useUnions: Boolean = false
  ): Operator =
  {
    logger.debug(s"Assembling KR Lens with View $view")
    db.views.create(
      view,
      Aggregate(
        keys.map(Var(_)),
        values.flatMap { case (col, _) => 
          List(
            AggFunction(ID("first"), false, List(Var(col)), col),
            AggFunction(ID("count"), true, List(Var(col)), ID("MIMIR_KR_COUNT_",col)),
            AggFunction(ID("json_group_array"), false, List(Var(col)), ID("MIMIR_KR_HINT_COL_",col))
          )
        } ++ scoreCol.map { col => 
            AggFunction(ID("json_group_array"), false, List(Var(col)), ID("MIMIR_KR_HINT_SCORE"))
          },
        query
      )
    )

    val uncertainRows =
      Project(
        keys.map { col => ProjectArg(col, Var(col))} ++
        values.map { case (col, model) => 
          val vgTerm = 
            VGTerm(model.name, 0, keys.map(Var(_)), 
              Seq(
                Var(ID("MIMIR_KR_HINT_COL_",col)),
                scoreCol.
                  map { _ => Var(ID("MIMIR_KR_HINT_SCORE")) }.
                  getOrElse( NullPrimitive() )
              )
            )

          ProjectArg(col, 
            if(forceGuess){ vgTerm } 
            else {
              Conditional(
                Comparison(Cmp.Lte, Var(ID("MIMIR_KR_COUNT_",col)), IntPrimitive(1)),
                Var(col), vgTerm
              )
            }
          )
        },
        if(forceGuess || !useUnions){
          db.views.get(view).get.operator
        } else {
          Select(
            ExpressionUtils.makeOr(
              values.map { v => 
                Comparison(Cmp.Gt, Var(ID("MIMIR_KR_COUNT_",v._1)), IntPrimitive(1))
              }
            ),
            db.views.get(view).get.operator
          )
        }
      )
    if(forceGuess || !useUnions){ uncertainRows }
    else {
      val certainRows =
        Project(
          (keys ++ values.map { _._1 })
            .map { col => ProjectArg(col, Var(col))} ,
          Select(
            ExpressionUtils.makeAnd(
              values.map { v => 
                Comparison(Cmp.Lte, Var(ID("MIMIR_KR_COUNT_",v._1)), IntPrimitive(1))
              }
            ), 
            db.views.get(view).get.operator
          )
        )

      Union(certainRows, uncertainRows)
    }
  }

  def assembleFastPath(
    db: Database,
    query: Operator,
    keys: Seq[ID],
    values: Seq[(ID, Model)],
    scoreCol: Option[ID], 
    fastPathTable: ID
  ): Operator =
  {
    logger.debug(s"Assembling KR Lens FastPath $fastPathTable")
    val rowsWhere = (cond:Expression) => {
      db.table(fastPathTable)
        .filter(cond)
        .mapByID( keys.map { k => ID("MIMIR_KR_",k) -> Var(k)}:_* )
        .join(query)
        .filter { 
          ExpressionUtils.makeAnd(keys.map { k => 
            ID("MIMIR_KR_",k).eq(k)
          })
        }
        .projectByID( (keys++values.map(_._1)):_* )
    }

    val rowsWithDuplicates =
      rowsWhere(
        ID("NUM_INSTANCES").neq(1)
      )
    val rowsWithoutDuplicates =
      rowsWhere(
        ID("NUM_INSTANCES").eq(1)
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
