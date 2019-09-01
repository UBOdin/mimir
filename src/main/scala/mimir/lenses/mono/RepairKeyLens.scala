package mimir.lenses.mono

import java.sql.SQLException
import play.api.libs.json._
import sparsity.Name
import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir.Database
import mimir.algebra._
import mimir.lenses._
import mimir.exec.mode.UnannotatedBestGuess
import mimir.serialization.AlgebraJson._

case class RepairKeyLensConfig(
  key: Seq[ID],
  weight: Option[Expression]
)

object RepairKeyLensConfig
{
  implicit val format: Format[RepairKeyLensConfig] = Json.format
}

object RepairKeyLens 
  extends MonoLens
  with LazyLogging
{
  val SAMPLE_SIZE = 2000
  val DUPPED_VALUE_THRESHOLD = 0.2
  val TOTAL_ROWS = ID("TOTAL_ROWS")
  val COL_UNIQUE = ID("UNIQUE_", _:ID)
  val COL_ALTERNATIVES = ID("ALTERNATIVES_", _:ID)
  val CANDIDATE_KEY_TYPES = Set[Type](
    TString(),
    TDate(),
    TTimestamp(),
    TRowId()
  )

  def train(
    db: Database,
    name: ID,
    query: Operator,
    config: JsValue
  ): JsValue = 
  {
    val columnLookup = OperatorUtils.columnLookupFunction(query)
    val configCandidate:RepairKeyLensConfig = 
      config match {
        case JsNull => RepairKeyLensConfig(detectKey(db, query).toSeq, None)
        case JsObject(_) => config.as[RepairKeyLensConfig]
        case JsString(s) => RepairKeyLensConfig(Seq(columnLookup(Name(s))), None)
        case JsArray(keys) => RepairKeyLensConfig(
            keys.map { k => columnLookup(Name(k.as[String])) },
            None
          )
        case _ => throw new SQLException(s"Invalid lens configuration $config")
      }

    Json.toJson(configCandidate)
  }

  def detectKey(
    db: Database,
    query: Operator
  ): Option[ID] =
  {
    val candidateColumns = 
      db.typechecker
        .schemaOf(query)
        .filter { 
          case (col, t) => CANDIDATE_KEY_TYPES contains t
        }
        .map { _._1 }


    if(candidateColumns.isEmpty){
      logger.debug("No reasonable key columns available, defaulting to ROWID")
      return None
    }

    db.query(
      query
        .limit(SAMPLE_SIZE)
        .aggregate(
          (
            AggFunction(
              ID("count"),
              false,
              Seq(),
              TOTAL_ROWS
            ) +:
            candidateColumns.map { col =>
              AggFunction(
                ID("count"),
                true,
                Seq(),
                COL_UNIQUE(col)
              )
            }
          ):_*
        ),
      UnannotatedBestGuess
    ) { results =>
      if(!results.hasNext){
        throw new SQLException("No data, can't guess key column")
      }
      val row = results.next()
      val bestColumn =
        candidateColumns
             .maxBy { col => row(COL_UNIQUE(col)).asLong }
      val duppedValues = 
        1.0 - row(COL_UNIQUE(bestColumn)).asDouble / row(TOTAL_ROWS).asDouble
      if(duppedValues > DUPPED_VALUE_THRESHOLD){
        return None
      }
      return Some(bestColumn)
    }
  }

  def view(
    db: Database,
    name: ID,
    query: Operator,
    jsonConfig: JsValue,
    friendlyName: String
  ): Operator = 
  {
    val config = jsonConfig.as[RepairKeyLensConfig]

    // Shortcut for a no-op repair key lens
    if(config.key.isEmpty) { return query }

    if(config.weight != None){ ??? }

    val isKeyCol = config.key.toSet
    val nonKeyCols = query.columnNames.filter { !isKeyCol(_) }

    query
      .renameByID(
        config.key.map { k => k -> COL_UNIQUE(k) }:_*
      )
      .groupBy(config.key.map { k => Var(COL_UNIQUE(k)) }:_*)(
          nonKeyCols.flatMap { col => 
            Seq(
              AggFunction(
                ID("first"),
                false,
                Seq(Var(col)),
                COL_UNIQUE(col)
              ),
              AggFunction(
                ID("json_group_array"),
                true,
                Seq(CastExpression(Var(col), TString())),
                COL_ALTERNATIVES(col)
              )
            )
          }:_*
        )
      .mapByID(query.columnNames.map { 
        case col if isKeyCol(col) => col -> Var(COL_UNIQUE(col))
        case col => {
          val keyTuple: Seq[Expression] = 
            config.key.flatMap { k =>
              Seq(
                StringPrimitive(", "),
                StringPrimitive(s"$k: "),
                CastExpression(Var(COL_UNIQUE(k)), TString())
              )
            }.tail // strip off the leading ','
          val message = Function(ID("concat"), Seq[Expression](
              StringPrimitive(s"Non-unique key:")
            )++keyTuple++Seq[Expression](
              StringPrimitive(s"; Alternatives for $friendlyName.$col include "),
              Var(COL_ALTERNATIVES(col))
          ))
          val caveat = Caveat(
            name, 
            Var(COL_UNIQUE(col)),
            config.key.map { k => Var(COL_UNIQUE(k)) },
            message
          )

          col -> 
            Function(ID("json_array_length"), Seq(Var(COL_ALTERNATIVES(col))))
              .lte(1)
              .thenElse { Var(COL_UNIQUE(col)) } { caveat }
        }
      }:_*)

//     Project(
//       keys.map { col => ProjectArg(col, Var(col))} ++
//       values.map { case (col, model) => 
//         val vgTerm = 
//           VGTerm(model.name, 0, keys.map(Var(_)), 
//             Seq(
//               Var(ID("MIMIR_KR_HINT_COL_",col)),
//               scoreCol.
//                 map { _ => Var(ID("MIMIR_KR_HINT_SCORE")) }.
//                 getOrElse( NullPrimitive() )
//             )
//           )

//         ProjectArg(col, 
//           if(forceGuess){ vgTerm } 
//           else {
//             Conditional(
//               Comparison(Cmp.Lte, Var(ID("MIMIR_KR_COUNT_",col)), IntPrimitive(1)),
//               Var(col), vgTerm
//             )
//           }
//         )
//       },
//       Aggregate(
//         keys.map(Var(_)),
//         values.flatMap { case (col, _) => 
//           List(
//             AggFunction(ID("first"), false, List(Var(col)), col),
//             AggFunction(ID("count"), true, List(Var(col)), ID("MIMIR_KR_COUNT_",col)),
//             AggFunction(ID("json_group_array"), false, List(Var(col)), ID("MIMIR_KR_HINT_COL_",col))
//           )
//         } ++ scoreCol.map { col => 
//             AggFunction(ID("json_group_array"), false, List(Var(col)), ID("MIMIR_KR_HINT_SCORE"))
//           },
//         query
//       )
//     )    
  }
}

// package mimir.lenses;

// import java.sql._

// import mimir.Database
// import mimir.models._
// import mimir.algebra._
// import mimir.ctables._
// import com.typesafe.scalalogging.slf4j.LazyLogging

// object RepairKeyLens extends LazyLogging {
//   def create(
//     db: Database, 
//     name: ID, 
//     humanReadableName: String,
//     query: Operator, 
//     args:Seq[Expression]
//   ): (Operator, Seq[Model]) =
//   {

//     var schema = db.typechecker.schemaOf(query)
//     val schemaMap = schema.toMap
//     var scoreCol:Option[ID] = None

//     //////////  Parse Arguments  //////////
//     val keys: Seq[ID] = args.flatMap {
//       case Var(col) => {
//         if(schemaMap contains col){ Some(col) }
//         else {
//           throw new SQLException(s"Invalid column: $col in RepairKeyLens $name (available columns: ${schemaMap.keys.mkString(", ")}")
//         }
//       }
//       case Function(ID("score_by"), Seq(Var(col))) => {
//         scoreCol = Some(col)
//         None
//       }
//       case Function(ID("enable"), opts) => {
//         opts.foreach { 
//           case x => 
//             throw new SQLException(s"Invalid Key-Repair option: $x")
//         }
//         None
//       }
//       case somethingElse => throw new SQLException(s"Invalid argument ($somethingElse) for RepairKeyLens $name")
//     }

//     //////////  Assemble Models  //////////
//     val values: Seq[(ID, Model)] = 
//       schema.
//       filterNot( (keys ++ scoreCol) contains _._1 ).
//       map { case (col, t) => 
//         val model =
//           new RepairKeyModel(
//             ID(name,":", col), 
//             name.id, 
//             query, 
//             keys.map { k => (k, schemaMap(k)) }, 
//             col, t,
//             scoreCol
//           )
//         ( col, model )
//       }

//     //////////  Assemble the query  //////////
//     val lensQuery = 
//       assembleView(db, query, keys, values, scoreCol, ID("MIMIR_KR_SOURCE_",name))

//     ( lensQuery, values.map(_._2) )
//   }

//   def assemble(
//     query: Operator,
//     keys: Seq[ID],
//     values: Seq[(ID, Model)],
//     scoreCol: Option[ID],
//     forceGuess: Boolean = false
//   ): Operator =
//   {
//     logger.debug(s"Assembling KR Lens RAW")

//     Project(
//       keys.map { col => ProjectArg(col, Var(col))} ++
//       values.map { case (col, model) => 
//         val vgTerm = 
//           VGTerm(model.name, 0, keys.map(Var(_)), 
//             Seq(
//               Var(ID("MIMIR_KR_HINT_COL_",col)),
//               scoreCol.
//                 map { _ => Var(ID("MIMIR_KR_HINT_SCORE")) }.
//                 getOrElse( NullPrimitive() )
//             )
//           )

//         ProjectArg(col, 
//           if(forceGuess){ vgTerm } 
//           else {
//             Conditional(
//               Comparison(Cmp.Lte, Var(ID("MIMIR_KR_COUNT_",col)), IntPrimitive(1)),
//               Var(col), vgTerm
//             )
//           }
//         )
//       },
//       Aggregate(
//         keys.map(Var(_)),
//         values.flatMap { case (col, _) => 
//           List(
//             AggFunction(ID("first"), false, List(Var(col)), col),
//             AggFunction(ID("count"), true, List(Var(col)), ID("MIMIR_KR_COUNT_",col)),
//             AggFunction(ID("json_group_array"), false, List(Var(col)), ID("MIMIR_KR_HINT_COL_",col))
//           )
//         } ++ scoreCol.map { col => 
//             AggFunction(ID("json_group_array"), false, List(Var(col)), ID("MIMIR_KR_HINT_SCORE"))
//           },
//         query
//       )
//     )
//   }

//   def assembleView(
//     db: Database,
//     query: Operator,
//     keys: Seq[ID],
//     values: Seq[(ID, Model)],
//     scoreCol: Option[ID],
//     view: ID,
//     forceGuess: Boolean = false,
//     useUnions: Boolean = false
//   ): Operator =
//   {
//     logger.debug(s"Assembling KR Lens with View $view")
//     db.views.create(
//       view,
//       Aggregate(
//         keys.map(Var(_)),
//         values.flatMap { case (col, _) => 
//           List(
//             AggFunction(ID("first"), false, List(Var(col)), col),
//             AggFunction(ID("count"), true, List(Var(col)), ID("MIMIR_KR_COUNT_",col)),
//             AggFunction(ID("json_group_array"), false, List(Var(col)), ID("MIMIR_KR_HINT_COL_",col))
//           )
//         } ++ scoreCol.map { col => 
//             AggFunction(ID("json_group_array"), false, List(Var(col)), ID("MIMIR_KR_HINT_SCORE"))
//           },
//         query
//       )
//     )

//     val uncertainRows =
//       Project(
//         keys.map { col => ProjectArg(col, Var(col))} ++
//         values.map { case (col, model) => 
//           val vgTerm = 
//             VGTerm(model.name, 0, keys.map(Var(_)), 
//               Seq(
//                 Var(ID("MIMIR_KR_HINT_COL_",col)),
//                 scoreCol.
//                   map { _ => Var(ID("MIMIR_KR_HINT_SCORE")) }.
//                   getOrElse( NullPrimitive() )
//               )
//             )

//           ProjectArg(col, 
//             if(forceGuess){ vgTerm } 
//             else {
//               Conditional(
//                 Comparison(Cmp.Lte, Var(ID("MIMIR_KR_COUNT_",col)), IntPrimitive(1)),
//                 Var(col), vgTerm
//               )
//             }
//           )
//         },
//         if(forceGuess || !useUnions){
//           db.views.get(view).get.operator
//         } else {
//           Select(
//             ExpressionUtils.makeOr(
//               values.map { v => 
//                 Comparison(Cmp.Gt, Var(ID("MIMIR_KR_COUNT_",v._1)), IntPrimitive(1))
//               }
//             ),
//             db.views.get(view).get.operator
//           )
//         }
//       )
//     if(forceGuess || !useUnions){ uncertainRows }
//     else {
//       val certainRows =
//         Project(
//           (keys ++ values.map { _._1 })
//             .map { col => ProjectArg(col, Var(col))} ,
//           Select(
//             ExpressionUtils.makeAnd(
//               values.map { v => 
//                 Comparison(Cmp.Lte, Var(ID("MIMIR_KR_COUNT_",v._1)), IntPrimitive(1))
//               }
//             ), 
//             db.views.get(view).get.operator
//           )
//         )

//       Union(certainRows, uncertainRows)
//     }
//   }
// }
