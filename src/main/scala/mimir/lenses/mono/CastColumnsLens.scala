package mimir.lenses.mono

import java.sql.SQLException
import play.api.libs.json._

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Row, Encoders, Encoder,  Dataset}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import play.api.libs.json._

import sparsity.Name

import mimir.Database
import mimir.algebra._
import mimir.lenses._
import mimir.exec.mode.UnannotatedBestGuess
import mimir.util.StringUtils
import mimir.serialization.AlgebraJson._


case class CastColumnsLensVote(
  t: Type,
  count: Long
)

object CastColumnsLensVote
{
  implicit val format: Format[CastColumnsLensVote] = Json.format
  def apply(v: (Type, Long)): CastColumnsLensVote = CastColumnsLensVote(v._1, v._2)
}

case class CastColumnsLensColumnConfig(
  chosen: Type,
  trials: Long,
  votes: Seq[CastColumnsLensVote]
)
{
  def voteStringFor(t: Type): String = {
    t match {
      case TString() if votes.isEmpty => 
        "nothing else matched"

      case TString() => {
          val bestAlternative = 
            votes.maxBy { _.count }
          s"Only ${bestAlternative.count} / $trials for the best alternative ${bestAlternative.t}"
        }

      case _ => 
        votes.find { _.t.equals(t) } match {
          case None => "no vote data"
          case Some(v) => s"${v.count} / $trials records conforming"
        }
    }
  }
  def voteString = voteStringFor(chosen)
}

object CastColumnsLensColumnConfig
{
  implicit val format: Format[CastColumnsLensColumnConfig] = Json.format
}

object CastColumnsLens 
  extends MonoLens
{
  val SAMPLE_LIMIT = 1000
  
  def priority: Type => Int =
  {
    case TUser(_)     => 20
    case TInt()       => 10
    case TBool()      => 10
    case TDate()      => 10
    case TTimestamp() => 10
    case TInterval()  => 10
    case TType()      => 10
    case TFloat()     => 5
    case TString()    => 0
    case TRowId()     => -5
    case TAny()       => -10
  }

  def detectType(v: String): Iterable[Type] = {
    Type.tests.flatMap({ case (t, regexp) =>
      regexp.findFirstMatchIn(v).map(_ => t)
    })++
      TypeRegistry.matchers.flatMap({ case (regexp, name) =>
        regexp.findFirstMatchIn(v).map(_ => TUser(name))
      })
  }

  def train(
    db: Database,
    name: ID,
    query: Operator,
    jsonConfig: JsValue
  ): JsValue = 
  {
    val defaultConfig:Map[String,CastColumnsLensColumnConfig] = 
      jsonConfig match {
        case JsNull => Map()
        case JsObject(_) => jsonConfig.as[Map[String, CastColumnsLensColumnConfig]]
        case _ => throw new SQLException(s"Invalid initial configuration: $jsonConfig")
      }

    val schema = 
      db.typechecker.schemaOf(query)

    // Default to training only on non-specialized string columns.
    val stringColumns = 
      schema.filter { _._2.equals(TString()) }
            .map { _._1 }
            .toSet

    // Ignore columns for which we are already given a configuration
    val trainingColumns =
      (stringColumns -- defaultConfig.keys.map { ID(_) }.toSet).toSeq

    // Us a CastColumnsVoteList aggregate to extract types.
    // Most of the post-processing afterwards is to extract the result into something
    // more friendly.  The resulting table has one row for each training column with schema:
    // - number of rows evaluated
    // - votes cast: a seq
    //     - The Type in question
    //     - number of rows compliant with the type
    //     - fraction of rows compliant with the type
    val trainingResult: Seq[(Long, Seq[(Type, Long)])] = 
      db.compiler.compileToSparkWithRewrites(
        query.limit(SAMPLE_LIMIT, 0)
             .projectByID(trainingColumns:_*)
       ).agg(CastColumnsVoteList.toColumn)
        .head()
        .asInstanceOf[Row]
        .toSeq(0)
        .asInstanceOf[Seq[Row]]
        .map { colCounts => 
          (
            colCounts.getLong(0), // the total number of rows tested
            colCounts.getSeq[Row](1) // votes per type
                     .map { votes => 
                        (
                          Type.fromIndex(votes.getInt(0)),  // the type index
                          votes.getLong(1)                  // the number of votes for this type
                        )
                     }
          )
        }
        .toSeq

    val newConfigEntries: Map[String, CastColumnsLensColumnConfig] = 
      trainingColumns.zip(trainingResult)
        .map { case (columnId, (numberOfRowsTested, rawVotes)) => 
          // include 50% votes for String
          val votes = rawVotes :+ (TString(), numberOfRowsTested/2)

          // Rank votes by the number of votes, settling differences with the 
          // priority function given above.
          val bestType = 
            votes.maxBy { v => (v._2, priority(v._1)) }
                 ._1 // keep only the selected type

          columnId.id -> CastColumnsLensColumnConfig(
            bestType, 
            numberOfRowsTested,
            votes.map { CastColumnsLensVote(_) }
          )
        }
        .toMap

    Json.toJson(newConfigEntries)
  }

  def view(
    db: Database,
    name: ID,
    query: Operator,
    jsonConfig: JsValue,
    friendlyName: String
  ): Operator = 
  {
    val config = jsonConfig.as[Map[String, CastColumnsLensColumnConfig]]

    query.alterColumns(
            config.map { case (col, colConfig) =>
              val cast = CastExpression(Var(col), colConfig.chosen)
              val message = 
                Function(ID("concat"), Seq(
                  StringPrimitive("Couldn't cast '"),
                  CastExpression(Var(col), TString()),
                  StringPrimitive(s"' as ${StringUtils.withDefiniteArticle(colConfig.chosen.toString)} in $friendlyName.$col (${colConfig.voteString})")
                ))
              val assembledCaveat =
                Caveat(name, NullPrimitive(), Seq(Var(col)), message)
              col -> 
                Var(col)
                  .isNull
                  .thenElse { NullPrimitive() }
                            { cast.isNull 
                                  .thenElse { assembledCaveat }
                                            { cast }
                            }

            }.toSeq:_*
    )
  }

}



object CastColumnsVoteList
    extends Aggregator[Row,  Seq[(Long,Seq[(Int,Long)])], Seq[(Long,Seq[(Int,Long)])]] with Serializable 
{
  def zero = Seq[(Long,Seq[(Int, Long)])]()
  def reduce(acc: Seq[(Long, Seq[(Int, Long)])], x: Row) = {
     val newacc = x.toSeq.zipWithIndex.map(field => 
       field match {
         case (null, idx) => (0L, Seq[(Int, Long)]())
         case (_, idx) => {
           if(!x.isNullAt(idx)){
             val cellVal = x.getString(idx)
             (1L, CastColumnsLens.detectType(cellVal).toSeq.map(el => (Type.toIndex(el), 1L)))
           }
           else (0L, Seq[(Int, Long)]())
         }
       }  
     )
    merge(acc, newacc)
  }
  def reduce(acc: Seq[(Long, Seq[(Int, Long)])], x: Seq[Seq[Type]]) =
  {
    merge(acc, 
      x.map {  
        case Seq() => (0L, Seq[(Int, Long)]())
        case field => (1L, field.map { el => (Type.toIndex(el), 1L) })
      }
    )
  }
  def merge(acc1: Seq[(Long, Seq[(Int, Long)])], acc2: Seq[(Long, Seq[(Int, Long)])]) = acc1 match {
      case Seq() | Seq( (0L, Seq()) ) => acc2
      case x => acc2 match {
        case Seq() | Seq( (0L, Seq()) ) => acc1
        case x => {
          acc1.zip(acc2).map(oldNew => {
            (
              oldNew._1._1+oldNew._2._1, 
              (oldNew._1._2++oldNew._2._2)
                .groupBy { _._1 }
                .mapValues { _.map { _._2 }.sum }
                .toSeq
            )
          })
        }
      }
    }
    
  def finish(acc: Seq[(Long, Seq[(Int, Long)])]) = 
    acc.map { case (totalCount, countByType) =>
      (
        totalCount, 
        countByType.groupBy { _._1 }
                   .map { case (typeIndex, counts) => 
                      typeIndex ->
                        counts.map { _._2 }.sum
                   }
                   .toSeq
                   .sortBy { -_._2 }
      )
    }
  def bufferEncoder: Encoder[Seq[(Long, Seq[(Int, Long)])]] = ExpressionEncoder()
  def outputEncoder: Encoder[Seq[(Long, Seq[(Int, Long)])]] = ExpressionEncoder()
}

// package mimir.adaptive

// import com.typesafe.scalalogging.slf4j.LazyLogging
// import mimir.Database
// import mimir.algebra._
// import mimir.lenses._
// import mimir.models._
// import mimir.util.SqlUtils
// import mimir.exec.spark.MimirSpark

// object TypeInference
//   extends Multilens
//     with LazyLogging
// {

  
//   def detectType(v: String): Iterable[Type] = {
//     Type.tests.flatMap({ case (t, regexp) =>
//       regexp.findFirstMatchIn(v).map(_ => t)
//     })++
//       TypeRegistry.matchers.flatMap({ case (regexp, name) =>
//         regexp.findFirstMatchIn(v).map(_ => TUser(name))
//       })
//   }

//   def initSchema(db: Database, config: MultilensConfig): TraversableOnce[Model] =
//   {
//     logger.debug(s"Creating TypeInference: $config")
//     val viewName = config.schema
    
//     val stringDefaultScore: Double = 
//       config.args match {
//         case Seq() => 0.5
//         case Seq(FloatPrimitive(f)) if (f >= 0.0 && f <= 1.0) => f
//         case _ => throw new RAException(s"Invalid configuration for type inference lens: ${config.args}")
//       }

//     // Initialize the vote counters
//     val modelColumns = 
//       db.typechecker.schemaOf(config.query).flatMap({
//         case (col, (TString() | TAny())) => Some(col)
//         case _ => None
//       }).toIndexedSeq

    
//     val attributeTypeModel = 
//       new TypeInferenceModel(
//         viewName.withPrefix("MIMIR_TI_ATTR_"),
//         config.humanReadableName,
//         modelColumns,
//         stringDefaultScore,
//         MimirSpark.get,
//         Some(db.compiler.compileToSparkWithRewrites(
//             config.query.limit(TypeInferenceModel.sampleLimit, 0)
//         ))
//       )

//     val warningModel = 
//       new WarningModel(
//         ID("MIMIR_TI_WARNING_",viewName),
//         Seq(TString(), TString(), TString(), TRowId())
//       )

//     val columnIndexes = 
//       modelColumns.zipWithIndex.toMap

//     logger.debug(s"Training $attributeTypeModel.name on ${config.query}")
//     //model.train(db.backend.execute(config.query))
    
//     Seq(attributeTypeModel, warningModel)
//   }

//   def tableCatalogFor(db: Database, config: MultilensConfig): Operator =
//   {
//     HardTable(
//       Seq(
//         ID("TABLE_NAME") -> TString()
//       ),
//       Seq(
//         Seq(
//           StringPrimitive("DATA")
//         )
//       )
//     )
//   }
  
//   def attrCatalogFor(db: Database, config: MultilensConfig): Operator =
//   {
//     val model = db.models.get(ID("MIMIR_TI_ATTR_",config.schema)).asInstanceOf[TypeInferenceModel]
//     val columnIndexes = model.columns.zipWithIndex.toMap
//     lazy val qSchema = db.typechecker.schemaOf(config.query).toMap
//     HardTable(
//       Seq(
//         ID("TABLE_NAME") -> TString(), 
//         ID("ATTR_NAME")  -> TString(),
//         ID("IS_KEY")     -> TBool(), 
//         ID("IDX")        -> TInt(),
//         ID("HARD_TYPE")  -> TType()
//       ),
//       config.query.columnNames.map(col => 
//         Seq(
//           StringPrimitive("DATA"), 
//           StringPrimitive(col.id), 
//           BoolPrimitive(false),
//           IntPrimitive(columnIndexes.getOrElse(col, -1).toLong),
//           if(columnIndexes contains col){ NullPrimitive() } 
//             else { TypePrimitive(qSchema(col)) }
//         )
//       )
//     ).addColumns(
//       "ATTR_TYPE" -> 
//         Var(ID("HARD_TYPE"))
//           .isNull
//           .thenElse {
//             VGTerm(config.schema.withPrefix("MIMIR_TI_ATTR_"), 0, Seq(Var(ID("IDX"))), Seq())
//           } {
//             Var(ID("HARD_TYPE"))
//           }
//     ).removeColumns("IDX", "HARD_TYPE")
//   }
        
//   def viewFor(db: Database, config: MultilensConfig, table: ID): Option[Operator] =
//   {
//     if(table.equals(ID("DATA"))){
//       val model = db.models.get(ID("MIMIR_TI_ATTR_",config.schema)).asInstanceOf[TypeInferenceModel]
//       val columnIndexes = model.columns.zipWithIndex.toMap
//       Some(Project(
//         config.query.columnNames.zipWithIndex.map { case (colName, colPosition) => {
//           ProjectArg(colName, 
//             if(columnIndexes contains colName){ 
//               val bestGuessType = model.bestGuess(0, Seq(IntPrimitive(columnIndexes(colName))), Seq())
//               val castExpression = CastExpression(Var(colName), bestGuessType.asInstanceOf[TypePrimitive].t)
//               Conditional(
//                 IsNullExpression(Var(colName)),
//                 NullPrimitive(),
//                 Conditional(
//                   IsNullExpression(castExpression),
//                   DataWarning(
//                     config.schema.withPrefix("MIMIR_TI_WARNING_"),
//                     NullPrimitive(),
//                     Function("CONCAT", 
//                       StringPrimitive("Couldn't Cast [ "),
//                       Var(colName),
//                       StringPrimitive(" ] to "+bestGuessType+" on row "),
//                       RowIdVar(),
//                       StringPrimitive(s" of ${config.humanReadableName}.${colName}")
//                     ),
//                     Seq(StringPrimitive(colName.id), Var(colName), StringPrimitive(bestGuessType.toString), RowIdVar()),
//                     colPosition
//                   ),
//                   castExpression
//                 )
//               )
//             } else {
//               Var(colName)
//             }
//           )
//         }}, config.query
//       ))  
//     } else { None }
//   }

  
// }

