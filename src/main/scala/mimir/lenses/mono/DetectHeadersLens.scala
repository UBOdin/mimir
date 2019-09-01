package mimir.lenses.mono

import java.sql.SQLException
import com.typesafe.scalalogging.slf4j.LazyLogging
import play.api.libs.json._
import sparsity.Name
import mimir.Database
import mimir.algebra._
import mimir.lenses._
import mimir.exec.mode.UnannotatedBestGuess
import mimir.util.NameLookup
import mimir.serialization.AlgebraJson._

case class DetectHeadersLensConfig(
  header: Option[RowIdPrimitive],
  columns: Option[Seq[ID]],
  guess: Boolean
)
object DetectHeadersLensConfig
{
  implicit val format: Format[DetectHeadersLensConfig] = Json.format
}


object DetectHeadersLens 
  extends MonoLens
  with LazyLogging
{
  def train(
    db: Database, 
    name: ID, 
    query: Operator,
    configJson: JsValue 
  ): JsValue =
  {
    // Use this opportunity to validate the config
    val configRaw:Map[String, JsValue] = 
      configJson match {
        case j:JsObject => j.as[Map[String,JsValue]]
        case JsNull => Map()
        case _ => throw new SQLException(s"Invalid lens configuration: $configJson")
      }

    logger.debug(s"Input config: $configRaw")

    val isAGuess = configRaw.getOrElse("guess", JsBoolean(true)).as[Boolean]

    val newConfig = 
      if( isAGuess ){
        logger.debug("Checking for presence of headers")
        val header = detectHeaderRow(db, query)

        DetectHeadersLensConfig(
          header.map { _._1 },
          header.map { _._2 },
          true
        )
      } else {
        DetectHeadersLensConfig(
          None,
          None,
          false
        )
      }

    return Json.toJson(newConfig)
  }

  private def detectHeaderRow(
    db: Database,
    query: Operator
  ): Option[(RowIdPrimitive, Seq[ID])] = 
  {

    // Skim the top set of records off the top of the collection
    val headRecords = 
      db.query(
        query.limit(20),
        UnannotatedBestGuess
      ) { results =>
        results.map { row => (row.provenance, row.tuple) }
               .toIndexedSeq
      }

    logger.debug(s"Got ${headRecords.size} sample records")

    if(headRecords.isEmpty){ return None }

    // The first row is potentially the header
    val header = headRecords.head._2
    // If it is the header, we want its rowID
    val headerRowid = headRecords.head._1

    logger.debug(s"Possible header (id = $headerRowid): < ${header.mkString(", ")} >")

    // For non-first rows, we need to know their types
    val topRecordVotedTypes: Seq[Type] = 
      CastColumnsVoteList.finish(
        headRecords.tail
          .map { tuple =>
            tuple._2
                 .map { 
                  case NullPrimitive() => Seq[Type]()
                  case something => CastColumnsLens.detectType(something.asString).toSeq
                 }
          }
          .foldLeft(CastColumnsVoteList.zero)
            { (acc, row) => CastColumnsVoteList.reduce(acc, row) }
      ).map { _._2.headOption match { case Some((bestVote, _)) => Type.fromIndex(bestVote)
                                      case None => TAny() } }

    logger.debug(s"Detected types: < ${topRecordVotedTypes.mkString(", ")} >")

    // Find any "header" columns with duplicate names.  Dump them into a
    // mutable map that we can use to rename them COLNAME_#
    val duplicateHeaderColumns = 
        collection.mutable.Map((
                // groupBy(identity) pairs together columns with the same name
          header.filter { !_.equals(NullPrimitive) }
                .map { _.asString }
                .groupBy(identity)
                // pick out column names that appear multiple times
                .filter { case (_, copies) => copies.length > 1 }
                // Initialize the COLNAME_# counter to 0 (pre-increment)
                .map { case (colname, _) => (colname -> 0) }
        ).toSeq: _*)

    logger.debug(s"Duplicated header columns: $duplicateHeaderColumns")

    // In some rare cases, we might not be getting any type information for certain columns.  This 
    // makes it harder to determine whether a header is there, so at the very least, detect this
    // case and dump out some debugging text
    val columnsWithNoType = 
      topRecordVotedTypes.zipWithIndex
                         .collect { case (TAny(), idx) => idx }

    logger.debug(s"Untyped columns: $columnsWithNoType")

    // Throw some quick heuristic regexps at the potential header names to figure out if they "seem" 
    // right.
    val goodHeaderColumnIndexes = 
      header.zipWithIndex
            .filter { case (col, idx) => isReasonableHeaderName(col.asString) }
            .map { _._2 }
            .toSet

    logger.debug(s"Columns with a reasonable header name: $goodHeaderColumnIndexes")

    // Figure out if any of the header columns don't match the detected types
    val typeMismatchHeaderColumnIndexes =
      header.zip(topRecordVotedTypes)
            .zipWithIndex
            .filter { 
              // If the rest of the column is null, a non-null header is a mismatch
              case ((NullPrimitive(), TAny()), _) => false 
              case ((_, TAny()), _) => true 
              // Otherwise test for castability
              case ((col, t), idx) => Cast(t, col).equals(NullPrimitive()) 
            } 
            .map { _._2 }
            .toSet

    logger.debug(s"Columns with headers that don't match the type: $goodHeaderColumnIndexes")

    // Invert the union of the above two sets
    val badHeaderColumnIndexes = (
      (0 until header.length).toSet -- goodHeaderColumnIndexes
                                    -- typeMismatchHeaderColumnIndexes
    )

    logger.debug(s"Columns with first rows that probably aren't a header: $badHeaderColumnIndexes")

    if(badHeaderColumnIndexes.isEmpty){
      logger.debug("There probably is a header on this table")
      val columnNames = 
        header.zipWithIndex
              .map { 
                case (NullPrimitive(), idx) => ID(s"COLUMN_$idx")
                case (StringPrimitive(""), idx) => ID(s"COLUMN_$idx")
                case (col, _) if duplicateHeaderColumns contains col.asString => {
                  duplicateHeaderColumns(col.asString) += 1
                  ID(sanitizeColumnName(col.asString)+"_"+duplicateHeaderColumns(col.asString))
                }
                case (col, _) => sanitizeColumnName(col.asString)
              }
      return Some((headerRowid, columnNames))
    } else {
      logger.debug("There probably is not a header on this table")
      return None
    }
  }

  val headerRegex =  """[0-9]*[a-zA-Z_ :\/\\-]+[0-9]*[a-zA-Z0-9_ :\/\\-]*""".r
  def isReasonableHeaderName(header:String): Boolean = {
    header match {
      case "NULL" => false
      case headerRegex() => true
      case _ => false
    }
  }

  private def sanitizeColumnName(name: String): ID =
  {
    ID.upper(
      name
        .replaceAll("[^a-zA-Z0-9]+", "_")    // Replace sequences of non-alphanumeric characters with underscores
        .replaceAll("_+$", "")               // Strip trailing underscores
        .replaceAll("^_+", "")               // Strip leading underscores
    )
  }

  def view(
    db: Database, 
    name: ID, 
    query: Operator, 
    configJson: JsValue, 
    friendlyName: String
  ): Operator = 
  {
    val config = configJson.as[DetectHeadersLensConfig]

    (config.header, config.columns) match {
      case (Some(rowid), Some(newColNames)) => 
        query.filter { RowIdVar().neq(rowid)}
             .renameByID(
               query.columnNames
                    .zip(newColNames):_*
             )
      case _ => 
        query
    }
  }


}