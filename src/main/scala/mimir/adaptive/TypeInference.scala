package mimir.adaptive

import com.typesafe.scalalogging.slf4j.LazyLogging
import mimir.Database
import mimir.algebra._
import mimir.lenses._
import mimir.models._
import mimir.util.SqlUtils

object TypeInference
  extends Multilens
    with LazyLogging
{

  
  def detectType(v: String): Iterable[Type] = {
    Type.tests.flatMap({ case (t, regexp) =>
      regexp.findFirstMatchIn(v).map(_ => t)
    })++
      TypeRegistry.matchers.flatMap({ case (regexp, name) =>
        regexp.findFirstMatchIn(v).map(_ => TUser(name))
      })
  }

  def initSchema(db: Database, config: MultilensConfig): TraversableOnce[Model] =
  {
    logger.debug(s"Creating TypeInference: $config")
    val viewName = config.schema
    
    val stringDefaultScore: Double = 
      config.args match {
        case Seq() => 0.5
        case Seq(FloatPrimitive(f)) if (f >= 0.0 && f <= 1.0) => f
        case _ => throw new RAException(s"Invalid configuration for type inference lens: ${config.args}")
      }

    // Initialize the vote counters
    val modelColumns =
      db.typechecker.schemaOf(config.query).flatMap { 
        case (col, (TString() | TAny())) => Some(col)
        case _ => None
      }.toIndexedSeq

    val totalVotes = collection.mutable.IndexedSeq[Double](modelColumns.map(_ => 0.0):_*)

    val votes = 
      modelColumns.map(col=> {
        collection.mutable.Map(
        ((Type.tests ++ TypeRegistry.registeredTypes).map(tup => {
          (Type.fromString(tup._1.toString):Type, 0.0)
        })).toSeq:_*)  
      })

    // Scan through each record to find each column's type.
    db.query(
      Project(
        modelColumns.map( c => ProjectArg(c, Var(c)) ),
        config.query
      )
    ) { results => 
      results.toSeq.map( row => {
        row.tuple.zipWithIndex.map{
          case (v, idx) =>  v match {
            case null            => ()
            case NullPrimitive() => ()
            case _               => {
              totalVotes(idx) += 1.0
              val candidates = detectType(v.asString)
              logger.debug(s"Guesses for '$v': $candidates")
              val votesForCurrentIdx = votes(idx)
              candidates.map(t => {
                votesForCurrentIdx(t) = votesForCurrentIdx.getOrElse(t, 0.0) + 1.0
              })
            }
          }
        }
      })
    }

    logger.debug("Creating Backend Table for Type Inference")
    // Create backend table that contains all the
    
    val attrCatalog = "MIMIR_TI_ATTR_"+config.schema
    db.backend.update(s"""
      CREATE TABLE $attrCatalog (TABLE_NAME string, ATTR_NAME string,ATTR_TYPE string, IDX int, IS_KEY bool, SCORE real)""")

    logger.debug("Filling Type Inference backend table")
    ((votes zip modelColumns).zipWithIndex).map{
      case ((votesByTypeForCol, col), idx) => {
        // update the map of each column that tracks the type counts
        val totalVotesForCol = totalVotes(idx)
        votesByTypeForCol.toIndexedSeq.map {
          case (typ, score) => {
            val normalizedScore = 
              if(totalVotesForCol > 0.0){ score.toDouble / totalVotesForCol }
              else { 0.0 }
              assert(normalizedScore <= 1.0)
            db.backend.update(s"""
              INSERT INTO $attrCatalog(TABLE_NAME, ATTR_NAME, ATTR_TYPE, IDX, IS_KEY, SCORE) VALUES (?, ?, ?, ?, ?, ?)
            """, Seq(
              StringPrimitive(viewName),
              StringPrimitive(col),
              StringPrimitive(typ.toString),
              IntPrimitive(idx),
              BoolPrimitive(false),
              FloatPrimitive(normalizedScore)
            )) // update the table for repair key
          }
        }
        db.backend.update(s"""
          INSERT INTO $attrCatalog(TABLE_NAME, ATTR_NAME, ATTR_TYPE, IDX, IS_KEY, SCORE) VALUES (?, ?, 'string', ?, ?, ?)
        """, Seq(
          StringPrimitive(viewName),
          StringPrimitive(col),
          IntPrimitive(idx),
          BoolPrimitive(false),
          FloatPrimitive(stringDefaultScore)
        )) // update the table for repair key
      }
    }

    val name = attrCatalog + "_RK"
    val attrQuery = db.table(attrCatalog)
    db.bestGuessSchema(attrQuery).
      filterNot( Seq("IDX", "SCORE") contains _._1 ).
      map { case (col, t) => 
        val model =
          new TIRepairModel(
            s"$name:$col", 
            name, 
            attrQuery, 
            Seq(("IDX", TInt())), 
            col, t,
            Some("SCORE"),
            modelColumns
          )
        model.reconnectToDatabase(db)
        model 
      }
  }

  def tableCatalogFor(db: Database, config: MultilensConfig): Operator =
  {
    HardTable(Seq(("TABLE_NAME",TString())),Seq(Seq(StringPrimitive("MIMIR_TI_TABLE_"+config.schema))))
  }

  def attrCatalogFor(db: Database, config: MultilensConfig): Operator =
  {
    inferredTypesView(db, config.schema, db.table("MIMIR_TI_ATTR_"+config.schema).sort(("IDX",false)))
  }

  def viewFor(db: Database, config: MultilensConfig, table: String): Option[Operator] =
  {
    Some(Project(
        db.query(
          attrCatalogFor(db, config)
            .project("IDX", "ATTR_TYPE")
        ) { results => {
            val cols = db.typechecker.schemaOf(config.query).unzip._1
            results.toSeq.map { row =>
              val colName = cols(row(0).asInt)
              val colType = row(1).asString
              ProjectArg(
                colName,
                Function("CAST", Seq(Var(colName), TypePrimitive(Type.fromString(colType))))
              )
            }.toIndexedSeq}
    }, config.query))  
  }

  final def inferredTypesView(db: Database, schema:String, query:Operator): Operator =
  {
    val typeGuessModel: Model = db.models.get(s"MIMIR_TI_ATTR_${schema}_RK:ATTR_TYPE")
    RepairKeyLens.assemble(
      query,
      Seq("IDX"),
      Seq(("ATTR_TYPE", typeGuessModel)),
      Some("SCORE")
    )
  }
}


@SerialVersionUID(1001L)
class TIRepairModel(
  name: String, 
  context: String, 
  source: Operator, 
  keys: Seq[(String, Type)], 
  target: String,
  targetType: Type,
  scoreCol: Option[String],
  tiCols:IndexedSeq[String]
) extends RepairKeyModel(name, context, source, keys, target, targetType, scoreCol)
{
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
  
  override def bestGuess(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): PrimitiveValue =
    getFeedback(idx, args) match {
      case Some(choice) => choice
      case None => getTopPick(idx, args, hints)
    }
  
  private final def getTopPick(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]) : TypePrimitive = {
    getTopPick(getDomain(idx, args, hints))
  }
  
  private final def getTopPick(domain:Seq[(PrimitiveValue, Double)]) : TypePrimitive = {
    val sortedPossibilities = domain.sortBy(-_._2)
    sortedPossibilities.filter(_._2 ==  sortedPossibilities.head._2) match {
      case Seq(topPick) => TypePrimitive(Type.fromString(topPick._1.asString))
      case topPicks:Seq[(PrimitiveValue, Double)] => TypePrimitive(Type.fromString(topPicks.sortBy(rankFn).head._1.asString))
    }
  }
  
  private final def rankFn(x:(PrimitiveValue, Double)) =
    (x._2, -1*priority(Type.fromString(x._1.asString)) )
    
  override def reason(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): String =
  {
    getFeedback(idx, args) match {
      case None => {
        val possibilities = getDomain(idx, args, hints)
        s"In $context, there were ${possibilities.length} options for $target on the column identified by <index:$idx name:${tiCols(idx)}>, and I picked <${getTopPick(possibilities)}> because it had the highest score and priority"
      }
      case Some(choice) => 
        s"In $context, ${getReasonWho(idx,args)} told me to use ${choice.toString} for $target on the column identified by <index:$idx name:${tiCols(idx)}>"
    }
  }
}