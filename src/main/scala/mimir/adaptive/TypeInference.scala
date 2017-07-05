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

  val TYPED_TABLE_NAME = "TYPED"

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

    var totalVotes: scala.collection.mutable.ArraySeq[Double] = 
      { val v = new scala.collection.mutable.ArraySeq[Double](modelColumns.length)
        for(col <- (0 until modelColumns.size)){ v.update(col, 0.0) }
        v
      }

    var votes: IndexedSeq[scala.collection.mutable.Map[Type, Double]] = 
      modelColumns.map(_ => {
        val temp = scala.collection.mutable.Map[Type, Double]()
        (Type.tests ++ TypeRegistry.registeredTypes).map((tup) => {
          temp.put(Type.fromString(tup._1.toString),0.0)
        })
        temp
      })

    // Scan through each record to find each column's type.
    db.query(
      Project(
        modelColumns.map( c => ProjectArg(c, Var(c)) ),
        config.query
      )
    ) { results => 
      for(row <- results){
        for((v,idx) <- row.tuple.zipWithIndex){
          v match {
            case null            => ()
            case NullPrimitive() => ()
            case _               => {
              totalVotes(idx) += 1.0
              val candidates = detectType(v.asString)
              logger.debug(s"Guesses for '$v': $candidates")
              val votesForCurrentIdx = votes(idx)
              for(t <- candidates){
                votesForCurrentIdx(t) = votesForCurrentIdx.getOrElse(t, 0.0) + 1.0
              }
            }
          }

        }
      }
    }

    logger.debug("Creating Backend Table for Type Inference")
    val schTable = s"MIMIR_TI_SCH_${config.schema}"

    // Create backend table that contains all the
    db.backend.update(s"""
        CREATE TABLE $schTable (ATTR_NAME TEXT, ATTR_TYPE TEXT, SCORE REAL);
      """)

    logger.debug("Filling Type Inference backend table")

    for( ((votesByTypeForCol, col), idx) <- ((votes zip modelColumns).zipWithIndex)){
      // update the map of each column that tracks the type counts
      val totalVotesForCol = totalVotes(idx)

      for( (typ, score) <- votesByTypeForCol ){
        // loop over types

        val normalizedScore = 
          if(totalVotesForCol > 0.0){ score.toDouble / totalVotesForCol }
          else { 0.0 }
          assert(score <= 1.0)

        db.backend.update(s"""
          INSERT INTO $schTable(ATTR_NAME, ATTR_TYPE, SCORE) VALUES (?, ?, ?)
        """, Seq(
          StringPrimitive(col),
          StringPrimitive(typ.toString),
          FloatPrimitive(normalizedScore)
        )) // update the table for the RK lens
      }
      db.backend.update(s"""
        INSERT INTO $schTable(ATTR_NAME, ATTR_TYPE, SCORE) VALUES (?, 'string', ?)
      """, Seq(
        StringPrimitive(col),
        FloatPrimitive(stringDefaultScore)
      )) // update the table for the RK lens
    }

    // Create the model used by Type Inference: (Name, Context, Query Operator, Collapsed Columns, Distinct Column, Type, ScoreBy)
    // keys are the distinct values

    val repairKeyName = schTable + "_RK"
    val repairKeyOp = db.table(schTable)
    val repairKeyArgs:Seq[Expression] = Seq(Var("ATTR_NAME"),Function("SCORE_BY",Seq(Var("SCORE")))) // args for the RK lens
    val (_, repairKeyModels) = RepairKeyLens.create(db,repairKeyName,repairKeyOp,repairKeyArgs)

    return repairKeyModels
  }

  def tableCatalogFor(db: Database, config: MultilensConfig): Operator =
  {
    SingletonTable(
      Seq( ("TABLE_NAME", TString()) ),
      Seq( StringPrimitive(TYPED_TABLE_NAME) )
    )
  }

  def attrCatalogFor(db: Database, config: MultilensConfig): Operator =
  {
    inferredTypesView(db, config)
      .addColumn( 
        "TABLE_NAME" -> StringPrimitive(TYPED_TABLE_NAME),
        "IS_KEY" -> BoolPrimitive(false)
      )
  }

  def viewFor(db: Database, config: MultilensConfig, table: String): Option[Operator] =
  {
    table.toUpperCase match {
      case TYPED_TABLE_NAME => 
        db.query(
          inferredTypesView(db, config)
            .filter(Var("TABLE_NAME").eq(StringPrimitive(TYPED_TABLE_NAME)))
            .project("ATTR_NAME", "ATTR_TYPE")
        ) { results =>

          val remappedColumns =
            results.map { row =>
              val colName = row(0).asString
              val colType = row(1).asString

              ProjectArg(
                colName,
                Function("CAST", Seq(Var(colName), TypePrimitive(Type.fromString(colType))))
              )
            }.toIndexedSeq

          Some(Project(remappedColumns, config.query))
        }

      case _ => None

    }
  }

  final def inferredTypesView(db: Database, config: MultilensConfig): Operator =
  {
    val typeGuessModel: Model = db.models.get(s"MIMIR_TI_SCH_${config.schema}_RK:ATTR_TYPE")

    RepairKeyLens.assemble(
      config.query,
      Seq("ATTR_NAME"),
      Seq(("ATTR_TYPE", typeGuessModel)),
      Some("SCORE")
    )
  }

}