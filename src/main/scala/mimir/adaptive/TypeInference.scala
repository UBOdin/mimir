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
      db.bestGuessSchema(config.query).flatMap({
        case (col, (TString() | TAny())) => Some(col)
        case _ => None
      }).toIndexedSeq

    val model = 
      new TypeInferenceModel(
        s"MIMIR_TI_ATTR_${viewName}",
        modelColumns,
        stringDefaultScore
      )

    val columnIndexes = 
      modelColumns.zipWithIndex.toMap

    logger.debug(s"Training $model.name on ${config.query}")
    model.train(db, config.query)
    
    Seq(model)
  }

  def tableCatalogFor(db: Database, config: MultilensConfig): Operator =
  {
    HardTable(Seq(("TABLE_NAME",TString()),("SCHEMA_NAME",TString())),Seq(Seq(StringPrimitive(config.schema),StringPrimitive("MIMIR"))))
  }
  
  def attrCatalogFor(db: Database, config: MultilensConfig): Operator =
  {
   val model = db.models.get(s"MIMIR_TI_ATTR_${config.schema}").asInstanceOf[TypeInferenceModel]
   val table = HardTable(
      Seq(("TABLE_NAME" , TString()), ("ATTR_NAME" , TString()),("ATTR_TYPE", TType()),("IS_KEY", TBool()), ("SCHEMA_NAME", TString()), ("IDX", TInt())),
      model.columns.zipWithIndex.map(col => 
        Seq(StringPrimitive(config.schema), StringPrimitive(col._1), model.bestGuess(col._2, Seq(), Seq()) ,BoolPrimitive(false),StringPrimitive("MIMIR"),IntPrimitive(col._2))
     )) 
   val oper = Project( table.schema.map {
     case ("ATTR_TYPE", _) => ProjectArg("ATTR_TYPE", VGTerm(model.name, 0, Seq(Var("IDX")), Seq(Var("ATTR_TYPE"))))
     case (col, _) => ProjectArg(col, Var(col))
   }, table)
   oper
  }
        
  def viewFor(db: Database, config: MultilensConfig, table: String): Option[Operator] =
  {
    val model = db.models.get(s"MIMIR_TI_ATTR_${config.schema}").asInstanceOf[TypeInferenceModel]
    Some(Project(
        db.query(
          attrCatalogFor(db, config)
        ) { results => {
            val cols = model.columns
            results.toSeq.map { row =>
              val colIdx = row(5).asInt
              val colName = cols(colIdx)
              val colType = row(2).asString
              val schemaTupStr = s"($colName, $colType)"
              ProjectArg(
                colName,
                Function("CAST", Seq(Var(colName), TypePrimitive(Type.fromString(colType))))
              )
            }.toIndexedSeq}
    }, config.query))  
  }

  
}

