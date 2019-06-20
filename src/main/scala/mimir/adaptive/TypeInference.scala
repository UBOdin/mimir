package mimir.adaptive

import com.typesafe.scalalogging.slf4j.LazyLogging
import mimir.Database
import mimir.algebra._
import mimir.lenses._
import mimir.models._
import mimir.util.SqlUtils
import mimir.backend.SparkBackend
import mimir.backend.BackendWithSparkContext

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
      db.typechecker.schemaOf(config.query).flatMap({
        case (col, (TString() | TAny())) => Some(col)
        case _ => None
      }).toIndexedSeq

    
    val attributeTypeModel = 
      new TypeInferenceModel(
        viewName.withPrefix("MIMIR_TI_ATTR_"),
        config.humanReadableName,
        modelColumns,
        stringDefaultScore,
        db.backend.asInstanceOf[BackendWithSparkContext].getSparkContext(),
        Some(db.backend.execute(db.compileBestGuess(config.query.limit(TypeInferenceModel.sampleLimit, 0))))
      )

    val warningModel = 
      new WarningModel(
        ID("MIMIR_TI_WARNING_",viewName),
        Seq(TString(), TString(), TString(), TRowId())
      )

    val columnIndexes = 
      modelColumns.zipWithIndex.toMap

    logger.debug(s"Training $attributeTypeModel.name on ${config.query}")
    //model.train(db.backend.execute(config.query))
    
    Seq(attributeTypeModel, warningModel)
  }

  def tableCatalogFor(db: Database, config: MultilensConfig): Operator =
  {
    HardTable(
      Seq(
        ID("TABLE_NAME") -> TString()
      ),
      Seq(
        Seq(
          StringPrimitive("DATA")
        )
      )
    )
  }
  
  def attrCatalogFor(db: Database, config: MultilensConfig): Operator =
  {
    val model = db.models.get(ID("MIMIR_TI_ATTR_",config.schema)).asInstanceOf[TypeInferenceModel]
    val columnIndexes = model.columns.zipWithIndex.toMap
    lazy val qSchema = db.typechecker.schemaOf(config.query).toMap
    HardTable(
      Seq(
        ID("TABLE_NAME") -> TString(), 
        ID("ATTR_NAME")  -> TString(),
        ID("IS_KEY")     -> TBool(), 
        ID("IDX")        -> TInt(),
        ID("HARD_TYPE")  -> TType()
      ),
      config.query.columnNames.map(col => 
        Seq(
          StringPrimitive("DATA"), 
          StringPrimitive(col.id), 
          BoolPrimitive(false),
          IntPrimitive(columnIndexes.getOrElse(col, -1).toLong),
          if(columnIndexes contains col){ NullPrimitive() } 
            else { TypePrimitive(qSchema(col)) }
        )
      )
    ).addColumns(
      "ATTR_TYPE" -> 
        Var(ID("HARD_TYPE"))
          .isNull
          .thenElse {
            VGTerm(config.schema.withPrefix("MIMIR_TI_ATTR_"), 0, Seq(Var(ID("IDX"))), Seq())
          } {
            Var(ID("HARD_TYPE"))
          }
    ).removeColumns("IDX", "HARD_TYPE")
  }
        
  def viewFor(db: Database, config: MultilensConfig, table: ID): Option[Operator] =
  {
    if(table.equals(ID("DATA"))){
      val model = db.models.get(ID("MIMIR_TI_ATTR_",config.schema)).asInstanceOf[TypeInferenceModel]
      val columnIndexes = model.columns.zipWithIndex.toMap
      Some(Project(
        config.query.columnNames.map { colName => {
          ProjectArg(colName, 
            if(columnIndexes contains colName){ 
              val bestGuessType = model.bestGuess(0, Seq(IntPrimitive(columnIndexes(colName))), Seq())
              val castExpression = CastExpression(Var(colName), bestGuessType.asInstanceOf[TypePrimitive].t)
              Conditional(
                IsNullExpression(Var(colName)),
                NullPrimitive(),
                Conditional(
                  IsNullExpression(castExpression),
                  DataWarning(
                    config.schema.withPrefix("MIMIR_TI_WARNING_"),
                    NullPrimitive(),
                    Function("CONCAT", 
                      StringPrimitive("Couldn't Cast [ "),
                      Var(colName),
                      StringPrimitive(" ] to "+bestGuessType+" on row "),
                      RowIdVar()
                    ),
                    Seq(StringPrimitive(colName.id), Var(colName), StringPrimitive(bestGuessType.toString), RowIdVar())
                  ),
                  castExpression
                )
              )
            } else {
              Var(colName)
            }
          )
        }}, config.query
      ))  
    } else { None }
  }

  
}

