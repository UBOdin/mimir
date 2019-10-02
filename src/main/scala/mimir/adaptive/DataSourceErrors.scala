package mimir.adaptive

import com.typesafe.scalalogging.LazyLogging
import mimir.Database
import mimir.algebra._
import mimir.lenses._
import mimir.models._
import mimir.util.SqlUtils

object DataSourceErrors
  extends Multilens
    with LazyLogging
{

  val mimirDataSourceErrorColumn    = ID("MIMIR_DATASOURCE_ROW_HAS_ERROR")
  val mimirDataSourceErrorRowColumn = ID("MIMIR_DATASOURCE_ERROR_ROW_RAW")
  
  def initSchema(db: Database, config: MultilensConfig): TraversableOnce[Model] =
  {
    logger.debug(s"Creating TypeInference: $config")
    val viewName = config.schema
    
    val srcSchema = db.typechecker.schemaOf(config.query)
    
    val srcCols = srcSchema.map(_._1)
    
    if(!(srcCols.contains(mimirDataSourceErrorColumn) && srcCols.contains(mimirDataSourceErrorRowColumn))){
      throw new Exception(s"The input query for DataSourceErrors adaptive schema must contain columns: $mimirDataSourceErrorColumn and $mimirDataSourceErrorRowColumn")
    }

    val warningModel = 
      new WarningModel(
        ID("MIMIR_DSE_WARNING_",viewName),
        Seq(TRowId())
      )

    val columnIndexes = 
      srcSchema.zipWithIndex.toMap

    Seq(warningModel)
  }

  def tableCatalogFor(db: Database, config: MultilensConfig): Operator =
  {
    HardTable(
      Seq(
        (ID("TABLE_NAME"),TString())
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
    lazy val qSchema = db.typechecker.schemaOf(config.query).toMap
    HardTable(
      Seq(
        (ID("TABLE_NAME") -> TString()), 
        (ID("ATTR_NAME")  -> TString()),
        (ID("IS_KEY")     -> TBool()), 
        (ID("ATTR_TYPE")  -> TType())
      ),
      config.query.columnNames.map(col => 
        Seq(
          StringPrimitive("DATA"), 
          StringPrimitive(col.id), 
          BoolPrimitive(false),
          TypePrimitive(qSchema(col))
        )
      )
    )
  }
        
  def viewFor(db: Database, config: MultilensConfig, table: ID): Option[Operator] =
  {
    if(table.equals(ID("DATA"))){
      // query -> 
      //   Select( if(hasError){ Warning(True) } else { True }, query )
      Some(
        config.query
              .filter { 
                Var(mimirDataSourceErrorColumn).thenElse {
                  DataWarning(
                    config.schema.withPrefix("MIMIR_DSE_WARNING_"),
                    BoolPrimitive(true),
                    Function("CONCAT", 
                      StringPrimitive("There is an error(s) in the data source on row "),
                      RowIdVar(),
                      StringPrimitive(s" of ${config.humanReadableName}. The raw value of the row in the data source is [ "),
                      Var(mimirDataSourceErrorRowColumn),
                      StringPrimitive(" ]")
                    ),
                    Seq(RowIdVar())
                  )
                } { BoolPrimitive(true) }
              }
              .removeColumnsByID(
                mimirDataSourceErrorColumn,
                mimirDataSourceErrorRowColumn
              )
      )
    } else { None }
  }

  
}

