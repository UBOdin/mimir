package mimir.adaptive

import com.typesafe.scalalogging.slf4j.LazyLogging
import mimir.Database
import mimir.algebra._
import mimir.lenses._
import mimir.models._
import mimir.util.SqlUtils
import mimir.sql.SparkBackend
import mimir.sql.BackendWithSparkContext

object DataSourceErrors
  extends Multilens
    with LazyLogging
{

  val mimirDataSourceErrorColumn = "MIMIR_DATASOURCE_ROW_HAS_ERROR"
  val mimirDataSourceErrorRowColumn = "MIMIR_DATASOURCE_ERROR_ROW_RAW"
  
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
        s"MIMIR_DSE_WARNING_${viewName}",
        Seq(TString(), TString(), TString())
      )

    val columnIndexes = 
      srcSchema.zipWithIndex.toMap

    Seq(warningModel)
  }

  def tableCatalogFor(db: Database, config: MultilensConfig): Operator =
  {
    HardTable(
      Seq(
        ("TABLE_NAME",TString())
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
        ("TABLE_NAME" , TString()), 
        ("ATTR_NAME" , TString()),
        ("IS_KEY", TBool()), 
        ("ATTR_TYPE", TType())
      ),
      config.query.columnNames.map(col => 
        Seq(
          StringPrimitive("DATA"), 
          StringPrimitive(col), 
          BoolPrimitive(false),
          TypePrimitive(qSchema(col))
        )
      )
    )
  }
        
  def viewFor(db: Database, config: MultilensConfig, table: String): Option[Operator] =
  {
    val dataCols = config.query.columnNames.filterNot(col => col.equals(mimirDataSourceErrorColumn) || col.equals(mimirDataSourceErrorRowColumn))
    if(table.equals("DATA")){
     Some(Project(
        dataCols.map { colName => {
          ProjectArg(colName, 
            Conditional(
              Var(mimirDataSourceErrorColumn),
              DataWarning(
                s"MIMIR_DSE_WARNING_${config.schema}",
                Var(colName),
                Function("CONCAT", Seq(
                  StringPrimitive("The value [ "),
                  Conditional(IsNullExpression(Var(colName)),StringPrimitive("NULL"),Var(colName)),
                  StringPrimitive(" ] is uncertain because there is an error(s) in the data source on row "),
                  RowIdVar(),
                  StringPrimitive(". The raw value of the row in the data source is [ "),
                  Var(mimirDataSourceErrorRowColumn),
                  StringPrimitive(" ]")
                )),
                Seq(StringPrimitive(colName), Var(colName), Var(mimirDataSourceErrorRowColumn))
              ),
              Var(colName)
            )
          )
        }}, config.query
      ))  
    } else { None }
  }

  
}

