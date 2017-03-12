package mimir.adaptive

import java.io._
import com.typesafe.scalalogging.slf4j.LazyLogging
import scala.collection.JavaConverters._

import mimir.Database
import mimir.algebra._
import mimir.lenses._
import mimir.models._
import mimir.statistics.FuncDep

object DiscalaAbadiNormalizer
  extends Multilens
  with LazyLogging
{
  def initSchema(db: Database, config: MultilensConfig): TraversableOnce[Model] =
  {
    logger.debug(s"Creating DiscalaAbadiNormalizer: $config")

    logger.debug("Creating FD Graph")
      val fd = new FuncDep()
      fd.buildEntities(db, config.query, config.schema)

    logger.debug("Dumping FD Graph")
      val fdTable = s"MIMIR_DA_FDG_${config.schema}"
      db.backend.update(s"""
        CREATE TABLE $fdTable (FD_KEY int, FD_VALUE int);
      """)
      db.backend.fastUpdateBatch(s"""
        INSERT INTO $fdTable (FD_KEY, FD_VALUE) VALUES (?, ?);
      """, 
        fd.fdGraph.getEdges.asScala.map { edge =>
          Seq(IntPrimitive(edge._1), IntPrimitive(edge._2))
        }
      )

    val (_, models) = KeyRepairLens.create(
      db, fdTable,
      db.getTableOperator(fdTable),
      Seq(Var("FD_KEY"))
    )

    models
  }
  def tableCatalogFor(db: Database, config: MultilensConfig): Operator =
  {
    ???
  }
  def attrCatalogFor(db: Database, config: MultilensConfig): Operator =
  {
    ???
  }
  def viewFor(db: Database, config: MultilensConfig, table: String): Operator = 
  {
    ???
  }

}


// val fdStats = new FuncDep()
// val query = sql.convert(adaptiveSchema.getSelectBody())
// val schema : Seq[(String,Type)] = query.schema

// val viewList : java.util.ArrayList[String] = 
//   fdStats.buildEntities(
//     schema, 
//     backend.execute(adaptiveSchema.getSelectBody.toString()), 
//     adaptiveSchema.getTable.getName
//   )
// viewList.foreach((view) => {
//   println(view)
//   // update(view)
// })      