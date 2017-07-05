package mimir.adaptive

import com.typesafe.scalalogging.slf4j.LazyLogging
import mimir.Database
import mimir.algebra._
import mimir.ctables.VGTerm
import mimir.lenses._
import mimir.models._
import mimir.util.SqlUtils

object TypeInference
  extends Multilens
    with LazyLogging
{

  var totalVotes: scala.collection.mutable.ArraySeq[Double] = null

  var votes: IndexedSeq[scala.collection.mutable.Map[Type, Double]] = null

  def initSchema(db: Database, config: MultilensConfig): TraversableOnce[Model] =
  {

    val modelColumns =
      config.query.schema.map({
        case (col, (TString() | TAny())) => Some(col)
        case _ => None
      }).flatten.toIndexedSeq

    totalVotes =
      { val v = new scala.collection.mutable.ArraySeq[Double](modelColumns.length)
        for(col <- (0 until modelColumns.size)){ v.update(col, 0.0) }
        v
      }
    votes =
      modelColumns.map(_ => {
        val temp = scala.collection.mutable.Map[Type, Double]()
        (Type.tests ++ TypeRegistry.registeredTypes).map((tup) => {
          temp.put(Type.fromString(tup._1.toString),0.0)
        })
        temp
      })

    logger.debug(s"Creating TypeInference: $config")

    logger.debug("Creating Backend Table for Type Inference")
    val schTable = s"MIMIR_TI_SCH_${config.schema}"

    // Create backend table that contains all the
    db.backend.update(s"""
        CREATE TABLE $schTable (COLUMNNAME TEXT, TYPEGUESS TEXT, PROJSCORE REAL, TYPEEXPLICIT TEXT, SCORE REAL);
      """)

    logger.debug("Filling Type Inference backend table")

    var loc = 0
    (votes zip modelColumns).map((x) => { // update the map of each column that tracks the type counts
      val v = x._1
      val col = x._2
      val totalV = totalVotes(loc)
      loc += 1
      v.map((tup) => { // loop over types
        val typ:String = tup._1.toString()
        var score = 0.0
        if(totalV > 0.0){
          score = tup._2 / totalV
          if(score > 1.0){
            throw new Exception("Score is greater than 1.0 in Type Inference, something is wrong")
          }
        }
        db.backend.update(s"INSERT INTO $schTable values('$col', '$typ', $score, null, $score);") // update the table for the RK lens
      })
    })

    // Create the model used by Type Inference: (Name, Context, Query Operator, Collapsed Columns, Distinct Column, Type, ScoreBy)
    // keys are the distinct values

    val RKname = schTable + "_RK"
    val RKq = s"SELECT * FROM $schTable;" // query for the RK lens
    val RKop = SqlUtils.plainSelectStringtoOperator(db,RKq)
    val RKargs:Seq[Expression] = Seq(Var("COLUMNNAME"),Function("SCORE_BY",Seq(Var("SCORE")))) // args for the RK lens
    val tup = KeyRepairLens.create(db,RKname,RKop,RKargs)
    val RKmodel:Seq[Model] = tup._2

/*
    val groupingModel =
      new KeyRepairModel(
        s"MIMIR_TI_CHOSEN_${config.schema}:MIMIR_TI",
        config.schema,
        config.query,
        Seq(("COLUMNNAME",TString())),
        Seq("TYPEGUESS", "PROJSCORE", "TYPEEXPLICIT"),
        TString(),
        Some("SCORE")
      )
    groupingModel.reconnectToDatabase(db)
*/
    return RKmodel
  }


  final def typeInferenceLens(db: Database, config: MultilensConfig): Operator =
  {
    val model1: Model = db.models.get(s"MIMIR_TI_SCH_${config.schema}_RK:TYPEGUESS")
    val model2: Model = db.models.get(s"MIMIR_TI_SCH_${config.schema}_RK:PROJSCORE")
    val model3: Model = db.models.get(s"MIMIR_TI_SCH_${config.schema}_RK:TYPEEXPLICIT")

    KeyRepairLens.assemble(
      config.query,
      Seq("COLUMNNAME"),
      Seq(("TYPEGUESS", model1),("PROJSCORE", model2),("TYPEEXPLICIT", model3)),
      Some("SCORE")
    )
  }


  final def convertNodesToNamesInQuery(
                                        db: Database,
                                        config: MultilensConfig,
                                        nodeCol: String,
                                        labelCol: String,
                                        typeCol: Option[String],
                                        query: Operator
                                      ): Operator =
  {
    Project(
      query.schema.map(_._1).map { col =>
        if(col.equals(nodeCol)){
          ProjectArg(labelCol, Var("ATTR_NAME"))
        } else {
          ProjectArg(col, Var(col))
        }
      } ++ typeCol.map { col =>
        ProjectArg(col, Var("ATTR_TYPE"))
      },
      Select(Comparison(Cmp.Eq, Var(nodeCol), Var("ATTR_NODE")),
        Join(
          db.getTableOperator(s"MIMIR_TI_SCH_${config.schema}"),
          query
        )
      )
    )
  }


  /*
    Used to project the tables used,
  */

  def tableCatalogFor(db: Database, config: MultilensConfig): Operator =
  {
    val ti = typeInferenceLens(db, config)
    logger.trace(s"Table Catalog TI: \n$ti")
    val tableQuery = null
/*      convertNodesToNamesInQuery(db, config, "TABLE_SCH", None,
        OperatorUtils.makeDistinct(
          Project(Seq(ProjectArg("TABLE_SCH", Var("MIMIR_TI_PARENT"))),
            ti
          )
        )
      )
*/
    logger.trace(s"Table Catalog Query: \n$tableQuery")
    return tableQuery
  }

  /*

  */

  def attrCatalogFor(db: Database, config: MultilensConfig): Operator =
  {
    val spanningTree = typeInferenceLens(db, config)
    logger.trace(s"Attr Catalog Spanning Tree: \n$spanningTree")
    val childAttributeQuery =
      OperatorUtils.projectInColumn("IS_KEY", BoolPrimitive(false),
        convertNodesToNamesInQuery(db, config, "MIMIR_TI_SCH", "ATTR_NAME", Some("ATTR_TYPE"),
          convertNodesToNamesInQuery(db, config, "MIMIR_TI_PARENT", "TABLE_NAME", None,
            spanningTree
          )
        )
      )
    val parentAttributeQuery =
      Project(Seq(
        ProjectArg("TABLE_NAME", Var("TABLE_NAME")),
        ProjectArg("ATTR_NAME", Var("TABLE_NAME")),
        ProjectArg("ATTR_TYPE", Var("ATTR_TYPE")),
        ProjectArg("IS_KEY", BoolPrimitive(true))
      ),
        convertNodesToNamesInQuery(db, config, "TABLE_NODE", "TABLE_NAME", Some("ATTR_TYPE"),
          // SQLite does something stupid with FIRST that prevents it from figuring out that
          // -1 is an integer.  Add 1 to force it to realize that it's dealing with a number
          Select(Comparison(Cmp.Gt, Arithmetic(Arith.Add, Var("TABLE_NODE"), IntPrimitive(1)), IntPrimitive(0)),
            OperatorUtils.makeDistinct(
              Project(Seq(ProjectArg("TABLE_NODE", Var("MIMIR_TI_PARENT"))),
                spanningTree
              )
            )
          )
        )
      )
    val jointQuery =
      Union(childAttributeQuery, parentAttributeQuery)
    logger.trace(s"Attr Catalog Query: \n$jointQuery")
    return jointQuery
  }

  /*
    Creates the view for TI, this is similar to the lens
  */

  def viewFor(db: Database, config: MultilensConfig, table: String): Option[Operator] =
  {
    db.query(
      Project(
        Seq(ProjectArg("ATTR_NAME", Var("ATTR_NAME"))),
        Select(
          Comparison(Cmp.Eq, Var("TABLE_NAME"), StringPrimitive(table)),
          attrCatalogFor(db, config)
        )
      )
    ) { results =>
      val attrs:Seq[String] = results.map { row => row(0).asString }.toSeq

      if(attrs.isEmpty){ return None; }

      var baseQuery =
        Project(
          attrs.map( attr => ProjectArg(attr, Var(attr)) ),
          config.query
        )

      if(table == "ROOT"){
        Some(baseQuery)
      } else {
        val repairModels = attrs.
          filter { !_.equals(table) }.
          map { attr =>
            (
              attr,
              db.models.get(s"MIMIR_TI_CHOSEN_${config.schema}:MIMIR_NORM:$table:$attr")
            )
          }
        Some(
          KeyRepairLens.assemble(
            baseQuery,
            Seq(table),
            repairModels,
            None
          )
        )
      }
    }
  }
}