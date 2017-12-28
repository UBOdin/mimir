package mimir.adaptive

import com.typesafe.scalalogging.slf4j.LazyLogging
import mimir.Database
import mimir.algebra._
import mimir.lenses._
import mimir.models._
import mimir.util.SqlUtils
import scala.util.Random

object SchemaMatching
  extends Multilens
    with LazyLogging
{


  def initSchema(db: Database, config: MultilensConfig): TraversableOnce[Model] =
  {
    logger.debug(s"Creating SchemaMatching: $config")
    val viewName = config.schema
    
    val targetSchema =
      config.args.
        map(field => {
          val split = db.interpreter.evalString(field).split(" +")
          val varName  = split(0).toUpperCase
          val typeName = split(1)
          (
            varName.toString.toUpperCase -> 
              Type.fromString(typeName.toString)
          )
        }).
        toList

    val modelsByType = 
      ModelRegistry.schemamatchers.toSeq.map {
        case (
          modelCategory:String, 
          constructor:ModelRegistry.SchemaMatchConstructor
        ) => {
          val modelsByColAndType =
            constructor(
              db, 
              s"$viewName:$modelCategory", 
              Left(config.query), 
              Right(targetSchema)
            ).toSeq.map {
              case (col, (model, idx)) => (col, (model, idx, Seq[Expression]()))
            }

          (modelCategory, modelsByColAndType)
        }
      }

    val (
      candidateModels: Map[String,Seq[(String,Int,Seq[Expression],String)]],
      modelEntities: Seq[Model]
    ) = 
      LensUtils.extractModelsByColumn(modelsByType)

    // Sanity check...
    targetSchema.map(_._1).foreach( target => {
      if(!candidateModels.contains(target)){ 
        throw new Exception("No valid schema matching model for column '"+target+"' in lens '"+viewName+"'");
      }
    })


    val proxyModel = SchemaMatchingProxyModel(s"$viewName:PROXY:SCHEMA_MATCHING", targetSchema, targetSchema.map(sche => modelEntities(modelEntities.indexWhere(_.name.endsWith(s":${sche._1}")) ).name ))   
    modelEntities :+ proxyModel
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
    HardTable(
      Seq(
        ("TABLE_NAME" , TString()), 
        ("ATTR_TYPE", TType()),
        ("IS_KEY", TBool()),
        ("COLUMN_ID", TInt())
      ),
      (0 until config.args.size).map { col =>
        Seq(
          StringPrimitive("DATA"),
          TypePrimitive(TString()),
          BoolPrimitive(false),
          IntPrimitive(col)
        )
      }
    ).addColumn(
      "ATTR_NAME" -> VGTerm(s"${config.schema}:PROXY:SCHEMA_MATCHING" , 1, Seq(Var("COLUMN_ID")), Seq())
    ).removeColumn("COLUMN_ID")
  }
        
  def viewFor(db: Database, config: MultilensConfig, table: String): Option[Operator] =
  {
    if(table.equals("DATA")){
      val model = db.models.get(s"${config.schema}:PROXY:SCHEMA_MATCHING").asInstanceOf[SchemaMatchingProxyModel]
      Some(Project(
        model.targetSchema.zipWithIndex.map { case ((colName, colType), colIdx) => 
          ProjectArg(colName,  Var(model.bestGuess(0, Seq(IntPrimitive(colIdx)), Seq()).asString) )
        }, config.query
      ))  
    } else { None }
  } 
}

case class SchemaMatchingProxyModel(override val name: String, val targetSchema: Seq[(String, Type)], models:Seq[String]) 
  extends Model(name) 
  with Serializable 
  with NeedsDatabase
{
  def argTypes(idx: Int) = List(TInt())
  def hintTypes(idx: Int) = Seq()
  def varType(idx: Int, args: Seq[Type]) = TString()
  def bestGuess(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]) = idx match {
    case 0 => db.models.get(models(args(0).asInt)).bestGuess(idx, args.tail, hints)
    case 1 => StringPrimitive(targetSchema(args(0).asInt)._1)
  }
  def sample(idx: Int, randomness: Random, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]) = 
    db.models.get(models(args(0).asInt)).sample(idx, randomness, args.tail, hints)
  def reason(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): String = 
    db.models.get(models(args(0).asInt)).reason(idx, args.tail, hints)
  def feedback(idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue): Unit = 
    db.models.get(models(args(0).asInt)).feedback(idx, args.tail, v)
  def isAcknowledged (idx: Int, args: Seq[PrimitiveValue]): Boolean = 
    db.models.get(models(args(0).asInt)).isAcknowledged(idx, args.tail)
  def confidence(idx: Int, args: Seq[PrimitiveValue], hints: Seq[PrimitiveValue]): Double = 
    db.models.get(models(args(0).asInt)).confidence(idx, args.tail, hints)
}

