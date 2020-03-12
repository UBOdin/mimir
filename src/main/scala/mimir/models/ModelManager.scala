package mimir.models;

import java.sql.SQLException
import com.typesafe.scalalogging.LazyLogging
import mimir.Database
import mimir.algebra._
import mimir.util._
import mimir.metadata._
/**
 * The ModelManager handles model persistence.  
 *
 * The main function of the ModelManager is to provide a persistent
 * (Name -> Model) mapping.  Associations are created through
 * `persist`, removed with `drop`, and accessed with
 * `get`.  The name of the model is given by Model's name field
 *
 * The secondary function is garbage collection.  The manager also 
 * tracks a second set of 'owner' entities.  Owners can be used to
 * cascade deletes on the owner entity to the models, and allows
 * for reference counting with multiple owners.
 *
 * See below in this file for some traits used to decode things.
 */
class ModelManager(db:Database) 
  extends LazyLogging
{
  
  val decoders = Map[String,((Database, Array[Byte]) => Model)](
    "JAVA"          -> decodeSerializable _
  )
  val cache = scala.collection.mutable.Map[ID,Model]()
  var modelTable: MetadataMap = null
  var ownerTable: MetadataManyMany = null 

  /**
   * Prepare the backend database for use with the ModelManager
   */
  def init(): Unit =
  {
    modelTable = db.metadata.registerMap(ID("MIMIR_MODELS"), Seq(InitMap(Seq(
      ID("ENCODED") -> TString(),
      ID("DECODER") -> TString()
    ))))
    ownerTable = db.metadata.registerManyMany(ID("MIMIR_MODEL_OWNERS"))
  }

  /**
   * Declare (and cache) a new Name -> Model association
   */
  def persist(model: Model): Unit =
  {
    val (serialized,decoder) = model.serialize

    modelTable.put(model.name, Seq(
      StringPrimitive(SerializationUtils.b64encode(serialized)),
      StringPrimitive(decoder.toUpperCase)
    ))
    cache.put(model.name, model)
  }

  /**
   * Remove an existing Name -> Model association if it exists
   */
  def drop(name: ID): Unit =
  {
    modelTable.rm(name)
    cache.remove(name)
  }

  /**
   * Retrieve a model by its name
   */
  def get(name: ID): Model =
  {
    getOption(name) match {
      case Some(model) => return model
      case None => throw new RAException(s"Invalid Model: $name")
    }
  }

  /**
   * Provide model feedback
   */
  def feedback(name: ID, idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue)
  {
    val model = get(name)
    feedback(model, idx, args, v)
  }

  /**
   * Provide model feedback
   */
  def feedback(model: Model, idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue)
  {
    model.feedback(idx, args, v)
    persist(model)
  }

  /**
   * Retreive a model by its name when it may not be present
   */
  def getOption(name: ID): Option[Model] =
  {
    if(!cache.contains(name)){ prefetch(name) }
    return cache.get(name)
  }
  
  def list() : Seq[ID] =
    modelTable.keys
  
  def getAllModels() : Seq[Model] =
    list.map { get(_) }

  /**
   * Declare (and cache) a new Name -> Model association, and 
   * assign the model to an owner entity.
   */
  def persist(model:Model, owner:ID): Unit =
  {
    persist(model);
    associate(model.name, owner);
  }

  /**
   * Assign a model to an owner entity
   */
  def associate(model:ID, owner:ID): Unit =
    ownerTable.add(model, owner)

  /**
   * Disassociate a model from an owner entity and cascade the delete
   * to the model if this is the last owner
   */
  def disassociate(model:ID, owner:ID): Unit =
  {
    ownerTable.rm(model, owner)
    garbageCollectIfNeeded(model)
  }

  /**
   * Drop an owner cascade the delete to any models owned by the
   * owner entity
   */
  def dropOwner(owner:ID): Unit =
  {
    logger.debug(s"Drop Owner: $owner")
    val models = associatedModels(owner)
    logger.debug("Associated: $models")
    for(model <- models) { disassociate(model, owner) }
  }

  /**
   * A list of all models presently associated with a given owner.
   */
  def associatedModels(owner: ID): Seq[ID] =
  {
    ownerTable.getByRHS(owner)
  }
  
  /**
   * A get owner presently associated with a given model.
   */
  def modelOwner(model: ID): Option[ID] =
  {
    ownerTable.getByLHS(model)
      .headOption
  }

  /**
   * Prefetch a given model
   */
  def prefetch(model: ID): Unit =
  {
    modelTable.get(model) match {
      case Some( (_, encoded) ) => {
        val decoderImpl:(Array[Byte] => Model) = 
          decoders.get(encoded(1).asString) match {
            case None => throw new RAException("Unknown Model Decoder '"+encoded(1).asString+"'")
            case Some(impl) => impl(db, _)
          }

        val model = decoderImpl( SerializationUtils.b64decode(encoded(0).asString) )
        cache.put(model.name, model)
      }
      case None => {}
    }
  }

  /**
   * Prefetch models for a given owner
   */
  def prefetchForOwner(owner: ID): Unit =
  {
    for(model <- ownerTable.getByRHS(owner)){ prefetch(model) }
  }

  private def garbageCollectIfNeeded(model: ID): Unit =
  {
    if(ownerTable.getByLHS(model).isEmpty) { drop(model) }
  }

  private def decodeSerializable(db: Database, data: Array[Byte]): Model =
  {
    val ret = SerializationUtils.deserialize[Model](data)
    if(ret.isInstanceOf[NeedsReconnectToDatabase]){
      ret.asInstanceOf[NeedsReconnectToDatabase].reconnectToDatabase(db)
    }
    return ret
  }
}

trait NeedsReconnectToDatabase {
  def reconnectToDatabase(db: Database)
}
trait NeedsDatabase extends NeedsReconnectToDatabase 
{
  @transient var db:Database = null
  def reconnectToDatabase(db: Database) = { this.db = db }
}