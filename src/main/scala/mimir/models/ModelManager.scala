package mimir.models;

import java.sql.SQLException
import com.typesafe.scalalogging.slf4j.LazyLogging
import mimir.Database
import mimir.algebra._
import mimir.util._

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
  val cache = scala.collection.mutable.Map[String,Model]()
  val modelTable = "MIMIR_MODELS"
  val ownerTable = "MIMIR_MODEL_OWNERS"

  /**
   * Prepare the backend database for use with the ModelManager
   */
  def init(): Unit =
  {
    if(db.metadataBackend.getTableSchema(modelTable).isEmpty){
      db.metadataBackend.update(s"""
        CREATE TABLE $modelTable(
          name varchar(100), 
          encoded text,
          decoder varchar(30),
          PRIMARY KEY (name)
        )
      """)
    }
    if(db.metadataBackend.getTableSchema(ownerTable).isEmpty){
      db.metadataBackend.update(s"""
        CREATE TABLE $ownerTable(
          model varchar(100), 
          owner varchar(100)
        )
      """)
    }
  }

  /**
   * Declare (and cache) a new Name -> Model association
   */
  def persist(model: Model): Unit =
  {
    val (serialized,decoder) = model.serialize

    db.metadataBackend.update(s"""
      INSERT OR REPLACE INTO $modelTable(name, encoded, decoder)
             VALUES (?, ?, ?)
    """, List(
      StringPrimitive(model.name),
      StringPrimitive(SerializationUtils.b64encode(serialized)),
      StringPrimitive(decoder.toUpperCase)
    ))
    cache.put(model.name, model)
  }

  /**
   * Remove an existing Name -> Model association if it exists
   */
  def drop(name: Name): Unit =
  {
    db.metadataBackend.update(s"""
      DELETE FROM $modelTable WHERE name = ?
    """,List(StringPrimitive(name)))
    cache.remove(name)
  }

  /**
   * Retrieve a model by its name
   */
  def get(name: Name): Model =
  {
    getOption(name) match {
      case Some(model) => return model
      case None => throw new RAException(s"Invalid Model: $name")
    }
  }

  /**
   * Provide model feedback
   */
  def feedback(name: Name, idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue) =
    feedback(model, idx, args, v)

  /**
   * Provide model feedback
   */
  def feedback(name: Model, idx: Int, args: Seq[PrimitiveValue], v: PrimitiveValue) =
  {
    val model = get(name)
    model.feedback(idx, args, v)
    persist(model)
  }

  /**
   * Retreive a model by its name when it may not be present
   */
  def getOption(name: Name): Option[Model] =
  {
    if(!cache.contains(name)){ prefetch(name) }
    return cache.get(name)
  }
  
  def getAllModels() : Seq[Model] =
  {
    db.metadataBackend.resultRows(s"""
      SELECT name FROM $modelTable
    """).map( row => get(row(0).asString))
  }

  /**
   * Declare (and cache) a new Name -> Model association, and 
   * assign the model to an owner entity.
   */
  def persist(model:Model, owner:String): Unit =
  {
    persist(model);
    associate(model.name, owner);
  }

  /**
   * Assign a model to an owner entity
   */
  def associate(model:String, owner:String): Unit =
  {
    db.metadataBackend.update(s"""
      INSERT INTO $ownerTable(model, owner) VALUES (?,?)
    """, List(
      StringPrimitive(model),
      StringPrimitive(owner)
    ))
  }

  /**
   * Disassociate a model from an owner entity and cascade the delete
   * to the model if this is the last owner
   */
  def disassociate(model:String, owner:String): Unit =
  {
    db.metadataBackend.update(s"""
      DELETE FROM $ownerTable WHERE model = ? AND owner = ?
    """, List(
      StringPrimitive(model),
      StringPrimitive(owner)
    ))
    garbageCollectIfNeeded(model)
  }

  /**
   * Drop an owner cascade the delete to any models owned by the
   * owner entity
   */
  def dropOwner(owner:String): Unit =
  {
    logger.debug(s"Drop Owner: $owner")
    val models = associatedModels(owner)
    logger.debug("Associated: $models")
    db.metadataBackend.update(s"""
      DELETE FROM $ownerTable WHERE owner = ?
    """, List(
      StringPrimitive(owner)
    ))
    for(model <- models) {
      logger.trace(s"Garbage Collect: $model")
      garbageCollectIfNeeded(model)
    }
  }

  /**
   * A list of all models presently associated with a given owner.
   */
  def associatedModels(owner: String): Seq[String] =
  {
    db.metadataBackend.resultRows(s"""
      SELECT model FROM $ownerTable WHERE owner = ?
    """, List(
      StringPrimitive(owner)
    )).map(_(0).asString)
  }
  
  /**
   * A get owner presently associated with a given model.
   */
  def modelOwner(model: String): Option[String] =
  {
    db.metadataBackend.resultRows(s"""
      SELECT owner FROM $ownerTable WHERE model = ?
    """, List(
      StringPrimitive(model)
    )).map(_(0).asString) match {
      case Seq() => None
      case seq:Seq[String] => Some(seq.head)
    }
  }

  /**
   * Prefetch a given model
   */
  def prefetch(model: String): Unit =
  {
    prefetchWithRows(
      db.metadataBackend.resultRows(s"""
        SELECT decoder, encoded FROM $modelTable WHERE name = ?
      """, List(
        StringPrimitive(model)
      ))
    )
  }

  /**
   * Prefetch models for a given owner
   */
  def prefetchForOwner(owner:String): Unit =
  {
    prefetchWithRows(
      db.metadataBackend.resultRows(s"""
        SELECT m.decoder, m.encoded 
        FROM $modelTable m, $ownerTable o
        WHERE m.name = o.name 
          AND o.owner = ?
      """, List(
        StringPrimitive(owner)
      ))
    )
  }

  private def prefetchWithRows(rows:TraversableOnce[Seq[PrimitiveValue]]): Unit =
  {
    rows.foreach({
      case List(decoder, encoded) => {
        val decoderImpl:(Array[Byte] => Model) = 
          decoders.get(decoder.asString) match {
            case None => throw new RAException("Unknown Model Decoder '"+decoder+"'")
            case Some(impl) => impl(db, _)
          }

        val model = decoderImpl( SerializationUtils.b64decode(encoded.asString) )
        cache.put(model.name, model)
      }

      case _ => 
        throw new SQLException("Error on backend: Expecting only 2 fields in result")
    })
  }

  private def garbageCollectIfNeeded(model: String): Unit =
  {
    val otherOwners = 
      db.metadataBackend.resultRows(s"""
        SELECT * FROM $ownerTable WHERE model = ?
      """, List(
        StringPrimitive(model)
      ))
    if(otherOwners.isEmpty){ drop(model) }
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