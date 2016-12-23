package mimir.models;

import java.sql.SQLException
import mimir.Database
import mimir.algebra._
import mimir.util._

/**
 * The ModelManager handles model persistence.  
 *
 * The main function of the ModelManager is to provide a persistent
 * (Name -> Model) mapping.  Associations are created through
 * `persistModel`, removed with `dropModel`, and accessed with
 * `getModel`.  The name of the model is given by Model's name field
 *
 * The secondary function is garbage collection.  The manager also 
 * tracks a second set of 'owner' entities.  Owners can be used to
 * cascade deletes on the owner entity to the models, and allows
 * for reference counting with multiple owners.
 *
 * See below in this file for some traits used to decode things.
 */
class ModelManager(db:Database) {
  
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
    if(db.backend.getTableSchema(modelTable).isEmpty){
      db.backend.update(s"""
        CREATE TABLE $modelTable(
          name varchar(100), 
          encoded text,
          decoder varchar(30),
          PRIMARY KEY (name)
        )
      """)
    }
    if(db.backend.getTableSchema(ownerTable).isEmpty){
      db.backend.update(s"""
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
  def persistModel(model:Model): Unit =
  {
    val (serialized,decoder) = model.serialize

    db.backend.update(s"""
      INSERT INTO $modelTable(name, encoded, decoder)
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
  def dropModel(name:String): Unit =
  {
    db.backend.update(s"""
      DELETE FROM $modelTable WHERE name = ?
    """,List(StringPrimitive(name)))
    cache.remove(name)
  }

  /**
   * Retrieve a model by its name
   */
  def getModel(name:String): Model =
  {
    if(!cache.contains(name)){ prefetch(name) }
    return cache(name)
  }

  /**
   * Declare (and cache) a new Name -> Model association, and 
   * assign the model to an owner entity.
   */
  def persistModel(model:Model, owner:String): Unit =
  {
    persistModel(model);
    associateOwner(model.name, owner);
  }

  /**
   * Assign a model to an owner entity
   */
  def associateOwner(model:String, owner:String): Unit =
  {
    db.backend.update(s"""
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
  def disassociateOwner(model:String, owner:String): Unit =
  {
    db.backend.update(s"""
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
    val models = 
      db.backend.resultRows(s"""
        SELECT model FROM $ownerTable WHERE owner = ?
      """).flatten.toList
    db.backend.update(s"""
      DELETE FROM $ownerTable WHERE owner = ?
    """, List(
      StringPrimitive(owner)
    ))
    models.map(_.asString).foreach(garbageCollectIfNeeded(_))
  }

  /**
   * Prefetch a given model
   */
  def prefetch(model: String): Unit =
  {
    prefetchWithRows(
      db.backend.resultRows(s"""
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
      db.backend.resultRows(s"""
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
      db.backend.resultRows(s"""
        SELECT FROM $ownerTable WHERE model = ?
      """, List(
        StringPrimitive(model)
      ))
    if(otherOwners.isEmpty){ dropModel(model) }
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