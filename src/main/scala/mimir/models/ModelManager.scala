package mimir.models;

import java.sql.SQLException
import mimir.Database
import mimir.algebra._

/**
 * The ModelManager handles model persistence.  
 *
 * The main function of the ModelManager is to provide a persistent
 * (Name -> Model) mapping.  Associations are created through
 * `registerModel`, removed with `dropModel`, and accessed with
 * `getModel`
 */
class ModelManager(db:Database) {
  
  val decoders = Map[String,((Database, Array[Byte]) => Model)](
    "WEKA"          -> WekaModel.decode _,
    "JAVA"          -> decodeSerializable _
  )
  val cache = scala.collection.mutable.Map[String,Model]()
  val modelTable = "MIMIR_MODELS"
  val base64in = java.util.Base64.getDecoder()
  val base64out = java.util.Base64.getEncoder()

  /**
   * Prepare the backend database for use with the ModelManager
   */
  def init(): Unit =
  {
    db.backend.update(s"""
      CREATE TABLE $modelTable(
        name varchar(100), 
        encoded text,
        decoder varchar(30),
        PRIMARY KEY (name)
      )
    """)
  }

  /**
   * Declare (and cache) a new Name -> Model association
   */
  def registerModel(name:String, model:Model): Unit =
  {
    val (serialized,decoder) = model.serialize

    db.backend.update(s"""
      INSERT INTO $modelTable(name, encoded, decoder)
             VALUES (?, ?, ?)
    """, List(
      StringPrimitive(name),
      StringPrimitive(base64out.encodeToString(serialized)),
      StringPrimitive(decoder.toUpperCase)
    ))
    cache.put(name, model)
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

  def getModel(name:String): Model =
  {
    val resultRows =
      db.backend.resultRows(s"""
        SELECT decoder, encoded FROM $modelTable WHERE name = ?
      """,List(StringPrimitive(name)));

    if(!resultRows.hasNext){
      throw new RAException("Undefined Model '"+name+"'")
    }
    val (decoder:String, encoded:String) = 
      resultRows.next match {
        case List(decoder, encoded) => (decoder.asString, encoded.asString)
        case _ => 
          throw new SQLException("Error on backend: Expecting only 2 fields in result")
      }
    if(resultRows.hasNext){
      throw new SQLException("Error on backend: Multiple rows for the same model name")
    }

    val decoderImpl:(Array[Byte] => Model) = 
      decoders.get(decoder) match {
        case None => throw new RAException("Unknown Model Decoder '"+decoder+"'")
        case Some(impl) => impl(db, _)
      }

    return decoderImpl( base64in.decode(encoded) )
  }

  def decodeSerializable(ignored: Database, data: Array[Byte]): Model =
  {
    val objects = new java.io.ObjectInputStream(
        new java.io.ByteArrayInputStream(data)
      )
    objects.readObject().asInstanceOf[Model]
  }
}