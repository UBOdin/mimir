package mimir.lenses

import play.api.libs.json._
import mimir.Database
import mimir.algebra._


/**
 * An interface for Lens implementations
 */
sealed trait Lens 
{
  /** 
   * Generate (or regenerate) a new configuration for the specified lens instance.
   * 
   * @param db           The global database instance
   * @param name         The identity of the specified lens
   * @param query        The query for the input to this lens
   * @param config       The user-provided configuration for this lens
   * @return             A valid configuration for this lens with unset fields set to defaults
   */ 
  def train(
    db: Database, 
    name: ID, 
    query: Operator,
    config: JsValue 
  ): JsValue
}

trait LensNeedsCleanup 
{
  /**
   * Cleanup the specified lens before it is dropped
   * 
   * @param db           The global database instance
   * @param name         The identity of the specified lens
   * @param config       The user-provided configuration for this lens
   * @return             True if the specified 
   */
  def drop(
    db: Database, 
    name: ID,
    config: JsValue 
  ): Unit  
}

trait MonoLens extends Lens
{
  /**
   * Generate the operator implementation of this lens
   * 
   * @param db           The global database instance
   * @param name         The identity of the specified lens
   * @param query        The query for the input to this lens
   * @param config       The user-provided configuration for this lens
   * @param readableName The human-readable name for this lens
   */
  def view(
    db: Database, 
    name: ID, 
    query: Operator, 
    config: JsValue, 
    friendlyName: String
  ): Operator

  /**
   * Generate the schema for this lens
   * 
   * @param db           The global database instance
   * @param name         The identity of the specified lens
   * @param query        The query for the input to this lens
   * @param config       The user-provided configuration for this lens
   * @param readableName The human-readable name for this lens
   */
  def schema(
    db: Database, 
    name: ID, 
    query: Operator, 
    config: JsValue, 
    friendlyName: String
  ): Seq[(ID, Type)] = db.typechecker.schemaOf(view(db, name, query, config, friendlyName))
}