package mimir.lenses

import mimir.Database
import mimir.algebra.{Operator, Expression}
import mimir.ctables.Model

/**
 * Created by vinayak on 7/20/15.
 */
class SchemaMatchingLens(name: String, args: List[Expression], source: Operator)
  extends Lens(name, args, source) {

  def lensType = "SCHEMA_MATCHING"

  /**
   * `view` emits an Operator that defines the Virtual C-Table for the lens
   */
  override def view: Operator = ???

  /**
   * Return the lens' model.  This model must define a mapping for all VGTerms created
   * by `view`
   */
  override def model: Model = ???

  /**
   * Initialize the lens' model by building it from scratch.  Typically this involves
   * using `db` to evaluate `source`
   */
  override def build(db: Database): Unit = ???
}
