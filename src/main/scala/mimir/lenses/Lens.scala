package mimir.lenses;

import mimir.Database
import mimir.algebra._
import mimir.ctables._;

/**
 * A Lens defines a Virtual PC-Table.  A lens consists of two components:
 * 
 * 1. A VG-Operator (i.e., a project+select including VGTerms)
 *    - Used to define the `C-Table` fragment of the VPC-Table.
 * 2. A Model (i.e., a probability distribution over VGTerms)
 *    - Used to define the `P` fragment of the VPC-Table
 * 
 */

abstract class Lens(val name: String, val args: List[Expression], val source: Operator)
{
  /** 
   * `view` emits an Operator that defines the Virtual C-Table for the lens
   */
  def view: Operator;

  /**
   * Return the lens' model.  This model must define a mapping for all VGTerms created
   * by `view`
   */
  def model: Model;

  /**
   * Initialize the lens' model by building it from scratch.  Typically this involves
   * using `db` to evaluate `source`
   */
  def build(db: Database): Unit;

  /**
   * Serialize the lens' model and store it in `db`.  The `name` parameter may be
   * used as a unique identifier for the model.
   */
  def save(db: Database): Unit = {};

  /**
   * Initialize the lens' model by loading the serialized representation stored by 
   * `save`.
   */
  def load(db: Database): Unit = build(db);

  def stringArg(id: Int): String = Eval.evalString(args(id))
  def longArg(id: Int): Long     = Eval.evalInt(args(id))
  def doubleArg(id: Int): Double = Eval.evalFloat(args(id))
  def lensType: String
  def schema(): List[(String, Type.T)]
}

