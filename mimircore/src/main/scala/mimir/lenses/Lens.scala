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

abstract case class Lens(name: String, args: List[Expression], source: Operator)
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

  def createBackingStore: Unit

  // def globalVar(vid: Int) = PVar(iview, id, vid, List[Expression]())
  def rowVar(vid: Int)   = VGTerm((name,model), vid, List[Expression](Var("ROWID_MIMIR")))
  def rowVar(vid: Int, args: List[Expression]) 
                         = VGTerm((name,model), vid, Var("ROWID_MIMIR") :: args)
  // def groupVar(vid: Int, group: List[Expression]) = PVar(iview, id, vid, group)
  // def varName(vid: Int): String = { iview+"_"+id+"_"+vid }

  def stringArg(id: Int): String = Eval.evalString(args(id))
  def longArg(id: Int): Long     = Eval.evalInt(args(id))
  def doubleArg(id: Int): Double = Eval.evalFloat(args(id))
  def lensType: String
  def schema(): List[(String, Type.T)]
}

