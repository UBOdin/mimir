package mimir.ctables;

import java.sql._;
import mimir.sql.Backend;
import mimir.ctables._;
import mimir.algebra._;
import mimir.util._;
import mimir.Database;

abstract case class Lens(modelName: String, params: List[String], source: Operator)
{
  // Wrap the specified source operator as per the module's definition.
  // The result should be the "cleaned" form of the operator, after all
  // PVars have been constructed.
  def view: Operator;
  def build(db: Database): Unit;
  def load(db: Database): Unit;
  def save(db: Database): Unit;
  def model: Model;

  // def varCount: Int
  def lensType: String
  def serializeParams: String = params.mkString(",")

  // def analyze(db: Database, v: PVar): CTAnalysis
    
  // def globalVar(vid: Int) = PVar(iview, id, vid, List[Expression]())
  def rowVar(vid: Int)   = VGTerm((modelName,model), vid, List[Expression](Var("ROWID")))
  def rowVar(vid: Int, args: List[Expression]) 
                         = VGTerm((modelName,model), vid, Var("ROWID") :: args)
  // def groupVar(vid: Int, group: List[Expression]) = PVar(iview, id, vid, group)
  // def varName(vid: Int): String = { iview+"_"+id+"_"+vid }
}