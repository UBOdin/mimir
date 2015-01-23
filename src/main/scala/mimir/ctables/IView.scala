package mimir.ctables;

import java.sql._;
import mimir.sql.Backend;
import mimir.ctables._;
import mimir.algebra._;
import mimir.util._;


abstract class IViewModule(iview: String, id: Int, params: List[String])
{
  // Wrap the specified source operator as per the module's definition.
  // The result should be the "cleaned" form of the operator, after all
  // PVars have been constructed.
  def wrap(source: Operator): Operator;
  def build(backend: Backend, source: Operator): Unit;
  def load(backend: Backend): Unit;
  def varCount(): Int
  def moduleType: String

  def moduleId = id;
  def moduleParams = params;

  def globalVar(vid: Int) = PVar(iview, id, vid, List[Expression]())
  def rowVar(vid: Int) = PVar(iview, id, vid, List[Expression](Var("ROWID")))
  def groupVar(vid: Int, group: List[Expression]) = PVar(iview, id, vid, group)
  def varName(vid: Int): String = { iview+"_"+id+"_"+vid }
}

class IView(name: String, source: Operator, modules: List[IViewModule]) 
{
  def get(): Operator = 
  {
    var ret = source;
    modules.map( (mod) => ret = mod.wrap(ret) );
    return ret;
  }
  def build(backend: Backend): Unit =
  {
    var curr = source;
    modules.map( (mod) => {
      println("Building features for module " + name + "_" + mod.moduleId);
      mod.build(backend, curr);
      curr = mod.wrap(curr);
    })
  }
  def load(backend: Backend): Unit =
  {
    modules.map( _.load(backend) );
  }
  def getModules() = modules;
} 