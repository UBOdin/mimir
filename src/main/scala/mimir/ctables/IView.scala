package mimir.ctables;

import java.sql._;
import mimir.sql.Backend;
import mimir.ctables._;
import mimir.algebra._;
import mimir.util._;
import mimir.Database;

case class IView(name: String, source: Operator, lenses: List[Lens]) 
{
  def get(): Operator = 
  {
    if(lenses.length < 1){ return source; }
    else { return lenses(lenses.length-1).view }
  }
  def build(db: Database): Unit =
  {
    lenses.map( _.build(db) )
  }
  def load(db: Database): Unit =
  {
    lenses.foreach( _.load(db) );
  }

  def analyze(db: Database, v: PVar): CTAnalysis =
    lenses(v.lens).analyze(db, v)

} 