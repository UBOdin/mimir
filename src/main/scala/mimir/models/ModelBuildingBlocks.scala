package mimir.models
import mimir.Database
import mimir.algebra._

trait NoArgModel
{
  def argTypes(x: Int): Seq[Type] = List()
  def hintTypes(idx: Int) = Seq()
}

  /**
   * This trait is a flag for a concurrent operation that allows progressive
   * updates to a given model as handled by the model manager. In practice,
   * models are updated in the background
   */
trait ProgressiveUpdate
{
  def progressiveTrain(db: Database, query:Operator): Unit
  def getQuery(): Operator
  def isCompleted(): Boolean
  def getNextSample(): Int
}