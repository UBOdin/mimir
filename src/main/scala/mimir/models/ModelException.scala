package mimir.models

import mimir.algebra._

case class ModelException(error:String) extends RAException(error)