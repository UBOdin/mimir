package mimir.adaptive

import mimir.algebra.ID

object MultilensRegistry
{

  val multilenses = Map[ID,Multilens](
    ID("DETECT_HEADER")     -> CheckHeader,
    ID("TYPE_INFERENCE")    -> TypeInference,
    ID("SCHEMA_MATCHING")   -> SchemaMatching,
    ID("SHAPE_WATCHER")     -> ShapeWatcher,
    ID("DATASOURCE_ERRORS") -> DataSourceErrors
  )

}
