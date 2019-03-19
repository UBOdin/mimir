package mimir.adaptive

import sparsity.Name

object MultilensRegistry
{

  val multilenses = Map[Name,Multilens](
    Name("DISCALA_ABADI")     -> DiscalaAbadiNormalizer,
    Name("DETECT_HEADER")     -> CheckHeader,
    Name("TYPE_INFERENCE")    -> TypeInference,
    Name("SCHEMA_MATCHING")   -> SchemaMatching,
    Name("SHAPE_WATCHER")     -> ShapeWatcher,
    Name("DATASOURCE_ERRORS") -> DataSourceErrors
  )

}
