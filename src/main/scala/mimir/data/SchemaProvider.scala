package mimir.data

trait SchemaProvider {
  // Implementations define these...
  def listTables: Iterable[ID]
  def tableSchema(table: ID): Option[Seq[(ID, Type)]]
  def implementation(table: ID): Option[TableImplementation]

  // These come for free but may be overridden
  def listTablesQuery: Operator =
    HardTable(
      Seq(ID("TABLE_NAME"), TString()), 
      listTables.map { _.id }
                .map { StringPrimitive(_) }
                .map { Seq(_) }
                .toSeq
    )
  def listAttributesQuery: Operator = 
    HardTable(
      Seq(
        ID("TABLE_NAME")  -> TString(), 
        ID("ATTR_NAME")   -> TString(),
        ID("ATTR_TYPE")   -> TString(),
        ID("IS_KEY")      -> TBool()
      ),
      listTables.flatMap { table => 
        tableSchema(table).map { case (attr, t) =>
          Seq(
            StringPrimitive(table.id), 
            StringPrimitive(attr.id),
            TypePrimitive(t),
            BoolPrimitive(false)
          )
        }
      }
    )
}
