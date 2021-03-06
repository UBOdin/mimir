package mimir.exec.spark

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.types.{ StringType, LongType, IntegerType }
import org.apache.spark.sql.catalyst.expressions.{
  Add,
  Subtract,
  MonotonicallyIncreasingID,
  Alias,
  SparkPartitionID,
  WindowExpression,
  WindowSpecDefinition,
  SortOrder,
  Ascending,
  UnspecifiedFrame,
  Literal,
  If,
  IsNull,
  ScalaUDF,
  HashExpression,
  Murmur3Hash
}
import org.apache.spark.sql.catalyst.expressions.aggregate.{
  AggregateExpression,
  Sum,
  Count,
  First,
  Complete
}

import mimir.Database
import mimir.algebra._


/**
 * Representation of a ROWID-annotated plan.
 * 
 * Calling getPlan() returns a Spark LogicalPlan annotated with SQLite-style ROWIDs
 */
case class RowIndexPlan(val lp:LogicalPlan,  val schema:Seq[(ID,Type)], val offset: Long = 1, val indexName: String = "ROWID") 
 {
  
  val unresolvedAttributes = schema.map(fld => UnresolvedAttribute(fld._1.id))
  
  /**
   * Partition-specific identifier assignment. (Spark LogicalPlan) 
   *
   * Most of the complexity in this class stems from the fact that 
   * Spark's MonotonicallyIncreasingID function doesn't operate
   * across workers: Each worker generates an independent sequence.
   *
   * We're guaranteed that a partition won't cross workers, so we
   * start from there.  
   *
   * partOp extends the source data with two columns:
   *   - partition_id : An integer identifier for each row's partition
   *   - inc_id : The intra-worker unique ID.
   */
  val partOp = org.apache.spark.sql.catalyst.plans.logical.Project(
      unresolvedAttributes ++
      Seq(Alias(SparkPartitionID(),"partition_id")(), 
          Alias(MonotonicallyIncreasingID(),"inc_id")(),
          Alias(hashExpression(),"row_hash")()), lp)
  
          
  def hashExpression(): HashExpression[Int] = {
    new Murmur3Hash(unresolvedAttributes)
  }
  /** 
   * id offset for input rows for a given session (Seq of Integers) 
   * 
   * For each partition, determine the difference between the identifier
   * assigned to elements of the partition, and the true ROWID. This 
   * offset value is computed as:
   *   [true id of the first element of the partition]
   *     - [first assigned id of the partition]
   *
   * The true ID is computed by a windowed aggregate over the counts
   * of all partitions with earlier identifiers. The window includes
   * the count of the current partition, so that gets subtracted off.
   * 
   * The first assigned ID is simply obtained by the FIRST aggregate.
   *   [[ Oliver: Might MIN be safer? ]]
   *
   * Calling this function pre-computes and caches the resulting 
   * partition-id -> offset map.  Because the partition-ids are 
   * sequentially assigned, starting from zero, we can represent the 
   * Map more efficiently as a Sequence.
   * 
   * The map might change every session, so the return value of this 
   * function should not be cached between sessions.
   */
  def fop(db:Database) =  
    db.raToSpark.dataset(
      org.apache.spark.sql.catalyst.plans.logical.Project(
        Seq(UnresolvedAttribute("partition_id"), 
            Alias(
              Add(
                Subtract(
                  Subtract(
                    WindowExpression(
                      AggregateExpression(
                        Sum(UnresolvedAttribute("cnt")),
                        Complete,false),
                      WindowSpecDefinition(
                        Seq(), 
                        Seq(SortOrder(UnresolvedAttribute("partition_id"), Ascending)), 
                        UnspecifiedFrame)
                    ), 
                    UnresolvedAttribute("cnt")),
                  UnresolvedAttribute("inc_id")
                ),
                Literal(offset)
              ),"cnt")()
        ), 
        org.apache.spark.sql.catalyst.plans.logical.Sort(
          Seq(SortOrder(UnresolvedAttribute("partition_id"), Ascending)), 
          true,   
          org.apache.spark.sql.catalyst.plans.logical.Aggregate(
            Seq(UnresolvedAttribute("partition_id")),
            Seq(
              UnresolvedAttribute("partition_id"), 
              Alias(AggregateExpression(Count(Seq(Literal(1))),Complete,false),"cnt")(), 
              Alias(AggregateExpression(First(UnresolvedAttribute("inc_id"),Literal(false)),Complete,false),"inc_id")()),
            partOp)
        )
      )).cache().collect().map(row => (row.getInt(0), row.getLong(1))).toMap      
  
  /**
   * Obtain a function that maps partition_id -> id_offset (Int -> Int)
   * 
   * Generate a ROWID offset mapping (see `fop`) for this session and
   * return the mapping as a function.
   */
  def inFunc(db:Database) = {
    val fopForSession = fop(db)

    (partitionId:Int) => {
     fopForSession(partitionId)
    }
  } 
  
  /**
   * Obtain a true ROWID mapping for a logical plan.
   * 
   * Using the pre-computed/pre-cached per-pertition offset map (`partOp`), 
   * correct the per-partition identifiers to a globally consistent
   * ROWID.
   * 
   * The UDF generated by `getUDF` below is used to obtain the offset for
   * the current row's partition, and the value is added to the current row's
   * partition-specific identifier.
   * 
   * The remaining complexity in this function is bookkeeping to keep
   * the magical `partition_offset` and `inc_id` columns out of the final
   * schema.
   */
  def getPlan(db:Database) = {
    org.apache.spark.sql.catalyst.plans.logical.Project(
      unresolvedAttributes :+ UnresolvedAttribute(indexName) :+ UnresolvedAttribute("partition_offset"), 
      org.apache.spark.sql.catalyst.plans.logical.Project(
        unresolvedAttributes :+
        Alias(org.apache.spark.sql.catalyst.expressions.Cast(
            Add(UnresolvedAttribute("partition_offset"), 
                Add(UnresolvedAttribute("inc_id"),UnresolvedAttribute("row_hash")))
                ,StringType),indexName)() :+ UnresolvedAttribute("partition_offset"),
        org.apache.spark.sql.catalyst.plans.logical.Project(
          unresolvedAttributes ++
          Seq(Alias(getUDF(db),"partition_offset")(), UnresolvedAttribute("inc_id"), UnresolvedAttribute("row_hash")), partOp)))    
  }

  /**
   * A Spark Expression that computes the offset for this partition.
   * 
   * This function simply generates a Spark Expression wrapper around
   * `fop` defined above
   */
  private def getUDF(db:Database) = {
    If(
      IsNull(UnresolvedAttribute("partition_id")), 
      Literal(null), 
      ScalaUDF(
        inFunc(db),
        LongType,
        Seq(UnresolvedAttribute("partition_id")),
        Seq(false),
        Seq(IntegerType),
        Some("mimir_row_index"),true, true
      )
    )
  }
}
