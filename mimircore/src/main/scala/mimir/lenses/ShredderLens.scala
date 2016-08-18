package mimir.lenses

import java.sql.SQLException
import scala.util._

import mimir.Database
import mimir.algebra.Type.T
import mimir.algebra._
import mimir.ctables._
import mimir.optimizer.{InlineVGTerms}


/**
 * This lens is intended for use with the DiScala JSON extractor.  It wraps a (very) wide table for which
 * various projections identify different entities.
 * 
 * The lens generates results for a single target entity, after identifying several secondary entities 
 * might potentially be coupled with it.
 *
 * The VG-RA expression applied by this lens is as follows:
 *
 *             U
 *          /  |  \
 *        /    |   \
 *      PRIM  SM   SM
 *             |    |
 *            INC  INC
 *             |    |
 *            Sec1  Sec2
 *
 * - U is Union
 * - PRIM is the primary projection of `source` (primaryEntity)
 * - SM is a schema-matcher structure targetting the schema of PRIM
 * - INC is of the form SELECT[{{IncludeSecN}}](...).  That is, it controls the inclusion of each secondary
 *   projection/entity.
 * - Sec1,Sec2,...,SecN is a secondary projection of `source`
 * 
 */
class ShredderLens(
  // Name of the lens
  name: String, 
  // Column names of `source` belonging to the primary entity
  val primaryEntity:List[String], 
  // For each secondary entity, a pair of the entity's name + columns of `source` belonging to the 
  // secondary entities.
  val secondaryEntities:List[(String,List[String])], 
  // The origin of all of the data.
  source: Operator
) extends Lens(
    name, 
    List[Expression](StringPrimitive(primaryEntity.mkString(",")))++
      secondaryEntities.map({ case (entityName,attrs) => 
        StringPrimitive(entityName+":"+attrs.mkString(",")) 
      }), 
    source
  ) 
{
  val attributeMappingModel = new ShredderAttributeModel(this)
  val entityParticipationModel = new ShredderEntityModel(this)

  val model = MergedModels(List(attributeMappingModel, entityParticipationModel))
  var db:Database = null

  def schema(): List[(String, Type.T)] = {
    val inSchema = source.schema.toMap;
    primaryEntity.map( col => (col, inSchema(col)) )
  }
  def lensType = "SHREDDER"

  /**
   * `view` emits an Operator that defines the Virtual C-Table for the lens
   */
  override def view: Operator = {
    val primarySource = OperatorUtils.projectDownToColumns(primaryEntity, source)
    val secondarySources = 
      secondaryEntities.zipWithIndex.
        map({ case ((entityName, attrs), entityIdx) =>
          // SchemaMatch based on attributeMappingModel
          LensFragments.schemaMatch(
            name, model, 0,
            // Include entities based on entityParticipationModel
            Select(
              VGTerm((name, model), entityIdx+1, List()),
              // And actually extract the base entity itself.
              OperatorUtils.projectDownToColumns(attrs, source)
            ),
            primaryEntity,
            ((_,_) => true),
            NullPrimitive()
          )
        })
    // And last step... union all of the secondary sources with the primary
    secondarySources.foldLeft(primarySource)(Union(_,_))
  }

  /**
   * Initialize the lens' model by building it from scratch.  Typically this involves
   * using `db` to evaluate `source`
   */
  def build(db: Database): Unit = {
    this.db = db
    entityParticipationModel.train();
    attributeMappingModel.train();
  }
}

class ShredderEntityModel(lens: ShredderLens) extends Model {

  val numVars = lens.secondaryEntities.length
  def train(): Unit = ???
  def bestGuess(idx: Int,args: List[PrimitiveValue]): PrimitiveValue = ???
  def reason(idx: Int,args: List[Expression]): String = ???
  def sample(idx: Int,randomness: scala.util.Random,args: List[PrimitiveValue]): PrimitiveValue = ???
  def varType(idx: Int,argTypes: List[Type.T]): Type.T = ???

}
class ShredderAttributeModel(lens: ShredderLens) extends Model {

  val numVars = 1
  def train(): Unit = ???
  def bestGuess(idx: Int,args: List[PrimitiveValue]): PrimitiveValue = ???
  def reason(idx: Int,args: List[Expression]): String = ???
  def sample(idx: Int,randomness: scala.util.Random,args: List[PrimitiveValue]): PrimitiveValue = ???
  def varType(idx: Int,argTypes: List[Type.T]): Type.T = ???


}
