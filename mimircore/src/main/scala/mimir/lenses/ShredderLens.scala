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
  discala: FuncDep,
  // Column names of `source` belonging to the primary entity
  val primaryEntity:(Int, List[Int]), 
  // For each secondary entity, a pair of the entity's name + columns of `source` belonging to the 
  // secondary entities.
  val secondaryEntities:List[(Int,List[Int])], 
  // The origin of all of the data.
  source: Operator
) extends Lens(
    name, 
    List[Expression](),
    source
  ) 
{
  val inSchema = source.schema
  val attributeMappingModel = new ShredderAttributeModel(this)
  val entityParticipationModel = new ShredderEntityModel(this)

  val model = IndependentVarsModel(List(attributeMappingModel, entityParticipationModel))
  val ATTRIBUTE_MAPPING_VAR = 0
  val ENTITY_PARTICIPATION_VAR = 0
  var db:Database = null

  def queryForEntity(entity:(Int, List[Int])): Operator =
    OperatorUtils.projectDownToColumns(
      schemaOfEntity(entity).map(_._1),
      source
    )

  def schema(): List[(String, Type.T)] = 
    schemaOfEntity(primaryEntity)
  
  def lensType = "SHREDDER"

  def nameForAttributeId(idx:Int) = inSchema(idx)._1
  def typeForAttributeId(idx:Int) = inSchema(idx)._2

  def columnIdsInEntity(entity:(Int, List[Int])): List[Int] = 
    (entity._1 :: entity._2)
  def schemaOfEntity(entity:(Int, List[Int])): List[(String, Type.T)] =
    columnIdsInEntity(entity).map( inSchema(_) )  
  def keyForMatchingEntity(idx:Int) = {
    val primaryIdx = primaryEntity._1
    if(idx < primaryIdx){ StringPrimitive(idx+","+primaryIdx) }
    else                { StringPrimitive(primaryIdx+","+idx) }
  }

  /**
   * `view` emits an Operator that defines the Virtual C-Table for the lens
   */
  override def view: Operator = {
    val primarySource = queryForEntity(primaryEntity)
    val secondarySources = 
      secondaryEntities.zipWithIndex.
        map({ case (secondaryEntity, secondaryEntityPosition) => 
          val baseEntityQuery = queryForEntity(secondaryEntity)

          val includeOnlyIfEntityMatch = 
            Select(
              VGTerm((name, model), 
                ENTITY_PARTICIPATION_VAR, 
                List(IntPrimitive(secondaryEntity._1))
              ),
              baseEntityQuery
            )

          val schemasAligned =
            LensFragments.schemaMatch(
              name, model, ATTRIBUTE_MAPPING_VAR,
              List(IntPrimitive(secondaryEntityPosition)),
              includeOnlyIfEntityMatch,
              schema().map(_._1),
              {
              // Validation Step: Returns true for 'possible' mapping

                // All key attributes match deterministically.  There are no
                // non-deterministic mappings; Instead, the default plugs in
                // a mapping for the key.
                case (0,_) => false // No non-deterministic mappings.  The key column

                // Non-key attributes can match iff they share a type
                case (targetPosition, sourcePosition) =>
                  typeForAttributeId( primaryEntity._2(targetPosition) ) ==
                    typeForAttributeId( secondaryEntity._2(sourcePosition) )
              },
              // Default value
              { 
                case 0 => Var(nameForAttributeId(secondaryEntity._1))
                case _ => NullPrimitive()
              }
            )

          schemasAligned
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
  }

  def secondaryEntityName(idx: Int) = secondaryEntities(idx)._1
}

/**
 * Model for secondary entity participation in a shredded relation.
 * 
 * A shredded entity model defines one boolean-valued variable for each 
 * secondary entity.  A value of true in a possible world means that 
 * the entity participates in the shredded view in that possible world, 
 * while a value of false means that the entity does not.
 */
class ShredderEntityModel(lens: ShredderLens) extends SingleVarModel {

  /**
   * Return true if the secondary entity 'idx' (range from 0-(N-1) inclusive)
   * belongs in the shredded view in the best-guess world.
   */
  def bestGuess(args:List[PrimitiveValue]): PrimitiveValue = 
  {
    val secondaryEntity = lens.secondaryEntities(args(0).asLong.toInt)
    BoolPrimitive(true)
  }
  /**
   * Return true if the secondary entity 'idx' (range from 0-(N-1) inclusive)
   * belongs in the shredded view in the best-guess world.
   */
  def sample(randomness: scala.util.Random,args: List[PrimitiveValue]): PrimitiveValue = {
    val secondaryEntity = lens.secondaryEntities(args(0).asLong.toInt)
    ???
  }
  def reason(args: List[Expression]): String = 
  {
    val secondaryEntity = lens.secondaryEntities(Eval.evalInt(args(0)).toInt)
    "I assumed that the entity: "+
      lens.nameForAttributeId(secondaryEntity._1)+
      " matched with the entity "+
      lens.nameForAttributeId(lens.primaryEntity._1)
  }
  def varType(argTypes: List[Type.T]): Type.T = Type.TBool
}

/**
 * Model for pairwise attribute comparisons in a shredded relation
 *
 * A shredded attribute model defines a single boolean-valued variable
 * with two skolem arguments: (target,source).  Target is a StringPrimitive
 * containing an attribute name in the target (primary) relation.  Source 
 * is the same, but for the source (secondary) relation.  The variable
 * is true if source should be mapped to target in the current possible
 * world.
 */
class ShredderAttributeModel(lens: ShredderLens) extends SingleVarModel {
  def bestGuess(args: List[PrimitiveValue]): PrimitiveValue = 
  {
    /**
     * The Int,List[Int] spec for the entity who's attributes are being matched
     */
    val sourceEntity = lens.secondaryEntities(args(0).asLong.toInt)
    /**
     * The base-data schema position of the entity being matched
     */
    val sourceEntityParent = sourceEntity._1
    /**
     * The "x,y" key for the match pair
     */
    val entityMatchingKey = lens.keyForMatchingEntity(sourceEntityParent)

    // The key attribute is at index 0, so the following attribute values 
    // are offset by 1 when we get them
    /**
     * The base-data schema position of the child attribute being targetted
     */
    val targetAttribute = lens.primaryEntity._2(args(1).asLong.toInt-1)
    /**
     * The base-data schema position of the child attribute being matched to targetAttribute
     */
    val sourceAttribute = sourceEntity._2(args(2).asLong.toInt-1)

    throw new Exception("WILL: IMPLEMENT THIS")
    // XXXXXXXX IMPLEMENT THIS XXXXXXXX
    BoolPrimitive(
      ???  
      // In the context of entityMatchingKey, 
      // is targetAttribute == sourceAttribute in the best guess possible world?
    )
  }
  def reason(args: List[Expression]): String = 
  {
    val sourceEntity = lens.secondaryEntities(Eval.evalInt(args(0)).toInt)
    val targetAttribute = lens.primaryEntity._2(Eval.evalInt(args(1)).toInt-1)
    val sourceAttribute = sourceEntity._2(Eval.evalInt(args(2)).toInt-1)

    "I assumed that the attribute "+lens.name+"."+
      lens.nameForAttributeId(targetAttribute)+
    " could be populated with data from "+
      lens.nameForAttributeId(sourceEntity._1)+"."+
      lens.nameForAttributeId(sourceAttribute)
  }
  def sample(randomness: scala.util.Random,args: List[PrimitiveValue]): PrimitiveValue =
  {
    throw new Exception("UNIMPLEMENTED")    

  }
  def varType(argTypes: List[Type.T]): Type.T = Type.TBool

}