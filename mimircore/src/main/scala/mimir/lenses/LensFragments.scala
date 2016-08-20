package mimir.lenses;

import mimir.algebra._;
import mimir.ctables._;

/**
 * Certain operator structures appear in multiple lenses.  For example
 * the schema matching structure is one good example:
 * (CASE WHEN {{A==B}} THEN B WHEN {{A==C}} THEN C ELSE NULL END AS A)
 * The Schedder and SchemaMatching lenses both make use of this 
 * structure, albeit with different models backing it.
 * 
 * Constructors for these structures are abstracted out into this module.
 */

object LensFragments {
  
  /**
   * The Schema Matching Structure:
   *  - Remap a source operator's schema to the provided target schema.
   * SchemaMatcher expects a model with a single boolean-valued variable
   * (identified by idx).  The model should accept two string-valued 
   * arguments, respectively: target and source column names.  
   * In principle, for a given target column, the model should only 
   * return true for at most one source column.  If this constraint is 
   * violated, the matcher will choose arbitrarily.
   *
   * A (mostly optional) pair validator allows some mappings to be 
   * pre-emptively excluded (e.g., on schema mismatches)
   *
   * If all of the potential mappings for a given target column return 
   * false, the matcher will default to the expression in `defaultMatch`.
   */
  def schemaMatch(
    name: String, model: Model, idx: Int,
    extraVGTermArgs:List[PrimitiveValue], 
    source: Operator, 
    target: List[String], 
    validate:((Int,Int) => Boolean), 
    defaultMatch:(Int => Expression)): Operator =
  {
    val sourceSchema = source.schema.map(_._1);
    Project(
      target.zipWithIndex.map({ case (targetName, targetIdx) =>

        val sourceCandidates = 
          sourceSchema.zipWithIndex.
            flatMap({ case (sourceName, sourceIdx) => 
              if(validate(targetIdx,sourceIdx)){
                Some(
                  ( 
                    VGTerm((name, model), idx, extraVGTermArgs ++ List(
                      IntPrimitive(targetIdx),
                      IntPrimitive(sourceIdx)
                    )),
                    Var(sourceName)
                  )
                )
              } else { None }
            })

        ProjectArg(targetName, 
          ExpressionUtils.makeCaseExpression(
            sourceCandidates, 
            defaultMatch(targetIdx)
          )
        )

      }),
      source
    )
  }

}