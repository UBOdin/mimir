package mimir.models;

import mimir.Database
import mimir.algebra._

/**
 * The Model Registry is a central point for organizing Models that
 * fulfil specific roles.  As of right now, we consider three basic
 * categories of model:
 *  - Imputation: Models that 'fill in' missing or erroneous values
 *  - SchemaMatch: Models that align columns of two relations
 *  - EntityMatch: Models that align rows of two relations (TBD)
 */
object ModelRegistry
{
  /**
   * Factory method type for Imputation model constructors
   *
   * Given a relation and a list of column names, an imputation model
   * is expected to be able to produce replacement values for cells in
   * the specified columns of the specified relation.
   *
   * Inputs:
   *    Database        -> The database to train on
   *    String          -> The name prefix for the model
   *    List[String]    -> A list of column names to impute on
   *    Operator        -> The relation to impute on
   * Outputs:
   *    Map[...]        -> A map from column name to a Model/Idx pair,
   *                       along with a sequence of "hint" expressions,
   *                       defining a way to repair the attribute
   *
   * Note that although we request models for a set of columns in a
   * single pass, the constructor is not required to respond with a
   * model for all, or even any of the columns it's given.
   *   - If the model in question is not applicable to the query, it
   *     should return an empty map.
   *   - If the model in question is not applicable to one or more
   *     columns, these columns need not be in the result map.
   *
   * Imputation models are expected to take a single variable,
   * identifying the ROWID of the specified row.  In other words,
   * it should support VGTerms of the form {{ TABLE;x[ROWID] }}.  The
   * domain of the variable should be the same as the type of the
   * specified column.
   *
   * Note that the constructor is NOT responsible for registering the
   * model with the manager.
   *
   * The original row may be constructed from the provided relation
   * operator: Select[ROWID = rowid](oper)
   */
  type ImputationConstructor =
    ((Database, String, List[String], Operator) => Map[String, (Model,Int,Seq[Expression])])

  /**
   * Factory method type for SchemaMatch model constructors
   *
   * A SchemaMatch model takes two relations or schemas and maps
   * the left-hand side's schema into the right-hand side's.
   *
   * Inputs:
   *    Database           -> The database to train on
   *    String             -> The name prefix for the model
   *    Operator | Schema  -> The source query or schema (to map from)
   *    Operator | Schema  -> The target query or schema (to map to)
   *
   * Outputs:
   *    Map[...]           -> A map from a target column name to a
   *                          Model/Idx pair defining a way to map the
   *                          specified target.
   *
   * Note that the constructor is not required to respond with a model
   * for all, or even any of the columns in the target schema
   *   - If the model in question is not applicable to the query, it
   *     should return an empty map.
   *   - If the model in question is not applicable to one or more
   *     columns, these columns need not be in the result map.
   *
   * SchemaMatch models take no variables.  Each Model/Index pair
   * dictates how one target column is to be matched.  The return value
   * of the model is a string with the name of the attribute that the
   * target column should be matched to.
   */
  type SchemaMatchConstructor =
    ((Database,
      String,
      Either[Operator,List[(String,Type)]],
      Either[Operator,List[(String,Type)]]) =>
        Map[String,(Model,Int)])

  /////////////////// PREDEFINED CONSTRUCTORS ///////////////////

  /**
   * Constructors for existing Imputation models
   *
   * Eventually, it would be nice to allow external components and plugins
   * to add new models here.
   */
  val imputations = Map[String,ImputationConstructor](
    "FUNCDEP" -> (FuncDepModel.train _),
    "WEKA" -> (WekaModel.train _)
  )

  /**
   * Constructors for existing SchemaMatch models
   *
   * Eventually, it would be nice to allow external components and plugins
   * to add new models here.
  */
  val schemamatchers = Map[String,SchemaMatchConstructor](
    "EDITDISTANCE" -> (EditDistanceMatchModel.train _)
  )
}
