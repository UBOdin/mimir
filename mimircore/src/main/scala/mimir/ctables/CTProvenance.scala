package mimir.ctables

import java.sql.SQLException

import mimir.algebra._
import mimir.util._

/**
 * CTProvenance wraps an AST-style provenance trace of an operator.
 * 
 * For simplicity, this provenance trace is specified expanded out
 * into a "union-normal form", or a query of the form:
 * Q1 U Q2 U Q3 U ... 
 * where each Qi has no unions.  We refer to each Qi as an origin
 *
 * The provenance trace assumes that each Qi is assigned a unique 
 * String identifier, and the top-level object of the provenance
 * trace is a Map from this identifier to the uncertainty 
 * provenance trace of Qi.
 * 
 * The provenance of each Qi is encoded as a pair: 
 *   Map[ Column -> Expression ]  -->  The provenance trace of the
 *                                     specified column values.
 *   Expression                   -->  The provenance trace of each
 *                                     individual tuple.
 * 
 * 
 */
class CTProvenance(
	provenance: Map[String, (Map[String, Expression], Expression)] = 
		Map.empty ++ List( ("", (Map[String,Expression](), BoolPrimitive(true))) )
) {

	/** 
		As an optimization, if the map contains only one element, 
		it can be accessed by just calling Provenance()
	*/
	def apply(): (Map[String,Expression], Expression) = apply(null);

	def apply(origin: String): 
		(Map[String,Expression], Expression)
			= if(origin == null) { provenance.head._2 } 
			  else { provenance(origin) }

	def column(col: String): Map[String, Expression] = 
	{
		provenance.mapValues( _._1(col) )
	}

	def condition(): Map[String, Expression] =
	{
		provenance.mapValues( _._2 )
	}

}
