package mimir.lenses

import java.io._
import org.specs2.reporter.LineLogger
import org.specs2.specification.core.{Fragment,Fragments}

import mimir.test._
import mimir.util._
import LoggerUtils.trace
import mimir.ctables._
import mimir.algebra.{NullPrimitive,MissingVariable}

object MissingValueSpec  
extends SQLTestSpecification("MissingValueSpec")
{
  
  "The Missing Value Lens" should {
  
    loadCSV(
		  	sourceFile = "test/data/mv.csv", 
		  	targetTable = "mv", 
		  	inferTypes = true,
		  	detectHeaders = true
		  )
  
    "Create the Lens and query it" >> {
      update("""
          CREATE LENS MV1 
          AS SELECT * FROM mv 
          WITH MISSING_VALUE('B');
        """)
        
        query("SELECT * FROM MV1;") { _.toList.map(_.tuple.last) must be equalTo List(f(5.0), f(3.5), f(2.0), f(4.0),f(5.0), f(3.5), f(2.0), f(2.0)) }
      
      }
  
  }
}