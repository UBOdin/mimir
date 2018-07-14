package mimir.lenses

import java.sql.SQLException

import scala.util._
import com.typesafe.scalalogging.slf4j.LazyLogging
import mimir.Database
//import mimir.Mimir.{output, terminal}
import mimir.algebra._
import mimir.ctables._
//import mimir.exec.{DefaultOutputFormat, PrettyOutputFormat}
//import mimir.exec.result.ResultIterator
import mimir.models._
import com.github.wnameless.json.flattener._
import scala.collection.JavaConverters._

object JsonExplorerLens extends LazyLogging
{
  def create(
              db:Database,
              name:String,
              query:Operator,
              args:Seq[Expression]
            )
  :Unit =
  //: (Operator, Seq[Model]) =
  {
    ???
  }
    /*
    val schema: IndexedSeq[String] = query.schema.map(_._1).toIndexedSeq

    // just a bunch of sanity checks
    if(schema.isEmpty){
      throw new ModelException("There is no schema for this query!")
    }

    if(args.isEmpty){
      throw new ModelException("Json Explorer requires a target column!")
    }

    if(args.size > 1){
      throw new ModelException("There should only be one argument that is the target column!")
    }

/*    if(query.schema.toMap.get(args(0).toString).toString != TString.toString()){
      throw new ModelException("This column should be of type String")
    }
*/
    if(!schema.contains(args(0).toString.replace("'","").toUpperCase())){
      throw new ModelException("Json Explorer's target is not part of the schema!")
    }

    // ok, input should be at least sane-ish. Let's give it a shot!

    // Create the type component, this will return all the possible types for a column, for now probably just one.
    // From this figure out which statistics can be projected for a column


    // Next gather statistics, each stat must have a type associated with it. This will cast the value to that type

    println("DUMPING RESULTS")
    output = DefaultOutputFormat


    // this is the list of functions that will be performed, it goes (name, function, inputType, outputType)
    // name is the function name you wish to call it by, this should be unique, this is the name the tables will use to reference it
    // function, this is the function that will be called and passed the value and metaValue
    // inputType, this is the sql supported type that your function will take as a parameter
    // outputType, this is the output type that will be stored in the table, this just makes it easier
    // Add to this list if a new function is needed
/*
    val func: scala.Function() = (list: List[Int]) => {list.max}

    val functionList: IndexedSeq[(String, scala.Function(), Type, Type)] = IndexedSeq(
      ("Max",func,TInt(),TInt())
//      ("Min",(list: List[Int]) => {list.min},TInt(),TInt())
    )
*/
    // map that contains the initial results to be written to tables, this is for all rows in the JSON table
    val computedResults: scala.collection.mutable.Map[String,IndexedSeq[(String,String)]] = scala.collection.mutable.Map()
    val typeMap: scala.collection.mutable.Map[String,scala.collection.mutable.Map[Type,Int]] = scala.collection.mutable.Map()
    val minMap: scala.collection.mutable.Map[String,Float] = scala.collection.mutable.Map()

    // for each row in JSON table, infer the type and other information that should be recorded
    db.query(query){_.foreach( row => row.tuple.map((r) => {
      var clean: String = r.toString.substring(1, r.toString.size - 1) // to remove the singe quotes
      clean = clean.replace("\\\\", "")
      clean = clean.replace("\\\"", "")
      clean = clean.replace("\\n", "")
      clean = clean.replace("\\r", "")
      clean = clean.replace("\n", "")
      clean = clean.replace("\r", "")
      try {
        val jsonRow = JsonFlattener.flattenAsMap(clean)
        val keySet: java.util.Set[String] = jsonRow.keySet()

        keySet.asScala.map((col) => {
          val v: String = jsonRow.get(col).toString()
          Type.tests.map((tup) => { //
            val t: Option[Type] = tup._2.findFirstMatchIn(v).map(_ => tup._1)
            t match { // start of type tracking
              case Some(stringType) => { // match of type found
                if (typeMap.contains(col)) { // column already tracked
                  typeMap.get(col) match {
                    case Some(typeCount) => { // the map for that column
                      typeCount.get(stringType) match { // to get the count of that type
                        case Some(count) => { // update the type
                          val n: Int = count + 1
                          typeCount.put(stringType, n)
                        }
                        //                        case None
                      }
                    }
                    //                    case None
                  }
                }
                else { // initialize column and map
                  val m: scala.collection.mutable.Map[Type, Int] = scala.collection.mutable.Map()
                  Type.tests.map((fill) => {
                    m.put(fill._1, 0)
                  })
                  m.put(TString(), 0)
                  typeMap.put(col, m) // now map is initialized

                  typeMap.get(col) match {
                    case Some(typeCount) => { // the map for that column
                      typeCount.get(stringType) match { // to get the count of that type
                        case Some(count) => { // update the type
                          val n: Int = count + 1
                          typeCount.put(stringType, n)
                        }
                        //                        case None
                      }
                    }
                    //                    case None
                  }
                }
              } // end some

              case None => { // is a string type
                typeMap.get(col) match {
                  case Some(typeCount) => {
                    typeCount.get(TString()) match {
                      case Some(count) => {
                        val n: Int = count + 1
                        typeCount.put(TString(), n)
                      }
                    }
                  }
                  case None => { // init
                    val m: scala.collection.mutable.Map[Type, Int] = scala.collection.mutable.Map()
                    Type.tests.map((fill) => {
                      m.put(fill._1, 0)
                    })
                    m.put(TString(), 0)
                    typeMap.put(col, m) // now map is initialized

                    typeMap.get(col) match {
                      case Some(typeCount) => {
                        typeCount.get(TString()) match {
                          case Some(count) => {
                            val n: Int = count + 1
                            typeCount.put(TString(), n)
                          }
                        }
                      }
                    }
                  }
                }
              } // end none
            } // end match
          }) // end of type update

          // start casting and collection

          // This is where looping would be done for collection array
          minMap.get(col) match {
            case Some(min) => { // check the min and update if needed
              try {
                if(v.toFloat < min){
                  minMap.put(col,v.toFloat)
                }
              }
              catch { // should be null
                case _ => // do nothing since it should be null
              }
            }
            case None => { // column not in the map yet
              try {
                minMap.put(col,v.toFloat)
              }
              catch { // should be null
                case _ => // do nothing since it should be null
              }
            }
          }

        }) // end of for row loop
      } // end try
      catch {
        case e: Exception => {
          println(clean + " is not in proper JSON form")
          println(e)
        }
      }

      // Now write out results
      // type is a rk while the other information is made up of a metadata table and a results table
    }) // get next row
    )}

    typeMap.foreach(println(_))
    minMap.foreach(println(_))
/*
    logger.debug(s"Training $model.name on $query")
    model.train(db, query)

    val repairs =
      query.schema.map(_._1).map( col => {
        if(columnIndexes contains col){
          ProjectArg(col,
            Function("CAST", Seq(
              Var(col),
              VGTerm(model, columnIndexes(col), Seq(), Seq())
            ))
          )
        } else {
          ProjectArg(col, Var(col))
        }
      })

    (
      Project(repairs, query),
      List(model)
    )
*/
  }
  */
    ???
}
