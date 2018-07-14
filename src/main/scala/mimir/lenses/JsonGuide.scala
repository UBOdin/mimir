package mimir.lenses

import com.typesafe.scalalogging.slf4j.LazyLogging
import mimir.Database
import mimir.algebra.{Expression, Operator, Project, ProjectArg, TString, Var}
import mimir.algebra.Type
import mimir.models._
import play.api.libs.json.{JsError, JsSuccess, JsValue, Json}

import scala.collection.JavaConverters._


object JsonGuide extends LazyLogging {
  def create(
              db: Database,
              name: String,
              query: Operator,
              args: Seq[Expression]
            )
  : Unit =
  //: (Operator, Seq[Model]) =
  {
    ???
/*
    val schema: IndexedSeq[String] = query.schema.map(_._1).toIndexedSeq

    // just a bunch of sanity checks
    if (schema.isEmpty) {
      throw new ModelException("There is no schema for this query!")
    }

    if (args.isEmpty) {
      throw new ModelException("Json Explorer requires a target column!")
    }

    if (args.size != 2) {
      throw new ModelException("There should only be one argument that is the target column!")
    }

    if (!schema.contains(args(0).toString.replace("'", "").toUpperCase())) {
      throw new ModelException("Json Explorer's target is not part of the schema!")
    }

    // ok, input should be at least sane-ish. Let's give it a shot!

    db.query()

    val input: String = ""
    val consistentSchema: String = ""

    val json: JsValue = Json.parse(input)
    json.validate[ExplorerObject] match {
      case s: JsSuccess[ExplorerObject] => {
        val row: ExplorerObject = s.get
        val allColumns: Seq[AllData] = row.data
        val updates: Seq[(String,Map[String,Int])] = allColumns.map((a: AllData) => {

          val colName: String = a.name
          val typeInfo: Seq[TypeData] = a.td
          val typeSeq: Seq[(String,Int)] = typeInfo.map((t) => {
            (t.typeName,t.typeCount)
          })
          // now add type map for that column to colMap
          (colName -> typeSeq.toMap)
        })
        columnMap = updateMap(columnMap, updates)

        // all column information has now been updated
      } // end case Explorer Object
      case e: JsError => // failed, probably not the right shape
    }





    // map that contains the initial results to be written to tables, this is for all rows in the JSON table
    val computedResults: scala.collection.mutable.Map[String, IndexedSeq[(String, String)]] = scala.collection.mutable.Map()
    val typeMap: scala.collection.mutable.Map[String, scala.collection.mutable.Map[Type, Int]] = scala.collection.mutable.Map()
    val minMap: scala.collection.mutable.Map[String, Float] = scala.collection.mutable.Map()

    // for each row in JSON table, infer the type and other information that should be recorded
    db.query(query) {
      _.foreach(row => row.tuple.map((r) => {
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
                  if (v.toFloat < min) {
                    minMap.put(col, v.toFloat)
                  }
                }
                catch { // should be null
                  case _ => // do nothing since it should be null
                }
              }
              case None => { // column not in the map yet
                try {
                  minMap.put(col, v.toFloat)
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
      )
    }

    typeMap.foreach(println(_))
    minMap.foreach(println(_))

    logger.debug(s"Training $model.name on $query")
    model.train(db, query)

    val repairs =
      query.schema.map(_._1).map(col => {
        if (columnIndexes contains col) {
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

  }
*/
}

}
