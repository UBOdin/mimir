package mimir.lenses

import scala.util._
import mimir.Database
import mimir.algebra._
import mimir.ctables._
import mimir.exec.ResultIterator

import scala.collection.mutable.ListBuffer

/*
 Current places that need to be changed to extend a value to the Type Inference Lens

 - In type inference (this class):
    - Need to add regex to TypeInferenceTypes
    - Need to add new type to Votes list in TypeInferer

 - mimir.algebra.Expression:
    - Need to add the Type to the enum list in Type
    - (Optional) can create Primitive types to define how their behavior works with the sqlite backend

 - mimir.utils.JDBCUtils:
    - In convert field you need to add the type here to define the behavior when the value is returned from the backend

 - mimir.sql.sqlite.SQLiteCompat:
    - Here is where you need to add the functionality, so if you want something special to happen when that type is detected
*/



class TypeInferenceLens(name: String, args: List[Expression], source: Operator)
  extends Lens(name, args, source) {

  var orderedSourceSchema: List[(String,Type)] = null
  var model: Model = null
  var db: Database = null

  def sourceSchema() = {
    if(orderedSourceSchema == null){
      orderedSourceSchema =
        source.schema.map( _ match { case (n,t) => (n.toUpperCase,t) } )
    }
    orderedSourceSchema
  }

  def schema(): List[(String, Type)] =
    model.asInstanceOf[TypeInferenceModel].inferredTypeMap.map( x => (x._1, x._2))

  def allKeys() = { sourceSchema.map(_._1) }

  def lensType = "TYPE_INFERENCE"

  /**
   * `view` emits an Operator that defines the Virtual C-Table for the lens
   */
  override def view: Operator = {
    Project(
      allKeys().
        zipWithIndex.
          map{ case (k, i) =>
            ProjectArg(
              k,
              Function(
                "CAST",
                List(Var(k), VGTerm((name, model), i, List()))
              )
            )
          },
      source
    )
  }

  /**
   * Initialize the lens' model by building it from scratch.  Typically this involves
   * using `db` to evaluate `source`
   */
  override def build(db: Database): Unit = {
    this.db = db
    val results = db.query(source)

    model = new TypeInferenceModel(this)
    model.asInstanceOf[TypeInferenceModel].init(results)
  }
}

class TypeInferenceModel(lens: TypeInferenceLens) extends Model
{
  var inferredTypeMap = List[(String, Type, Double)]()
  var threshold: Double = lens.args.head.asInstanceOf[FloatPrimitive].asDouble

  class TypeInferrer {

    var allTypes = new TypeInferenceTypes()

    private val votes = scala.collection.mutable.Map[Type,Int]() // Type, NumberVotes, Priority level
    votes.put(TInt(),0)
    votes.put(TFloat(),0)
    votes.put(TDate(),0)
    votes.put(TBool(),0)

    TypeList.typeList.foreach((tuple) => {
      votes.put(TUser(tuple._1,tuple._2,tuple._3), 0)
    })

    private var totalVotes = 0
    def detectAndVoteType(v: String): Unit = {

      allTypes.baseTypes(v,votes)
      totalVotes += 1
    }
    def infer(): (Type, Double) = {
      if(totalVotes == 0)
        return (TString(), 0)

      val possibleMatches:scala.collection.mutable.Map[Type,Double] = scala.collection.mutable.Map[Type,Double]()
      votes.foreach((tuple) => {
        val ratio: Double = tuple._2.toFloat / totalVotes
        if(ratio >= threshold) {
          possibleMatches.put(tuple._1,ratio)
        }
      })
      var currentBestType: Type = TString()
      var currentBestRatio: Double = 0.0

      if(possibleMatches.size > 0) {
        possibleMatches.foreach((tuple) => {
          tuple._1 match {
            case TUser(name, regex, sqlType) =>
              currentBestRatio = tuple._2
              currentBestType = tuple._1
            case _ =>
              currentBestType match {
                case TUser(name,regex,sqlType) =>

                case _ =>
                  if (tuple._2 > currentBestRatio) {
                  currentBestType = tuple._1
                  currentBestRatio = tuple._2
                }
              }
          }
        })
      }
      println("Type: " + currentBestType.toString())
      (currentBestType, currentBestRatio)
    }
  }


  def init(data: ResultIterator): Unit = {
    inferredTypeMap = learn(lens.sourceSchema(), data.allRows())
  }

  def learn(sch: List[(String, Type)],
                 data: List[List[PrimitiveValue]]): List[(String, Type, Double)] = {

    /**
     * Count votes for each type
     */
    val inferClasses =
      sch.map{ case(k, t) => (k, new TypeInferrer)}

    data.foreach( (row) =>
      sch.indices.foreach( (i) =>
        inferClasses(i)._2.detectAndVoteType(
          try{
            row(i).asString
          } catch {
            case e:TypeException => ""
          }
        )
      )
    )

    /**
     * Now infer types
     */
    inferClasses.map{
      case(k, inferClass) =>
        val inferred = inferClass.infer()
//        println(inferred._1.toString)
        (k, inferred._1, inferred._2)
    }
  }

  // Model Implementation
  def varType(idx: Int, argTypes: List[Type]): Type = TType()

  def sample(idx: Int, randomness: Random, args: List[PrimitiveValue]): PrimitiveValue = {
    bestGuess(idx, args)
  }
  def bestGuess(idx: Int, args: List[PrimitiveValue]): PrimitiveValue = {
    TypePrimitive(inferredTypeMap(idx)._2)
  }
  def reason(idx: Int, args: List[Expression]): (String) = {
    val percentage = (inferredTypeMap(idx)._3 * 100).round

    if(percentage == 0) {
      "I assumed that the type of " + inferredTypeMap(idx)._1 +
        " is string"
    } else {
      "I assumed that the type of " + inferredTypeMap(idx)._1 +
        " is " + TString().toString(inferredTypeMap(idx)._2) +
        " with " + percentage.toString + "% of the data conforming to the expected type"
    }
  }
}

class TypeInferenceTypes(){
    def baseTypes(v:String,votes:scala.collection.mutable.Map[Type,Int]): Unit ={
      if(v != null) {
        if(v.matches("(\\+|-)?([0-9]+)"))
          votes(TInt()) += 1
        if(v.matches("(\\+|-)?([0-9]*(\\.[0-9]+))"))
          votes(TFloat()) += 1
        if(v.matches("[0-9]{4}\\-[0-9]{2}\\-[0-9]{2}"))
          votes(TDate()) += 1
        if(v.matches("(?i:true|false)"))
          votes(TBool()) += 1

        TypeList.typeList.foreach((tuple)=>{
          if(v.matches(tuple._2))
            votes(TUser(tuple._1,tuple._2,tuple._3)) += 1
        })

      }
      else {
        votes.foreach{ case (t, v) => votes(t) += 1 }
      }
    }

}


/*
Adding new UserTypes to mimir (TUser Types)
- All new types must be added the TypeList in object TypeList to effect the system
- Parameterized types are now supported. The current parameter is Tuple(name:String, regex:String, sqlType:Type), remember to update Tuple# to the right number in object TypeList
  - name:String is the name of the new type, must be unique
  - regex:String is the regular expression that this new type should match on, if the column is of this type will be determined by the threshold, consider the basic types when making this
  - sqlType:Type this is the underlying type of the new type, example TFloat, TDouble... This is for potential speed up later and to better capture the output

Extending the UserType
- This is if you want to add new functionality to TUser, such as adding a new parameter
These are the files that need to change to extend the TUser
  - mimir.lenses.TypeInference:
    - In class TypeInferenceTypes add the new parameters to TUser
    - In class TypeInferer add the new parameters to TUser
  - mimir.utils.JDBCUtils:
    - changes here are made in the function convert, in the case class TUser. This only needs
  - mimir.algebra.Expression:
    - change TUser parameters to match the new desired parameters
    - change sealed trait Type so that all the instances of TUser match the new parameters
  - mimir.sql.sqlite.SQLiteCompat:
    - update TUser type parameters



 */
object TypeList{
  val typeList = ListBuffer[(String,String,Type)]()

  typeList += Tuple3("TUser","USER",TString())
  typeList += Tuple3("TWeight","KG*",TString())
  typeList += Tuple3("FireCompany","^[a-zA-Z]\\d{3}$",TString())
  typeList += Tuple3("ZipCode","^\\d{5}(?:[-\\s]\\d{4})?$",TInt())
  typeList += Tuple3("Container","[A-Z]{4}[0-9]{7}",TString())
  typeList += Tuple3("CarrierCode","[A-Z]{4}",TString())
  typeList += Tuple3("MMSI","MID\\d{6}|0MID\\d{5}|00MID\\{4}",TString())
  typeList += Tuple3("BillOfLanding","[A-Z]{8}[0-9]{8}",TString())

}


// CREATE LENS nt9 AS SELECT * FROM test WITH Type_Inference(.9);
// CREATE LENS nt1 AS SELECT firecomp, zipcode FROM cityraw WITH Type_Inference(.8);
// CREATE LENS mv1 AS SELECT * FROM nt1 WITH MISSING_VALUE('FIRECOMP','ZIPCODE');

// CREATE LENS t1 AS SELECT * FROM unevenraw WITH Type_Inference(.8);
// CREATE LENS mv1 AS SELECT * FROM t1 WITH MISSING_VALUE(*);
