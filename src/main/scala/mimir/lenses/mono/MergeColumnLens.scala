package mimir.lenses.mono

import java.sql.SQLException
import play.api.libs.json._
import sparsity.Name

import mimir.algebra._
import mimir.Database
import mimir.lenses._

case class MergeColumnLensConfig(
  target: ID,
  sources: Seq[ID]
)

object MergeColumnLensConfig
{
  implicit val format:Format[MergeColumnLensConfig] = Json.format
}

object MergeColumnLens extends MonoLens
{
  def train(
    db: Database,
    name: ID,
    query: Operator,
    config: JsValue
  ): JsValue = 
  {
    val columnLookup = OperatorUtils.columnLookupFunction(query)
    Json.toJson(
      config match {
        case o:JsObject => o.as[MergeColumnLensConfig]
        case a:JsArray => {
          val targets = a.as[Seq[String]]
                         .map { col => columnLookup(Name(col)) }
          MergeColumnLensConfig(
            targets.head,
            targets
          )
        }
        case _ => throw new SQLException(s"Invalid lens configuration $config")
      }
    )
  }

  def view(
    db: Database,
    name: ID,
    query: Operator,
    configJson: JsValue,
    friendlyName: String
  ): Operator = 
  {
    val config = configJson.as[MergeColumnLensConfig]
    val isSource = config.sources.toSet
    val normalColumns = query.columnNames.filter { !isSource(_) }

    val firstSource = config.sources.head
    val areEqualCondition = 
      ExpressionUtils.makeAnd(
        config.sources.tail.map { _.eq(firstSource) }
      )
    val message =
      Function(ID("concat"), Seq(
        StringPrimitive(s"When merging $friendlyName.${config.target}, expected "),
        Var(firstSource).as(TString()),
        StringPrimitive(" to be equal to ")
      ) ++ 
        config.sources.tail
          .flatMap { col => Seq(StringPrimitive(", "), Var(col)) }
          .tail
      )

    query.mapByID( (
      normalColumns.map { col => col -> Var(col) }
      :+ ( config.target -> 
              areEqualCondition.thenElse 
                { Var(firstSource) }
                { Caveat(name, Var(firstSource), Seq(RowIdVar()), message)}

      )
    ):_* )
  }
}

// package mimir.lenses

// import java.io.File
// import java.sql._
// import java.util

// import mimir.algebra._
// import mimir.ctables._
// import mimir.util.RandUtils
// import mimir.Database
// import mimir.parser._
// import mimir.models._

// import scala.collection.JavaConversions._
// import scala.util._

// import mimir.ml.spark.{SparkML, Classification}
  

// object PickerLens {

//   def create(
//     db: Database, 
//     name: ID, 
//     humanReadableName: String,
//     query: Operator, 
//     args:Seq[Expression]
//   ): (Operator, Seq[Model]) =
//   {
//     val operSchema = db.typechecker.schemaOf(query)
//     val schemaMap = operSchema.toMap
    
//     val (pickFromColumns, pickerColTypes ) = args.flatMap {
//       case Function(ID("pick_from"), cols ) => 
//         Some( cols.map { case col:Var => (col.name, schemaMap(col.name)) 
//                          case col => throw new RAException(s"Invalid pick_from argument: $col in PickerLens $name (not a column reference)")
//                        } )
//       case _ => None
//     }.toSeq.flatten.unzip
    
//     pickerColTypes.foldLeft(pickerColTypes.head)((init, element) => element match {
//       case `init` => init
//       case x => throw new RAException(s"Invalid PICK_FROM argument Type: $x in PickerLens $name (PICK_FROM columns must be of same type)")
//     })
    
//     val pickToCol = args.flatMap {
//       case Function(ID("pick_as"), Seq(Var(col))) => Some( col )
//       case _ => None
//     }.headOption
//      .getOrElse( ID("PICK_ONE_", ID(pickFromColumns, "_")) )
    
//     val projectedOutPicFromCols = args.flatMap {
//       case Function(ID("hide_pick_from"), Seq(Var(col))) => Some( col )
//       case _ => None
//     }
    
//     val classifyUpFront = args.flatMap {
//       case Function(ID("classify_up_front"), Seq(bp@BoolPrimitive(b))) => Some( bp )
//       case _ => None
//     } match {
//       case Seq() => true
//       case Seq(BoolPrimitive(b)) => b
//     }
    
    
//     val useClassifier = args.flatMap {
//       case Function(ID("classifier"), Seq(StringPrimitive(classifier))) => Some(ID(classifier))
//       case Function(ID("classifier"), Seq(Var(classifier))) => Some(classifier)
//       case _ => None
//     }.headOption

//     val pickerModel = PickerModel.train(db, 
//       ID(name,"_PICKER_MODEL:", ID(pickFromColumns, "_")), 
//       pickToCol, 
//       pickFromColumns, 
//       pickerColTypes, 
//       useClassifier, 
//       classifyUpFront, 
//       query
//     ) 
    
//     lazy val expressionSubstitutions : (Expression) => Expression = (expr) => {
//     expr match {
//       case Function(ID("avg"), Seq(Var(col))) => {
//         val exprSubQ = Project(Seq(ProjectArg(ID("AVG_",col), expr)), query)
//         db.query(exprSubQ)( results => {
//         var replacementExpr : Expression = NullPrimitive()
//         if(results.hasNext()){
//           replacementExpr = results.next()(0)
//         }
//         results.close()
//         replacementExpr
//         })
//       }
//       case x => x.recur(expressionSubstitutions(_))
//     }
//   }
    
//     val pickUncertainExprs : List[(Expression, Expression)] = args.flatMap {
//       case Function(ID("uexprs"), Seq(StringPrimitive(expr), StringPrimitive(resultExpr)) ) => Some( (
//           ExpressionParser.expr(expr), 
//           VGTerm(pickerModel.name, 0,Seq[Expression](RowIdVar()).union(pickFromColumns.map(Var(_))), Seq(expressionSubstitutions(ExpressionParser.expr(resultExpr)))) 
//           ) )
//       case _ => None
//     }.toList match {
//       case Seq() => {
//         List((BoolPrimitive(true), VGTerm(pickerModel.name, 0,Seq[Expression](RowIdVar()).union(pickFromColumns.map(Var(_))),Seq()) ))
//       }
//       case x => x 
//     }
    
//     val pickCertainExprs : List[(Expression, Expression)] = args.flatMap {
//       case Function(ID("exprs"), Seq(StringPrimitive(expr), StringPrimitive(resultExpr)) ) => Some( (
//           ExpressionParser.expr(expr), 
//           expressionSubstitutions(ExpressionParser.expr(resultExpr))
//           ) )
//       case _ => None
//     }.toList
    
       
//     val pickExpr = ExpressionUtils.makeCaseExpression(
//           pickCertainExprs.union(pickUncertainExprs),
//           NullPrimitive()
//         )
    
//     val pickerColsTypeMap = pickFromColumns.zip(pickerColTypes).toMap
    
//     val projectArgs = 
//       query.columnNames.
//         flatMap( col => pickerColsTypeMap.get(col) match {
//           case None => Some(ProjectArg(col, Var(col)))
//           case Some(pickFromCol) => {
//             if(projectedOutPicFromCols.contains(col))
//               None //none if you dont want the from cols 
//             else
//               Some(ProjectArg(col, Var(col)))
//           }
//         }).union(Seq(ProjectArg(pickToCol, db.compiler.optimize(pickExpr))))
    
//     return (
//       Project(projectArgs, query),
//       Seq(pickerModel)
//     )
//   }
  

// }

