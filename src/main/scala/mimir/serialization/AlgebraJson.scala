package mimir.serialization

import play.api.libs.json._

import mimir.Database
import mimir.algebra._
import mimir.util._
import mimir.views.ViewAnnotation
import play.api.libs.json.Reads._ // Custom validation helpers
import play.api.libs.functional.syntax._ // Combinator syntax

class JsonParseException(msg: String, json: String) extends RAException(msg)

object AlgebraJsonCodecs
{
  def ofOperator(o: Operator): JsObject =
  {
    o match {
      case Project(args, source) => 
        JsObject(Map[String, JsValue](
          "type" -> JsString("project"),
          "columns" -> JsArray(args.map { arg => 
            JsObject(Map[String, JsValue](
              "name" -> JsString(arg.name.id), 
              "expression" -> ofExpression(arg.expression)
            )) }),
          "source" -> ofOperator(source)
        ))

      case Select(cond, source) => 
        JsObject(Map[String, JsValue](
          "type" -> JsString("select"),
          "condition" -> ofExpression(cond),
          "source" -> ofOperator(source)
        ))        

      case Join(left, right) => 
        JsObject(Map[String, JsValue](
          "type" -> JsString("join"),
          "left" -> ofOperator(left),
          "right" -> ofOperator(right)
        ))      

      case LeftOuterJoin(left, right, cond) => 
        JsObject(Map[String, JsValue](
          "type" -> JsString("join_left_outer"),
          "left" -> ofOperator(left),
          "right" -> ofOperator(right),
          "condition" -> ofExpression(cond)
        ))         

      case Union(left, right) => 
        JsObject(Map[String, JsValue](
          "type" -> JsString("union"),
          "left" -> ofOperator(left),
          "right" -> ofOperator(right)
        ))        

      case Aggregate(gbCols, aggCols, source) => 
        JsObject(Map[String, JsValue](
          "type" -> JsString("aggregate"),
          "gb_columns" -> JsArray(gbCols.map { ofExpression(_) }),
          "agg_columns" -> 
            JsArray(aggCols.map { agg => 
              JsObject(Map[String, JsValue](
                "function" -> JsString(agg.function.id),
                "distinct" -> JsBoolean(agg.distinct),
                "args" -> JsArray(agg.args.map { ofExpression(_) }),
                "alias" -> JsString(agg.alias.id)
              ))
            }),
          "source" -> ofOperator(source)
        ))

      case Table(table, source, schema, meta) => 
        JsObject(Map[String, JsValue](
          "type" -> JsString("table_normal"),
          "table" -> JsString(table.id),
          "source" -> JsString(source.id),
          "schema" -> ofSchema(schema),
          "metadata" -> 
            JsArray(meta.map { elem => 
              JsObject(Map(
                "alias" -> JsString(elem._1.id),
                "value" -> ofExpression(elem._2),
                "type"  -> ofType(elem._3)
              ))
            })
        ))

      case HardTable(schema,data) => 
        JsObject(Map[String, JsValue](
          "type" -> JsString("table_hardcoded"),
          "schema" -> ofSchema(schema),
          "data" -> JsArray(data.map( row => JsArray( row.map( hv => ofPrimitive(hv)))))
        ))

      case View(name, query, annotations) => 
        JsObject(Map[String,JsValue](
          "type" -> JsString("table_view"),
          "name" -> JsString(name.id),
          "query" -> ofOperator(query),
          "annotations" -> JsArray(annotations.toSeq.map { _.toString }.map { JsString(_) })
        ))

      case AdaptiveView(model, name, query, annotations) => 
        JsObject(Map[String,JsValue](
          "type" -> JsString("table_adaptive"),
          "model" -> JsString(model.id),
          "name" -> JsString(name.id),
          "query" -> ofOperator(query),
          "annotations" -> JsArray(annotations.toSeq.map { _.toString }.map { JsString(_) })
        ))

      case Limit(offset, count, source) => 
        JsObject(Map[String, JsValue](
          "type" -> JsString("limit"),
          "offset" -> JsNumber(offset),
          "limit" -> 
            (count match {
              case None => JsNull
              case Some(s) => JsNumber(s)
            }),
          "source" -> ofOperator(source)
        ))

      case Sort(sortBy, source) => 
        JsObject(Map[String, JsValue](
          "type" -> JsString("sort"),
          "sort_by" -> 
            JsArray(sortBy.map { sort => 
              JsObject(Map[String, JsValue](
                "expression" -> ofExpression(sort.expression),
                "ascending" -> JsBoolean(sort.ascending)
              ))
            }),
          "source" -> ofOperator(source)
        ))
    }
  }
  def toOperator(json: JsValue): Operator = 
  {
    val elems = json.asInstanceOf[JsObject].value// .asInstanceOf[JsObject].value
    elems("type").asInstanceOf[JsString].value match {
      case "aggregate" =>
        Aggregate(
          elems("gb_columns").asInstanceOf[JsArray].value.map { toExpression(_).asInstanceOf[Var] },
          elems("agg_columns").asInstanceOf[JsArray].value.map { fieldJson => 
            val fields = fieldJson.asInstanceOf[JsObject].value

            AggFunction(
              ID(fields("function").asInstanceOf[JsString].value),
              fields("distinct").asInstanceOf[JsBoolean].value,
              fields("args").asInstanceOf[JsArray].value.map { toExpression(_) },
              ID(fields("alias").asInstanceOf[JsString].value)
            )
          },
          toOperator(elems("source"))
        )
      case "join" =>
        Join(
          toOperator(elems("left")),
          toOperator(elems("right"))
        )
      case "join_left_outer" =>
        LeftOuterJoin(
          toOperator(elems("left")),
          toOperator(elems("right")),
          toExpression(elems("condition"))
        )
      case "limit" =>
        Limit(
          elems("offset") match {
            case JsNumber(n) => n.longValue
            case JsNull => 0
            case _ => throw new RAException("Invalid offset clause in JSON")
          },
          elems("limit") match {
            case JsNumber(n) => Some(n.longValue)
            case JsNull => None
            case _ => throw new RAException("Invalid limit clause in JSON")
          },
          toOperator(elems("source"))
        )
      case "project" =>
        Project(
          elems("columns").asInstanceOf[JsArray].value.map { fieldJson =>
            val fields = fieldJson.asInstanceOf[JsObject].value

            ProjectArg(
              ID(fields("name").asInstanceOf[JsString].value),
              toExpression(fields("expression"))
            )
          },
          toOperator(elems("source"))
        )
      case "sort" =>
        Sort(
          elems("sort_by").asInstanceOf[JsArray].value.map { fieldJson =>
            val fields = fieldJson.asInstanceOf[JsObject].value

            SortColumn(
              toExpression(fields("expression")),
              fields("ascending").asInstanceOf[JsBoolean].value
            )
          },
          toOperator(elems("source"))
        )
      case "select" =>
        Select(
          toExpression(elems("condition")),
          toOperator(elems("source"))
        )
      case "table_hardcoded" =>
        val schema = toSchema(elems("schema"))
        HardTable(
          schema,
          elems("data").as[JsArray].value.map { rowJS =>
            rowJS.as[JsArray].value.zipWithIndex.map { vJS => AlgebraJson.castJsonToPrimitive(schema(vJS._2)._2, vJS._1) }
          }
        )
        
      case "table_normal" =>
        Table(
          ID(elems("table").asInstanceOf[JsString].value), 
          elems.get("source") match {
            case None => mimir.data.LoadedTables.SCHEMA // Most tables are now sourced to LoadedTables
            case Some(source) => ID(source.asInstanceOf[JsString].value)
          },
          toSchema(elems("schema")),
          elems("metadata").asInstanceOf[JsArray].value.map { metaJson =>
            val meta = metaJson.asInstanceOf[JsObject].value

            (
              ID(meta("alias").asInstanceOf[JsString].value),
              toExpression(meta("value")),
              toType(meta("type"))
            )
          }
        )
      case "table_view" =>
        View(
          ID(elems("name").asInstanceOf[JsString].value),
          toOperator(elems("query")),
          elems("annotations").asInstanceOf[JsArray].value.map { annot =>
            ViewAnnotation.withName(annot.asInstanceOf[JsString].value)
          }.toSet
        )
      case "table_adaptive" =>
        AdaptiveView(
          ID(elems("model").asInstanceOf[JsString].value),
          ID(elems("name").asInstanceOf[JsString].value),
          toOperator(elems("query")),
          elems("annotations").asInstanceOf[JsArray].value.map { annot =>
            ViewAnnotation.withName(annot.asInstanceOf[JsString].value)
          }.toSet
        )
      case "union" =>
        Union(
          toOperator(elems("left")),
          toOperator(elems("right"))
        )
    }

  }

  def ofExpression(e: Expression): JsObject = 
  {
    e match {
      case p : PrimitiveValue =>
        JsObject(Map[String, JsValue](
          "type" -> ofType(p.getType),
          "value" -> ofPrimitive(p)
        )) 

      case Arithmetic(op, left, right) => 
        JsObject(Map[String, JsValue](
          "type" -> JsString("arithmetic"),
          "op" -> JsString(op.toString),
          "left" -> ofExpression(left),
          "right" -> ofExpression(right)
        ))

      case Comparison(op, left, right) => 
        JsObject(Map[String, JsValue](
          "type" -> JsString("comparison"),
          "op" -> JsString(op.toString),
          "left" -> ofExpression(left),
          "right" -> ofExpression(right)
        ))

      case Conditional(condition, thenClause, elseClause) => 
        JsObject(Map[String, JsValue](
          "type" -> JsString("conditional"),
          "if" -> ofExpression(condition),
          "then" -> ofExpression(thenClause),
          "else" -> ofExpression(elseClause)
        ))

      case Function(fname, args) =>
        JsObject(Map[String, JsValue](
          "type" -> JsString("function"),
          "name" -> JsString(fname.id),
          "args" -> ofExpressionList(args)
        ))

      case IsNullExpression(arg) =>
        JsObject(Map[String, JsValue](
          "type" -> JsString("is_null"),
          "arg" -> ofExpression(arg)
        ))

      case Not(arg) =>
        JsObject(Map[String, JsValue](
          "type" -> JsString("not"),
          "arg" -> ofExpression(arg)
        ))

      case JDBCVar(t) => 
        JsObject(Map[String, JsValue](
          "type" -> JsString("jdbc_var"),
          "var_type" -> ofType(t)
        ))

      case Var(name) => 
        JsObject(Map[String, JsValue](
          "type" -> JsString("var"),
          "name" -> JsString(name.id)
        ))

      case p:Proc => 
        throw new RAException(s"Can Not Serialize Procedural Expressions: $p")

      case RowIdVar() => 
        JsObject(Map[String, JsValue](
          "type" -> JsString("rowid_var")
        ))

      case VGTerm(model, idx, args, hints) =>
        JsObject(Map[String, JsValue](
          "type" -> JsString("vgterm"),
          "model" -> JsString(model.id),
          "var_index" -> JsNumber(idx),
          "arguments" -> ofExpressionList(args),
          "hints" -> ofExpressionList(hints)
        ))

      case DataWarning(name, v, message, key, idx) =>
        JsObject(Map[String, JsValue](
          "type" -> JsString("data_warning"),
          "name" -> JsString(name.id),
          "value" -> ofExpression(v),
          "message" -> ofExpression(message),
          "key" -> ofExpressionList(key),
          "idx" -> JsNumber(idx)
        ))

      case Caveat(name, v, key, message) => 
        JsObject(Map[String, JsValue](
          "type" -> JsString("caveat"),
          "name" -> JsString(name.id),
          "value" -> ofExpression(v),
          "message" -> ofExpression(message),
          "key" -> ofExpressionList(key)
        ))


      case CastExpression(expr, t) =>
        JsObject(Map[String, JsValue](
          "type"   -> JsString("cast"),
          "expr"   -> ofExpression(expr),
          "castTo" -> ofType(t)
        ))

    }
  }
  def toExpression(json: JsValue): Expression = 
  {
    val fields = json.asInstanceOf[JsObject].value
    fields("type").asInstanceOf[JsString].value match {

      case "arithmetic" => 
        Arithmetic(
          Arith.withName(fields("op").asInstanceOf[JsString].value),
          toExpression(fields("left")),
          toExpression(fields("right"))
        )

      case "comparison" => 
        Comparison(
          Cmp.withName(fields("op").asInstanceOf[JsString].value),
          toExpression(fields("left")),
          toExpression(fields("right"))
        )

      case "conditional" => 
        Conditional(
          toExpression(fields("if")),
          toExpression(fields("then")),
          toExpression(fields("else"))
        )

      case "function" => 
        Function(
          ID(fields("name").asInstanceOf[JsString].value),
          toExpressionList(fields("args"))
        )

      case "cast" =>
        CastExpression(
          toExpression(fields("expr")),
          toType(fields("castTo"))
        )

      case "is_null" =>
        IsNullExpression(toExpression(fields("arg")))

      case "not" =>
        Not(toExpression(fields("arg")))
      
      case "jdbc_var" =>
        JDBCVar(toType(fields("var_type")))
      
      case "var" =>
        Var(ID(fields("name").asInstanceOf[JsString].value))

      case "rowid_var" =>
        RowIdVar()

      case "vgterm" =>
        VGTerm(
          ID(fields("model").asInstanceOf[JsString].value),
          fields("var_index").asInstanceOf[JsNumber].value.toLong.toInt,
          toExpressionList(fields("arguments")),
          toExpressionList(fields("hints"))
        )

      case "data_warning" => 
        DataWarning(
          ID(fields("name").asInstanceOf[JsString].value),
          toExpression(fields("value")),
          toExpression(fields("message")),
          toExpressionList(fields("key")),
          fields.get("idx")
                .map { _.asInstanceOf[JsNumber].value.toLong.toInt }
                .getOrElse(0)
        )

      case "caveat" => 
        Caveat(
          ID(fields("name").asInstanceOf[JsString].value),
          toExpression(fields("value")),
          toExpressionList(fields("key")),
          toExpression(fields("message"))
        )

      // fall back to treating it as a primitive type
      case t => 
        AlgebraJson.castJsonToPrimitive(Type.fromString(t), fields("value"))

    }
  }

  def ofExpressionList(e: Seq[Expression]): JsArray = 
    JsArray(e.map { ofExpression(_) })
  def toExpressionList(json: JsValue): Seq[Expression] = 
    json.asInstanceOf[JsArray].value.map { toExpression(_) }

  def ofSchema(schema: Seq[(ID,Type)]): JsArray = 
    JsArray(schema.map { case (name, t) =>
      JsObject(Map("name" -> JsString(name.id), "type" -> ofType(t)))
    })
  def toSchema(json: JsValue): Seq[(ID,Type)] = 
    json.asInstanceOf[JsArray].value.map { elem => 
      val fields = elem.asInstanceOf[JsObject].value
      (
        ID(fields("name").asInstanceOf[JsString].value),
        toType(fields("type"))
      )
    }

  def ofAnnotationType(at: ViewAnnotation.T): JsValue = 
    JsString(at.toString())

  def toAnnotationType(json: JsValue): ViewAnnotation.T = 
    ViewAnnotation.withName(json.asInstanceOf[JsString].value)
    
  def ofType(t: Type): JsValue = 
    JsString(Type.toString(t))

  def toType(json: JsValue): Type = 
    Type.fromString(json.asInstanceOf[JsString].value)

  def ofPrimitive(p: PrimitiveValue): JsValue =
  {
    p match {
      case NullPrimitive() => JsNull
      case IntPrimitive(i) => JsNumber(i)
      case FloatPrimitive(f) => JsNumber(f)
      case StringPrimitive(s) => JsString(s)
      case BoolPrimitive(b) => JsBoolean(b)
      case DatePrimitive(y,m,d) => JsString(f"$y%04d-$m%02d-$d%02d")
      case TimestampPrimitive(y,m,d,hr,min,sec,ms) => JsString(f"$y%04d-$m%02d-$d%02d $hr%02d:$min%02d:$sec%02d.$ms%04d")
      case IntervalPrimitive(p) => JsString(p.toString())
      case RowIdPrimitive(r) => JsString(r)
      case TypePrimitive(t) => JsString(t.toString)
    }
  }

 }

object AlgebraJson {
  implicit val readsExpression: Reads[Expression] = 
    JsPath.read[JsObject].map { AlgebraJsonCodecs.toExpression(_) }
  implicit val writesExpression = 
    new Writes[Expression]{ def writes(e:Expression) = AlgebraJsonCodecs.ofExpression(e) }
  implicit val readsOperator: Reads[Operator] = 
    JsPath.read[JsObject].map { AlgebraJsonCodecs.toOperator(_) }
  implicit val writesOperator = 
    new Writes[Operator]{ def writes(o:Operator) = AlgebraJsonCodecs.ofOperator(o) }
  implicit val readsType: Reads[Type] = 
    JsPath.read[JsObject].map { AlgebraJsonCodecs.toType(_) }
  implicit val writesType = 
    new Writes[Type]{ def writes(t:Type) = AlgebraJsonCodecs.ofType(t) }

  implicit val intPrimitiveReads   : Reads[IntPrimitive   ] = JsPath.read[Long]  .map { IntPrimitive(_) }
  implicit val floatPrimitiveReads : Reads[FloatPrimitive ] = JsPath.read[Double].map { FloatPrimitive(_) }
  implicit val stringPrimitiveReads: Reads[StringPrimitive] = JsPath.read[String].map { StringPrimitive(_) }
  implicit val typePrimitiveReads  : Reads[TypePrimitive  ] = JsPath.read[String].map { Type.fromString(_) }.map { TypePrimitive(_) }
  implicit val rowidPrimitiveReads : Reads[RowIdPrimitive ] = JsPath.read[String].map { RowIdPrimitive(_) }
  implicit val boolPrimitiveReads  : Reads[BoolPrimitive] = (
    JsPath.read[Boolean] or
    JsPath.read[String].map { _.toLowerCase }.map { 
      case "yes" | "true" => true
      case "no" | "false" => false
    }
  ).map { BoolPrimitive(_) }
  implicit val datePrimitiveReads  : Reads[DatePrimitive] = (
    (JsPath \ "year").read[Int] and
    (JsPath \ "month").read[Int] and
    (JsPath \ "date").read[Int]
  )( DatePrimitive.apply _ )
  implicit val timePrimitiveReads  : Reads[TimestampPrimitive] = (
    (JsPath \ "year").read[Int] and
    (JsPath \ "month").read[Int] and
    (JsPath \ "date").read[Int] and
    (JsPath \ "hour").readNullable[Int].map { _.getOrElse(0) } and
    (JsPath \ "min").readNullable[Int].map { _.getOrElse(0) } and
    (JsPath \ "sec").readNullable[Int].map { _.getOrElse(0) } and
    (JsPath \ "msec").readNullable[Int].map { _.getOrElse(0) }
  )( TimestampPrimitive.apply _ )
  implicit val primitiveValueReads : Reads[PrimitiveValue] = (
    JsPath.read[FloatPrimitive].map { _.asInstanceOf[PrimitiveValue] } or 
    JsPath.read[Boolean].map { BoolPrimitive(_).asInstanceOf[PrimitiveValue] } or 
    JsPath.read[TimestampPrimitive].map { _.asInstanceOf[PrimitiveValue] } or
    JsPath.read[StringPrimitive].map { _.asInstanceOf[PrimitiveValue] }
  )

  implicit val primitiveValueWrites = new Writes[PrimitiveValue] { def writes(p:PrimitiveValue) = p match {
    case _:NullPrimitive => JsNull
    case x:IntPrimitive => JsNumber(x.v)
    case x:FloatPrimitive => JsNumber(x.v)
    case x:StringPrimitive => JsString(x.v)
    case x:TypePrimitive => JsString(x.t.toString)
    case x:RowIdPrimitive => JsString(x.v)
    case x:BoolPrimitive => JsBoolean(x.v)
    case DatePrimitive(y, m, d) => JsObject(Map[String,JsValue](
      "year" -> JsNumber(y),
      "month" -> JsNumber(m),
      "date" -> JsNumber(d)
    ))
    case TimestampPrimitive(y, m, d, hh, mm, ss, ms) => JsObject(Map[String,JsValue](
      "year"  -> JsNumber(y),
      "month" -> JsNumber(m),
      "date"  -> JsNumber(d),
      "hour"  -> JsNumber(hh),
      "min"   -> JsNumber(mm),
      "sec"   -> JsNumber(ss),
      "msec"  -> JsNumber(ms)
    ))
    case x:IntervalPrimitive => JsString(x.toString)
  }}

  def castJsonToPrimitive(t: Type, json: JsValue): PrimitiveValue =
  {
    (json,t) match {
      case (JsNull, _)              => NullPrimitive()

      case (JsNumber(v), TInt())    => IntPrimitive(v.toInt)
      case (JsNumber(v), TFloat())  => FloatPrimitive(v.toDouble)
      case (JsNumber(v), TString()) => StringPrimitive(v.toString)
      case (JsNumber(_), _)         => throw new IllegalArgumentException(s"Invalid JSON ($json) for Type $t")

      case (JsString(v), _)         => TextUtils.parsePrimitive(t, v)

      case (JsBoolean(v), TBool())  => BoolPrimitive(v)
      case (JsBoolean(v), _)        => throw new IllegalArgumentException(s"Invalid JSON ($json) for Type $t")

      case (JsArray(_), _)          => throw new IllegalArgumentException(s"Invalid JSON ($json) for Type $t")
      case (JsObject(_), _)         => throw new IllegalArgumentException(s"Invalid JSON ($json) for Type $t")
    }
  }
}
