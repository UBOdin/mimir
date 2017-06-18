package mimir.serialization

import play.api.libs.json._

import mimir.Database
import mimir.algebra._
import mimir.util._
import mimir.views.ViewAnnotation

class JsonParseException(msg: String, json: String) extends RAException(msg)

object Json
{
  def ofOperator(o: Operator): JsObject =
  {
    o match {
      case Project(args, source) => 
        JsObject(Map[String, JsValue](
          "type" -> JsString("project"),
          "columns" -> JsArray(args.map { arg => 
            JsObject(Map[String, JsValue](
              "name" -> JsString(arg.name), 
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
                "function" -> JsString(agg.function),
                "distinct" -> JsBoolean(agg.distinct),
                "args" -> JsArray(agg.args.map { ofExpression(_) }),
                "alias" -> JsString(agg.alias)
              ))
            }),
          "source" -> ofOperator(source)
        ))

      case Table(table, alias, schema, meta) => 
        JsObject(Map[String, JsValue](
          "type" -> JsString("table_normal"),
          "table" -> JsString(table),
          "alias" -> JsString(alias),
          "schema" -> ofSchema(schema),
          "metadata" -> 
            JsArray(meta.map { elem => 
              JsObject(Map(
                "alias" -> JsString(elem._1),
                "value" -> ofExpression(elem._2),
                "type"  -> ofType(elem._3)
              ))
            })
        ))

      case EmptyTable(schema) => 
        JsObject(Map[String, JsValue](
          "type" -> JsString("table_empty"),
          "schema" -> ofSchema(schema)
        ))

      case View(name, query, annotations) => 
        JsObject(Map[String,JsValue](
          "type" -> JsString("table_view"),
          "name" -> JsString(name),
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

      case ProvenanceOf(source) => 
        JsObject(Map[String, JsValue](
          "type" -> JsString("provenance_of"),
          "source" -> ofOperator(source)
        ))

      case Annotate(source, annotations) => 
        JsObject(Map[String, JsValue](
          "type" -> JsString("annotate"),
          "source" -> ofOperator(source),
          "annotations" -> JsArray(annotations.map { ofAnnotation(_) })
        ))

      case Recover(source, annotations) => 
        JsObject(Map[String, JsValue](
          "type" -> JsString("recover"),
          "source" -> ofOperator(source),
          "annotations" -> JsArray(annotations.map { ofAnnotation(_) })
        ))


    }
  }
  def toOperator(json: JsValue): Operator = 
  {
    val elems = json.as[JsObject].value
    elems("type").as[JsString].value match {
      case "aggregate" =>
        Aggregate(
          elems("gb_columns").as[JsArray].value.map { toExpression(_).asInstanceOf[Var] },
          elems("agg_columns").as[JsArray].value.map { fieldJson => 
            val fields = fieldJson.as[JsObject].value

            AggFunction(
              fields("function").as[JsString].value,
              fields("distinct").as[JsBoolean].value,
              fields("args").as[JsArray].value.map { toExpression(_) },
              fields("alias").as[JsString].value
            )
          },
          toOperator(elems("source"))
        )
      case "annotate" =>
        Annotate(
          toOperator(elems("source")),
          elems("annotations").as[JsArray].value.map { toAnnotation(_) }
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
          elems("columns").as[JsArray].value.map { fieldJson =>
            val fields = fieldJson.as[JsObject].value

            ProjectArg(
              fields("name").as[JsString].value,
              toExpression(fields("expression"))
            )
          },
          toOperator(elems("source"))
        )
      case "sort" =>
        Sort(
          elems("sort_by").as[JsArray].value.map { fieldJson =>
            val fields = fieldJson.as[JsObject].value

            SortColumn(
              toExpression(fields("sort_by")),
              fields("ascending").as[JsBoolean].value
            )
          },
          toOperator(elems("source"))
        )
      case "select" =>
        Select(
          toExpression(elems("condition")),
          toOperator(elems("source"))
        )
      case "table_empty" =>
        EmptyTable(toSchema(elems("schema")))

      case "table_normal" =>
        Table(
          elems("table").as[JsString].value, 
          elems("alias").as[JsString].value,
          toSchema(elems("schema")),
          elems("metadata").as[JsArray].value.map { metaJson =>
            val meta = metaJson.as[JsObject].value

            (
              meta("alias").as[JsString].value,
              toExpression(meta("value")),
              toType(meta("type"))
            )
          }
        )
      case "table_view" =>
        View(
          elems("name").as[JsString].value,
          toOperator(elems("query")),
          elems("annotations").as[JsArray].value.map { annot =>
            ViewAnnotation.withName(annot.as[JsString].value)
          }.toSet
        )
      case "union" =>
        Union(
          toOperator(elems("left")),
          toOperator(elems("right"))
        )
    }

  }

  def ofAnnotation(annot: AnnotateArg): JsObject = 
    JsObject(Map[String, JsValue](
      "name"       -> JsString(annot.name),
      "type"       -> ofType(annot.typ),
      "expression" -> ofExpression(annot.expr)
    ))
  def toAnnotation(json: JsValue): AnnotateArg = 
  {
    val fields = json.as[JsObject].value
    AnnotateArg(
      fields("name").as[JsString].value,
      toType(fields("type")),
      toExpression(fields("expression"))
    )
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
          "name" -> JsString(fname),
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
          "name" -> JsString(name)
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
          "model" -> JsString(model),
          "var_index" -> JsNumber(idx),
          "arguments" -> ofExpressionList(args),
          "hints" -> ofExpressionList(hints)
        ))

    }
  }
  def toExpression(json: JsValue): Expression = 
  {
    val fields = json.as[JsObject].value
    fields("type").as[JsString].value match {

      case "arithmetic" => 
        Arithmetic(
          Arith.withName(fields("op").as[JsString].value),
          toExpression(fields("left")),
          toExpression(fields("right"))
        )

      case "comparison" => 
        Comparison(
          Cmp.withName(fields("op").as[JsString].value),
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
          fields("name").as[JsString].value,
          toExpressionList(fields("args"))
        )

      case "is_null" =>
        IsNullExpression(toExpression(fields("arg")))

      case "not" =>
        Not(toExpression(fields("arg")))
      
      case "jdbc_var" =>
        JDBCVar(toType(fields("var_type")))
      
      case "var" =>
        Var(fields("name").as[JsString].value)

      case "rowid_var" =>
        RowIdVar()

      case "vgterm" =>
        VGTerm(
          fields("model").as[JsString].value,
          fields("var_index").as[JsNumber].value.toLong.toInt,
          toExpressionList(fields("arguments")),
          toExpressionList(fields("hints"))
        )

      // fall back to treating it as a primitive type
      case t => 
        toPrimitive(Type.fromString(t), fields("value"))

    }
  }

  def ofExpressionList(e: Seq[Expression]): JsArray = 
    JsArray(e.map { ofExpression(_) })
  def toExpressionList(json: JsValue): Seq[Expression] = 
    json.as[JsArray].value.map { toExpression(_) }

  def ofSchema(schema: Seq[(String,Type)]): JsArray = 
    JsArray(schema.map { case (name, t) =>
      JsObject(Map("name" -> JsString(name), "type" -> ofType(t)))
    })
  def toSchema(json: JsValue): Seq[(String,Type)] = 
    json.as[JsArray].value.map { elem => 
      val fields = elem.as[JsObject].value
      (
        fields("name").as[JsString].value,
        toType(fields("type"))
      )
    }

  def ofType(t: Type): JsValue = 
    JsString(Type.toString(t))

  def toType(json: JsValue): Type = 
    Type.fromString(json.as[JsString].value)

  def ofPrimitive(p: PrimitiveValue): JsValue =
  {
    p match {
      case NullPrimitive() => JsNull
      case IntPrimitive(i) => JsNumber(i)
      case FloatPrimitive(f) => JsNumber(f)
      case StringPrimitive(s) => JsString(s)
      case BoolPrimitive(b) => JsBoolean(b)
      case DatePrimitive(y,m,d) => JsString(f"$y%04d-$m%02d-$d%02d")
      case TimestampPrimitive(y,m,d,hr,min,sec) => JsString(f"$y%04d-$m%02d-$d%02d $hr%02d:$min%02d:$sec%02d")
      case RowIdPrimitive(r) => JsString(r)
      case TypePrimitive(t) => JsString(t.toString)
    }
  }

  def toPrimitive(t: Type, json: JsValue): PrimitiveValue =
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

  def parse(json: String): JsValue = play.api.libs.json.Json.parse(json)
}