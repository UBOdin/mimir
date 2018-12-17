package mimir.serialization

import play.api.libs.json._

import mimir.Database
import mimir.algebra._
import mimir.algebra.typeregistry.TypeRegistry
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

      case HardTable(schema,data) => 
        JsObject(Map[String, JsValue](
          "type" -> JsString("table_hardcoded"),
          "schema" -> ofSchema(schema),
          "data" -> JsArray(data.map( row => JsArray( row.map( hv => ofPrimitive(hv)))))
        ))

      case View(name, query, annotations) => 
        JsObject(Map[String,JsValue](
          "type" -> JsString("table_view"),
          "name" -> JsString(name),
          "query" -> ofOperator(query),
          "annotations" -> JsArray(annotations.toSeq.map { _.toString }.map { JsString(_) })
        ))

      case AdaptiveView(model, name, query, annotations) => 
        JsObject(Map[String,JsValue](
          "type" -> JsString("table_adaptive"),
          "model" -> JsString(model),
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
            "annotations" -> JsArray(annotations.map ( annotation => {
                JsObject(Map[String, JsValue](
                  "name" -> JsString(annotation._1),
                  "annotation" -> ofAnnotation(annotation._2) 
                )) 
              }
            ))
          
        ))

      case Recover(source, annotations) => 
        JsObject(Map[String, JsValue](
          "type" -> JsString("recover"),
          "source" -> ofOperator(source),
          "annotations" -> JsArray(annotations.map ( annotation => {
                JsObject(Map[String, JsValue](
                  "name" -> JsString(annotation._1),
                  "annotation" -> ofAnnotation(annotation._2) 
                )) 
              }
            ))
        ))


    }
  }
  def toOperator(json: JsValue, types:TypeRegistry): Operator = 
  {
    val elems = json.asInstanceOf[JsObject].value// .asInstanceOf[JsObject].value
    elems("type").asInstanceOf[JsString].value match {
      case "aggregate" =>
        Aggregate(
          elems("gb_columns").asInstanceOf[JsArray].value.map { toExpression(_, types).asInstanceOf[Var] },
          elems("agg_columns").asInstanceOf[JsArray].value.map { fieldJson => 
            val fields = fieldJson.asInstanceOf[JsObject].value

            AggFunction(
              fields("function").asInstanceOf[JsString].value,
              fields("distinct").asInstanceOf[JsBoolean].value,
              fields("args").asInstanceOf[JsArray].value.map { toExpression(_, types) },
              fields("alias").asInstanceOf[JsString].value
            )
          },
          toOperator(elems("source"), types)
        )
      case "annotate" =>
        Annotate(
          toOperator(elems("source"), types),
          elems("annotations").asInstanceOf[JsArray].value.map( annot => {
            val nameAnnot = annot.asInstanceOf[JsObject].value
            ( nameAnnot("name").asInstanceOf[JsString].value, toAnnotation( nameAnnot("annotation").asInstanceOf[JsObject], types) ) 
            })
        )
      case "join" =>
        Join(
          toOperator(elems("left"), types),
          toOperator(elems("right"), types)
        )
      case "join_left_outer" =>
        LeftOuterJoin(
          toOperator(elems("left"), types),
          toOperator(elems("right"), types),
          toExpression(elems("condition"), types)
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
          toOperator(elems("source"), types)
        )
      case "project" =>
        Project(
          elems("columns").asInstanceOf[JsArray].value.map { fieldJson =>
            val fields = fieldJson.asInstanceOf[JsObject].value

            ProjectArg(
              fields("name").asInstanceOf[JsString].value,
              toExpression(fields("expression"), types)
            )
          },
          toOperator(elems("source"), types)
        )
      case "sort" =>
        Sort(
          elems("sort_by").asInstanceOf[JsArray].value.map { fieldJson =>
            val fields = fieldJson.asInstanceOf[JsObject].value

            SortColumn(
              toExpression(fields("expression"), types),
              fields("ascending").asInstanceOf[JsBoolean].value
            )
          },
          toOperator(elems("source"), types)
        )
      case "select" =>
        Select(
          toExpression(elems("condition"), types),
          toOperator(elems("source"), types)
        )
      case "table_hardcoded" =>
        val schema = toSchema(elems("schema"), types)
        HardTable(
          schema,
          elems("data").as[JsArray].value.map { rowJS =>
            rowJS.as[JsArray].value.zipWithIndex.map { vJS => toPrimitive(types.rootType(schema(vJS._2)._2), vJS._1) }
          }
        )
        
      case "table_normal" =>
        Table(
          elems("table").asInstanceOf[JsString].value, 
          elems("alias").asInstanceOf[JsString].value,
          toSchema(elems("schema"), types),
          elems("metadata").asInstanceOf[JsArray].value.map { metaJson =>
            val meta = metaJson.asInstanceOf[JsObject].value

            (
              meta("alias").asInstanceOf[JsString].value,
              toExpression(meta("value"), types),
              toType(meta("type"), types)
            )
          }
        )
      case "table_view" =>
        View(
          elems("name").asInstanceOf[JsString].value,
          toOperator(elems("query"), types),
          elems("annotations").asInstanceOf[JsArray].value.map { annot =>
            ViewAnnotation.withName(annot.asInstanceOf[JsString].value)
          }.toSet
        )
      case "table_adaptive" =>
        AdaptiveView(
          elems("model").asInstanceOf[JsString].value,
          elems("name").asInstanceOf[JsString].value,
          toOperator(elems("query"), types),
          elems("annotations").asInstanceOf[JsArray].value.map { annot =>
            ViewAnnotation.withName(annot.asInstanceOf[JsString].value)
          }.toSet
        )
      case "union" =>
        Union(
          toOperator(elems("left"), types),
          toOperator(elems("right"), types)
        )
    }

  }

  def ofAnnotation(annot: AnnotateArg): JsObject = 
    JsObject(Map[String, JsValue](
      "annotation_type" -> ofAnnotationType(annot.annotationType),
      "name"       -> JsString(annot.name),
      "type"       -> ofType(annot.typ),
      "expression" -> ofExpression(annot.expr)
    ))
  def toAnnotation(json: JsValue, types: TypeRegistry): AnnotateArg = 
  {
    val fields = json.asInstanceOf[JsObject].value
    AnnotateArg(
      toAnnotationType(fields("annotation_type")),
      fields("name").asInstanceOf[JsString].value,
      toType(fields("type"), types),
      toExpression(fields("expression"), types)
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
  def toExpression(json: JsValue, types: TypeRegistry): Expression = 
  {
    val fields = json.asInstanceOf[JsObject].value
    fields("type").asInstanceOf[JsString].value match {

      case "arithmetic" => 
        Arithmetic(
          Arith.withName(fields("op").asInstanceOf[JsString].value),
          toExpression(fields("left"), types),
          toExpression(fields("right"), types)
        )

      case "comparison" => 
        Comparison(
          Cmp.withName(fields("op").asInstanceOf[JsString].value),
          toExpression(fields("left"), types),
          toExpression(fields("right"), types)
        )

      case "conditional" => 
        Conditional(
          toExpression(fields("if"), types),
          toExpression(fields("then"), types),
          toExpression(fields("else"), types)
        )

      case "function" => 
        Function(
          fields("name").asInstanceOf[JsString].value,
          toExpressionList(fields("args"), types)
        )

      case "is_null" =>
        IsNullExpression(toExpression(fields("arg"), types))

      case "not" =>
        Not(toExpression(fields("arg"), types))
      
      case "jdbc_var" =>
        JDBCVar(toType(fields("var_type"), types))
      
      case "var" =>
        Var(fields("name").asInstanceOf[JsString].value)

      case "rowid_var" =>
        RowIdVar()

      case "vgterm" =>
        VGTerm(
          fields("model").asInstanceOf[JsString].value,
          fields("var_index").asInstanceOf[JsNumber].value.toLong.toInt,
          toExpressionList(fields("arguments"), types),
          toExpressionList(fields("hints"), types)
        )

      // fall back to treating it as a primitive constant
      case t => 
        toPrimitive(types.rootType(types.fromString(t)), fields("value"))

    }
  }

  def ofExpressionList(e: Seq[Expression]): JsArray = 
    JsArray(e.map { ofExpression(_) })
  def toExpressionList(json: JsValue, types: TypeRegistry): Seq[Expression] = 
    json.asInstanceOf[JsArray].value.map { toExpression(_, types) }

  def ofSchema(schema: Seq[(String,Type)]): JsArray = 
    JsArray(schema.map { case (name, t) =>
      JsObject(Map("name" -> JsString(name), "type" -> ofType(t)))
    })
  def toSchema(json: JsValue, types: TypeRegistry): Seq[(String,Type)] = 
    json.asInstanceOf[JsArray].value.map { elem => 
      val fields = elem.asInstanceOf[JsObject].value
      (
        fields("name").asInstanceOf[JsString].value,
        toType(fields("type"), types)
      )
    }

  def ofAnnotationType(at: ViewAnnotation.T): JsValue = 
    JsString(at.toString())

  def toAnnotationType(json: JsValue): ViewAnnotation.T = 
    ViewAnnotation.withName(json.asInstanceOf[JsString].value)
    
  def ofType(t: Type): JsValue = 
    JsString(t.toString)

  def toType(json: JsValue, types: TypeRegistry): Type = 
  {
    val name = json.asInstanceOf[JsString].value
    BaseType.fromString(name)
      .getOrElse { 
        if(types supportsUserType name) { TUser(name) }
        else {
          throw new IllegalArgumentException("Invalid Type: "+name)
        }
      }
  }

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

  def toPrimitive(t: BaseType, json: JsValue): PrimitiveValue =
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
