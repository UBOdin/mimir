package mimir.parser

import play.api.libs.json.JsNull
import fastparse._, MultiLineWhitespace._
import sparsity.parser.{
  SQL,
  StreamParser,
  Elements => Sparsity,
  Expression => ExprParser
}
import sparsity.statement.{Statement, Select}
import sparsity.expression.{Expression,StringPrimitive}
import sparsity.Name
import java.sql.SQLException
import java.io.Reader
import mimir.sql.SqlToRA
import mimir.algebra.ID

object MimirSQL
{
  
  def apply(input: Reader): Iterator[Parsed[MimirStatement]] = 
    new StreamParser[MimirStatement](
      parse(_:Iterator[String], terminatedStatement(_), verboseFailures = true), 
      input
    )
  def apply(input: String): Parsed[MimirStatement] = 
    parse(input, statement(_))

  def Select(input: String): Select =
    apply(input) match {
      case Parsed.Success(SQLStatement(select: Select), _) => select
      case Parsed.Success(_, _) => throw new SQLException(s"Invalid query (not a select) $input")
      case Parsed.Failure(msg, idx, extra) => throw new SQLException(s"Invalid query (failure @ $idx: ${extra.trace().longMsg}) $input")
    }
  def Get(input: String): MimirStatement =
    apply(input) match {
      case Parsed.Success(stmt, _) => stmt
      case Parsed.Failure(msg, idx, extra) => throw new SQLException(s"Invalid query (failure @ $idx: ${extra.trace().longMsg}) $input")
    }
  def Get(input: Reader): Iterator[MimirStatement] =
    apply(input).map {
      case Parsed.Success(stmt, _) => stmt
      case f@Parsed.Failure(msg, idx, extra) => throw new SQLException(s"Invalid query (failure @ $idx: ${f.longMsg})")
    }

  def Expression(input: String): sparsity.expression.Expression =
    sparsity.parser.Expression(input)
  def ExpressionList(input: String): Seq[sparsity.expression.Expression] =
    parse(input, sparsity.parser.Expression.expressionList(_)) match { 
      case Parsed.Success(exprList, index) => exprList
      case f:Parsed.Failure => throw new sparsity.parser.ParseException(f)
    }

  def terminatedStatement[_:P]: P[MimirStatement] = P(
    statement ~ ";"
  )

  def statement[_:P]: P[MimirStatement] = P(
    Pass()~ // Strip off leading whitespace
    (
      analyzeFeatures // must come before 'analyze'
    | analyze
    | alterTable
    | compare
    | createLens     // must come before 'basicStatement'
    | createSample   // must come before 'basicStatement'
    | dropLens       // must come before 'basicStatement'
    | drawPlot
    | load
    | reload
    | feedback
    | basicStatement
    ) 
  )

  def argument[_:P] = P(
    Sparsity.identifier.map { _.upper } ~ 
    "=" ~/ 
    ExprParser.primitive
  )

  def basicStatement[_:P] = P(
    SQL.statement.map { SQLStatement(_) }
  )

  def analyzeFeatures[_:P] = P(
    MimirKeyword("ANALYZE") ~
    MimirKeyword("FEATURES") ~/
    MimirKeyword("IN") ~
    SQL.select.map { AnalyzeFeatures(_) }
  )
  def analyze[_:P] = P(
    (
      MimirKeyword("ANALYZE") ~
      (
        MimirKeyword("WITH") ~/
        MimirKeyword("ASSIGNMENTS").map { _ => true }
      ).?.map { _.getOrElse(false) } ~
      (
        Sparsity.identifier ~
        MimirKeyword("OF")
      ).? ~
      (
        Sparsity.quotedString.map { StringPrimitive(_) } ~
        MimirKeyword("IN")
      ).? ~
      SQL.select
    ).map { case (withAssignments, column, row, query) =>
      Analyze(query, column, row, withAssignments)
    }
  )

  def alterTableOperation[_:P] = P[AlterTableOperation](
    ( 
      MimirKeyword("CREATE") ~ MimirKeyword("DEPENDENCY") ~/
      Sparsity.dottedPair.map { schemaAndTable => CreateDependency(schemaAndTable._1, 
                                                                   schemaAndTable._2)}
    ) | (
      MimirKeyword("DROP") ~ MimirKeyword("DEPENDENCY") ~/
      Sparsity.dottedPair.map { schemaAndTable => DropDependency(schemaAndTable._1, 
                                                                 schemaAndTable._2)}
    )
  )
  
  def alterTable[_:P] = P(
    (
      MimirKeyword("ALTER") ~
      MimirKeyword("TABLE") ~/
      Sparsity.dottedPair ~
      alterTableOperation
    ).map { case (targetSchema, target, operator) => AlterTable(targetSchema, target, operator) }
  )

  def compare[_:P] = P(
    (
      MimirKeyword("COMPARE") ~/
      SQL.select ~
      MimirKeyword("WITH") ~/
      SQL.select
    ).map { case (target, expected) => 
      Compare(target, expected)
    }
  )

  def createLens[_:P] = P(
    (
      MimirKeyword("CREATE") ~
      MimirKeyword("LENS") ~/
      Sparsity.identifier ~
      MimirKeyword("AS") ~/
      SQL.select ~
      MimirKeyword("WITH") ~/
      Sparsity.identifier ~
      (
        "(" ~ JsonParser.jsonExpr ~ ")"
      ).?.map { _.getOrElse { JsNull } }
    ).map { case (name, query, lensType, args) => 
      CreateLens(name, query, lensType, args)
    }
  ) 

  def basicSample[_:P] = P(
    (
      MimirKeyword("FRACTION") ~/ "=".? ~/
      Sparsity.decimal
    ).map { mimir.algebra.sampling.SampleRowsUniformly(_) }
  )

  def simpleStratifiedSample[_:P] = P(
    (
      MimirKeyword("STRATIFIED") ~/
      MimirKeyword("ON") ~/
      Sparsity.identifier ~
      "(" ~/ (
        ExprParser.primitive ~ "~" ~/
        Sparsity.decimal
      ).rep(sep = Sparsity.comma) ~ ")"
    ).map { case (col, rawStrata) => 
      if(rawStrata.isEmpty){
        throw new SQLException("CREATE SAMPLE _ STRATIFIED ON requires one or more strata probabilities")
      }
      val (rawStrataBins, strataProbabilities) = rawStrata.unzip
      val strataBins = rawStrataBins.map { SqlToRA.convertPrimitive(_) }

      val strataBinTypes = strataBins
      //Ensure all of the strata bins are of a consistent type
      val t = strataBins(0).getType
      if(strataBins.exists { !_.getType.equals(t) }){
        throw new SQLException("CREATE SAMPLE _ STRATIFIED ON requires all strata bin identifiers to be of the same type")
      }

      mimir.algebra.sampling.SampleStratifiedOn(
        ID.upper(col),
        t,
        strataBins.zip(strataProbabilities).toMap
      )
    }
  )

  def samplingMode[_:P] = P[mimir.algebra.sampling.SamplingMode](
      basicSample
    | simpleStratifiedSample
  )

  def createSample[_:P] = P(
    (
      MimirKeyword("CREATE") ~
      (MimirKeyword("OR") ~ MimirKeyword("REPLACE")).!.?.map { !_.isEmpty } ~
      MimirKeyword("SAMPLE") ~/
      MimirKeyword("VIEW").!.?.map { !_.isEmpty } ~/
      Sparsity.identifier ~
      MimirKeyword("FROM") ~/
      Sparsity.identifier ~
      MimirKeyword("WITH") ~/
      samplingMode
    ).map { case (orReplace, asView, name, source, mode) =>
      CreateSample(name, mode, source, orReplace, asView)
    }

  )

  def dropLens[_:P] = P(
    (
      MimirKeyword("DROP") ~
      MimirKeyword("LENS") ~/
      SQL.ifExists ~
      Sparsity.identifier
    ).map { case (ifExists, name) =>
      DropLens(name, ifExists)
    }
  )

  def plotLine[_:P] = P(
    (
      "(" ~/ 
      ExprParser.expression ~ 
      "," ~/
      ExprParser.expression ~
      ")" ~/
      argument.rep( sep = "," )
    ).map { case (x, y, args) =>
      PlotLine(x, y, args)
    }
  )

  def drawPlot[_:P] = P(
    (
      MimirKeyword("DRAW") ~/
      MimirKeyword("PLOT") ~/
      SQL.fromElement ~
      (
        MimirKeyword("WITH") ~/
        argument.rep( sep = "," )
      ).?.map { _.toSeq.flatten } ~
      (
        MimirKeyword("USING") ~/
        plotLine.rep( sep = "," )
      ).?.map { _.toSeq.flatten }
    ).map { case (body, args, lines) => 
      DrawPlot(body, lines, args)
    }
  )
  def load[_:P] = P(
    (
      MimirKeyword("LOAD", "LINK").! ~/
      Sparsity.quotedString ~
      (
        MimirKeyword("AS") ~/
        Sparsity.identifier ~/
        (
          "(" ~/ argument.rep( sep = "," ) ~ ")"
        ).?.map { _.getOrElse(Seq()) }
      ).? ~
      (
        MimirKeyword("INTO") ~/
        Sparsity.identifier
      ).? ~
      ( 
        MimirKeyword("WITH") ~/
        MimirKeyword("STAGING").!.map { _.toUpperCase }
      ).?
    ).map { 
      case (loadOrLink, file, Some((format, args)), target, staging) =>
        Load(file, target, Some(format), args, staging != None, loadOrLink.equalsIgnoreCase("LINK"))
      case (loadOrLink, file, None, target, staging) =>
        Load(file, target, None, Seq(), staging != None, loadOrLink.equalsIgnoreCase("LINK"))
    }
  )
  def reload[_:P] = P(
    MimirKeyword("RELOAD") ~/
    Sparsity.identifier.map { Reload(_) }
  )
  def feedback[_:P] = P(
    (
      MimirKeyword("FEEDBACK") ~/
      (
        Sparsity.quotedIdentifier | 
        Sparsity.rawIdentifier.rep( sep = ":" ).map { 
          case elems if elems.isEmpty => Name("")
          case elems => elems.tail.fold(elems.head) {_ + Name(":") + _ }
        }
      ) ~
      Sparsity.integer ~
      ( "(" ~/ 
        ExprParser.primitive.rep( sep = "," ) ~ 
        ")" 
      ).?.map { _.getOrElse(Seq()) } ~
      MimirKeyword("IS") ~/
      ExprParser.primitive
    ).map { case (model, index, args, value) =>
      Feedback(model, index, args, value)
    }
  )

}
