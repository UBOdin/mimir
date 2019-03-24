package mimir.parser

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
import fastparse.Parsed

object MimirSQL
{
  
  def apply(input: Reader): Iterator[Parsed[MimirStatement]] = 
    new StreamParser[MimirStatement](
      parse(_:Iterator[String], statement(_)), 
      input
    )
  def apply(input: String): Parsed[MimirStatement] = 
    parse(input, statement(_))

  def Select(input: String): Select =
    apply(input) match {
      case Parsed.Success(SQLStatement(select: Select), _) => select
      case _ => throw new SQLException(s"Invalid query $input")
    }
  def Get(input: String): MimirStatement =
    apply(input) match {
      case Parsed.Success(stmt, _) => stmt
      case _ => throw new SQLException(s"Invalid statement $input")
    }
  def Get(input: Reader): Iterator[MimirStatement] =
    apply(input).map {
      case Parsed.Success(stmt, _) => stmt
      case _ => throw new SQLException(s"Invalid statement $input")
    }

  def Expression(input: String): sparsity.expression.Expression =
    sparsity.parser.Expression(input)

  
  def directive[_:P]: P[MimirStatement] = P(
    Components.slashCommand | statement
  )

  def statement[_:P]: P[MimirStatement] = P(
    Pass()~ // Strip off leading whitespace
    (
      (&(StringInIgnoreCase("ANALYZE") ~ StringInIgnoreCase("FEATURES")) ~
       analyzeFeatures)
    | analyze
    | compare
    | (&(StringInIgnoreCase("CREATE")) ~/ (
          (&(StringInIgnoreCase("ADAPTIVE")) ~/ createAdaptive)
        | (&(StringInIgnoreCase("LENS")) ~/ createLens)
        | basicStatement
      ))
    | (&(StringInIgnoreCase("DROP")) ~/ (
          (&(StringInIgnoreCase("ADAPTIVE")) ~/ dropAdaptive)
        | (&(StringInIgnoreCase("LENS")) ~/ dropLens)
        | basicStatement
      ))
    | drawPlot
    | load
    | feedback
    | basicStatement
    ) ~ ";"
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
    StringInIgnoreCase("ANALYZE") ~
    StringInIgnoreCase("FEATURES") ~/
    StringInIgnoreCase("IN") ~
    SQL.select.map { AnalyzeFeatures(_) }
  )
  def analyze[_:P] = P(
    (
      StringInIgnoreCase("ANALYZE") ~
      (
        StringInIgnoreCase("WITH") ~/
        StringInIgnoreCase("ASSIGNMENTS").map { _ => true }
      ).?.map { _.getOrElse(false) } ~
      (
        Sparsity.identifier ~
        StringInIgnoreCase("OF")
      ).? ~
      (
        Sparsity.quotedString.map { StringPrimitive(_) } ~
        StringInIgnoreCase("IN")
      ).? ~
      SQL.select
    ).map { case (withAssignments, column, row, query) =>
      Analyze(query, column, row, withAssignments)
    }
  )
  def compare[_:P] = P(
    (
      StringInIgnoreCase("COMPARE") ~/
      SQL.select ~
      StringInIgnoreCase("WITH") ~/
      SQL.select
    ).map { case (target, expected) => 
      Compare(target, expected)
    }
  )

  def createAdaptive[_:P] = P(
    (
      StringInIgnoreCase("CREATE") ~
      StringInIgnoreCase("ADAPTIVE") ~/
      StringInIgnoreCase("SCHEMA") ~/
      Sparsity.identifier ~
      StringInIgnoreCase("AS") ~/
      SQL.select ~
      StringInIgnoreCase("WITH") ~/
      Sparsity.identifier ~
      "(" ~ ExprParser.expressionList ~ ")"
    ).map { case (name, query, lensType, args) =>
      CreateAdaptiveSchema(name, query, lensType, args)
    }
  )
  def createLens[_:P] = P(
    (
      StringInIgnoreCase("CREATE") ~
      StringInIgnoreCase("LENS") ~/
      Sparsity.identifier ~
      StringInIgnoreCase("AS") ~/
      SQL.select ~
      StringInIgnoreCase("WITH") ~/
      Sparsity.identifier ~
      "(" ~ ExprParser.expressionList ~ ")"
    ).map { case (name, query, lensType, args) => 
      CreateLens(name, query, lensType, args)
    }
  )

  def dropAdaptive[_:P] = P(
    (
      StringInIgnoreCase("DROP") ~
      StringInIgnoreCase("ADAPTIVE") ~/
      StringInIgnoreCase("SCHEMA") ~/
      SQL.ifExists ~
      Sparsity.identifier
    ).map { case (ifExists, name) =>
      DropAdaptiveSchema(name, ifExists)
    }
  )

  def dropLens[_:P] = P(
    (
      StringInIgnoreCase("DROP") ~
      StringInIgnoreCase("LENS") ~/
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
      StringInIgnoreCase("DRAW") ~/
      StringInIgnoreCase("PLOT") ~/
      SQL.fromElement ~
      (
        StringInIgnoreCase("WITH") ~/
        argument.rep( sep = "," )
      ).?.map { _.toSeq.flatten } ~
      (
        StringInIgnoreCase("USING") ~/
        plotLine.rep( sep = "," )
      ).?.map { _.toSeq.flatten }
    ).map { case (body, args, lines) => 
      DrawPlot(body, lines, args)
    }
  )
  def load[_:P] = P(
    (
      StringInIgnoreCase("LOAD") ~/
      Sparsity.quotedString ~
      (
        StringInIgnoreCase("AS") ~/
        Sparsity.identifier ~/
        (
          "(" ~/ argument.rep( sep = "," ) ~ ")"
        ).?.map { _.getOrElse(Seq()) }
      ).? ~
      (
        StringInIgnoreCase("INTO") ~/
        Sparsity.identifier
      ).?
    ).map { 
      case (file, Some((format, args)), target) =>
        Load(file, target, Some(format), args)
      case (file, None, target) =>
        Load(file, target, None, Seq())
    }
  )
  def feedback[_:P] = ???

}