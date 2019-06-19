package mimir.parser

import org.specs2.mutable._

import mimir.util.LoggerUtils
import sparsity.Name
import sparsity.statement.Select
import sparsity.select.{ SelectBody, SelectExpression, FromTable }
import sparsity.expression._

class EndlessParserSpec extends Specification
{

  "The Endless Parser" >> {

    "Support basic parsing" >> {

      val p = new EndlessParser()

      p.load("\\foo\n") 
      p.next() should be equalTo(
        EndlessParserCommand(SlashCommand("foo"))
      )
      p.next() should be equalTo(
        EndlessParserNeedMoreData()
      )
      p.isEmpty should beTrue
      p.load("\\bar\n")
      p.load("\\baz\n")
      p.isEmpty should beFalse
      p.next() should be equalTo(
        EndlessParserCommand(SlashCommand("bar"))
      )
      p.next() should be equalTo(
        EndlessParserCommand(SlashCommand("baz"))
      )
      p.next() should be equalTo(
        EndlessParserNeedMoreData()
      )
      p.load("SELECT 1;")
      p.next() should be equalTo(
        EndlessParserCommand(SQLCommand(SQLStatement(
          Select(
            SelectBody(
              target = Seq(SelectExpression(LongPrimitive(1)))
            )
          )
        )))
      )
      p.load("SELECT A\n")
      p.next() should be equalTo(
        EndlessParserNeedMoreData()
      )
      p.load("FROM R;")
      p.next() should be equalTo(
        EndlessParserCommand(SQLCommand(SQLStatement(
          Select(
            SelectBody(
              target = Seq(SelectExpression(Column(Name("A")))),
              from = Seq(FromTable(None, Name("R"), None))
            )
          )
        )))
      )
      p.load("SELECT A AS AS AS\n")
      p.next() should beAnInstanceOf[EndlessParserParseError]
      p.load("SELECT 1;\n")
      p.next() should beAnInstanceOf[EndlessParserCommand]

    }

  }

}