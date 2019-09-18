package mimir.parser

import org.specs2.specification._
import org.specs2.matcher.FileMatchers
import org.specs2.mutable._

import sparsity.parser.SQL

import mimir.algebra._
import mimir.test._
import mimir.data.FileFormat


class SqlParserRegressions 
  extends SQLTestSpecification("SqlParserRegressions")
  with FileMatchers
  with BeforeAll
{
  def beforeAll = 
  {
    db.views.create(
      name = ID("R"),
      query = HardTable(
        Seq(ID("Alphabet") -> TInt(), ID("B") -> TInt()),
        Seq()
      )
    )
  }

  "The SQL to RA converter" >> {

    "handle case-insensitive group-by attributes" >> {
      val ideal = db.sqlToRA(MimirSQL.Select("SELECT ALPHABET FROM R GROUP BY ALPHABET"))

      db.sqlToRA(MimirSQL.Select("SELECT ALPHABET FROM R GROUP BY ALPHABET")) must beEqualTo(ideal)
      db.sqlToRA(MimirSQL.Select("SELECT Alphabet FROM R GROUP BY ALPHABET")) must beEqualTo(ideal)
      db.sqlToRA(MimirSQL.Select("SELECT Alphabet FROM R GROUP BY r.ALPHABET")) must beEqualTo(ideal)
    }

  }
}