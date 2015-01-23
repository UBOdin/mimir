package mimir.ctables;

import java.io.{StringReader,FileReader}

import net.sf.jsqlparser.parser.{CCJSqlParser}
import org.specs2.mutable._

import mimir.algebra._
import mimir.sql._

object CTablesSpec extends Specification {
  
  def sql(s: String) = { SqlToRA.convert(new CCJSqlParser(new StringReader(s)).Select()) }
  def sexpr(s: String, ctx: Operator) = { 
      SqlToRA.convert(
        new CCJSqlParser(
          new StringReader(s)).SimpleExpression(), 
        ctx.schema.keys.map( (x) => (x,x) ).toMap
      )
    }
  def bexpr(s: String, ctx: Operator) = { 
    SqlToRA.convert(
      new CCJSqlParser(
        new StringReader(s)).Expression(), 
      ctx.schema.keys.map( (x) => (x,x) ).toMap
    )
  }
  def file(f: String) = { SqlToRA.convert(new CCJSqlParser(new FileReader(f)).Select()) }
  def load(s: String) = { SqlToRA.load(new CCJSqlParser(new StringReader(s)).CreateTable()) }
  def ra(o: Operator) = RAToSql.convert(o).toString
  
  def percolate = CTables.percolate _
  
  def project(args:List[(String,String)], source: Operator):Operator = 
    Project(
      args.map ( _ match { 
        case (col, e) => ProjectArg(col, sexpr(e, source)) } ),
      source)
  def select(cond: String, source: Operator) = 
    Select(bexpr(cond, source), source)
  def join(l: Operator, r: Operator) = 
    Join(l, r)
  def table(t: String) = 
    SqlToRA.schema.get(t).get match {
      case SqlTable(sch) =>
        Table(t, sch.map( _ match { case (v, ty)=>(t+"_"+v, ty) } ).toMap)
      case SqlView(oper) => 
        oper
    }
  def tvar(x: String) = PVar(x, List[Expression]())
  def rvar(x: String, t: String): PVar = rvar(x, t, List[Expression]())
  def rvar(x: String, t: String, args: List[Expression]) = 
    PVar(x, List[Expression](Var(t+"_ROWID")) ++ args)
      
  
  "The Loader" should {
    load("CREATE TABLE R(A int, B int)")
    load("CREATE TABLE S(B int, C int)")
  }
  
  "The Percolator" should { 
    
    "work on deterministic P queries" in {
      percolate(sql(
        "SELECT A, A*B AS B FROM R"
      )) must be equalTo
        project(List(("A", "R_A"), ("B", "R_A * R_B")), 
          table("R"))
    }
    "work on deterministic SP queries" in {
      percolate(sql(
        "SELECT A, A*B AS B FROM R WHERE A = B"
      )) must be equalTo
        project(List(("A", "R_A"), ("B", "R_A * R_B")), 
          select("R_A = R_B", 
            table("R")))
    }
    "work on deterministic SPJ queries" in {
      percolate(sql(
        "SELECT A, C FROM R, S WHERE R.B = S.B"
      )) must be equalTo
        project(List(("A", "R_A"), ("C", "S_C")), 
          select("R_B = S_B", 
            join(
              table("R"),
              table("S"))))
    }
    "work on deterministic PSP queries" in {
      percolate(sql(
        "SELECT A, C FROM (SELECT A, A*B AS C FROM R) Q WHERE A = C"
      )) must be equalTo
        project(List(
            ("A", "Q_A"), 
            ("C", "Q_C")),
          select("Q_A = Q_C",
            project(List(
                ("Q_A", "R_A"), 
                ("Q_C", "R_A*R_B")), 
              table("R"))))
    }
    "work on nondeterministic P queries" in {
      percolate(sql(
          "SELECT A, B, VAR('X') AS C FROM R"
        )) must be equalTo
          Project(List(
              ProjectArg("A", Var("R_A")), 
              ProjectArg("B", Var("R_B")), 
              ProjectArg("C", tvar("X"))),
            table("R"))
    }
    "work on nondeterministic SP queries" in {
      percolate(sql(
          "SELECT A, B, VAR('X') AS C FROM R WHERE A = B"
        )) must be equalTo
          Project(List(
              ProjectArg("A", Var("R_A")), 
              ProjectArg("B", Var("R_B")), 
              ProjectArg("C", tvar("X"))),
            select("R_A = R_B",
              table("R")))
    }
    "work on nondeterministic PSP queries" in {
      percolate(sql(
        "SELECT A, C FROM (SELECT A, B, VAR('X') AS C FROM R) Q WHERE A = B"
      )) must be equalTo
        Project(List(
            ProjectArg("A", Var("R_A")), 
            ProjectArg("C", tvar("X"))),
          select("R_A = R_B",
            table("R")))
    }
    "handle nondeterministic selections" in {
      percolate(sql(
        "SELECT A, C FROM (SELECT A, B, VAR('X') AS C FROM R) Q WHERE B = C"
      )) must be equalTo
        Project(List(
            ProjectArg("A", Var("R_A")), 
            ProjectArg("C", tvar("X")),
            ProjectArg("__MIMIR_CONDITION", 
              Comparison(Cmp.Eq, Var("R_B"), tvar("X")))),
          table("R"))
    }
    "handle selections that are both deterministic and nondeterministic" in {
      percolate(sql(
        "SELECT A, C FROM (SELECT A, B, VAR('X') AS C FROM R) Q WHERE B = C AND A = B"
      )) must be equalTo
        Project(List(
            ProjectArg("A", Var("R_A")), 
            ProjectArg("C", tvar("X")),
            ProjectArg("__MIMIR_CONDITION", 
              Comparison(Cmp.Eq, Var("R_B"), tvar("X")))),
          select("R_A = R_B",
              table("R")))
    }
    "handle left-nondeterministic joins" in {
      percolate(sql(
        "SELECT * FROM (SELECT A, B, VAR('X') AS N FROM R) Q, S WHERE Q.B = S.B"
      )) must be equalTo
        Project(List(
            ProjectArg("B", Var("S_B")), 
            ProjectArg("B", Var("R_B")), 
            ProjectArg("A", Var("R_A")), 
            ProjectArg("N", tvar("X")),
            ProjectArg("C", Var("S_C"))
          ),
          select("R_B = S_B",
            join(
              table("R"),
              table("S"))))
    }
    "handle right-nondeterministic joins" in {
      percolate(sql(
        "SELECT * FROM R, (SELECT B, C, VAR('X') AS N FROM S) Q WHERE R.B = Q.B"
      )) must be equalTo
        Project(List(
            ProjectArg("B", Var("R_B")), 
            ProjectArg("B", Var("S_B")), 
            ProjectArg("N", tvar("X")),
            ProjectArg("C", Var("S_C")),
            ProjectArg("A", Var("R_A"))
          ),
          select("R_B = S_B",
            join(
              table("R"),
              table("S"))))
    }
    "handle full-nondeterministic joins" in {
      percolate(sql(
        "SELECT * FROM (SELECT A, B, VAR('X') AS N FROM R) Q1, (SELECT B, C, VAR('Y') AS M FROM S) Q2 WHERE Q1.B = Q2.B"
      )) must be equalTo
        Project(List(
            ProjectArg("A", Var("R_A")), 
            ProjectArg("C", Var("S_C")),
            ProjectArg("B", Var("R_B")), 
            ProjectArg("N", tvar("X")),
            ProjectArg("B", Var("S_B")), 
            ProjectArg("M", tvar("Y"))
          ),
          select("R_B = S_B",
            join(
              table("R"),
              table("S"))))
    }
    "handle full-nondeterministic join conflicts" in {
      percolate(sql(
        "SELECT * FROM (SELECT A, B, VAR('X') AS N FROM R) Q1, (SELECT A, B, VAR('Y') AS M FROM R) Q2 WHERE Q1.B = Q2.B"
      )) must be equalTo
        Project(List(
            ProjectArg("A", Var("__LHS_R_A")), 
            ProjectArg("B", Var("__LHS_R_B")), 
            ProjectArg("N", tvar("X")),
            ProjectArg("A", Var("__RHS_R_A")), 
            ProjectArg("B", Var("__RHS_R_B")),
            ProjectArg("M", tvar("Y"))
          ),
          select("__LHS_R_B = __RHS_R_B",
            join(
              project(List(
                  ("__LHS_R_A", "R_A"), 
                  ("__LHS_R_B", "R_B"), 
                  ("__LHS_R_ROWID", "R_ROWID")),
                table("R")),
              project(List(
                  ("__RHS_R_A", "R_A"), 
                  ("__RHS_R_B", "R_B"),
                  ("__RHS_R_ROWID", "R_ROWID")),
                table("R")))))
    }
    "handle row-ids correctly" in {
      percolate(sql(
        "SELECT A, R.B, Var('X', R.ROWID, R.A) AS N, C FROM R, S WHERE R.B = S.B"
      )) must be equalTo
        Project(List(
            ProjectArg("A", Var("R_A")),
            ProjectArg("B", Var("R_B")),
            ProjectArg("N", rvar("X", "R", List[Expression](Var("R_A")))),
            ProjectArg("C", Var("S_C"))
          ),
          select("R_B = S_B", 
            join(table("R"), table("S"))))
    }
    "handle nested row-ids" in {
      percolate(sql(
        "SELECT * FROM (SELECT A, B, VAR('X', R.ROWID) AS N FROM R) Q, S WHERE Q.B = S.B"
      )) must be equalTo
        Project(List(
            ProjectArg("B", Var("S_B")), 
            ProjectArg("B", Var("R_B")), 
            ProjectArg("A", Var("R_A")), 
            ProjectArg("N", rvar("X", "R")),
            ProjectArg("C", Var("S_C"))
          ),
          select("R_B = S_B",
            join(
              table("R"),
              table("S"))))
    }
    "handle conflict-nested row-ids" in {
      percolate(sql(
        "SELECT * FROM (SELECT A, B, VAR('X', R.ROWID) AS N FROM R) Q1, (SELECT A, B FROM R) Q2 WHERE Q1.B = Q2.B"
      )) must be equalTo
        Project(List(
            ProjectArg("A", Var("__LHS_R_A")), 
            ProjectArg("B", Var("__LHS_R_B")), 
            ProjectArg("N", rvar("X", "__LHS_R")),
            ProjectArg("A", Var("__RHS_R_A")), 
            ProjectArg("B", Var("__RHS_R_B"))
          ),
          select("__LHS_R_B = __RHS_R_B",
            join(
              project(List(
                  ("__LHS_R_A", "R_A"), 
                  ("__LHS_R_B", "R_B"), 
                  ("__LHS_R_ROWID", "R_ROWID")),
                table("R")),
              project(List(
                  ("__RHS_R_A", "R_A"), 
                  ("__RHS_R_B", "R_B"),
                  ("__RHS_R_ROWID", "R_ROWID")),
                table("R")))))
    }
    
  }
  
  "The RA to Sql Converter" should {
    "convert p queries" in {
      ra(project(
          List(("Z", "R_A+R_B"), ("B", "R_B")), 
            table("R"))) must be equalTo
        "SELECT R_A + R_B AS Z, R_B AS B FROM "+
          "(SELECT R.A AS R_A, R.B AS R_B, "+
                  "R.ROWID AS R_ROWID FROM R) SUBQ_R_A"
    }
    "convert ps queries" in {
      ra(project(
          List(("Z", "R_A+R_B"), ("B", "R_B")), 
            select("R_A = R_B",
              table("R")))) must be equalTo
        "SELECT R_A + R_B AS Z, R_B AS B FROM "+
          "(SELECT R.A AS R_A, R.B AS R_B, "+
                  "R.ROWID AS R_ROWID FROM R) SUBQ_R_A "+
        "WHERE R_A = R_B"
    }
    "convert s queries" in {
      ra(select("R_A = R_B",
            table("R"))) must be equalTo
        "SELECT R_A AS R_A, R_B AS R_B, R_ROWID AS R_ROWID FROM "+
          "(SELECT R.A AS R_A, R.B AS R_B, "+
                  "R.ROWID AS R_ROWID FROM R) SUBQ_R_A "+
        "WHERE R_A = R_B"
    }
    "convert psj queries" in {
      ra(project(
        List(("Z", "R_A+S_C"), ("B", "R_B")), 
          select("R_B = S_B",
            join(
              table("R"),
              table("S"))))) must be equalTo
        "SELECT R_A + S_C AS Z, R_B AS B FROM "+
          "(SELECT R.A AS R_A, R.B AS R_B, "+
                  "R.ROWID AS R_ROWID FROM R) SUBQ_R_A, "+
          "(SELECT S.B AS S_B, S.C AS S_C, "+
                  "S.ROWID AS S_ROWID FROM S) SUBQ_S_B "+
        "WHERE R_B = S_B"
    }
    "convert up queries" in {
      ra(Union(
        project(List(("A", "R_A"), ("B", "R_B")),
          table("R")),
        project(List(("A", "S_C"), ("B", "S_B")),
          table("S")))) must be equalTo
          "(SELECT R_A AS A, R_B AS B FROM "+
            "(SELECT R.A AS R_A, R.B AS R_B, "+
                    "R.ROWID AS R_ROWID FROM R) SUBQ_R_A) "+
          "UNION "+
          "(SELECT S_C AS A, S_B AS B FROM "+
            "(SELECT S.B AS S_B, S.C AS S_C, "+
                    "S.ROWID AS S_ROWID FROM S) SUBQ_S_B)"
    }
  }
  
}