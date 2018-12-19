package mimir.parser;

import java.sql.SQLException;

import scala.util.parsing.combinator.RegexParsers

import mimir.algebra._
import mimir.ctables._
import mimir.models._

object ExpressionParser extends RegexParsers {
	override type Elem = Char

	def id = """[a-zA-Z_][a-zA-Z0-9_]*""".r
	def cint = """-?(0|[1-9]\d*)""".r ^^ { _.toInt }
	def cflt = """-?(0\.[0-9]*|[1-9]*[0-9]*\.[0-9]*)""".r ^^ { _.toDouble }

	def expr(s: String): Expression = 
		parseAll(exprBase, s) match {
			case Success(ret, _) => ret
			case x => throw new RAException(x.toString)
		}
	def exprList(s: String): List[Expression] = 
		if(s == "") { List[Expression]() } else {
			parseAll(exprListBase, s) match {
				case Success(ret, _) => ret
				case x => throw new RAException(x.toString)
			}
		}
	def function(s: String): Function =
		parseAll(functionClause, s) match {
			case Success(ret: Function, _) => ret
			case x => throw new RAException(x.toString)
		}

	
	def exprBase : Parser[Expression] = 
		boolExpr

	def boolExpr : Parser[Expression] =
		( isNull
		| arith ~ cmpSym ~ arith ^^ { 
				case lhs ~ op ~ rhs => Comparison(op, lhs, rhs)
			}
		| arith ~ "AND" ~ boolExpr ^^ {
				case lhs ~ _ ~ rhs => Arithmetic(Arith.And, lhs, rhs)
			}
		| arith ~ "OR" ~ boolExpr ^^ {
				case lhs ~ _ ~ rhs => Arithmetic(Arith.Or, lhs, rhs)
			}
		| "NOT(" ~> arith <~ ")" ^^ { Not(_) }
		| arith 
		)

	def isNull = 
		( arith <~ "IS NULL" ^^ { case e => IsNullExpression(e); } 
		| arith <~ "IS NOT NULL" ^^ { case e => Not(IsNullExpression(e)); } 
		)

	def cmpSym = 
		(
			"=" ^^ { _ => Cmp.Eq }
		|	"<=" ^^ { _ => Cmp.Lte }
		|	">=" ^^ { _ => Cmp.Gte }
		|	"<" ^^ { _ => Cmp.Lt }
		|	">" ^^ { _ => Cmp.Gt }
		|	"!=" ^^ { _ => Cmp.Neq }
		)


	def parens = "(" ~> exprBase <~ ")"
	def arith  = 
		( leaf ~ arithSym ~ exprBase ^^ {
			case lhs ~ op ~ rhs => Arithmetic(op, lhs, rhs)
		  } 
          | leaf
        )

	def ifStmt = 
	 	"IF"~> exprBase ~ "THEN" ~ exprBase ~ "ELSE"~ exprBase <~"END" ^^ {
			case (condition ~ _ ~ thenClause ~ _ ~ elseClause) => 
				Conditional(condition, thenClause, elseClause)
		}

	def leaf = 
		parens | floatLeaf | intLeaf | boolLeaf | stringLeaf | typeLeaf | 
		ifStmt | functionClause | vgterm | varLeaf

	def intLeaf = cint ^^ { IntPrimitive(_) }
	def floatLeaf = cflt ^^ { FloatPrimitive(_) }
	def boolLeaf = (
		  "true"  ^^ { _ => BoolPrimitive(true)  }
		| "false" ^^ { _ => BoolPrimitive(false) }
		| "TRUE"  ^^ { _ => BoolPrimitive(true)  }
		| "FALSE" ^^ { _ => BoolPrimitive(false) }
	)

	def stringLeaf = """'(([^']|\\')*?)'""".r ^^ {
		(x:String) => 
			StringPrimitive(x.substring(1,x.length-1)) 
	}
	def varLeaf = 
		id ^^ { case "ROWID" => RowIdVar()
		        case x => Var(x) }

	def arithSym = Arith.matchRegex ^^ { Arith.fromString(_) }

	def functionClause: Parser[Expression] = id ~ ("(" ~> opt(exprList) <~ ")") ^^ { 
		case "NOT" ~ Some(List(arg)) => Not(arg)
		case fname ~ args => 
			Function(fname, args.getOrElse(List()))
	}

	def exprListBase: Parser[List[Expression]] =
		exprBase ~ ", *".r ~ exprListBase ^^ { case hd ~ _ ~ tl => hd :: tl } |
	    exprBase                          ^^ { List(_) }

	def exprList: Parser[List[Expression]] =
		  (opt(exprListBase) ^^ { _.getOrElse(List()) }) |
		  (exprBase ~ "AS" ~ typeLeaf ^^ { 
			case target ~ _ ~ verb => List(target, verb)
			})


	def vgterm = ("\\{\\{ *".r ~> id ~ 
					opt(("[" ~> exprList <~ "]") ~
						opt("[" ~> exprList <~ "]")
					) <~ 
					" *\\}\\}".r) ^^ {
		case v ~ argsAndHints => {
			val (args:Seq[Expression], hints:Seq[Expression]) =
				argsAndHints match {
					case Some(args ~ Some(hints)) => (args, hints)
					case Some(args ~ None) => (args, Seq())
					case None => (Seq(), Seq())
				}

			val fields = v.split("_")
			VGTerm(fields(0), fields(1).toInt, args, hints)
		}
	}

	def exprType: Parser[Type] = (
		"int" | "decimal" | "date" | "string" | "rowid" | "type" | "float" | "real" | "varchar" | "any"
	) ^^ { Type.fromString(_) }

	def typeLeaf: Parser[Expression] = 
		exprType ^^ { (t) => TypePrimitive(t) }
}

