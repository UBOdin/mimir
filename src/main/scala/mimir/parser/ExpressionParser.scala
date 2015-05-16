package mimir.parser;

import scala.util.parsing.combinator.RegexParsers

import mimir.algebra._
import mimir.ctables._

class ExpressionParser(modelLookup: (String => Model)) extends RegexParsers {
	override type Elem = Char

	def id = """[a-zA-Z_][a-zA-Z0-9_]*""".r
	def cint = """-?(0|[1-9]\d*)""".r ^^ { _.toInt }
	def cflt = """-?(0\.[0-9]*|[1-9]*[0-9]*\.[0-9]*)""".r ^^ { _.toDouble }

	def expr(s: String): Expression = 
		parseAll(exprBase, s) match {
			case Success(ret, _) => ret
			case x => throw new Exception(x.toString)
		}
	
	def exprBase : Parser[Expression] = 
		isNull


	def isNull = 
		( arith <~ "IS NULL" ^^ { case e => IsNullExpression(e, false); } 
		  | arith 
		)



	def parens = "\\(" ~> exprBase <~ "\\)"
	def arith  = 
		( leaf ~ arithSym ~ exprBase ^^ {
			case lhs ~ op ~ rhs => Arithmetic(op, lhs, rhs)
		  } 
          | leaf
        )

	def caseStmt = 
		"CASE"~>(whenThenSeq~("ELSE"~>exprBase))<~"END" ^^ {
		   	case w ~ e => CaseExpression(w,e) 
	    }
	def whenThenSeq: Parser[List[WhenThenClause]] =
		( whenThenClause ~ whenThenSeq ^^ { case a ~ b => a :: b } 
		| whenThenClause ^^ { List(_) } 
		)
	def whenThenClause = 
		"WHEN"~>(exprBase~("THEN"~>exprBase)) ^^ {
			case w ~ t => WhenThenClause(w, t)
		} 

	def leaf = parens | floatLeaf | intLeaf | stringLeaf | caseStmt | function | vgterm | varLeaf

	def intLeaf = cint ^^ { IntPrimitive(_) }
	def floatLeaf = cflt ^^ { FloatPrimitive(_) }
	def stringLeaf = "'(([^']|\\')*)'".r ^^ { 
		(x:String) => 
			StringPrimitive(x.substring(1,x.length-1)) 
	}
	def varLeaf = id ^^ { Var(_) }

	def arithSym = Arith.matchRegex ^^ { Arith.fromString(_) }

	def function: Parser[Expression] = id ~ ("(" ~> opt(exprList) <~ ")") ^^ { 
		case fname ~ args => 
			Function(fname, args.getOrElse(List()))
	}

	def exprList: Parser[List[Expression]] =
		exprBase ~ ", *".r ~ exprList ^^ { case hd ~ _ ~ tl => hd :: tl } |
	    exprBase                      ^^ { List(_) }

	def vgterm = ("\\{\\{ *".r ~> id ~ 
					opt("[" ~> exprList <~ "]") <~ 
					" *\\}\\}".r) ^^ {
		case v ~ args => {
			val fields = v.split("_")
			VGTerm((fields(0),modelLookup(fields(0))), fields(1).toInt,
				   args.getOrElse(List()))
		}
	}
}