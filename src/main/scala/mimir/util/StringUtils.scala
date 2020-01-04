package mimir.util

import scala.annotation.tailrec

object StringUtils
{
  def withDefiniteArticle(str: String): String =
  {
    val firstLetter = str.toLowerCase()(0)
    if(Set('a', 'e', 'i', 'o', 'u') contains firstLetter){
      return "an "+str
    } else {
      return "a "+str
    }
  }

  def oxfordComma(values: Seq[String], conjunct:String): String = 
  {
    values match { 
      case Seq() => ""
      case Seq(a) => a
      case Seq(a, b) => s"$a $conjunct $b"
      case _ => {
        val reversed = values.reverse
        val headString = reversed.tail.reverse.mkString(", ")
        s"$headString, $conjunct ${reversed.head}"
      }
    }
  }
  
  def countSubstring(str1:String, str2:String):Int={
     @tailrec def count(pos:Int, c:Int):Int={
        val idx=str1 indexOf(str2, pos)
        if(idx == -1) c else count(idx+str2.size, c+1)
     }
     count(0,0)
  }

}