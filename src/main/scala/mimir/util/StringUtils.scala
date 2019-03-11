package mimir.util

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

  def nMore(count: Int, values: Seq[String], sep: String = ", ", conjunction: String = "and") =
    values.take(count).mkString(sep)+(
      if(values.size <= count) { "" }
      else { s"${sep}${conjunction} ${values.size - count} more"}
    )
}