package mimir.parser
//
// Adapted from http://www.lihaoyi.com/fastparse/#Json
//
// Original code Copyright (c) 2014 Li Haoyi (haoyi.sg@gmail.com) 
// and shared under the MIT License (MIT)
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software 
// and associated documentation files (the "Software"), to deal in the Software without 
// restriction, including without limitation the rights to use, copy, modify, merge, publish, 
// distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the 
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or 
// substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
// NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND 
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, 
// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, 
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
//

import fastparse._, NoWhitespace._
import play.api.libs.json._

object JsonParser {

  def doubleQuotedStringChars(c: Char) = c != '\"' && c != '\\'
  def singleQuotedStringChars(c: Char) = c != '\'' && c != '\\'

  def space[_: P]         = P( CharsWhileIn(" \r\n", 0) )
  def digits[_: P]        = P( CharsWhileIn("0-9") )
  def exponent[_: P]      = P( CharIn("eE") ~ CharIn("+\\-").? ~ digits )
  def fractional[_: P]    = P( "." ~ digits )
  def integral[_: P]      = P( "0" | CharIn("1-9")  ~ digits.? )

  def number[_: P] = P(  
    (CharIn("+\\-").? ~ integral ~ fractional.? ~ exponent.? ).!.map {
      x => JsNumber(x.toDouble)
    }
  )

  def `null`[_: P]        = P( "null" ).map(_ => JsNull)
  def `false`[_: P]       = P( "false" ).map(_ => JsBoolean(false))
  def `true`[_: P]        = P( "true" ).map(_ => JsBoolean(true))

  def hexDigit[_: P]      = P( CharIn("0-9a-fA-F") )
  def unicodeEscape[_: P] = P( "u" ~ hexDigit ~ hexDigit ~ hexDigit ~ hexDigit )
  def escape[_: P]        = P( "\\" ~ (CharIn("\"/\\\\bfnrt") | unicodeEscape) )

  def doubleStrChars[_: P] = P( CharsWhile(doubleQuotedStringChars) )
  def singleStrChars[_: P] = P( CharsWhile(singleQuotedStringChars) )
  def string[_: P] =
    P( 
        (space ~ "\"" ~/ (doubleStrChars | escape).rep.! ~ "\"")
      | (space ~ "'" ~/ (singleStrChars | escape).rep.! ~ "'")
    ).map { JsString } 

  def array[_: P] =
    P( "[" ~/ jsonExpr.rep(sep=","./) ~ space ~ "]").map { JsArray(_) } 

  def pair[_: P] = P( string.map(_.value) ~ space ~/ ":" ~ space ~/ jsonExpr )

  def obj[_: P] =
    P( "{" ~/ pair.rep(sep=","./) ~ space ~ "}").map { fields => JsObject(fields.toMap) }

  def jsonExpr[_: P]: P[JsValue] = P(
    space ~ (obj | array | string | `true` | `false` | `null` | number) ~ space
)
}