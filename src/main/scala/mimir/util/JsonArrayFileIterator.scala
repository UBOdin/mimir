package mimir.util

import java.io.{BufferedReader, File, FileReader}


class JsonArrayFileIterator(sourceFile: File) extends JsonLoader {

  val ObjectIterator = new BufferedReader(new FileReader(sourceFile))

  def getNext(): String = {
    ???
  }

  def reset(): Unit = ???
  def close(): Unit = ???

}
