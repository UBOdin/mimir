package mimir.util

abstract class JsonLoader {
  /**
    * returns a json row from the file, checks for empty strings
    * @return returns a row, returns null when file is done
    */
  def getNext(): String
  def reset(): Unit
  def close(): Unit
}
