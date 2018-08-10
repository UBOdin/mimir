package mimir.util

import java.io.{BufferedReader, File, FileReader}

/**
  * @param sourceFile the input file to read from
  * @param naive if true then naively call readLine if false then take the substring that is the fist { and last }
  */

class LoadJson2ElectricBoogaloo(sourceFile: File, naive: Boolean = false){

  var input: BufferedReader = new BufferedReader(new FileReader(sourceFile))

  /**
    * returns a json row from the file, checks for empty strings
    * @return returns a row, returns null when file is done
    */
  def getNext(): String = {
    val line = input.readLine()
    if(line == null){
      return null
    } else if(line.equals("")) {
      return getNext()
    } else if(naive) { // just return the row
      return line
    } else { // just take first { and last }
      try {
        val r = line.substring(line.indexOf('{'), line.lastIndexOf('}') + 1)
        return r
      } catch {
        case e: java.lang.StringIndexOutOfBoundsException => return getNext()
      }
    }
  }

  /**
    * close the file to prevent leaks
    */
  def close() = input.close()

}
