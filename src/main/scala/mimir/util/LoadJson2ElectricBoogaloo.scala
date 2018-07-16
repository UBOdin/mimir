package mimir.util

import java.io.{BufferedReader, File, FileReader}


class LoadJson2ElectricBoogaloo(sourceFile: File, escaped: Boolean = false, naive: Boolean = false, rowed: Boolean = true){

  var input: BufferedReader = new BufferedReader(new FileReader(sourceFile))

  // returns null when done
  def getNext(): String = {
    if(rowed){
      val line = input.readLine()
      if(line == null){
        return null
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
    } else {
      return null // need to implement if not every row is json
    }
  }

  def reset() = input.reset()
  def close() = input.close()

}
