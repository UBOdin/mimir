package mimir.util

import java.io._

import scala.collection.JavaConverters._
//import pattern_mixture_summarization.{Cluster, ClusteringResult, NaiveSummary, NaiveSummaryEntry}

import scala.collection.mutable.ListBuffer

class VisualizeHTML (dir: String){
/*
  var counter = 0
  var globalCounter = 0 // used as a unique id for collapse id's

  val path: String = s"C:\\Users\\Will\\Documents\\GitHub\\mimirClustering\\$dir"
  val schemaReader = new BufferedReader(new FileReader(new File(s"$dir\\schema.txt")))

  val schema: List[String] = schemaReader.readLine().split(",").toList
  val clusterResult: ClusteringResult = new ClusteringResult(s"$path\\fvoutput.txt",s"$path\\multoutput.txt",s"$path\\hamming_labels.txt")
  val clusterList = clusterResult.getClusters.asScala.toList

  var constantsList: ListBuffer[Int] = ListBuffer[Int]()
  val clusterHierarchy: List[NaiveSummary] = clusterResult.getNaiveSummaryHierarchy.asScala.toList
//  val intersectList = NaiveSummaryIntersect(clusterHierarchy)

  val htmlHeader: String = ("<!DOCTYPE HTML>\n<html>\n<head>\n"
  + "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n<link rel=\"stylesheet\" href=\"https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css\">\n<script src=\"https://ajax.googleapis.com/ajax/libs/jquery/3.2.1/jquery.min.js\"></script>\n<script src=\"https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/js/bootstrap.min.js\">"
  + "</script>\n</head>\n<body>\n<h2>" + dir + "</h2>\n")

  val htmlVisualization: String = GenerateHTML(clusterHierarchy, schema)

  val htmlTail: String = "\n</body>\n</html>"

  val output: String = htmlHeader + htmlVisualization + htmlTail
  try { // try to write output
    val htmlWriter: BufferedWriter = new BufferedWriter(new FileWriter(dir.split("\\\\").last + "output.html"))
    htmlWriter.write(output)
    htmlWriter.close()
  }

  // set of methods for determining the intersection of NaiveSummary objects, so NaiveSummaryEnteries that are repeated in all
  def NaiveSummaryIntersect(nsList: List[NaiveSummary]): List[NaiveSummaryEntry] = {
    var intersect: List[NaiveSummaryEntry] = List[NaiveSummaryEntry]()
    nsList.foreach((ns) => {
      if(intersect.size > 0)
        intersect = Intersect(intersect,NaiveSummaryIntersect(ns))
      else
        intersect = NaiveSummaryIntersect(ns)
    })
    return intersect
  }

  def NaiveSummaryIntersect(ns: NaiveSummary): List[NaiveSummaryEntry] = {
    var entryList: List[NaiveSummaryEntry] = ns.entries.asScala.toList
    val children: List[NaiveSummary] = ns.children.asScala.toList
    if(children.size > 0) // has children
      children.foreach((x) => {
        if(entryList.size > 0)
          entryList = Intersect(entryList,NaiveSummaryIntersect(x))
        else
          entryList = NaiveSummaryIntersect(x)
      })
    return entryList
  }

  def Intersect(l1: List[NaiveSummaryEntry], l2: List[NaiveSummaryEntry]): List[NaiveSummaryEntry] = {
    var ret: ListBuffer[NaiveSummaryEntry] = ListBuffer[NaiveSummaryEntry]()
    val l2ID: List[Int] = l2.map(_.featureID)
    l1.foreach((nse) => {
      if(l2ID.contains(nse.featureID))
        ret += nse
    })
    ret.toList
  }


  // generates the html used for display, the basic form is to recursively call this function which will output nested collapse-able tags for each NaiveSummary
  def GenerateHTML(nsList: List[NaiveSummary], schema: List[String]): String = {
    var retHTML: String = ""
    val constantHeader: String = "<h2 class=\"list-group-item\">Common Features</h2>"
    val listHead: String = "<ol class=\"list-group\"> <!-- Main List Open -->\n"
    val listTail: String = "</ol> <!-- Main List Close -->\n"
    retHTML = retHTML + listHead + constantHeader
    val constants: List[NaiveSummaryEntry] = NaiveSummaryIntersect(nsList)
    retHTML = retHTML + pathPanel(constants, schema, List[Int]())
    nsList.foreach((ns) => {
      val ret = GenerateHTML(ns, schema, constants.map(_.featureID))
      if(!ret.equals("")) // should generate a list of panel groups
        retHTML = retHTML + ret
    })
    retHTML + listTail
  }

  def GenerateHTML(ns: NaiveSummary, schema: List[String], usedColumns: List[Int]): String = {
    var constantsHTML: String = ""
    val constantHeader: String = "<h2 class=\"list-group-item\">Common Features</h2>"
    val childrenHeader: String = "<h2 class=\"list-group-item\">Children</h2>"
    val featureHeader: String = "<h2 class=\"list-group-item\">Features</h2>"
    val listGroupHeader: String = "<li class=\"list-group-item\"> <!-- GenerateHTML Open -->\n"
    val listGroupTail: String = "</li> <!-- GenerateHTML Close -->\n"
    var childHTML = ""
    var clusterError: Double = ns.Error

    var usedColumnsUpdated = usedColumns
    if(ns.children.size > 0) { // The NaiveSummary has children
      // deal with constants
      val constants: List[NaiveSummaryEntry] = RemoveUsedColumns(NaiveSummaryIntersect(ns),usedColumnsUpdated)
      if(constants.size > 0) {
        constantsHTML = pathPanel(constants, schema, usedColumnsUpdated)
        usedColumnsUpdated = usedColumnsUpdated ++ constants.map(_.featureID)
      }
      // used columns is now updated
      ns.children.asScala.toList.foreach((child) => {
        val returnedHTML: String = GenerateHTML(child, schema, usedColumnsUpdated)
        if(!returnedHTML.equals(""))
          childHTML = childHTML + returnedHTML
      })
    }
    var output: String = ""
    val featureHTML: String = pathPanel(ns.entries.toArray.toList.asInstanceOf[List[NaiveSummaryEntry]], schema, usedColumnsUpdated)
    if(!featureHTML.equals("")) {
      var clusterTitle: String = "Summary_" + counter.toString
      counter += 1

      val panelHeader: String = (listGroupHeader + "<details> <summary>"+ clusterTitle + " &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Error: " + truncDouble(clusterError) + "</summary>\n")
      globalCounter += 1
      val panelFooter: String = "</details> <!-- From Summary "+ (counter-1).toString +" -->\n"
      output = output  + panelHeader
      if (!constantsHTML.equals(""))
        output = output + constantHeader + constantsHTML
      if (!childHTML.equals(""))
        output = output + childrenHeader + childHTML
      output = output + featureHeader + featureHTML + panelFooter + listGroupTail
    }
    output
  }


  // helper method, converts NaiveSummaryEntry to html, this will create a tag for each element with shading and applies the name from schema
  def NaiveSummaryEntryToHTML(nseList: List[NaiveSummaryEntry], schema: List[String], usedColumns: List[Int]): String = {
    var featureList: String = ""
    nseList.foreach((nse) => {
      featureList = featureList + NaiveSummaryEntryToHTML(nse,schema, usedColumns)
    })
    val listHead: String = "<ol class=\"list-group\"> <!-- From NaiveSummaryEntry Open -->\n"
    val listTail: String = "</ol> <!-- From NaiveSummaryEntry Close -->\n"
    //style= "padding-left: 50px;"
    if(!featureList.equals(""))
      listHead + featureList + listTail
    else
      ""
  }

  def NaiveSummaryEntryToHTML(nse: NaiveSummaryEntry, schema: List[String], usedColumns: List[Int]): String = {
    if(!usedColumns.contains(nse.featureID)) {
      val name: String = schema(nse.featureID)
      val opacity: Double = nse.marginal
      if(opacity <= .25)
        "<li class=\"list-group-item\" style=\"color:red;\">" + name + "</li>\n"
      else
        "<li class=\"list-group-item\" style=\"opacity:" + opacity + "\">" + name + "</li>\n"
    }
    else
      ""
  }

  def RemoveUsedColumns(constants: List[NaiveSummaryEntry], used: List[Int]): List[NaiveSummaryEntry] = {
    val ret: ListBuffer[NaiveSummaryEntry] = ListBuffer[NaiveSummaryEntry]()
    constants.foreach((nse) => {
      if(!used.contains(nse.featureID))
        ret += nse
    })
    ret.toList
  }

  def pathPanel(nseList: List[NaiveSummaryEntry], schema: List[String], usedColumns: List[Int]): String = { // creates groupings of features
    var groupMap: scala.collection.mutable.Map[String,ListBuffer[NaiveSummaryEntry]] = scala.collection.mutable.Map[String,ListBuffer[NaiveSummaryEntry]]()
    nseList.foreach((nse) => {
      val name: String = schema(nse.featureID)
      val key: String = name.split("\\.")(0)
      groupMap.get(key) match {
        case Some(map) =>
          map += nse
          groupMap.put(key, map)
        case None =>
          groupMap.put(key, ListBuffer[NaiveSummaryEntry](nse))
      }
    })
    var middleHTML: String = ""
    groupMap.foreach((f) => {
      val path = f._1 // the path
      val list = f._2 // the list of SummaryEnteries
      val avgMarginal: Double = GetAvg(list.toList,usedColumns)
      if(list.size > 1) { // collapse panel
        val panelMiddle: String = NaiveSummaryEntryToHTML(list.toList,schema,usedColumns)
        if(!panelMiddle.equals("")) {
          val listGroupHeader: String = "<li class=\"list-group-item\">\n"
          val listGroupTail: String = "</li>\n"

          val panelHeader: String = (listGroupHeader + "<details> <summary>"+ path + " &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Average Marginal: " + truncDouble(avgMarginal) + " &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Max: " + truncDouble(GetMax(list.toList,usedColumns)) + " &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Min: " + truncDouble(GetMin(list.toList,usedColumns)) + "</summary>\n")
          globalCounter += 1
          val panelFooter: String = "</details>\n"
          middleHTML = middleHTML + panelHeader + panelMiddle + panelFooter + listGroupTail
        }
        else
          "" // do nothing
      }
      else if (list.size == 1) { // regular line
        middleHTML = middleHTML + NaiveSummaryEntryToHTML(list(0),schema,usedColumns)
      }
      else {
        // for sanity
      }
    })
    val listHead: String = "<ol class=\"list-group\"> <!-- From PathPanel Open -->\n"
    val listTail: String = "</ol> <!-- From PathPanel Close -->\n"
    //style= "padding-left: 50px;"
    if(!middleHTML.equals(""))
      listHead + middleHTML + listTail
    else
      ""
  }

  def GetAvg(nseList: List[NaiveSummaryEntry], usedColumns: List[Int]): Double = {
    nseList.map(_.marginal).foldLeft(0.0)(_+_) / nseList.size
  }
  def GetMax(nseList: List[NaiveSummaryEntry], usedColumns: List[Int]): Double = {
    nseList.map(_.marginal).foldLeft(0.0)(math.max(_,_))
  }
  def GetMin(nseList: List[NaiveSummaryEntry], usedColumns: List[Int]): Double = {
    nseList.map(_.marginal).foldLeft(1.0)(math.min(_,_))
  }

  def Max(list:List[Int]): Int = list.foldLeft(0)((r,c) => math.max(r,c))

  def truncDouble(d: Double, size: Int = 2): String = {
    val dList = d.toString.split("\\.").toList
    if(dList(1).size < size)
      dList(0) + "." + dList(1)
    else if(d <= .01)
        "0.01"
    else
      dList(0) + "." + dList(1).substring(0,size)
  }*/
}
