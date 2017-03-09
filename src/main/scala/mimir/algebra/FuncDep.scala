package mimir.algebra

import java.io._
import java.util

import mimir.exec.{ResultIterator, ResultSetIterator}
import mimir.algebra.Type.T
import java.util.TreeMap
import java.util.ArrayList
import java.util.HashMap
import javax.swing.{JFrame, JPanel, JScrollPane}

import edu.uci.ics.jung.algorithms.layout.{CircleLayout, Layout, TreeLayout}
import edu.uci.ics.jung.graph.{DelegateTree, DirectedGraph, DirectedSparseMultigraph, Forest, Graph, SparseMultigraph, Tree, UndirectedSparseMultigraph}
import edu.uci.ics.jung.graph.util.EdgeType
import edu.uci.ics.jung.visualization.BasicVisualizationServer
import edu.uci.ics.jung.visualization.decorators.ToStringLabeller
import org.apache.commons.collections15.Transformer
import java.awt.{BasicStroke, Color, Dimension, Paint, Rectangle, Stroke}
import java.sql.ResultSet

import com.jgraph.layout.tree.JGraphTreeLayout
import mimir.util.JDBCUtils
import edu.uci.ics.jung.visualization.renderers.Renderer.VertexLabel.Position
import org.javacc.parser.OutputFile

import scala.collection.JavaConverters._

  /*
  Steps to building ER

  PreprocessingFDG:
    - Takes in data from an iterator and stores it for later use
    - The data collected is the count of the number of occurrences of each value in every column, and the percentage of nulls
    - The data comes in row-wise, this is not efficent, ideally change so each column calls SELECT colName so it can be done in parallel and not store the data in RAM

  ConstructFDG:
    - Compare each column to every other column
    - From this determine the strength between two columns
    - Strength is defined using the formula strength(column1,column2) = (# unique column1 - # unique column1 mode(column2) pairs) / (# unique (column1,column2) pairs - # unique column1 mode(column2) pairs)
    - This strength relationship is directional
    - For a dependency to exist the strength must be >= threshold, and density column1 >= density column2 (Density requirement sometimes ommitted for tests)
    - Find the longest path in this graph for each column
    - Create a tree from this heuristic

   */

@SerialVersionUID(100L)
class FuncDep 
  extends Serializable
{

  // conditionals for output generation and other variables
  val threshhold:Double = .99 // this is the threshold that determines if there is a functional dependency between two columns
  val flattenParentTable:Boolean = false // used if wanting to flatten the parent table so it uses the child of root
  val outputEntityGraphs:Boolean = true // true if you want the entity graphs to be output
  val combineEntityGraphs : Boolean = true // if false then each entity will have it's own graph
  val showFDGraph : Boolean = true // if you want the Functional Dependency Graph to be shown
  val blackListThreshold = .05 // minimum percentage of non-null columns to be included in the calculations
  val writeFile : Boolean = false

  // tables containing data for computations
  var sch:List[(String, T)] = null // the schema, is a lookup for the type and name
  var tableName : String = null
  var table:ArrayList[ArrayList[PrimitiveValue]] = null // This table contains the input table
  var countTable:ArrayList[TreeMap[String,Integer]] = null // contains a count of every occurrence of every value in the column
  var densityTable:ArrayList[Integer] = null // gives the density for column, that is percentage of non-null values
  var blackList:ArrayList[Integer] = null // a list of all the columns that are null or mostly null
  var fdGraph: DirectedSparseMultigraph[Integer, String] = null

  var parentTable: TreeMap[Integer, ArrayList[Integer]] = null
  var entityPairMatrix:TreeMap[String,TreeMap[Integer,TreeMap[Integer,Float]]] = null // outer string is entity pair so column#,column#: to a treemap that is essentially a look-up matrix for columns that contain column to column strengths
  var entityGraphList:ArrayList[DelegateTree[Integer, String]] = null
  var entityGraphString:ArrayList[DelegateTree[String, String]] = null
  var singleEntityGraph : DelegateTree[Integer, String] = null
  var entityPairList:ArrayList[(Integer,Integer)] = null

  // timers
  var startTime:Long = 0
  var endTime:Long = 0


  // buildEntities calls all the functions required for ER creation, optionally each function could be called if only part of the computation is required

  def buildEntities(schema: List[(String, T)],data: ResultIterator, tableName : String): ArrayList[String] = {
    var i = 0
    schema.map((s)=>{
      println(i + " : " + s._1)
      i += 1
    })
    preprocessFDG(schema,data,tableName)
    constructFDG()
    updateEntityGraph()
//    mergeEntities()
    val viewList = createViews()
    viewList
  }

  def buildEntities(schema: List[(String, T)],data: ResultSet, tableName : String): ArrayList[String] = {
    var i = 0
    schema.map((s)=>{
      println(i + " : " + s._1)
      i += 1
    })
    preprocessFDG(schema,data,tableName)
    constructFDG()
    updateEntityGraph()
//    mergeEntities()
    val viewList = createViews()
    viewList
  }


  /* Preprocess collects all the data needed to build the functional dependency graph and to create the entities. It consumes the ResultSet and ResultIterator, this information is kept
     inside this program so it has a high upfront ram cost, this can be changed on implementation to have each column call the database and in parallel collect this information
     The data collected is a count of each unique value per column and the total number of nulls
  */
  def preprocessFDG(schema: List[(String, T)],data: ResultIterator,tName : String): Unit = {

    // initalize tables
    table = new ArrayList[ArrayList[PrimitiveValue]]()
    tableName = tName
    entityPairMatrix = new TreeMap[String,TreeMap[Integer,TreeMap[Integer,Float]]]()
    sch = schema
    countTable = new ArrayList[TreeMap[String,Integer]]()
    densityTable = new ArrayList[Integer]()
    sch.map{ case(k, t) => {
      table.add(new util.ArrayList[PrimitiveValue]())
      countTable.add(new TreeMap[String,Integer])
      densityTable.add(0)
    }
    }


    while(data.getNext()){ // adds every row to table
      (1 until data.numCols).map( (i) => {
        val v:PrimitiveValue = data(i)
        table.get(i-1).add(v)
        if(!v.toString.equals("NULL")){
          var temp = densityTable.get(i-1)
          temp+=1
          densityTable.set(i-1,temp)
        }
        if(countTable.get(i-1).containsKey(v.toString)){
          countTable.get(i-1).replace(v.toString,countTable.get(i-1).get(v.toString),countTable.get(i-1).get(v.toString)+1)
        }
        else{
          countTable.get(i-1).put(v.toString,1)
        }
      })
    }

    for(i <- 0 until densityTable.size()){
      if((densityTable.get(i).toFloat / table.get(0).size().toFloat) < blackListThreshold ){
        blackList.add(i)
      }
      if(countTable.get(i).size() < 2){
        blackList.add(i)
      }
    }

  }

  def preprocessFDG(schema: List[(String, T)],data: ResultSet,tName:String): Unit = {

    // initalize tables
    blackList = new ArrayList[Integer]()
    tableName = tName
    var d:List[List[PrimitiveValue]] = mimir.util.JDBCUtils.extractAllRows(data)
    table = new ArrayList[ArrayList[PrimitiveValue]]()
    entityPairMatrix = new TreeMap[String,TreeMap[Integer,TreeMap[Integer,Float]]]()
    sch = schema
    countTable = new ArrayList[TreeMap[String,Integer]]()
    densityTable = new ArrayList[Integer]()
    sch.map{ case(k, t) => {
      table.add(new util.ArrayList[PrimitiveValue]())
      countTable.add(new TreeMap[String,Integer])
      densityTable.add(0)
    }
    }


    d.map((row)=>{
        var loc = 0 // this is to track the column number because the data comes in row-wise
        row.map((pv) => { // for everyone row in every column collect the data needed

          table.get(loc).add(pv) // add the value from the iterator to table for later lookup and use

          // Check to see if the value is null, this gives the percent null
          if(!pv.toString.equals("NULL")){
            var temp = densityTable.get(loc)
            temp += 1
            densityTable.set(loc,temp)

            // add new values to countTable or update the count plus 1 for existing values
            if(countTable.get(loc).containsKey(pv.toString)){
              var tempCount = countTable.get(loc).get(pv.toString)
              tempCount += 1
              countTable.get(loc).remove(pv.toString())
              countTable.get(loc).put(pv.toString,tempCount)
            }
            else{
              countTable.get(loc).put(pv.toString,1)
            }
          }

          loc += 1 // move to the next column for this row
        })
    })

    for(i <- 0 until densityTable.size()){
      if((densityTable.get(i).toFloat / table.get(0).size().toFloat) < blackListThreshold ){
        blackList.add(i)
      }
      if(countTable.get(i).size() < 2){
        blackList.add(i)
      }
    }

  }


  /*
  ConstructFDG constructs the functional dependency graph
  The steps are as follows:
    - Compare each column to every other column
      - Compre using something other than strings, create tuple instead of merging and merge only when two conflicting PrimitiveValue types
    - From this determine the strength between two columns
    - Strength is defined using the formula strength(column1,column2) = (# unique column1 - # unique column1 mode(column2) pairs) / (# unique (column1,column2) pairs - # unique column1 mode(column2) pairs)
    - This strength relationship is directional
      - The merging to avoid cycles could be done better
    - For a dependency to exist the strength must be >= threshold, and density column1 >= density column2 (Density requirement sometimes ommitted for tests)
    - Find the longest path in this graph for each column
    - Create a tree from this heuristic
   */

  def constructFDG() {

/*
    println(sch(19))
    println(densityTable.get(19))
    for(i <- 0 until densityTable.size()){
      println("Density at " + sch(i) + " : " + densityTable.get(i))
    }
    println(table.get(1).size())
    println(sch(34))
    println(sch(33))
    println(sch(32))
*/

    //Timers
    var startQ:Long = System.nanoTime();
    startTime = System.currentTimeMillis()

    // Initalize tables
    var nodeTable: ArrayList[Integer] = new ArrayList[Integer]() // contains a list of all the nodes
    // double is the strength for that edge
    var edgeTable: ArrayList[(String,Double)] = new ArrayList[(String,Double)]() // contains the node numbers for the dependency graph, the names are numbers from the schema 0 to sch.length are the possibilities
    var maxTable: ArrayList[String] = new ArrayList[String]() // contains the max values for each column, used for phase1 formula
    parentTable = new TreeMap[Integer, ArrayList[Integer]]()


    var columnLocation = 0
    // Finds the maxKey for each column, this is the most occurring value for each column and puts them into maxTable
    countTable.asScala.map((tree)=>{
      var maxKey = ""
      var maxValue = 0
      var keyIt = tree.keySet().iterator()
      while (keyIt.hasNext) {
        var value:String = keyIt.next()
        if(!value.toString.equals("NULL")) {
          if (maxValue < tree.get(value)) {
            maxKey = value
            maxValue = tree.get(value)
          }
        }
      }
      if(maxKey.equals("")){
        blackList.add(columnLocation)
      }

      maxTable.add(maxKey)
      columnLocation = columnLocation + 1
    })

    // add the column number to each column for parallelization
    for(c <- 0 until table.size()){
      table.get(c).add(new IntPrimitive(c))
    }


    // when done nodeTable will contain all column numbers that are involved in the FD graph, and edge table will contain all edges between the columns
    table.asScala.map((leftColumn)=>{

      // left and right are respective ways to keep track of comparing every column
      // Initalize values and tables needed for the left column
      val leftColumnNumber:Int = leftColumn.get(leftColumn.size()-1).asString.toInt // the location of the column in the schema, used for look-ups
      val leftType = sch(leftColumnNumber)._2
      val leftMap = new ArrayList[PrimitiveValue](leftColumn) // this is a copy of the left column to perform operations on
      val leftColumnName = sch(leftColumnNumber)._1
      val leftDensity = densityTable.get(leftColumnNumber).toFloat / (leftColumn.size() -1).toFloat // -1 for the added column number
      leftMap.remove(leftMap.size()-1) // remove the column number that was added above just to be picky

      // right column would be the column that is being compared to left column pairwise, could be thought of as column1 and column2
      table.asScala.map((rightColumn)=>{

        // Initalize tables and values
        val rightColumnNumber = rightColumn.get(rightColumn.size()-1).asString.toInt
        if (leftColumnNumber != rightColumnNumber && !maxTable.get(leftColumnNumber).equals("") && !maxTable.get(rightColumnNumber).equals("") && !blackList.contains(leftColumnNumber) && !blackList.contains(rightColumnNumber)){ // sanity check to avoid useless calculations
          val rightType = sch(rightColumnNumber)._2
          val rightMap = new ArrayList[PrimitiveValue](rightColumn)
          val rightColumnName = sch(rightColumnNumber)._1
          val rightDensity = densityTable.get(rightColumnNumber).toFloat / (rightColumn.size() -1).toFloat // -1 for the added column number
          if(leftDensity >= rightDensity) { // no point in doing this computation since we won't use it if leftDen is less than rightDen
            var pairMap: ArrayList[String] = new ArrayList[String]() // the size of this will be the unique number of a1,a2 pairs
            rightMap.remove(rightMap.size() - 1) // remove the column number that was added above just to be picky
            val leftIter = leftMap.iterator()
            val rightIter = rightMap.iterator()
            var rightMaxOccurrenceCount = 0 // how many unique pairings there are where the rightCol value is its maxValue; Part of the formula

            // fills the pairMap with all pairings of leftColumn and rightColumn
            while (leftIter.hasNext && rightIter.hasNext) {
              val leftVal: PrimitiveValue = leftIter.next()
              val rightVal: PrimitiveValue = rightIter.next()
              if (!leftVal.toString.equals("NULL")) {
                val value: String = leftVal.toString() + ",M," + rightVal.toString() // doing this to avoid accidental tuple collisions, super not efficient in so many ways
                if(!pairMap.contains(value)) {
                  pairMap.add(value)
                  if (rightVal.toString().equals(maxTable.get(rightColumnNumber))) {
                    rightMaxOccurrenceCount += 1
                  }
                }
              }
            }

            // compute the strength from the formula
            if (pairMap.size() != 0) {
              val strengthNumerator: Double = (countTable.get(leftColumnNumber).size().toFloat - rightMaxOccurrenceCount.toFloat)
              val strengthDenominator: Double = (pairMap.size().toFloat - rightMaxOccurrenceCount.toFloat)
              val strength: Double = strengthNumerator / strengthDenominator
/*
              println("Count left col: " + countTable.get(leftColumnNumber).size().toFloat)
              println("Number of pairs: " + pairMap.size().toFloat)
              println("Right Occurence: " + rightMaxOccurrenceCount)
              println("Numerator: " + strengthNumerator)
              println("Denominator: " + strengthDenominator)
              println("Strength: " + strength)
              println("")
*/
              if(strength > 1.0){
                throw new Exception("Strength is > 1.0, something isn't right! In FuncDep")
              }
              if (strength >= threshhold && strengthDenominator > 0) {
                // phase one constraints
                edgeTable.add(new Tuple2(leftColumnNumber.toString + "," + rightColumnNumber.toString, strength))
                if (!nodeTable.contains(leftColumnNumber)) {
                  nodeTable.add(leftColumnNumber)
                }
                if (!nodeTable.contains(rightColumnNumber)) {
                  nodeTable.add(rightColumnNumber)
                }
              }
            }
          }
        }
      })
    })

    // remove the column number that was added
    for(c <- 0 until table.size()){
      table.get(c).remove(table.get(c).size() - 1)
    }

    println("NodeTable Size: " + nodeTable.size())
    println("Starting graph generation")

    fdGraph = new DirectedSparseMultigraph[Integer, String](); // parentCol, childCol

    if (!nodeTable.isEmpty()) {
      // all nodes will be added at the end of this, -1 is root
      fdGraph.addVertex(-1)
      val nodeIter = nodeTable.iterator()
      while (nodeIter.hasNext) {
        fdGraph.addVertex(nodeIter.next())
      }
    }
    else{
      throw new Exception("Node table is empty when creating FDG")
    }

    // now connect each node with root
    if (!nodeTable.isEmpty()) {
      val nodeIter = nodeTable.iterator()
      while (nodeIter.hasNext) {
        val value = nodeIter.next()
        fdGraph.addEdge("-1 to " + value.toString, -1, value, EdgeType.DIRECTED)
      }
    }

    var removedCount = 0 // used to track how many collisions there are
    // now connect nodes with func dependencies
    if (!edgeTable.isEmpty()) {
      val edgeIter = edgeTable.iterator()
      while (edgeIter.hasNext) {
        val value: String = edgeIter.next()._1
        val a1: String = (value.split(",")) (0)
        val a2: String = (value.split(",")) (1)
        if(!fdGraph.containsEdge(a2 + " to " + a1)){
          fdGraph.addEdge(a1 + " to " + a2, a1.toInt, a2.toInt, EdgeType.DIRECTED)
        }
        else{
          removedCount += 1
          fdGraph.removeEdge(a2 + " to " + a1)
          fdGraph.addEdge(a1 + " to " + a2, a1.toInt, a2.toInt, EdgeType.DIRECTED)
        }
      }
    }

    if(showFDGraph) {
      showGraph(fdGraph)
    }
    println("There were " + removedCount + " removed edges in the FD-graph.")

    if(writeFile) {
      writeToFile(nodeTable, "twitter100Cols10kRowsWithScore")
    }


    if (!nodeTable.isEmpty()) {
      // will create a map, the keyset is the parents and the arraylist of each key is the grouping 'new tables', any ones with -1 as longest path are parentless
      val nodeIter = nodeTable.iterator()
      while (nodeIter.hasNext) {
        val value = nodeIter.next()
        val parent = parentOfLongestPath(fdGraph, value) // will return an integer that is the parent of the longestpath
        if(value != -1 && parent != -1){
//          println("VALUE,PARENT: " + sch(value)._1 + " , " + sch(parent)._1)
        }
        if (parentTable.containsKey(parent)) {
          parentTable.get(parent).add(value)
        }
        else {
          var aList = new ArrayList[Integer]()
          aList.add(value)
          parentTable.put(parent, aList)
        }
      }
    }

/*
    nodeTable.asScala.map((i) => {
      println("PARENT OF " + i + " IS " + parentOfLongestPath(fdGraph,i))
    })
    println("ParentTable Size: " + parentTable.size())
*/


    // create an ArrayList of all entity graphs, this is for display and traversal purposes
    if(combineEntityGraphs){
      singleEntityGraph = new DelegateTree[Integer, String]()
      if (!parentTable.isEmpty()) {
        val rootChildren: ArrayList[Integer] = parentTable.get(-1)
        singleEntityGraph.addVertex(-1)
        val childrenIterator = rootChildren.iterator()
        while (childrenIterator.hasNext) {
          val child: Integer = childrenIterator.next()
          singleEntityGraph.addChild("-1 to " + child, -1, child)
          buildEntityGraph(singleEntityGraph, child)
        }
      }
      else {
        throw new Exception("parent table is empty when creating FDG")
      }
    }
    else { // this will split all nodes with root as it's parent into it's own graph for viewing
      entityGraphList = new ArrayList[DelegateTree[Integer, String]]()
      entityGraphString = new ArrayList[DelegateTree[String, String]]()
      if (!parentTable.isEmpty()) {
        val rootChildren: ArrayList[Integer] = parentTable.get(-1)
        val childrenIterator = rootChildren.iterator()
        while (childrenIterator.hasNext) {
          val child: Integer = childrenIterator.next()
          var entityGraph = new DelegateTree[Integer, String]
          entityGraph.addVertex(-1)
          entityGraph.addChild("-1 to " + child, -1, child)
          buildEntityGraph(entityGraph, child)
          entityGraphList.add(entityGraph)
        }
      }
      else {
        throw new Exception("parent table is empty when creating FDG")
      }
    }

    // used for testing, disregard
    if(flattenParentTable) {

      var parentKeys: ArrayList[Integer] = new ArrayList[Integer]()
      var parentKeyIter = parentTable.keySet().iterator()
      while (parentKeyIter.hasNext) {
        parentKeys.add(parentKeyIter.next())
      }

      var removeParents: ArrayList[Integer] = new ArrayList[Integer]()

      for (outerLoc <- 0 until parentKeys.size()) {
        var outerParent: Integer = parentKeys.get(outerLoc) // should be an interger that is the parent
        if (outerParent != -1) {
          var outerList: ArrayList[Integer] = parentTable.get(outerParent)
          for (innerLoc <- 0 until parentKeys.size()) {
            var innerParent: Integer = parentKeys.get(innerLoc)
            if (innerParent != -1) {
              val innerList: ArrayList[Integer] = parentTable.get(innerParent)
              if (outerList.contains(innerParent)) {
                var newList: ArrayList[Integer] = new ArrayList[Integer]()
                newList.addAll(outerList)
                newList.addAll(innerList)
                parentTable.put(outerParent, newList)
                removeParents.add(innerParent)
              }
            }
          }
        }
      }

      for (loc <- 0 until removeParents.size()) {
        parentTable.remove(removeParents.get(loc))
      }
    }

//    println("ParentTable Size: " + parentTable.size())

    // output for testing
/*
    var keySet:util.Set[Integer] = parentTable.keySet()
    keySet.asScala.map((y) => {
      if(y != -1){
        println(sch(y))
        println("Number of children: " + parentTable.get(y).size())
      }
      else{
        println("Roots children: ")
        var keyS = parentTable.get(y).asScala
        keyS.map((l) => {
          println(sch(l))
          println("Root Child: " + l)
        })
      }
    })
*/

    var endQ:Long = System.nanoTime();
    println("PhaseOne TOOK: "+((endQ - startQ)/1000000) + " MILLISECONDS")

  }

  def updateEntityGraph():Unit = {
    if(outputEntityGraphs){
      if(combineEntityGraphs){
        showEntity(singleEntityGraph)
      }
      else {
        val entityIter = entityGraphList.iterator()
        while (entityIter.hasNext) {
          val entityGraph = entityIter.next()
          showEntity(entityGraph)
        }
      }

      var openFlag = true
      while(openFlag){
        println("For displaying graphs, please enter q or quit to close.")
        val in = new java.util.Scanner(System.in)
        val input = in.next()
        if(input.toLowerCase().equals("q") || input.toLowerCase().equals("quit")){
          openFlag = false
        }

      }
    }
  }


  /*
  Attempts to merge entities generated above with one another:

  */

  def mergeEntities(){
    // PHASE 2

    entityPairList = new ArrayList[(Integer,Integer)]()
    var parentList:ArrayList[Integer] = new ArrayList[Integer]() // List of all possible entities, excludes root
    var parentKeySetIter = parentTable.keySet().iterator()
    while(parentKeySetIter.hasNext){
      val parentVal = parentKeySetIter.next()
      if(parentVal != -1){
        parentList.add(parentVal)
      }
    }
    println("Number of parents: " + parentList.size())

//    var phase2Graph: DirectedSparseMultigraph[Integer, String] = new DirectedSparseMultigraph[Integer, String]();

    val graphPairs = new TreeMap[String,UndirectedSparseMultigraph[Integer,String]]()

    if(parentList.size() > 1){ // need at least 2 to compare, this compares the entities to each other and the values of their children
      for(i <- 0 until parentList.size()){
        for(j <- i until parentList.size()){
          if(i != j){

            var phase2Graph:UndirectedSparseMultigraph[Integer,String] = new UndirectedSparseMultigraph[Integer,String]()
            var leftEntity:ArrayList[Integer] = parentTable.get(parentList.get(i)) // these are the atttributes of the left parent
            var rightEntity:ArrayList[Integer] = parentTable.get(parentList.get(j)) // these two sets should be disjoint
            var leftEntityColumn:ArrayList[PrimitiveValue] = table.get(parentList.get(i)) // The column of values of the parent
            var rightEntityColumn:ArrayList[PrimitiveValue] = table.get(parentList.get(j))
            var childrenMatrix:ArrayList[ArrayList[Integer]] = new ArrayList[ArrayList[Integer]]() // this is a matrix that contains the values
            var numberOfJoins:Int = 0

            for(t <- 0 until parentTable.get(parentList.get(i)).size()){
              var tempL:ArrayList[Integer] = new ArrayList[Integer]
              for(g <- 0 until parentTable.get(parentList.get(j)).size()){
                tempL.add(0)
              }
              childrenMatrix.add(tempL) // initalize an arraylist with value 0 for each possible entry
            }
            for(location <- 0 until leftEntityColumn.size()) {
              // will iterate through every row of table
              if (leftEntityColumn.get(location).toString.toUpperCase() != "NULL" && rightEntityColumn.get(location).toString.toUpperCase() != "NULL") {
                if ((leftEntityColumn.get(location).toString).equals(rightEntityColumn.get(location).toString)) {
                  // same thing as join on the
                  // now need to look at the values of their children and update the matrix
                  for (g <- 0 until leftEntity.size()) {
                    for (t <- 0 until rightEntity.size()) {
                      if (leftEntity.get(g) != parentList.get(i) && leftEntity.get(g) != parentList.get(j) && rightEntity.get(t) != parentList.get(i) && rightEntity.get(t) != parentList.get(j)) {
                        val leftValue: PrimitiveValue = table.get(leftEntity.get(g)).get(location)
                        val rightValue: PrimitiveValue = table.get(rightEntity.get(t)).get(location)
                        if ((leftValue.toString).equals(rightValue.toString)) {
                          // then these could map to the same concept
                          var temp = childrenMatrix.get(g).get(t)
                          temp += 1
                          childrenMatrix.get(g).set(t, temp)
                        }
                      }
                    }
                  }
                  numberOfJoins += 1
                }
              }
            }

            var tempEntPairMatrix:TreeMap[Integer,TreeMap[Integer,Float]] = new TreeMap[Integer,TreeMap[Integer,Float]]() // you can look up the strength for both left and right entity attribute pairs

            if((numberOfJoins.toFloat/table.get(0).size().toFloat) >= (.01).toFloat) { // do this is reduce any poor results
              // this will add the children matrix to the entityPairMatrix with the key being the string of the entity pairs
              for (t <- 0 until childrenMatrix.size()) { // This will be the left EntAttributes
              var rightAtt:TreeMap[Integer,Float] = new TreeMap[Integer,Float]()
                for (g <- 0 until childrenMatrix.get(t).size()) { // This will be the right EntAttributes
                  rightAtt.put(rightEntity.get(g),childrenMatrix.get(t).get(g).toFloat/numberOfJoins.toFloat)
                }
                tempEntPairMatrix.put(leftEntity.get(t),rightAtt)
              }
            }

            if((numberOfJoins.toFloat/table.get(0).size().toFloat) >= (.01).toFloat) { // do this is reduce any poor results
              // this will add the children matrix to the entityPairMatrix with the key being the string of the entity pairs
              for (t <- 0 until rightEntity.size()) { // This will be the left EntAttributes
              var leftAtt:TreeMap[Integer,Float] = new TreeMap[Integer,Float]()
                for (g <- 0 until leftEntity.size()) { // This will be the right EntAttributes
                  leftAtt.put(leftEntity.get(g),childrenMatrix.get(g).get(t).toFloat/numberOfJoins.toFloat)
                }
                tempEntPairMatrix.put(rightEntity.get(t),leftAtt)
              }
            }

            if(tempEntPairMatrix.size() >= 1 && tempEntPairMatrix != null) {
              entityPairMatrix.put(parentList.get(i) + "," + parentList.get(j), tempEntPairMatrix)
            }

            val bestColumn = true

            for(t <- 0 until childrenMatrix.size()) {
              if (bestColumn) {
                var highestValue: Double = 0.0
                var highestPlace: Int = -1
                for (g <- 0 until childrenMatrix.get(t).size()) {
                  if (numberOfJoins > 0) {
                    // now check for
                    if (childrenMatrix.get(t).get(g).toFloat >= highestValue.toFloat) {
                      highestValue = childrenMatrix.get(t).get(g).toFloat
                      highestPlace = g
                    }
                  }
                }
                if (highestValue.toFloat/numberOfJoins.toFloat >= (threshhold.toFloat * threshhold.toFloat) && (numberOfJoins.toFloat/table.get(0).size().toFloat) >= (.01).toFloat) {
                  // then childrenMatrix at t is a pairing
                  if (highestPlace != -1) {
                    if (!(phase2Graph.containsVertex(leftEntity.get(t)))) {
                      phase2Graph.addVertex(leftEntity.get(t))
                    }
                    if (!(phase2Graph.containsVertex(rightEntity.get(highestPlace)))) {
                      phase2Graph.addVertex(rightEntity.get(highestPlace))
                    }
                    phase2Graph.addEdge(leftEntity.get(t) + " And " + rightEntity.get(highestPlace), leftEntity.get(t), rightEntity.get(highestPlace))
                  }
                }
              }
              else{
                for (g <- 0 until childrenMatrix.get(t).size()) {
                  if (childrenMatrix.get(t).get(g).toFloat/numberOfJoins.toFloat >= (threshhold.toFloat * threshhold.toFloat)) {
                    // then childrenMatrix at t is a pairing
                    if (!(phase2Graph.containsVertex(leftEntity.get(t)))) {
                      phase2Graph.addVertex(leftEntity.get(t))
                    }
                    if (!(phase2Graph.containsVertex(rightEntity.get(g)))) {
                      phase2Graph.addVertex(rightEntity.get(g))
                    }
                    phase2Graph.addEdge(leftEntity.get(t) + " And " + rightEntity.get(g), leftEntity.get(t), rightEntity.get(g))
                    //                      println("STR: "+childrenMatrix.get(t).get(g).toFloat/numberOfJoins.toFloat + " > " + (threshhold.toFloat * threshhold.toFloat))
                  }
                }
              }
            }

            if(phase2Graph.getVertexCount > 1){
              graphPairs.put(parentList.get(i)+","+parentList.get(j),phase2Graph)
              entityPairList.add((parentList.get(i), parentList.get(j)))
            }
          }
        }
      }
    }

    if(!entityPairList.isEmpty){
      entityPairList.asScala.map((tuple) => {
        val col1 = tuple._1
        val col2 = tuple._2
        println(col1 + " AND " + col2 + " CAN BE MERGED")
      })
    }
    else{
      println("NO ENTITIES CAN BE MERGED")
    }

    endTime = System.currentTimeMillis()
    println("PHASE1 and PHASE2 TOOK: "+(endTime - startTime) + " MILLISECONDS")
    //writer.println("PHASE1 and PHASE2 TOOK: "+(endTime - startTime) + " MILLISECONDS")
    //writer.close()

    //    matchEnt(graphPairs)
//    entityPairMatrixResult()
  }


  /*
    Create's views from parent table, basic implimentation that looks at the parentTable and creates a view for each parent if it has > 1 child
  */

  def createViews():ArrayList[String] = {
    val parentIter = parentTable.keySet().iterator()
    val viewList : ArrayList[String] = new ArrayList[String]()
    while(parentIter.hasNext) {
      val parent = parentIter.next()
      val childrenArray = parentTable.get(parent)
      if(childrenArray.size() > 1) {
        var viewOutput = "CREATE VIEW "
        if (parent == -1) {
          viewOutput += " ROOT AS SELECT"
        }
        else {
          viewOutput += sch(parent)._1 + " AS SELECT"
        }
        childrenArray.asScala.map((x) => {
          viewOutput = viewOutput + " " + sch(x)._1 + ","
        })
        viewOutput = viewOutput.substring(0, viewOutput.size - 1) // to remove the last comma
        viewOutput += " FROM " + tableName + ";"
        println(viewOutput)
        viewList.add(viewOutput)
      }
    }
    return viewList
  }

  def matchEnt(graphPairs:TreeMap[String,UndirectedSparseMultigraph[Integer,String]],parentTable: TreeMap[Integer, ArrayList[Integer]]): Unit ={

    var pairIter = graphPairs.keySet().iterator()
    while(pairIter.hasNext){
      var graphKey = pairIter.next()
      var pair:UndirectedSparseMultigraph[Integer,String] = graphPairs.get(graphKey)
      println(graphKey)
//      showGraph(pair)
    }

  }

  def getPairs(entity:Integer): ArrayList[String] ={ // Returns an arrayList with all of the pairs that that entity is part of, entiity is a single entity
    var pairs:ArrayList[String] = new ArrayList[String]()
    entityPairList.asScala.foreach( rawPair => {
      if(rawPair._1 == entity || rawPair._2 == entity){
        pairs.add(rawPair._1+","+rawPair._2)
      }
    }
    )
    pairs
  }

  // entity is the root entity you're looking for
  def best(pair:String,entity:Integer): ArrayList[((Integer,Integer),Float)] = { // takes in an entiity pair as it's input

    // the return value, (integer,integer) is the pair, the left will be part of the entity
    // passed in and the right will be the corresponding attribute number from the other entiity
    // from the pair, float is the strength for that attribute pair in that entiity pair

    var list:ArrayList[((Integer,Integer),Float)] = new ArrayList[((Integer,Integer),Float)]()

    var attributeList:ArrayList[Integer] = parentTable.get(entity) // the attributelist
    var lookupMatrix:TreeMap[Integer,TreeMap[Integer,Float]] = entityPairMatrix.get(pair)
    for(i <- 0 until attributeList.size()){ // loop through all the specific entities attributes
      var highestStrength:Float = (0.0).toFloat
      var bestAttributeMatch = -1
      var attribute:Integer = attributeList.get(i)
      var strengthList:TreeMap[Integer,Float] = lookupMatrix.get(attribute)
      var keyIter:util.Iterator[Integer] = strengthList.keySet().iterator()

      while(keyIter.hasNext){
        val correspondingAttribute:Integer = keyIter.next()
        val str:Float = strengthList.get(correspondingAttribute)
        if(str > highestStrength){
          highestStrength = str
          bestAttributeMatch = correspondingAttribute
        }
      }
      if(highestStrength >= (0.0).toFloat && bestAttributeMatch != -1){ //sanity check
        list.add(((attribute,bestAttributeMatch),highestStrength))
      }
      else{
        println("Something went wrong in best funcDep I think")
      }
    }
    list
  }

  def entityPairMatrixResult():Unit = {
    entityPairList.asScala.foreach( rawPair => {
      val entityPair:String = rawPair._1+","+rawPair._2
      val attributeMatrix:TreeMap[Integer,TreeMap[Integer,Float]]= entityPairMatrix.get(entityPair)
      println("ENTITY PAIR: "+ entityPair)
      /*      for(j <- 0 until parentTable.get(entityPair.split(",")(0).toInt).size()){
              val leftEntityAtt:Int = parentTable.get(entityPair.split(",")(0).toInt).get(j)
              println("")
            } */
      val outerIter = attributeMatrix.keySet().iterator()
      while(outerIter.hasNext){
        val outerVal:Integer = outerIter.next()
        val innerIter = attributeMatrix.get(outerVal).keySet().iterator()
        while(innerIter.hasNext){
          val innerVal:Integer = innerIter.next()
          println("FOUND THESE VALUES: " + outerVal + " , " + innerVal + " WITH STR: " + attributeMatrix.get(outerVal).get(innerVal))
        }
      }
    })
  }


  def parentOfLongestPath(g:DirectedSparseMultigraph[Integer,String], v:Int): Int = {
    if(g.getPredecessors(v) == null){
      println("GRAPH DOES NOT CONTAIN THIS NODE")
    }
    if(g.getPredecessorCount(v) == 1){
      return -1 // must be the root
    }
    else{
      val longestPathVar:ArrayList[Integer] = longestPath(g,g.getPredecessors(v),new ArrayList[Integer]())
      return longestPathVar.get(0)
    }
  }



  def longestPath(g:DirectedSparseMultigraph[Integer,String],predList:util.Collection[Integer],currentPath:ArrayList[Integer]): ArrayList[Integer] = {
    var longestPathV:ArrayList[Integer] = null
    if(predList.size() == 1){ // because of root
        return currentPath
    }
    else{
      var listIter = predList.iterator()
      while(listIter.hasNext) {
        var temp = listIter.next()
        if(temp != -1 && !currentPath.contains(temp)){
          var ret1:ArrayList[Integer] = new ArrayList[Integer](currentPath)
          ret1.add(temp)
          var returnPathV = longestPath(g, g.getPredecessors(temp), ret1)
          if(returnPathV != null){
            if(longestPathV != null){
              if (returnPathV.size >= longestPathV.size) {
                longestPathV = new ArrayList(returnPathV)
              }
            }
            else{
              longestPathV = new ArrayList(returnPathV)
            }
          }
        }
      }
    }
    return longestPathV
  }


  // constructs the entity tree, -1 and parent must be added first and g initalized
  def buildEntityGraph(g:DelegateTree[Integer,String], parent:Integer):Unit = {
    if(parentTable.containsKey(parent)){
      val childrenList = parentTable.get(parent)
      val childrenIter = childrenList.iterator()
      while(childrenIter.hasNext){
        val child = childrenIter.next()
        g.addChild(parent.toString + " to " + child.toString, parent, child)
        buildEntityGraph(g,child)
      }
    }
  }

  def showGraph(g:DirectedSparseMultigraph[Integer,String]): Unit ={
    // The Layout<V, E> is parameterized by the vertex and edge types
    var layout: Layout[Integer, String] = new CircleLayout(g);
    layout.setSize(new Dimension(1200, 1200));
    // sets the initial size of the space
    // The BasicVisualizationServer<V,E> is parameterized by the edge types
    var vv: BasicVisualizationServer[Integer, String] = new BasicVisualizationServer[Integer, String](layout);
    vv.setPreferredSize(new Dimension(1200, 1200));

    var vertexPaint:Transformer[Integer,Paint] = new Transformer[Integer,Paint]() {
      def transform(i: Integer): Paint = {
        Color.GREEN
      }
    };

    var dash:Array[Float] = Array(10.0f);
    val edgeStroke:Stroke = new BasicStroke(1.0f, BasicStroke.CAP_BUTT, BasicStroke.JOIN_MITER, 10.0f, dash, 0.0f);
    var edgeStrokeTransformer:Transformer[String, Stroke] = new Transformer[String, Stroke]() {
      def transform(s: String): Stroke = {
        edgeStroke;
      }
    };

    vv.getRenderContext().setVertexFillPaintTransformer(vertexPaint);
    vv.getRenderContext().setEdgeStrokeTransformer(edgeStrokeTransformer);
    vv.getRenderContext().setVertexLabelTransformer(new ToStringLabeller());
    vv.getRenderContext().setEdgeLabelTransformer(new ToStringLabeller());
    vv.getRenderer().getVertexLabelRenderer().setPosition(Position.CNTR);


    //Sets the viewing area size
    var frame: JFrame = new JFrame("Simple Graph View");
    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    frame.getContentPane().add(vv);
    frame.pack();
    frame.setVisible(true);
  }

  def showEntity(g:DelegateTree[Integer,String]): Unit ={
    // The Layout<V, E> is parameterized by the vertex and edge types
    var layout: Layout[Integer, String] = new TreeLayout(g);
//    layout.setSize(new Dimension(1200, 1200));
    // sets the initial size of the space
    // The BasicVisualizationServer<V,E> is parameterized by the edge types
    var vv: BasicVisualizationServer[Integer, String] = new BasicVisualizationServer[Integer, String](layout);
    vv.setBounds(1000,0,3000,3000)

    var vertexPaint:Transformer[Integer,Paint] = new Transformer[Integer,Paint]() {
      def transform(i: Integer): Paint = {
        Color.GREEN
      }
    };

    var dash:Array[Float] = Array(10.0f);
    val edgeStroke:Stroke = new BasicStroke(1.0f, BasicStroke.CAP_BUTT, BasicStroke.JOIN_MITER, 10.0f, dash, 0.0f);
    var edgeStrokeTransformer:Transformer[String, Stroke] = new Transformer[String, Stroke]() {
      def transform(s: String): Stroke = {
        edgeStroke;
      }
    };

    vv.getRenderContext().setVertexFillPaintTransformer(vertexPaint);
    vv.getRenderContext().setEdgeStrokeTransformer(edgeStrokeTransformer);
    vv.getRenderContext().setVertexLabelTransformer(new ToStringLabeller());
    vv.getRenderContext().setEdgeLabelTransformer(new ToStringLabeller());
    vv.getRenderer().getVertexLabelRenderer().setPosition(Position.CNTR);

    //Sets the viewing area size
    var frame : JFrame = new JFrame("Entity Graph")
    frame.setBounds(0,1000,3000,3000)
    frame.add(vv);
    frame.setVisible(true);
    frame.setVisible(true)
  }

  def serialize(): Array[Byte] = 
  {
    val byteBucket = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(byteBucket);
    out.writeObject(this)
    byteBucket.toByteArray
  }

  def serializeTo(db: mimir.Database, name: String): Unit =
  {
    FuncDep.initBackstore(db)
    db.backend.update(
      "INSERT OR REPLACE INTO "+FuncDep.BACKSTORE_TABLE_NAME+"(name, data) VALUES (?,?)", 
      List(StringPrimitive(name), BlobPrimitive(serialize()))
    )
  }

  def depth(node:Integer,score:Integer): Integer ={
    val pList = fdGraph.getPredecessors(node)
    var highestScore = 0
    if(pList.size() == 1){ // because of root
      return score + 1
    }
    else{
      var listIter = pList.iterator()
      while(listIter.hasNext) {
        var temp = listIter.next()
        if(temp != -1){
          val retScore = depth(temp,score + 1)
          if(retScore > highestScore){
            highestScore = retScore
          }
        }
      }
    }
    return highestScore
  }

  def writeToFile(nodeTable:ArrayList[Integer],outputFileName: String): Unit = {
    var writer: BufferedWriter = null
    try {
      writer = new BufferedWriter(new FileWriter(outputFileName+".csv"))
    }
    catch {
      case e: FileNotFoundException => e.printStackTrace()
      case e: UnsupportedEncodingException => e.printStackTrace()
    }
    writer.write("ATTR,PARENT,SCORE\n")

    var deepestPath = 0
    val scoreMap: util.TreeMap[Integer,Integer] = new util.TreeMap[Integer,Integer]()

    nodeTable.asScala.map((node) => {
      val score = depth(node,0)
      scoreMap.put(node,score)
      if(score > deepestPath){
        deepestPath = score
      }
    })

    nodeTable.asScala.map((node) => {
      var parent: util.Collection[Integer] = fdGraph.getPredecessors(node)
      if(parent != null) {
        if (!parent.isEmpty) {
          val pIter = parent.iterator()
          while (pIter.hasNext) {
            val p = pIter.next()
/*
            println("FOR NODE: " + node)
            println(scoreMap.get(node))
            println(deepestPath)
            println((scoreMap.get(node).toFloat/deepestPath.toFloat))
            println((scoreMap.get(node).toFloat/deepestPath.toFloat).toString)
            println("")
*/
            if(p != -1) {
              writer.write(node.toString + "," + p.toString + "," + (scoreMap.get(p).toFloat / deepestPath.toFloat).toString + "\n")
            }
            else{
              writer.write(node.toString + "," + p.toString + "," + (1.toFloat / deepestPath.toFloat).toString + "\n")
            }
          }
        }
        else{
          println("parent is empty!")
        }
      }
      else{
        println(node)
      }
    })
    writer.close()
  }

}


object FuncDep {

  val BACKSTORE_TABLE_NAME = "MIMIR_FUNCDEP_BLOBS"

  def initBackstore(db: mimir.Database)
  {
    if(db.getTableSchema(FuncDep.BACKSTORE_TABLE_NAME) == None){
      db.backend.update(
        "CREATE TABLE "+FuncDep.BACKSTORE_TABLE_NAME+"(name varchar(40), data blob, PRIMARY KEY(name))"
      )
    }
  }
  def deserialize(data: Array[Byte]): FuncDep = 
  {
    val in = new ObjectInputStream(new ByteArrayInputStream(data))
    val obj = in.readObject()
    obj.asInstanceOf[FuncDep]
  }
  def deserialize(db: mimir.Database, name: String): FuncDep =
  {
    val blob = 
      db.backend.singletonQuery(
        "SELECT data FROM "+BACKSTORE_TABLE_NAME+" WHERE name=?", 
        List(StringPrimitive(name))
      ).asInstanceOf[BlobPrimitive]
    deserialize(blob.v)
  }

}
