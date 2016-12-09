package mimir.algebra

import java.io._
import java.util

import mimir.exec.{ResultIterator, ResultSetIterator}
import mimir.algebra.Type.T
import java.util.TreeMap
import java.util.ArrayList
import java.util.HashMap
import javax.swing.JFrame

import edu.uci.ics.jung.algorithms.layout.{CircleLayout, Layout}
import edu.uci.ics.jung.graph.{DirectedSparseMultigraph, Graph, SparseMultigraph, UndirectedSparseMultigraph}
import edu.uci.ics.jung.graph.util.EdgeType
import edu.uci.ics.jung.visualization.BasicVisualizationServer
import edu.uci.ics.jung.visualization.decorators.ToStringLabeller
import org.apache.commons.collections15.Transformer
import java.awt.BasicStroke
import java.awt.Color
import java.awt.Dimension
import java.awt.Paint
import java.awt.Stroke
import java.sql.ResultSet
import mimir.util.JDBCUtils

import edu.uci.ics.jung.visualization.renderers.Renderer.VertexLabel.Position

import scala.collection.JavaConverters._

@SerialVersionUID(100L)
class FuncDep 
  extends Serializable
{

  val threshhold:Double = 0.99

  var table:ArrayList[ArrayList[PrimitiveValue]] = null
  var countTable:ArrayList[TreeMap[String,Integer]] = null
  var densityTable:ArrayList[Integer] = null // gives the density for column, that is percentage of non-null values
  var sch:List[(String, T)] = null // the schema, is a lookup for the type and
  var parentTable: TreeMap[Integer, ArrayList[Integer]] = null
  var entityPairMatrix:TreeMap[String,TreeMap[Integer,TreeMap[Integer,Float]]] = null // outer string is entity pair so column#,column#: to a treemap that is essentially a look-up matrix for columns that contain column to column strengths
  var startTime:Long = 0
  var endTime:Long = 0
  // Outputs
  var entityPairList:List[(Integer,Integer)] = Nil


  /* inserts the input into table and fills the count table, table is the same as a generic sql table, and countTable is a count of each unique value for each column of the table */

  def buildAbadi(schema: List[(String, T)],data: ResultIterator): Unit = {

    table = new ArrayList[ArrayList[PrimitiveValue]]() // contains every row from resultIter, is the data from the input query
    entityPairMatrix = new TreeMap[String,TreeMap[Integer,TreeMap[Integer,Float]]]()
    sch = schema
    countTable = new ArrayList[TreeMap[String,Integer]]() // contains the rows
    densityTable = new ArrayList[Integer]()
//    println(sch(56)._1)

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
        if(!v.toString.toUpperCase().equals("NULL")){
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

    phaseOne()
  }

  def buildAbadi(schema: List[(String, T)],data: ResultSet): Unit = {

    table = new ArrayList[ArrayList[PrimitiveValue]]() // contains every row from resultIter aka data
    entityPairMatrix = new TreeMap[String,TreeMap[Integer,TreeMap[Integer,Float]]]()
    sch = schema
    countTable = new ArrayList[TreeMap[String,Integer]]() // contains the rows
    densityTable = new ArrayList[Integer]()

    sch.map{ case(k, t) => {
      table.add(new util.ArrayList[PrimitiveValue]())
      countTable.add(new TreeMap[String,Integer])
      densityTable.add(0)
    }
    }

    var d:List[List[PrimitiveValue]] = mimir.util.JDBCUtils.extractAllRows(data)

    d.map((row)=>{
        var loc = 0
        row.map((pv) => {
          table.get(loc).add(pv)

          if(!pv.toString.toUpperCase().equals("NULL")){
            var temp = densityTable.get(loc)
            temp+=1
            densityTable.set(loc,temp)
          }
          if(countTable.get(loc).containsKey(pv.toString)){
            countTable.get(loc).replace(pv.toString,countTable.get(loc).get(pv.toString),countTable.get(loc).get(pv.toString)+1)
          }
          else{
            countTable.get(loc).put(pv.toString,1)
          }

          loc += 1
        })
    })

    phaseOne()
  }

  def phaseOne() {
    var startQ:Long = System.nanoTime();

    startTime = System.currentTimeMillis()
    var nodeTable: ArrayList[Integer] = new ArrayList[Integer]() // contains a list of all the nodes
    var edgeTable: ArrayList[String] = new ArrayList[String]() // contains the node numbers for the dependency graph, the names are numbers from the schema 0 to sch.length are the possibilities
    var maxTable: ArrayList[String] = new ArrayList[String]() // contains the max values for each column, used for phase1 formula
    val flattenParentTable:Boolean = false // used if wanting to flatten the parent table so it uses the child of root

    parentTable = new TreeMap[Integer, ArrayList[Integer]]()

    // Finds the maxKey for each column
    countTable.asScala.map((tree)=>{
      var maxKey = ""
      var maxValue = 0
      var keyIt = tree.keySet().iterator()
      while (keyIt.hasNext) {
        var value:String = keyIt.next()
        if(!value.toUpperCase().equals("NULL")) {
          if (maxValue < tree.get(value)) {
            maxKey = value
            maxValue = tree.get(value)
          }
        }
      }
      maxTable.add(maxKey)
    })

    for(c <- 0 until table.size()){
      table.get(c).add(new IntPrimitive(c))
    }

    table.asScala.par.map((locationJ)=>{

      var outerLocation:Int = locationJ.get(locationJ.size()-1).asString.toInt
      val leftType = sch(outerLocation)._2
      val leftMap = new ArrayList[PrimitiveValue](locationJ)
      val leftColumnName = sch(outerLocation)._1
      val leftDensity = densityTable.get(outerLocation).toFloat / locationJ.size()
      leftMap.remove(leftMap.size()-1)
      table.asScala.map((locationK)=>{
        var innerLocation = locationK.get(locationK.size()-1).asString.toInt
        if (outerLocation != innerLocation){
          val rightType = sch(innerLocation)._2
          val rightMap = new ArrayList[PrimitiveValue](locationK)
          val rightColumnName = sch(innerLocation)._1
          val rightDensity = densityTable.get(innerLocation).toFloat / locationK.size()
          var tempMap: HashMap[String, Integer] = new HashMap[String, Integer]() // the size of this will be the unique number of a1,a2 pairs
          rightMap.remove(rightMap.size()-1)

          val leftIter = leftMap.iterator()
          val rightIter = rightMap.iterator()

          var secondCount = 0 // how many unique pairings there are

          while (leftIter.hasNext && rightIter.hasNext) {
            val leftVal: PrimitiveValue = leftIter.next()
            val rightVal: PrimitiveValue = rightIter.next()
            val value: String = leftVal.toString() + ",M," + rightVal.toString() // doing this because I'm weird and will be taken out later when optimized
            if (tempMap.containsKey(value)) {
              tempMap.replace(value, tempMap.get(value), tempMap.get(value) + 1)
            }
            else {
              tempMap.put(value, 1)
              if (rightVal.toString().equals(maxTable.get(innerLocation))) {
                secondCount += 1
              }
            }
          }
          if (tempMap.size() != 0) {
            val strength: Double = (countTable.get(outerLocation).size().toFloat - secondCount.toFloat) / (tempMap.size().toFloat - secondCount.toFloat) // using first formula from paper right now
            if (strength >= threshhold && leftDensity >= rightDensity && secondCount > 9 && countTable.get(outerLocation).size() != table.get(outerLocation).size() && countTable.get(innerLocation).size() != table.get(innerLocation).size()) { // phase one constraints
            //if (strength >= threshhold  && countTable.get(outerLocation).size() != table.get(outerLocation).size() && countTable.get(innerLocation).size() != table.get(innerLocation).size()) { // phase one constraints
              /*                println("SECONDCOUNT IS: " + secondCount)
                              println("MAX VALUE IS: "+ maxTable.get(k))
                              println("Functional Dependancy between: " + leftColumnName + " and " + rightColumnName + " STR: " + strength)
                              println("Str EQUALS: " + countTable.get(j).size + " / " + tempMap.size())
              */                edgeTable.add(outerLocation.toString + "," + innerLocation.toString)
              if (!nodeTable.contains(outerLocation)) {
                nodeTable.add(outerLocation)
              }
              if (!nodeTable.contains(innerLocation)) {
                nodeTable.add(innerLocation)
              }
            }
          }
        }
      })
    })

    for(c <- 0 until table.size()){
      table.get(c).remove(table.get(c).size() - 1)
    }

    println("NodeTable Size: " + nodeTable.size())

    var g: DirectedSparseMultigraph[Integer, String] = new DirectedSparseMultigraph[Integer, String]();

    if (!nodeTable.isEmpty()) {
      // all nodes will be added at the end of this, -1 is root
      g.addVertex(-1)
      val nodeIter = nodeTable.iterator()
      while (nodeIter.hasNext) {
        g.addVertex(nodeIter.next())
      }
    }

    // now connect each node with root
    if (!nodeTable.isEmpty()) {
      val nodeIter = nodeTable.iterator()
      while (nodeIter.hasNext) {
        val value = nodeIter.next()
        g.addEdge("-1 to " + value.toString, -1, value, EdgeType.DIRECTED)
      }
    }
    // now connect nodes with func dependencies
    if (!edgeTable.isEmpty()) {
      val edgeIter = edgeTable.iterator()
      while (edgeIter.hasNext) {
        val value: String = edgeIter.next()
        val a1: String = (value.split(",")) (0)
        val a2: String = (value.split(",")) (1)
        if(!g.containsEdge(a2 + " to " + a1)){
          g.addEdge(a1 + " to " + a2, a1.toInt, a2.toInt, EdgeType.DIRECTED)
        }
      }
    }


    if (!nodeTable.isEmpty()) {
      // will create a map, the keyset is the parents and the arraylist of each key is the grouping 'new tables', any ones with -1 as longest path are parentless
      val nodeIter = nodeTable.iterator()
      while (nodeIter.hasNext) {
        val value = nodeIter.next()
        val parent = parentOfLongestPath(g, value) // will return an integer that is the parent of the longestpath
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

    println("ParentTable Size: " + parentTable.size())

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

    var endQ:Long = System.nanoTime();
    println("PhaseOne TOOK: "+((endQ - startQ)/1000000) + " MILLISECONDS")

    phaseTwo()

  }

  def phaseTwo(){
    // PHASE 2

    var parentList:ArrayList[Integer] = new ArrayList[Integer]() // List of all possible entities, excludes root
    var parentKeySetIter = parentTable.keySet().iterator()
    while(parentKeySetIter.hasNext){
      val parentVal = parentKeySetIter.next()
      if(parentVal != -1){
        parentList.add(parentVal)
      }
    }
    println("Number of parents: "+parentList.size())

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
              entityPairList = (parentList.get(i), parentList.get(j)) :: entityPairList
            }
          }
        }
      }
    }

    endTime = System.currentTimeMillis()
    println("PHASE1 and PHASE2 TOOK: "+(endTime - startTime) + " MILLISECONDS")
    //writer.println("PHASE1 and PHASE2 TOOK: "+(endTime - startTime) + " MILLISECONDS")
    //writer.close()

    //    matchEnt(graphPairs)
//    entityPairMatrixResult()
  }

  def matchEnt(graphPairs:TreeMap[String,UndirectedSparseMultigraph[Integer,String]],parentTable: TreeMap[Integer, ArrayList[Integer]]): Unit ={

    var pairIter = graphPairs.keySet().iterator()
    while(pairIter.hasNext){
      var graphKey = pairIter.next()
      var pair:UndirectedSparseMultigraph[Integer,String] = graphPairs.get(graphKey)
      println(graphKey)
      showGraph(pair)
    }

  }

  def getPairs(entity:Integer): ArrayList[String] ={ // Returns an arrayList with all of the pairs that that entity is part of, entiity is a single entity
    var pairs:ArrayList[String] = new ArrayList[String]()
    entityPairList.foreach( rawPair => {
      if(rawPair._1 == entity || rawPair._2 == entity){
        pairs.add(rawPair._1+","+rawPair._2)
      }
    }
    )
    pairs
  }

  // entity is the root entity you're looking for
  def best(pair:String,entity:Integer): ArrayList[((Integer,Integer),Float)] ={ // takes in an entiity pair as it's input

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
    entityPairList.foreach( rawPair => {
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
          var ret1:ArrayList[Integer] = currentPath
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

  def showGraph(g:Graph[Integer,String]): Unit ={
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
