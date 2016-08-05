package mimir.algebra

import java.awt.Dimension
import java.util

import mimir.exec.ResultIterator
import mimir.algebra.Type.T
import java.util.TreeMap
import java.util.ArrayList
import java.util.HashMap
import javax.swing.JFrame

import edu.uci.ics.jung.algorithms.layout.{CircleLayout, Layout}
import edu.uci.ics.jung.graph.DirectedSparseMultigraph
import edu.uci.ics.jung.graph.util.EdgeType
import edu.uci.ics.jung.visualization.BasicVisualizationServer
import edu.uci.ics.jung.graph.UndirectedSparseMultigraph


class FuncDep {

  val threshhold:Double = 0.99

  var table:ArrayList[ArrayList[PrimitiveValue]] = null
  var countTable:ArrayList[TreeMap[String,Integer]] = null

  /* inserts the input into table and fills the count table, table is the same as a generic sql table, and countTable is a count of each unique value for each column of the table */

  def initialStep(sch: List[(String, T)],data: ResultIterator): Unit = {
    table = new ArrayList[ArrayList[PrimitiveValue]]() // contains every row from resultIter aka data
    countTable = new ArrayList[TreeMap[String,Integer]]() // contains the rows
    sch.map{ case(k, t) => {
      table.add(new util.ArrayList[PrimitiveValue]())
      countTable.add(new TreeMap[String,Integer])
    }
    }

    while(data.getNext()){ // adds every row to table
      (1 until data.numCols).map( (i) => {
        val v = data(i)
        table.get(i-1).add(v)
        if(countTable.get(i-1).containsKey(v.toString)){
          countTable.get(i-1).replace(v.toString,countTable.get(i-1).get(v.toString),countTable.get(i-1).get(v.toString)+1)
        }
        else{
          countTable.get(i-1).put(v.toString,1)
        }
      })
    }

    phaseOne(sch,data)
  }

  def phaseOne(sch: List[(String, T)],data: ResultIterator) {

    var nodeTable: ArrayList[Integer] = new ArrayList[Integer]() // contains a list of all the nodes
    var edgeTable: ArrayList[String] = new ArrayList[String]() // contains the node numbers for the dependency graph, the names are numbers from the schema 0 to sch.length are the possibilities
    var parentTable: TreeMap[Integer, ArrayList[Integer]] = new TreeMap[Integer, ArrayList[Integer]]()
    var maxTable: ArrayList[String] = new ArrayList[String]() // contains the max values for each column, used for phase1 formula

    for (i <- 0 until countTable.size()) {
      var maxKey = ""
      var maxValue = 0
      var keyIt = countTable.get(i).keySet().iterator()
      while (keyIt.hasNext) {
        var va = keyIt.next()
        if (maxValue < countTable.get(i).get(va)) {
          maxKey = va
          maxValue = countTable.get(i).get(va)
        }
      }
      maxTable.add(maxKey)
    }

    // now compare every column to every other column

    for (j <- 0 until table.size()) {
      // leftMap would be the column of a1 from the paper
      val leftType = sch(j)._2
      val leftMap = table.get(j)
      val leftColumnName = sch(j)._1
      for (k <- 0 until table.size()) {
        // rightMap would be the column of a2 from the paper
        if (j != k) {
          val rightType = sch(k)._2
          val rightMap = table.get(k)
          val rightColumnName = sch(k)._1
          var tempMap: HashMap[String, Integer] = new HashMap[String, Integer]() // the size of this will be the unique number of a1,a2 pairs

          val leftIter = leftMap.iterator()
          val rightIter = rightMap.iterator()

          var secondCount = 0

          while (leftIter.hasNext && rightIter.hasNext) {
            val leftVal = leftIter.next()
            val rightVal = rightIter.next()
            val value: String = leftVal.toString() + ",M," + rightVal.toString() // doing this because I'm weird and will be taken out later when optimized
            if (tempMap.containsKey(value)) {
              tempMap.replace(value, tempMap.get(value), tempMap.get(value) + 1)
            }
            else {
              tempMap.put(value, 1)
              if (rightVal.toString().equals(maxTable.get(k))) {
                secondCount += 1
              }
            }
          }
          if (tempMap.size() != 0) {
            val strength: Double = (countTable.get(j).size().toFloat - secondCount.toFloat) / (tempMap.size().toFloat - secondCount.toFloat) // using first formula from paper right now
            if (strength >= threshhold) {
              println("SECONDCOUNT IS: " + secondCount)
              println("Functional Dependancy between: " + leftColumnName + " and " + rightColumnName + " STR: " + strength)
              println("Str EQUALS: " + countTable.get(j).size + " / " + tempMap.size())
              edgeTable.add(j.toString + "," + k.toString) // list of
              if (!nodeTable.contains(j)) {
                nodeTable.add(j)
              }
              if (!nodeTable.contains(k)) {
                nodeTable.add(k)
              }
            }
          }
        }
      }
    }

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
        g.addEdge(a1 + " to " + a2, a1.toInt, a2.toInt, EdgeType.DIRECTED)
      }
    }

    println(g.toString)

    // The Layout<V, E> is parameterized by the vertex and edge types
    var layout: Layout[Integer, String] = new CircleLayout(g);
    layout.setSize(new Dimension(1200, 1200));
    // sets the initial size of the space
    // The BasicVisualizationServer<V,E> is parameterized by the edge types
    var vv: BasicVisualizationServer[Integer, String] = new BasicVisualizationServer[Integer, String](layout);
    vv.setPreferredSize(new Dimension(1200, 1200));
    //Sets the viewing area size
    var frame: JFrame = new JFrame("Simple Graph View");
    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    frame.getContentPane().add(vv);
    frame.pack();
    frame.setVisible(true);

    if (!nodeTable.isEmpty()) {
      // will create a map, the keyset is the parents and the arraylist of each key is the grouping 'new tables', any ones with -1 as longest path are parentless
      val nodeIter = nodeTable.iterator()
      while (nodeIter.hasNext) {
        val value = nodeIter.next()
        val parent = parentOfLongestPath(g, value) // will return an integer that is the parent of the longestpath
        println("VALUE,PARENT: " + value + " , " + parent)
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

    phaseTwo(parentTable)

  }

  def phaseTwo(parentTable: TreeMap[Integer, ArrayList[Integer]]){
    // PHASE 2

    var parentList:ArrayList[Integer] = new ArrayList[Integer]() // List of all possible entities, excludes root
    var parentKeySetIter = parentTable.keySet().iterator()
    while(parentKeySetIter.hasNext){
      val parentVal = parentKeySetIter.next()
      if(parentVal != -1){
        parentList.add(parentVal)
      }
    }

//    var phase2Graph: DirectedSparseMultigraph[Integer, String] = new DirectedSparseMultigraph[Integer, String]();

    var graphPairs:TreeMap[String,UndirectedSparseMultigraph[Integer,String]] = new TreeMap[String,UndirectedSparseMultigraph[Integer,String]]()

    if(parentList.size() > 1){ // need at least 2 to compare, this compares the entities to each other and the values of their children
      for(i <- 0 until parentList.size()){
        for(j <- i until parentList.size()){
          if(i != j){

            var phase2Graph:UndirectedSparseMultigraph[Integer,String] = new UndirectedSparseMultigraph[Integer,String]()
            var leftEntity:ArrayList[Integer] = parentTable.get(parentList.get(i))
            var rightEntity:ArrayList[Integer] = parentTable.get(parentList.get(j)) // these two sets should be disjoint
            var leftEntityColumn:ArrayList[PrimitiveValue] = table.get(parentList.get(i))
            var rightEntityColumn:ArrayList[PrimitiveValue] = table.get(parentList.get(j))
            var childrenMatrix:ArrayList[ArrayList[Integer]] = new ArrayList[ArrayList[Integer]]()
            var numberOfJoins:Int = 0

            for(t <- 0 until parentTable.get(parentList.get(i)).size()){
              var tempL:ArrayList[Integer] = new ArrayList[Integer]
              for(g <- 0 until parentTable.get(parentList.get(j)).size()){
                tempL.add(0)
              }
              childrenMatrix.add(tempL) // initalize an arraylist with value 0 for each possible entry
            }
            for(location <- 0 until leftEntityColumn.size()){ // will iterate throuh every row of table
              if((leftEntityColumn.get(location).asString).equals(rightEntityColumn.get(location).asString)){ // same thing as join on the
                // now need to look at the values of their children and update the matrix
                for(g <- 0 until leftEntity.size()){
                  for(t <- 0 until rightEntity.size()) {
                    if (leftEntity.get(g) !=  parentList.get(i) && leftEntity.get(g) !=  parentList.get(j) && rightEntity.get(t) !=  parentList.get(i) && rightEntity.get(t) !=  parentList.get(j)) {
                      val leftValue: PrimitiveValue = table.get(leftEntity.get(g)).get(location)
                      val rightValue: PrimitiveValue = table.get(rightEntity.get(t)).get(location)
                      if ((leftValue.asString).equals(rightValue.asString)) {
                      // then these could map to the same concept
                       var temp = childrenMatrix.get(g).get(t)
                       temp += 1
                        childrenMatrix.get(g).set(t, temp)
                    }
                  }
                  }
                }
                numberOfJoins+=1
              }
            }
            for(t <- 0 until childrenMatrix.size()) {
              var highestValue:Double = 0.0
              var highestPlace:Int = -1;
              for(g <- 0 until childrenMatrix.get(t).size()) {
                if (numberOfJoins > 0) {
                  // now check for
                  if (childrenMatrix.get(t).get(g).toFloat >= highestValue.toFloat) {
                    highestValue = childrenMatrix.get(t).get(g).toFloat
                    highestPlace = g
                  }
                }
              }
              if (highestValue.toFloat >= (threshhold.toFloat * threshhold.toFloat)) {
                 // then childrenMatrix at t is a pairing
                 if(highestPlace != -1) {
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

            if(phase2Graph.getVertexCount > 1){
              graphPairs.put(parentList.get(i)+","+parentList.get(j),phase2Graph)
            }
          }
        }
      }
    }
  }

  var longestPathDepth:Int = 0
  var longestPathVar:ArrayList[Integer] = null

  def parentOfLongestPath(g:DirectedSparseMultigraph[Integer,String], v:Int): Int = {
    if(g.getPredecessors(v) == null){
      println("GRAPH DOES NOT CONTAIN THIS NODE")
    }
    if(g.getPredecessorCount(v) == 1){
      return -1 // must be the root
    }
    else{
      longestPathDepth = 0
      longestPathVar = new ArrayList[Integer]()
      longestPath(g,g.getPredecessors(v),0,new ArrayList[Integer]())
      return longestPathVar.get(0)
    }
  }


  def longestPath(g:DirectedSparseMultigraph[Integer,String],predList:util.Collection[Integer],depth:Int,currentPath:ArrayList[Integer]): Unit = {
    if(predList.size() == 1){ // because of root
      if(depth > longestPathDepth){
        longestPathVar = currentPath
      }
    }
    else{
      var listIter = predList.iterator()
      while(listIter.hasNext) {
        var temp = listIter.next()
        if(temp != -1){
          var ret1:ArrayList[Integer] = currentPath
        ret1.add(temp)
        longestPath(g, g.getPredecessors(temp), depth + 1, ret1)
        }
      }
    }
  }

}
