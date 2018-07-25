package mimir

import java.io.File

import mimir.util.NaiveTypeCount2

object TypeMain {

  // name -> fileName, naive
  val dataList = Map[String,(String,Boolean)](("twitter"->("test/data/dump-30.txt",true)),("yelp"->("test/data/yelp_dataset",true)),("nasa"->("test/data/nasa.json",false)),
    ("phonelab"->("test/data/carlDataSample.out",false)),("enron"->("test/data/enron.json",true)),("medicine"->("test/data/medicine.json",true)),("meteorite"->("test/data/meteorite.json",false)),("test"->("test/data/testTypes.json",true)))
  def main(args: Array[String]) = {

    val dataset: String = "twitter"
    val rowLimit = 0 // 0 means all rows
    val sampleData = true
    val stash = !true
    val unstash = !false

    val loadJson = new NaiveTypeCount2(datasetName=dataset, inputFile=new File(dataList.get(dataset).get._1), rowLimit=rowLimit, naive=dataList.get(dataset).get._2, Sample=sampleData, Stash=stash, Unstash=unstash)

    // what is the goal, what are the constraints, what makes a good approximation, linear optimizer?
    // bazeezee paper
    // write up type
  }
}
