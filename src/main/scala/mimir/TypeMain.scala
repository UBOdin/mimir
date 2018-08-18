package mimir

import java.io.File

import mimir.util.NaiveTypeCount2
import mimir.util.YelpGT

object TypeMain {

  // name -> fileName, naive
  val dataList = Map[String,(String,Boolean)](("twitter"->("test/data/dump-30.txt",true)),("yelp"->("test/data/yelp_dataset",true)),("nasa"->("test/data/nasa.json",false)),
    ("phonelab"->("test/data/carlDataSample.out",false)),("enron"->("test/data/enron.json",true)),("medicine"->("test/data/medicine.json",true)),("meteorite"->("test/data/meteorite.json",false)),("test"->("test/data/testTypes.json",true))
  ,("citi"->("cleanJsonOutput/citiStations.json",true)),("weatherUG"->("cleanJsonOutput/WUG.json",true)))
  def main(args: Array[String]) = {

    val dataset: String = "twitter"
    val rowLimit = 0 // 0 means all rows
    val sampleData = true
    val stash = false
    val unstash = true
    val visualize = true
    val hasSchema = false
    val verbose = true

    //val loadJson = new NaiveTypeCount2(datasetName=dataset, inputFile=new File(dataList.get(dataset).get._1), rowLimit=rowLimit, naive=dataList.get(dataset).get._2, Sample=sampleData, Stash=stash, Unstash=unstash, Visualize = visualize, hasSchema = hasSchema)
    val loadJson = new NaiveTypeCount2()
    loadJson.run(datasetName=dataset, inputFile=new File(dataList.get(dataset).get._1), Verbose = verbose, RowLimit=rowLimit, Naive=dataList.get(dataset).get._2, Sample=sampleData, Stash=stash, Unstash=unstash, Visualize = visualize, hasSchema = hasSchema)


    //val gt = new YelpGT(new File(dataList.get("yelp").get._1))
    //gt.run()

    //val fjf = new mimir.util.FormatJsonFile(new File("rawJsonInput/Amherst.json"),"WUG.json")
    //fjf.cleanWeatherUG()



    // what is the goal, what are the constraints, what makes a good approximation, linear optimizer?
    // bazeezee paper
    // write up type
  }
}
