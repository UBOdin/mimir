package mimir

import java.io.File

import javafx.scene.control.Tab
import mimir.util.NaiveTypeCount2
import scalafx.Includes._
import scalafx.application
import scalafx.application.JFXApp
import scalafx.beans.Observable
import scalafx.collections.ObservableBuffer
import scalafx.scene.Scene
import scalafx.scene.control.ScrollPane.ScrollBarPolicy
import scalafx.scene.control.{Button, ListView, ScrollPane, TabPane}
import scalafx.scene.layout.FlowPane

object TypeMain {

  // name -> fileName, naive
  val dataList = Map[String,(String,Boolean)](("twitter"->("test/data/dump-30.txt",true)),("yelp"->("test/data/yelp_dataset",true)),("nasa"->("test/data/nasa.json",false)),
    ("phonelab"->("test/data/carlDataSample.out",false)),("enron"->("test/data/enron.json",true)),("medicine"->("test/data/medicine.json",true)),("meteorite"->("test/data/meteorite.json",false)))
  def main(args: Array[String]) = {

    val dataset: String = "phonelab"
    val rowLimit = 0
    val sampleData = true

/*
    val app = new JFXApp {
      stage = new application.JFXApp.PrimaryStage{
        title = "Hi"
        scene = new Scene(400,600){
          val tabPane = new TabPane()
          val tab1 = new Tab
          tab1.text = "tab1Text"
          val tab2 = new Tab
          tab2.text = "tab2Text"
          val s = new FlowPane()
          val button = new Button("Click")
          val items = List("[{id=7.4835798640233677E17, id_str=748357986402336772\n, indices=[31.0, 54.0], media_url=http://pbs.twimg.com/media/CmKzn6hWgAQ6PZH.jpg, media_url_https=https://pbs.twimg.com/media/CmKzn6hWgAQ6PZH.jpg, url=https://t.co/JB73770KXA, display_url=pic.twitter.com/JB73770KXA, expanded_url=http://twitter.com/StevStiffler/status/748357988583342080/photo/1, type=photo, sizes={large={w=540.0, h=350.0, resize=fit}, small={w=540.0, h=350.0, resize=fit}, thumb={w=150.0, h=150.0, resize=crop}, medium={w=540.0, h=350.0, resize=fit}}, source_status_id=7.4835798858334208E17, source_status_id_str=748357988583342080, source_user_id=1.33682123E8, source_user_id_str=133682123}]","[{id=7.4794807011404186E17, id_str=747948070114041857, indices=[114.0, 137.0], media_url=http://pbs.twimg.com/media/CmE-zpWWIAEvWKx.jpg, media_url_https=https://pbs.twimg.com/media/CmE-zpWWIAEvWKx.jpg, url=https://t.co/l6qpBqIq2x, display_url=pic.twitter.com/l6qpBqIq2x, expanded_url=http://twitter.com/jayybeyond/status/747948076854288384/photo/1, type=photo, sizes={thumb={w=150.0, h=150.0, resize=crop}, medium={w=1080.0, h=1080.0, resize=fit}, small={w=680.0, h=680.0, resize=fit}, large={w=1080.0, h=1080.0, resize=fit}}, source_status_id=7.4794807685428838E17, source_status_id_str=747948076854288384, source_user_id=9.2608519E7, source_user_id_str=92608519}]","[{id=5.0750321608360346E17, id_str=507503216083603456, indices=[55.0, 78.0], media_url=http://pbs.twimg.com/media/BwsDfHUIEAAHA4n.jpg, media_url_https=https://pbs.twimg.com/media/BwsDfHUIEAAHA4n.jpg, url=https://t.co/vJTBo328Lz, display_url=pic.twitter.com/vJTBo328Lz, expanded_url=http://twitter.com/DamnItsFood/status/507503217811283968/photo/1, type=photo, sizes={small={w=340.0, h=227.0, resize=fit}, medium={w=600.0, h=400.0, resize=fit}, thumb={w=150.0, h=150.0, resize=crop}, large={w=600.0, h=400.0, resize=fit}}, source_status_id=5.0750321781128397E17, source_status_id_str=507503217811283968, source_user_id=2.362423705E9, source_user_id_str=2362423705}]")
          val v = new ListView(items)
          v.prefHeight = 500
          v.prefWidth = 350
          s.getChildren.addAll(button,v)
          tab1.setContent(s)
          tabPane.tabs = List(tab1,tab2)
          root = tabPane
        }
      }
    }
    app.main(args)
*/
    val loadJson = new NaiveTypeCount2(dataset,new File(dataList.get(dataset).get._1), rowLimit, dataList.get(dataset).get._2,Sample=sampleData)
  }
}
