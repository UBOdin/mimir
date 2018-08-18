package mimir

import java.io.File

import javafx.application.Application
import javafx.stage.Stage
import javafx.stage.FileChooser
import javafx.stage.FileChooser.ExtensionFilter
import javafx.scene.Scene
import javafx.scene.layout.{GridPane, HBox, Pane, VBox}
import javafx.scene.text.Font
import javafx.scene.text.FontWeight
import javafx.scene.text.Text
import javafx.scene.paint.Color
import javafx.scene.control.Label
import javafx.scene.control.Button
import javafx.geometry.Pos
import javafx.geometry.Insets
import javafx.event.{ActionEvent, EventHandler}
import java.io.File

import scala.collection.JavaConverters._
import javafx.scene.canvas.Canvas
import scalafx.scene.control.{ComboBox, ListView, ScrollPane}


class JsonExplorerUI() extends Application {

  var savedStage: Stage = null
  var selectedFiles: List[String] = List[String]()
  var actionStatus: Text = new Text()
  actionStatus.setFont(Font.font("Calibri", FontWeight.NORMAL, 20))
  val titleTxt: String = "Json Explorer"
  var actions: HBox = null
  var mainBox: VBox = null


  @Override
  def start(primaryStage: Stage) = {

    primaryStage.setTitle(titleTxt)

    actionStatus.setText("Step 1: Select Json Files")

    // Window label
    var label = new Label("Select File Choosers");
    label.setTextFill(Color.DARKBLUE);
    label.setFont(Font.font("Calibri", FontWeight.BOLD, 36))
    var labelHb: HBox = new HBox();
    labelHb.setAlignment(Pos.CENTER);
    labelHb.getChildren().add(label);

    val chooseFiles: Button = new Button("Select Json files...")
    chooseFiles.setOnAction(new fileSelectorButtonListener())

    val jsonExplorer: Button = new Button("Run Json Explorer")
    jsonExplorer.setOnAction(new jsonExplorerButtonListener())

    // ScrollPane for schema
    val middleScene = new ScrollPane()


    // put all the actions on one bar
    actions = new HBox(chooseFiles, jsonExplorer)

    // Vbox
    mainBox = new VBox(10)
    mainBox.setPadding(new Insets(5, 25, 25, 25))
    mainBox.getChildren().addAll(actionStatus, middleScene, actions)

    // Scene
    val mainScene = new Scene(mainBox, 500, 300)
    primaryStage.setScene(mainScene)
    primaryStage.show()

    savedStage = primaryStage
  }

  class fileSelectorButtonListener() extends EventHandler[ActionEvent] {

    def createComboBox(): ComboBox[String] = {
      val cb = new ComboBox[String]()
      cb.getItems.add("Naive")
      cb.getItems.add("Embeded Array")
      cb
    }

    def selectFile() = {
      val fileChooser = new FileChooser()
      fileChooser.setTitle("Select Json files")
      fileChooser.setInitialDirectory(new File("."))
      //fileChooser.getExtensionFilters.addAll(new ExtensionFilter("PDF Files", "*.pdf"))

      val files = fileChooser.showOpenMultipleDialog(savedStage)

      if (files != null){
        selectedFiles = files.asScala.map(_.getName()).toList
        val grid = new GridPane()
        grid.add(new Label("File Name"),0,0)
        grid.add(new Label("Load Strategy"),1,0)
        grid.getChildren.addAll(createComboBox())
        //selectedFiles.map(grid.getChildren.addAll(_,createComboBox()))
        mainBox.getChildren.removeAll()
        mainBox.getChildren.addAll(actionStatus,grid,actions)
        actionStatus.setText("Step 2: Run Json Explorer")
      } else {
        actionStatus.setText("Json file selection cancelled.\nStep 1: Select Json Files")
      }
    }

    @Override
    def handle(e: ActionEvent): Unit = {
      selectFile()
    }
  }

  class jsonExplorerButtonListener() extends EventHandler[ActionEvent] {

    def runJsonExplorer() = {
      val schemaList = List("a","b","c","d")
      mainBox.getChildren.removeAll()
      mainBox.getChildren.addAll(actionStatus,new ListView[String](schemaList),actions)
    }

    @Override
    def handle(e: ActionEvent): Unit = {
      if(selectedFiles != null) {
        if (selectedFiles.size == 0) {
          actionStatus.setText("Please select at least one file.\nStep 1: Select Json Files")
        } else {
          runJsonExplorer()
        }
      } else {
        actionStatus.setText("Please select at least one file.\nStep 1: Select Json Files")
      }

    }
  }


  object JsonExplorerUI {
    def main(args: Array[String]): Unit = {
      Application.launch(args:_*)
    }
  }

}
