name := "Mimir-Core"
version := "0.2-SNAPSHOT"
organization := "info.mimirdb"
scalaVersion := "2.10.5"

dependencyOverrides += "org.scala-lang" % "scala-library" % scalaVersion.value

// Needed to avoid cryptic EOFException crashes in forked tests
// in Travis with `sudo: false`.
// See https://github.com/sbt/sbt/issues/653
// and https://github.com/travis-ci/travis-ci/issues/3775
javaOptions += "-Xmx2G"

scalacOptions ++= Seq(
  "-feature"
)

connectInput in run := true
outputStrategy in run := Some(StdoutOutput)

resolvers += "MimirDB" at "http://maven.mimirdb.info/"
resolvers += "osgeo" at "http://download.osgeo.org/webdav/geotools/"
resolvers += "MVNRepository" at "http://mvnrepository.com/artifact/"

libraryDependencies ++= Seq(
  ////////////////////// Command-Line Interface Utilities //////////////////////
  "org.rogach"                    %%  "scallop"               % "0.9.5",
  "org.jline"                     %   "jline"                 % "3.2.0",
  "info.mimirdb"                  %   "jsqlparser"            % "1.0.1",

  ////////////////////// Dev Tools -- Logging, Testing, etc... //////////////////////
  "com.typesafe.scala-logging"    %%  "scala-logging-slf4j"   % "2.1.2",
  "ch.qos.logback"                %   "logback-classic"       % "1.1.7",
  "org.specs2"                    %%  "specs2-core"           % "3.8.4" % "test",
  "org.specs2"                    %%  "specs2-matcher-extra"  % "3.8.4" % "test",
  "org.specs2"                    %%  "specs2-junit"          % "3.8.4" % "test",

  //////////////////////// Data Munging Tools //////////////////////
  "com.github.nscala-time"        %%  "nscala-time"           % "1.2.0",
  "org.apache.lucene"             %   "lucene-spellchecker"   % "3.6.2",
  "org.apache.servicemix.bundles" %   "org.apache.servicemix.bundles.collections-generic" 
                                                              % "4.01_1",
  "org.apache.commons"            %   "commons-csv"           % "1.4",
  "commons-io"                    %   "commons-io"            % "2.5",
  "com.github.wnameless"          %   "json-flattener"        % "0.2.2",
  "com.typesafe.play"             %%  "play-json"             % "2.4.11",

  //////////////////////// Lens Libraries //////////////////////
  // WEKA - General-purpose Classifier Training/Deployment Library
  // Used by the imputation lens
  ("nz.ac.waikato.cms.weka"       %   "weka-stable"           % "3.8.1").
    exclude("nz.ac.waikato.cms.weka",  "weka-dev").
    exclude("nz.ac.waikato.cms.weka.thirdparty", "java-cup-11b-runtime"),
  ("nz.ac.waikato.cms.moa"        %   "moa"                   % "2014.11").
    exclude("nz.ac.waikato.cms.weka",  "weka-dev").
    exclude("nz.ac.waikato.cms.weka.thirdparty", "java-cup-11b-runtime"),
 
  // Jung - General purpose graph manipulation library
  // Used to detect and analyze Functional Dependencies
  "net.sf.jung"                   %   "jung-graph-impl"       % "2.0.1",
  "net.sf.jung"                   %   "jung-algorithms"       % "2.0.1",
  "net.sf.jung"                   %   "jung-visualization"    % "2.0.1",
  "jgraph"                        %   "jgraph"                % "5.13.0.0",
  "javax.media" 				  %   "jai_core"              % "1.1.3",
  
  // Geotools - Geospatial data transformations
  // Used by the CURE scenario
  "org.geotools"                  %   "gt-referencing"        % "16.2",
  "org.geotools"                  %   "gt-referencing"        % "16.2",
  "org.geotools"                  %   "gt-epsg-hsql"          % "16.2",

  //////////////////////// JDBC Backends //////////////////////
  "org.xerial"                    %   "sqlite-jdbc"           % "3.16.1",


  ///////////////////  GProM/Native Integration //////////////
  "net.java.dev.jna"              %    "jna"                  % "4.2.2",
  "net.java.dev.jna"              %    "jna-platform"         % "4.2.2",
  "log4j"                         %    "log4j"                % "1.2.17"
  
)

lazy val parser = taskKey[Unit]("Builds the SQL Parser")
parser := {
  val logger = streams.value.log
  Process(List(
    "rm", "-f",
    "src/main/java/mimir/parser/MimirJSqlParser.java",
    "src/main/java/mimir/parser/MimirJSqlParserConstants.java",
    "src/main/java/mimir/parser/MimirJSqlParserTokenManager.java",
    "src/main/java/mimir/parser/ParseException.java",
    "src/main/java/mimir/parser/SimpleCharStream.java",
    "src/main/java/mimir/parser/Token.java",
    "src/main/java/mimir/parser/TokenMgrError.java"
  )) ! logger match {
    case 0 => // Success
    case n => sys.error(s"Could not clean up after old SQL Parser: $n")
  }
  Process(List(
    "java -cp lib/javacc.jar javacc",
    "-OUTPUT_DIRECTORY=src/main/java/mimir/parser",
    "src/main/java/mimir/parser/JSqlParserCC.jj"
  ).mkString(" ")) ! logger match {
    case 0 => // Success
    case n => sys.error(s"Could not build SQL Parser: $n")
  }
}

lazy val datasets = taskKey[Unit]("Loads Datasets for Test Cases")
datasets := {
  // Redirect stderr to stdin
  val logger = 
    new ProcessLogger {
      def buffer[T](x: => T): T = x
      def error(x: => String) = streams.value.log.info(x)
      def info(x: => String) = streams.value.log.info(x)
    }

  if(!new File("test/pdbench").exists()){
    Process(List(
      "curl", "-O", "http://odin.cse.buffalo.edu/public_data/pdbench-1g-columnar.tgz"
    )) ! logger match {
      case 0 => // Success
      case n => sys.error(s"Could not download PDBench Data: $n")
    }
    Process(List("mkdir", "test/pdbench")) ! logger match {
      case 0 => // Success
      case n => sys.error(s"Could not create PDBench directory")
    }
    Process(List(
      "tar", "-xvf", 
      "pdbench-1g-columnar.tgz", 
      "--strip-components=1", 
      "--directory=test/pdbench"
    )) ! logger match {
      case 0 => // Success
      case n => sys.error(s"Could not clean up after old SQL Parser: $n")
    }
    Process(List("rm", "pdbench-1g-columnar.tgz")) ! logger match {
      case 0 => // Success
      case n => sys.error(s"Could not create PDBench directory")
    }
  } else {
    logger.info("Found `pdbench` data in test/pdbench");
  }
}


lazy val mcdbdatasets = taskKey[Unit]("Loads Datasets for Test Cases")
mcdbdatasets := {
  val logger = streams.value.log
  if(!new File("test/mcdb").exists()){
    Process(List(
      "curl", "-O", "http://odin.cse.buffalo.edu/public_data/tpch.tgz"
    )) ! logger match {
      case 0 => // Success
      case n => sys.error(s"Could not download MCDB Data: $n")
    }
    Process(List("mkdir", "test/mcdb")) ! logger match {
      case 0 => // Success
      case n => sys.error(s"Could not create MCDB directory")
    }
    Process(List(
      "tar", "-xvf", 
      "tpch.tgz", 
      "--strip-components=1", 
      "--directory=test/mcdb"
    )) ! logger match {
      case 0 => // Success
      case n => sys.error(s"Could not clean up after old SQL Parser: $n")
    }
    Process(List("rm", "tpch.tgz")) ! logger match {
      case 0 => // Success
      case n => sys.error(s"Could not create MCDB directory")
    }
  } else {
    println("Found `mcdb` data in test/mcdb");
  }
}

scalacOptions in Test ++= Seq("-Yrangepos")

parallelExecution in Test := false

resolvers ++= Seq("snapshots", "releases").map(Resolver.sonatypeRepo)

fork := true

testOptions in Test ++= Seq( Tests.Argument("junitxml"), Tests.Argument("console") )

////// Assembly Plugin //////
// We use the assembly plugin to create self-contained jar files
// https://github.com/sbt/sbt-assembly

test in assembly := {}
assemblyJarName in assembly := "Mimir.jar"
mainClass in assembly := Some("mimir.Mimir")

////// Publishing Metadata //////
// use `sbt publish make-pom` to generate 
// a publishable jar artifact and its POM metadata

publishMavenStyle := true

pomExtra := <url>http://mimirdb.info</url>
  <licenses>
    <license>
      <name>Apache License 2.0</name>
      <url>http://www.apache.org/licenses/</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>git@github.com:ubodin/mimir.git</url>
    <connection>scm:git:git@github.com:ubodin/mimir.git</connection>
  </scm>

/////// Publishing Options ////////
// use `sbt publish` to update the package in 
// your own local ivy cache
publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))
