import scala.sys.process._

name := "Mimir-Core"
version := "0.2"
organization := "info.mimirdb"
scalaVersion := "2.11.11"

dependencyOverrides += "org.scala-lang" % "scala-library" % scalaVersion.value

// Needed to avoid cryptic EOFException crashes in forked tests
// in Travis with `sudo: false`.
// See https://github.com/sbt/sbt/issues/653
// and https://github.com/travis-ci/travis-ci/issues/3775
//javaOptions ++= Seq("-Xmx2G" )


scalacOptions ++= Seq(
  "-feature"
)

unmanagedResourceDirectories in Compile += baseDirectory.value / "lib_extra"
includeFilter in (Compile, unmanagedResourceDirectories):= ".dylib,.dll,.so"
unmanagedClasspath in Runtime += baseDirectory.value / "conf"
unmanagedResourceDirectories in Test += baseDirectory.value / "conf"

fork := true
outputStrategy in run := Some(StdoutOutput)
connectInput in run := true
cancelable in Global := true
javaOptions ++= Seq("-Dcom.github.fommil.netlib.BLAS=com.github.fommil.netlib.F2jBLAS", "-Dcom.github.fommil.netlib.LAPACK=com.github.fommil.netlib.F2jLAPACK", "-Dcom.github.fommil.netlib.ARPACK=com.github.fommil.netlib.F2jARPACK")
scalacOptions in Test ++= Seq("-Yrangepos")
parallelExecution in Test := false
testOptions in Test ++= Seq( Tests.Argument("junitxml"), Tests.Argument("console") )
mainClass in Compile := Some("mimir.Mimir")

lazy val runMimirVizier = inputKey[Unit]("run MimirVizier")
runMimirVizier := {
  val args = sbt.complete.Parsers.spaceDelimited("[main args]").parsed
  val classpath = (fullClasspath in Compile).value
  val classpathString = Path.makeString(classpath map { _.data })
  val jvmArgs = Seq("-Xmx4g", "-Dcom.github.fommil.netlib.BLAS=com.github.fommil.netlib.F2jBLAS", "-Dcom.github.fommil.netlib.LAPACK=com.github.fommil.netlib.F2jLAPACK", "-Dcom.github.fommil.netlib.ARPACK=com.github.fommil.netlib.F2jARPACK")
  val (jh, os, bj, bd, jo, ci, ev) = (javaHome.value, outputStrategy.value, Vector[java.io.File](), 
		Some(baseDirectory.value), (jvmArgs ++ Seq("-classpath", classpathString)).toVector, connectInput.value, envVars.value)
  Fork.java(
    ForkOptions(jh, os, bj, bd, jo, ci, ev),
    "mimir.MimirVizier" +: args
  )
}

//for tests that need to run in their own jvm because they need specific envArgs or otherwise
testGrouping in Test := {
	val (jh, os, bj, bd, jo, ci, ev) = (javaHome.value, outputStrategy.value, Vector[java.io.File](), 
		baseDirectory.value, javaOptions.value.toVector, connectInput.value, envVars.value)
	val testsToForkSeperately = Seq("mimir.gprom.algebra.OperatorTranslationSpec")
	val seperateForkedEnvArgs = Map(("mimir.gprom.algebra.OperatorTranslationSpec", sys.props.get("os.name") match {
	  	case Some(osname) if osname.startsWith("Mac OS X") => Map(("DYLD_INSERT_LIBRARIES",System.getProperty("java.home")+"/lib/libjsig.dylib"))
	  	case Some(otherosname) => Map(("LD_PRELOAD",System.getProperty("java.home")+"/lib/"+System.getProperty("os.arch")+"/libjsig.so"))
	  	case None => envVars.value
	  }))
	val (forkedTests, otherTests) = (definedTests in Test).value.partition { test => testsToForkSeperately.contains(test.name) }
    Seq(Tests.Group(name = "Single JVM tests", tests = otherTests, runPolicy = Tests.SubProcess(
	    ForkOptions( jh, os, bj, Some(bd), jo, ci, ev)
	    ))) ++ forkedTests.map { test =>
	  Tests.Group(name = test.name, tests = Seq(test), runPolicy = Tests.SubProcess(
	    ForkOptions( jh, os, bj, Some(bd), jo, ci, seperateForkedEnvArgs.getOrElse(test.name, ev))
	    ))
	}
}

//if you want to debug tests uncomment this
//javaOptions in (Test) += "-Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005"

resolvers += "MimirDB" at "http://maven.mimirdb.info/"
resolvers += "osgeo" at "http://download.osgeo.org/webdav/geotools/"
resolvers += "MVNRepository" at "http://mvnrepository.com/artifact/"
resolvers ++= Seq("snapshots", "releases").map(Resolver.sonatypeRepo)

libraryDependencies ++= Seq(
  ////////////////////// Command-Line Interface Utilities //////////////////////
  "org.rogach"                    %%  "scallop"                  % "0.9.5",
  "org.jline"                     %   "jline"                    % "3.2.0",
  "info.mimirdb"                  %   "jsqlparser"               % "1.0.2",

  ////////////////////// Dev Tools -- Logging, Testing, etc... //////////////////////
  "com.typesafe.scala-logging"    %%  "scala-logging-slf4j"      % "2.1.2",
  "ch.qos.logback"                %   "logback-classic"          % "1.1.7",
  "org.specs2"                    %%  "specs2-core"              % "3.8.4" % "test",
  "org.specs2"                    %%  "specs2-matcher-extra"     % "3.8.4" % "test",
  "org.specs2"                    %%  "specs2-junit"             % "3.8.4" % "test",

  //////////////////////// Data Munging Tools //////////////////////
  "com.github.nscala-time"        %%  "nscala-time"              % "1.2.0",
  "org.apache.lucene"             %   "lucene-spellchecker"      % "3.6.2",
  "org.apache.servicemix.bundles" %   "org.apache.servicemix.bundles.collections-generic" 
                                                                 % "4.01_1",
  "org.scala-lang.modules"        %%  "scala-parser-combinators" % "1.0.6",
  "org.apache.commons"            %   "commons-csv"              % "1.4",
  "commons-io"                    %   "commons-io"               % "2.5",
  "com.github.wnameless"          %   "json-flattener"           % "0.2.2",
  "com.typesafe.play"             %%  "play-json"                % "2.4.11",

  //////////////////////// Lens Libraries //////////////////////
  // WEKA - General-purpose Classifier Training/Deployment Library
  // Used by the imputation lens
  ("nz.ac.waikato.cms.weka"       %   "weka-stable"              % "3.8.1").
    exclude("nz.ac.waikato.cms.weka",  "weka-dev").
    exclude("nz.ac.waikato.cms.weka.thirdparty", "java-cup-11b-runtime"),
  ("nz.ac.waikato.cms.moa"        %   "moa"                      % "2014.11").
    exclude("nz.ac.waikato.cms.weka",  "weka-dev").
    exclude("nz.ac.waikato.cms.weka.thirdparty", "java-cup-11b-runtime"),
    
  //spark ml
  "org.apache.spark" 			  %   "spark-sql_2.11" 		  % "2.2.0",
  "org.apache.spark" 			  %   "spark-mllib_2.11" 	  % "2.2.0",
 
  //////////////////////// Jung ////////////////////////
  // General purpose graph manipulation library
  // Used to detect and analyze Functional Dependencies
  "net.sf.jung"                   %   "jung-graph-impl"          % "2.0.1",
  "net.sf.jung"                   %   "jung-algorithms"          % "2.0.1",
  "net.sf.jung"                   %   "jung-visualization"       % "2.0.1",
  "jgraph"                        %   "jgraph"                   % "5.13.0.0",
  "javax.media" 		              %   "jai_core"                 % "1.1.3",  
  //

  //////////////////////// Geotools ////////////////////////
  // Geospatial data transformations, Used by the CURE scenario
  "org.geotools"                  %   "gt-referencing"           % "16.2",
  "org.geotools"                  %   "gt-referencing"           % "16.2",
  "org.geotools"                  %   "gt-epsg-hsql"             % "16.2",

  //////////////////////// JDBC Backends //////////////////////
  "org.xerial"                    %   "sqlite-jdbc"              % "3.16.1",


  ///////////////////  GProM/Native Integration //////////////
  "net.java.dev.jna"              %    "jna"                     % "4.2.2",
  "net.java.dev.jna"              %    "jna-platform"            % "4.2.2",
  "log4j"                         %    "log4j"                   % "1.2.17",
  
  ///////////////////// Viztrails Integration ///////////////////
  
  "net.sf.py4j" 				  %	   "py4j" 				  % "0.10.4",
  
  //////////////////////// Visualization //////////////////////
  // For now, all of this happens in python with matplotlib
  // and so we don't need any external dependencies.
  //"org.vegas-viz"                 %%  "vegas"                  % "0.3.9",
  //"org.sameersingh.scalaplot"     % "scalaplot"                % "0.0.4"
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

////// Assembly Plugin //////
// We use the assembly plugin to create self-contained jar files
// https://github.com/sbt/sbt-assembly

test in assembly := {}
assemblyJarName in assembly := "Mimir.jar"
mainClass in assembly := Some("mimir.Mimir")
assemblyMergeStrategy in assembly := {
  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case PathList("ch", "qos", xs @ _*) => MergeStrategy.last
  case PathList("org", "slf4j", xs @ _*) => MergeStrategy.last
  case PathList("org", "codehaus", xs @ _*) => MergeStrategy.last
  case PathList("com", "googlecode", xs @ _*) => MergeStrategy.last
  case "overview.html" => MergeStrategy.rename
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}


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
