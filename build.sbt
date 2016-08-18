name := "mimirwebapp"

version := "0.1"

scalaVersion := "2.10.5"

lazy val mimircore = project

lazy val mimirwebapp = project.in(file(".")).enablePlugins(play.PlayScala).aggregate(mimircore).dependsOn(mimircore)

libraryDependencies ++= Seq(
  jdbc,
  cache,
  ws,
  specs2 % Test
)

resolvers += "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"

// Play provides two styles of routers, one expects its actions to be injected, the
// other, legacy style, accesses its actions statically.
routesGenerator := InjectedRoutesGenerator

// logging
javaOptions in Test += "-Dlogger.file=conf/logback.xml"