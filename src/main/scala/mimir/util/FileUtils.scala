package mimir.util

import java.io.File
import java.lang.reflect.Method
import java.net.URL


object FileUtils {
  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
        d.listFiles.filter(_.isFile).toList
    } else {
        List[File]()
    }
  }
  def addJarToClasspath(jar: File): Unit = {
// Get the ClassLoader class
    val cl: ClassLoader = ClassLoader.getSystemClassLoader
    val clazz: Class[_] = cl.getClass
// Get the protected addURL method from the parent URLClassLoader class
    val method: Method =
      clazz.getSuperclass.getDeclaredMethod("addURL", Seq(classOf[URL]):_*)
// Run projected addURL method to add JAR to classpath
    method.setAccessible(true)
    method.invoke(cl, Seq(jar.toURI().toURL()):_*)
  }
}