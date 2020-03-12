package mimir.util

import java.lang.reflect.Method
import java.net.URL
import java.io.{File, FileOutputStream, BufferedOutputStream, InputStream}
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveInputStream}
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import java.io.BufferedInputStream
import java.nio.file.Files

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
  
  def untar(in:InputStream, destinationDir: String): File = {

    val dest = new File(destinationDir)
    dest.mkdir()

    var tarIn: TarArchiveInputStream = null

    try {
      tarIn = new TarArchiveInputStream(
        new GzipCompressorInputStream(
          new BufferedInputStream(in)))
      var tarEntry = tarIn.getNextTarEntry
      while (tarEntry != null) {

        // create a file with the same name as the tarEntry
        val destPath = new File(dest, tarEntry.getName)
        if (tarEntry.isDirectory) {
          destPath.mkdirs()
        } else {
          // Create any necessary parent dirs
          val parent = destPath.getParentFile
          if (!Files.exists(parent.toPath)) {
            parent.mkdirs()
          }

          destPath.createNewFile()

          val btoRead = new Array[Byte](1024)
          var bout: BufferedOutputStream = null

          try {
            bout = new BufferedOutputStream(new FileOutputStream(destPath))

            var len = 0
            while (len != -1) {
              len = tarIn.read(btoRead)
              if (len != -1) {
                bout.write(btoRead, 0, len)
              }
            }
          } finally {
            if (bout != null) {
              bout.close()
            }
          }
        }
        tarEntry = tarIn.getNextTarEntry
      }
    } finally {
      if (tarIn != null) {
        tarIn.close()
      }
    }
    dest
  }
  
}