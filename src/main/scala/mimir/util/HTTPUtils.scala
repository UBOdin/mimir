package mimir.util

import play.api.libs.json._

object HTTPUtils {
  @throws(classOf[java.io.IOException])
  @throws(classOf[java.net.SocketTimeoutException])
  def get(url: String,
          connectTimeout: Int = 8000,
          readTimeout: Int = 8000,
          requestMethod: String = "GET") =
  {
      import java.net.{URL, HttpURLConnection}
      val connection = (new URL(url)).openConnection.asInstanceOf[HttpURLConnection]
      connection.setConnectTimeout(connectTimeout)
      connection.setReadTimeout(readTimeout)
      connection.setRequestMethod(requestMethod)
      val inputStream = connection.getInputStream
      val content = scala.io.Source.fromInputStream(inputStream).mkString
      if (inputStream != null) inputStream.close
      content
  }
  
  def getJson(url:String, path: Option[String] = None,
          connectTimeout: Int = 8000,
          readTimeout: Int = 8000,
          requestMethod: String = "GET"): JsValue = {
    path match {
      case None => play.api.libs.json.Json.parse(get(url, connectTimeout, readTimeout, requestMethod))
      case Some(path) => JsonUtils.seekPath(play.api.libs.json.Json.parse(get(url, connectTimeout, readTimeout, requestMethod)), path)
    }
  }
}