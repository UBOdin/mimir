package mimir.util

import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import org.apache.log4j.AppenderSkeleton
import org.apache.log4j.helpers.LogLog
import org.apache.log4j.spi.LoggingEvent
import scala.beans.{BeanProperty, BooleanBeanProperty}
import java.nio.charset.Charset

//remove if not needed
import scala.collection.JavaConversions._
import ch.qos.logback.classic.Level
import mimir.util.HttpRocketClient._
import ch.qos.logback.classic.spi.ILoggingEvent

class WebLogAppender extends ch.qos.logback.core.Appender[ILoggingEvent] {

  private val loggingEventQueue: BlockingQueue[ILoggingEvent] =
    new LinkedBlockingQueue[ILoggingEvent]()

  private var thread: Thread = new Thread(new Runnable() {
    def run(): Unit = {
      processQueue()
    }
  })
  
  thread.setDaemon(true)
     thread.start()
  
  private var closed = false
  private var started = false

  // Members declared in ch.qos.logback.core.spi.LifeCycle
   def isStarted(): Boolean = started
   def start(): Unit = {
     
   }
   def stop(): Unit = close()
  
  private def processQueue(): Unit = {
    started = true
    while (true) try {
      val event: ILoggingEvent = loggingEventQueue.poll(5L, TimeUnit.SECONDS)
      if (event != null) {
        processEvent(event)
      }
    } catch {
      case e: InterruptedException => {}

    }
  }


  /*
   * These properties are set from the logback.xml file like:
   * <appender name="WEBLOG" class="mimir.util.WebLogAppender">
   *   <hookUrl>https://chat.odin.cse.buffalo.edu/hooks/</hookUrl>
   *   <token>Gv0QetrLu8CZvPERJ/6ArNSvX7p2/fake/euXzx7h6CRndjs3mTYbGz8DnepD6R7vh</token>
   * </appender>
   */

  @BeanProperty
  var hookUrl: String = _

  @BeanProperty
  var token: String = _

  @BeanProperty
  var property3: String = _

  private def processEvent(loggingEvent: ILoggingEvent): Unit = {
    if (loggingEvent != null) {
      val messageAttachment = s"${loggingEvent.getMessage.toString()}\n${loggingEvent.getThrowableProxy.getStackTraceElementProxyArray.mkString("\n")}"
      val attachments = Seq(Attachment(messageAttachment, loggingEvent.getLevel match {
        case Level.ERROR => Color.Red
        case Level.WARN => Color.Yellow
        case Level.INFO => Color.Gray
        case _ => Color.Green
      }))
      val msg = s"${loggingEvent.getLevel.toString()} ${loggingEvent.getLoggerName}" 
      HttpRocketClient(hookUrl, token, Seq[Notify](), msg, attachments)
    }
  }

  def close(): Unit = {
    synchronized {
      if (this.closed) {
        return
      }
      closeWS()
      thread.interrupt()
      LogLog.debug("Closing appender [" + name + "].")
      this.closed = true
    }
  }

  private def closeWS(): Unit = {
    //close connection to rocket
  }


  def requiresLayout(): Boolean = //to an external source
    false

  
  // Members declared in ch.qos.logback.core.Appender
   override def doAppend(event: ch.qos.logback.classic.spi.ILoggingEvent): Unit = loggingEventQueue.add(event)
   
   @BeanProperty
   var name : String = _
   
   // Members declared in ch.qos.logback.core.spi.ContextAware
   def addError(x$1: String,x$2: Throwable): Unit = {}
   def addError(x$1: String): Unit = {}
   def addInfo(x$1: String,x$2: Throwable): Unit = {}
   def addInfo(x$1: String): Unit = {}
   def addStatus(x$1: ch.qos.logback.core.status.Status): Unit = {}
   def addWarn(x$1: String,x$2: Throwable): Unit = {}
   def addWarn(x$1: String): Unit = {}
   
   //def getContext(): ch.qos.logback.core.Context = ???
   //def setContext(x$1: ch.qos.logback.core.Context): Unit = ???
   @BeanProperty
   var context : ch.qos.logback.core.Context = _
   
   // Members declared in ch.qos.logback.core.spi.FilterAttachable
   def addFilter(x$1: ch.qos.logback.core.filter.Filter[ch.qos.logback.classic.spi.ILoggingEvent]): Unit = {}
   def clearAllFilters(): Unit = {}
   def getCopyOfAttachedFiltersList(): java.util.List[ch.qos.logback.core.filter.Filter[ch.qos.logback.classic.spi.ILoggingEvent]] = List()
   def getFilterChainDecision(x$1: ch.qos.logback.classic.spi.ILoggingEvent): ch.qos.logback.core.spi.FilterReply = ???
   

  override def finalize(): Unit = {
    close()
    super.finalize()
  }

}

object HttpRocketClient {
  sealed abstract class Color(val code: String)
  
  object Color {
    case object Red    extends Color("#FF0000")
    case object Green  extends Color("#008000")
    case object Gray   extends Color("#808080")
    case object Yellow extends Color("#FFFF00")
  }

  case class Attachment(text: String, color: Color)
  
  sealed abstract class Notify(val formatted: String)
  
  object Notify {
    case object Channel extends Notify("<!channel>")
    case class User(name: String) extends Notify(s"@$name")
  }
  
  sealed abstract class Error(val show: String)
  
  object Error {
    case class UnexpectedStatusCode(statusCode: Int, body: String) extends
      Error(s"Unexpected response status code $statusCode with response body '$body'")
    case class Exception(throwable: Throwable) extends
      Error(throwable.getStackTrace().mkString("\n")) 
  }
  val jsonContentType = "application/json"

  def isResponseSuccess(code: Int): Boolean = code >= 200 && code <= 299

  object PayloadMapper {
    def toBody(notify: Seq[Notify], msg: String, attachments: Seq[Attachment]): String = {
      import play.api.libs.json._

      val notifyMapped = ""//notify.map(_.formatted).mkString(" ") + { if(notify.nonEmpty) " " else "" }
      val json = Json.obj(
        "text" -> (notifyMapped + msg),
        "attachments" -> attachments.map { attachment =>
          Json.obj(
            "text" -> attachment.text,
            "color" -> attachment.color.code,
            "mrkdwn_in" -> Seq("text")
          )
        },
        "link_names" -> 1
      )
      Json.stringify(json)
    }
  }
  
  def apply(hooksUrl:String, token:String, notify: Seq[Notify], msg: String, attachments: Seq[Attachment]) = {
    import dispatch._

    import scala.concurrent.ExecutionContext.Implicits.global

    val body = PayloadMapper.toBody(notify, msg, attachments)

    println(s"HttpRocketClient: posting log to $hooksUrl$token")
    
    val response = Http.default(url(hooksUrl + token)
      .POST
      .setBody(body)
      .setContentType(jsonContentType, Charset.defaultCharset)).either
    response.map {
      case Left(t) => Left(Error.Exception(t))
      case Right(r) => if (isResponseSuccess(r.getStatusCode)) Right(())
                       else Left(Error.UnexpectedStatusCode(r.getStatusCode, r.getResponseBody))
    }
  }
  
}