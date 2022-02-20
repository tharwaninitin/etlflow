package etlflow.webserver

import etlflow.utils.ApplicationLogger
import zhttp.http._
import zhttp.socket.{Socket, WebSocketFrame}
import zio.clock.Clock
import zio.duration._
import zio.stream.ZStream
import zio.{Schedule, UIO}

case class WebsocketsAPI(auth: Authentication) extends ApplicationLogger {
  private val mb: Int          = 1024 * 1024
  private val runtime: Runtime = Runtime.getRuntime

  val memoryInfo =
    s"""{"memory": {"used": ${(runtime.totalMemory - runtime.freeMemory) / mb}, "free": ${runtime.freeMemory / mb}, "total": ${runtime.totalMemory / mb}, "max": ${runtime.maxMemory / mb}}}"""

  private[etlflow] val stream = ZStream
    .fromEffect(UIO(memoryInfo))
    .repeat(Schedule.forever && Schedule.spaced(1.seconds))

  def webSocketStream(token: String): ZStream[Clock, Nothing, WebSocketFrame] =
    if (auth.validateJwt(token)) {
      auth.isCached(token) match {
        case Some(_) => stream.map(s => WebSocketFrame.text(s))

        case None =>
          logger.warn(s"Expired token $token")
          ZStream.fromEffect(UIO(WebSocketFrame.Close(1001, Some("Expired token"))))
      }
    } else {
      logger.warn(s"Invalid token $token")
      ZStream.fromEffect(UIO(WebSocketFrame.Close(1001, Some("Invalid token"))))
    }

  private[etlflow] def socket(token: String): Socket[Clock, Nothing, Any, WebSocketFrame] =
    Socket.collect[Any] { case _ =>
      webSocketStream(token)
    }

  val webSocketApp: HttpApp[Clock, Nothing] =
    Http.collectZIO[Request] { case Method.GET -> !! / "ws" / "etlflow" / token =>
      socket(token).toResponse
    }
}
