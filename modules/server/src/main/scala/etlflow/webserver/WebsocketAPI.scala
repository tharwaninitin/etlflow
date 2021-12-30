package etlflow.webserver

import etlflow.utils.ApplicationLogger
import zhttp.http._
import zhttp.socket.{Socket, SocketApp, WebSocketFrame}
import zio.Runtime.default.unsafeRun
import zio.clock.Clock
import zio.duration._
import zio.stream.ZStream
import zio.{Schedule, UIO}

case class WebsocketAPI(auth: Authentication)  extends ApplicationLogger {
  private val mb: Int = 1024 * 1024
  private val runtime: Runtime = Runtime.getRuntime

  val memoryInfo = s"""{"memory": {"used": ${(runtime.totalMemory - runtime.freeMemory) / mb}, "free": ${runtime.freeMemory / mb}, "total": ${runtime.totalMemory / mb},"max": ${runtime.maxMemory / mb}}}"""

  private[etlflow] val stream = {
    ZStream
      .fromEffect(UIO(memoryInfo))
      .repeat(Schedule.forever && Schedule.spaced(1.seconds))
  }

  def websocketStream(token: String): ZStream[Any with Clock, Nothing, WebSocketFrame] = {
    if(auth.validateJwt(token)){
      unsafeRun(auth.isCached(token)) match {
        case Some(_) => stream.map(s => WebSocketFrame.text(s))

        case None =>
          logger.warn(s"Expired token $token")
          ZStream.fromEffect(UIO(WebSocketFrame.Close(1001,Some("Expired token"))))
      }
    } else {
      logger.warn(s"Invalid token $token")
      ZStream.fromEffect(UIO(WebSocketFrame.Close(1001,Some("Invalid token"))))
    }
  }

  private[etlflow] def socketApp(token: String): SocketApp[Any with Clock, Nothing] =
    SocketApp.open(
      Socket.collect[Any] {
        case _ => websocketStream(token)
      }
    )

  val webSocketApp =
    Http.collect[Request] {
      case Method.GET -> !! / "ws" / "etlflow" / token => socketApp(token).asResponse
    }

}

