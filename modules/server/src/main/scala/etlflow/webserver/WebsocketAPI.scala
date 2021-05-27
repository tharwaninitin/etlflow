package etlflow.webserver

import etlflow.log.ApplicationLogger
import zhttp.http._
import zhttp.socket.{Socket, SocketApp, WebSocketFrame}
import zio.clock.Clock
import zio.duration._
import zio.stream.ZStream
import zio.{Schedule, UIO}

case class WebsocketAPI(auth: Authentication)  extends ApplicationLogger {
  private val mb: Int = 1024 * 1024
  private val runtime: Runtime = Runtime.getRuntime

  private val stream = {
    ZStream
      .fromEffect(
        UIO(
        s"""{"memory": {"used": ${(runtime.totalMemory - runtime.freeMemory) / mb}, "free": ${runtime.freeMemory / mb}, "total": ${runtime.totalMemory / mb},"max": ${runtime.maxMemory / mb}}}"""
      )).repeat(Schedule.forever && Schedule.spaced(1.seconds))
  }

  def websocketStream(token: String): ZStream[Any with Clock, Nothing, WebSocketFrame] = {
    if(auth.validateJwt(token)){
      auth.isCached(token) match {
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

  private def openConnection(token: String): Socket[Any with Clock, Nothing, Any, WebSocketFrame] =
    Socket.collect[Any] {
      case _ => websocketStream(token)
    }

  private def socketApp(token: String): SocketApp[Any with Clock, Nothing] =
    SocketApp.open(openConnection(token))

  val webSocketApp =
    HttpApp.collect {
      case Method.GET -> Root / "ws" / "etlflow" / token => socketApp(token)
    }

}

