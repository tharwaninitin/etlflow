package etlflow.webserver

import etlflow.api.ServerTask
import etlflow.log.ApplicationLogger
import fs2.{Pipe, Stream}
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl
import org.http4s.server.websocket.WebSocketBuilder
import org.http4s.websocket.WebSocketFrame
import zio.clock.Clock
import zio.{Schedule, Task, UIO}
import zio.interop.catz._
import zio.stream.ZStream
import zio.stream.interop.fs2z._
import zio.duration._

case class WebsocketAPI(auth: Authentication) extends Http4sDsl[Task] with ApplicationLogger {
  private val mb: Int = 1024*1024
  private val runtime: Runtime = Runtime.getRuntime
  private val stream: Stream[Task, String] = {
    ZStream
      .fromEffect(
        UIO{
          s"""{"memory": {"used": ${(runtime.totalMemory - runtime.freeMemory) / mb}, "free": ${runtime.freeMemory / mb}, "total": ${runtime.totalMemory / mb},"max": ${runtime.maxMemory / mb}}}""".stripMargin
        }
      )
      .repeat(Schedule.forever && Schedule.spaced(5.seconds))
      .provideLayer(Clock.live)
      .toFs2Stream
  }

  def websocketStream(token: String): Stream[Task, WebSocketFrame] = {
    if(auth.validateJwt(token)){
      auth.isCached(token) match {
        case Some(_) => stream.map(s => WebSocketFrame.Text(s))
        case None =>
          logger.warn(s"Expired token $token")
          Stream.eval(Task.fromEither(WebSocketFrame.Close(1001,"Expired token")))
      }
    } else {
      logger.warn(s"Invalid token $token")
      Stream.eval(Task.fromEither(WebSocketFrame.Close(1001,"Invalid token")))
    }
  }

  val streamRoutes: HttpRoutes[ServerTask] =
    HttpRoutes.of[ServerTask] {
      case GET -> Root / token =>
        val toClient: Stream[ServerTask, WebSocketFrame] = websocketStream(token)
        val fromClient: Pipe[ServerTask, WebSocketFrame, Unit] = _.as(())
        WebSocketBuilder[ServerTask].build(toClient, fromClient, onClose = UIO(logger.info("Closed Web socket")))
    }
}

