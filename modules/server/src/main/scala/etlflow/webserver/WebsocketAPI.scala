package etlflow.webserver

import etlflow.api.EtlFlowTask
import etlflow.log.ApplicationLogger
import fs2.{Pipe, Stream}
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl
import org.http4s.server.websocket.WebSocketBuilder
import org.http4s.websocket.WebSocketFrame
import zio.{Task, UIO}
import zio.interop.catz._
import scala.concurrent.duration._

case class WebsocketAPI(auth: Authentication) extends Http4sDsl[EtlFlowTask] with ApplicationLogger {
  private val mb: Int = 1024*1024
  private val runtime: Runtime = Runtime.getRuntime
  private def ticker(stream: Stream[EtlFlowTask, String]): Stream[EtlFlowTask, String] =
    (Stream.emit(Duration.Zero) ++ Stream.awakeEvery[EtlFlowTask](5.seconds))
      .as(stream)
      .flatten

  val stream: Stream[EtlFlowTask, String] =
    ticker(
      Stream.eval(UIO{
        s"""{"memory": {"used": ${(runtime.totalMemory - runtime.freeMemory) / mb}, "free": ${runtime.freeMemory / mb}, "total": ${runtime.totalMemory / mb},"max": ${runtime.maxMemory / mb}}}""".stripMargin
      })
    )

  /*
  TESTING CASE 1 => Invalid Token
    ws = new WebSocket('ws://localhost:8080/ws/etlflow/abcd')
    ws.onclose = function(e) { console.error(e) }
  TESTING CASE 2 => Expired token
    ws = new WebSocket('ws://localhost:8080/ws/etlflow/xxxxx')
    ws.onclose = function(e) { console.error(e) }
  TESTING CASE 3 => Correct token
    ws = new WebSocket('ws://localhost:8080/ws/etlflow/xxxxx')
    ws.onclose = function(e) { console.error(e) }
    ws.onmessage = function(e) { console.info(e) }
    ws.close()
  */
  val streamRoutes: HttpRoutes[EtlFlowTask] =
    HttpRoutes.of[EtlFlowTask] {
      case GET -> Root / token =>
        val toClient: Stream[EtlFlowTask, WebSocketFrame] =
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
        val fromClient: Pipe[EtlFlowTask, WebSocketFrame, Unit] = _.as(())
        WebSocketBuilder[EtlFlowTask].build(toClient, fromClient, onClose = UIO(logger.info("Closed Web socket")))
    }
}

