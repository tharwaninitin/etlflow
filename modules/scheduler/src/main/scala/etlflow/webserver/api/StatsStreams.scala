package etlflow.webserver.api

import cats.effect.{ContextShift, Sync, Timer}
import cats.{Applicative, Functor}
import etlflow.utils.CacheHelper
import fs2.{Pipe, Stream}
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl
import org.http4s.server.websocket.WebSocketBuilder
import org.http4s.websocket.WebSocketFrame
import org.slf4j.{Logger, LoggerFactory}
import scalacache.Cache
import scala.concurrent.duration._

class StatsStreams[F[_]: Sync: ContextShift: Timer](cache: Cache[String]) extends Http4sDsl[F] {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  val mb: Int = 1024*1024
  val runtime: Runtime = Runtime.getRuntime

  private def ticker[F[_]: Functor: Timer, A](stream: Stream[F, A]): Stream[F, A] =
    (Stream.emit(Duration.Zero) ++ Stream.awakeEvery[F](5.seconds))
      .as(stream)
      .flatten

  def stream[F[_]: Sync: ContextShift: Timer]: Stream[F, String] =
    ticker(
      Stream.eval(Sync[F].delay{
        s"""
           |Used Memory: ${(runtime.totalMemory - runtime.freeMemory) / mb} </br>
           |Free Memory: ${runtime.freeMemory / mb} </br>
           |Total Memory: ${runtime.totalMemory / mb} </br>
           |Max Memory: ${runtime.maxMemory / mb} </br>
           |""".stripMargin
      })
    )

  lazy val streamRoutes: HttpRoutes[F] =
    HttpRoutes.of[F] {
      case GET -> Root / token =>
        val toClient: Stream[F, WebSocketFrame] =
          if(CacheHelper.getKey(cache,token).getOrElse("NA") == token){
            stream.map(s => WebSocketFrame.Text(s.toString))
          } else {
            Stream.eval(Sync[F].delay(WebSocketFrame.Close()))
        }
        val fromClient: Pipe[F, WebSocketFrame, Unit] = _.as(())
        WebSocketBuilder[F].build(toClient, fromClient, onClose = Applicative[F].pure(logger.info("Closed Web socket")))
    }
}

