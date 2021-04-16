package etlflow.webserver.api

import etlflow.log.ApplicationLogger
import etlflow.utils.CacheHelper
import scalacache.Cache
import scala.concurrent.duration._
import cats.effect.{ContextShift, Sync, Timer}
import cats.{Applicative, Functor}
import fs2.{Pipe, Stream}
import io.circe.Json
import io.circe.syntax._
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl
import org.http4s.server.websocket.WebSocketBuilder
import org.http4s.websocket.WebSocketFrame

class WebsocketAPI[F[_]: Sync: ContextShift: Timer](cache: Cache[String]) extends Http4sDsl[F] with ApplicationLogger {
  private val mb: Int = 1024*1024
  private val runtime: Runtime = Runtime.getRuntime

  private def ticker[F[_]: Functor: Timer, A](stream: Stream[F, A]): Stream[F, A] =
    (Stream.emit(Duration.Zero) ++ Stream.awakeEvery[F](5.seconds))
      .as(stream)
      .flatten

  private def stream[F[_]: Sync: ContextShift: Timer]: Stream[F, String] =
    ticker(
      Stream.eval(Sync[F].delay{
        s"""${Json.obj("data" -> s"""{"Used_Memory": ${(runtime.totalMemory - runtime.freeMemory) / mb}, "Free_Memory": ${runtime.freeMemory / mb},"Total_Memory": ${runtime.totalMemory / mb},"Max_Memory": ${runtime.maxMemory / mb}}""".asJson)}""".stripMargin
      })
    )

  lazy val streamRoutes: HttpRoutes[F] =
    HttpRoutes.of[F] {
      case GET -> Root / token =>
        val toClient: Stream[F, WebSocketFrame] =
          if(CacheHelper.getKey(cache,token).getOrElse("NA") == token){
            stream.map(s => WebSocketFrame.Text(s.toString()))
          } else {
            Stream.eval(Sync[F].delay(WebSocketFrame.Close()))
        }
        val fromClient: Pipe[F, WebSocketFrame, Unit] = _.as(())
        WebSocketBuilder[F].build(toClient, fromClient, onClose = Applicative[F].pure(logger.info("Closed Web socket")))
    }
}

