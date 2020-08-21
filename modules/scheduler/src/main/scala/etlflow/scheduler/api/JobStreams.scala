package etlflow.scheduler.api

import cats.Applicative
import cats.effect.Sync
import fs2.Pipe
import fs2.concurrent.Topic
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl
import org.http4s.server.websocket.WebSocketBuilder
import org.http4s.websocket.WebSocketFrame
import org.slf4j.{Logger, LoggerFactory}

case class JobStreams[F[_]: Sync](t: Topic[F, WebSocketFrame]) extends Http4sDsl[F] {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  lazy val routes: HttpRoutes[F] =
    HttpRoutes.of[F] {
      case GET -> Root =>
        val fromClient: Pipe[F, WebSocketFrame, Unit] = _.as(())
        WebSocketBuilder[F].build(t.subscribe(100), fromClient, onClose = Applicative[F].pure(logger.info("Closed Web socket")))
    }
}

