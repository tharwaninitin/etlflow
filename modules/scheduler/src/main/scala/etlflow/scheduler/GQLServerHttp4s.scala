package etlflow.scheduler

import caliban.Http4sAdapter
import etlflow.jdbc.DbManager
import etlflow.scheduler.EtlFlowHelper.EtlFlowHas
import etlflow.utils.JDBC
import etlflow.{BuildInfo => BI}
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl
import org.http4s.implicits._
import org.http4s.server.Router
import org.http4s.server.blaze.BlazeServerBuilder
import org.http4s.server.middleware.CORS
import org.slf4j.{Logger, LoggerFactory}
import zio.console.putStrLn
import zio.interop.catz._
import zio.{RIO, ZIO, _}
import scala.concurrent.duration._

private[scheduler] trait GQLServerHttp4s extends CatsApp with DbManager with BootstrapRuntime {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  val credentials: JDBC
  val etlFlowLayer: Layer[Throwable, EtlFlowHas]

  type EtlFlowTask[A] = RIO[ZEnv with EtlFlowHas, A]

  object ioz extends Http4sDsl[EtlFlowTask]
  import ioz._
  val otherRoutes:HttpRoutes[EtlFlowTask] = HttpRoutes.of[EtlFlowTask] {
    case _@GET -> Root => Ok(s"Hello, Welcome to EtlFlow API ${BI.version}, Build with scala version ${BI.scalaVersion}")
  }

  override def run(args: List[String]): ZIO[ZEnv, Nothing, Int] = {
    ZIO.runtime[ZEnv with EtlFlowHas]
      .flatMap{implicit runtime =>
        for {
          etlFlowInterpreter <- EtlFlowApi.api.interpreter
          _                  <- BlazeServerBuilder[EtlFlowTask]
                                .bindHttp(8080, "0.0.0.0")
                                .withConnectorPoolSize(10)
                                .withResponseHeaderTimeout(55.seconds)
                                .withIdleTimeout(60.seconds)
                                .withExecutionContext(platform.executor.asEC)
                                .withHttpApp(
                                  Router[EtlFlowTask](
                                    "/"    -> otherRoutes,
                                    "/api/etlflow"    -> CORS(Http4sAdapter.makeHttpService(etlFlowInterpreter)),
                                    "/ws/etlflow"     -> CORS(Http4sAdapter.makeWebSocketService(etlFlowInterpreter)),
                                  ).orNotFound
                                ).resource
                                .toManaged
                                .useForever
        } yield 0
      }
      .provideCustomLayer(etlFlowLayer)
      .catchAll(err => putStrLn(err.toString).as(1))
  }
}
