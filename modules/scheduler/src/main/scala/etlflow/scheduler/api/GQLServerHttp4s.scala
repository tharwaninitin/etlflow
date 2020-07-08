package etlflow.scheduler.api

import caliban.Http4sAdapter
import cats.data.Kleisli
import cats.effect.Blocker
import etlflow.jdbc.DbManager
import etlflow.scheduler.api.EtlFlowHelper.EtlFlowHas
import etlflow.utils.JDBC
import etlflow.{BuildInfo => BI}
import io.prometheus.client.CollectorRegistry
import org.http4s.metrics.prometheus.{Prometheus, PrometheusExportService}
import org.http4s.{HttpRoutes, StaticFile}
import org.http4s.dsl.Http4sDsl
import org.http4s.implicits._
import org.http4s.server.Router
import org.http4s.server.blaze.BlazeServerBuilder
import org.http4s.server.middleware.{CORS, Metrics}
import org.slf4j.{Logger, LoggerFactory}
import zio.blocking.Blocking
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
        (for {
          metricsSvc         <- PrometheusExportService.build[EtlFlowTask].toManagedZIO
          metrics            <- Prometheus.metricsOps[EtlFlowTask](CollectorRegistry.defaultRegistry, "server").toManagedZIO
          blocker            <- ZIO.access[Blocking](_.get.blockingExecutor.asEC).map(Blocker.liftExecutionContext).toManaged_
          etlFlowInterpreter <- EtlFlowApi.api.interpreter.toManaged_
          _                  <- BlazeServerBuilder[EtlFlowTask]
                                .bindHttp(8080, "0.0.0.0")
                                .withConnectorPoolSize(10)
                                .withResponseHeaderTimeout(55.seconds)
                                .withIdleTimeout(60.seconds)
                                .withExecutionContext(platform.executor.asEC)
                                .withHttpApp(
                                  Router[EtlFlowTask](
                                    "/about" -> otherRoutes,
                                    "/"               -> Kleisli.liftF(StaticFile.fromResource("static/index.html", blocker, None)),
                                    "/etlflow"        -> metricsSvc.routes,
                                    "/client.js"      -> Kleisli.liftF(StaticFile.fromResource("static/client.js", blocker, None)),
                                    "/api/etlflow"    -> CORS(Metrics[EtlFlowTask](metrics)(Http4sAdapter.makeHttpService(etlFlowInterpreter))),
                                    "/ws/etlflow"     -> CORS(new EtlFlowStreams[EtlFlowTask].streamRoutes),
                                  ).orNotFound
                                ).resource
                                .toManagedZIO
        } yield 0).useForever
      }
      .provideCustomLayer(etlFlowLayer)
      .catchAll(err => putStrLn(err.toString).as(1))
  }
}
