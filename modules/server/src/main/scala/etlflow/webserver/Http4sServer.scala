package etlflow.webserver

import caliban.Http4sAdapter
import cats.data.Kleisli
import cats.effect.Blocker
import doobie.hikari.HikariTransactor
import etlflow.utils.Config
import etlflow.utils.EtlFlowHelper._
import etlflow.webserver.api._
import etlflow.etljobs.{EtlJob => CoreEtlJob}
import etlflow.{EtlJobPropsMapping, EtlJobProps, BuildInfo => BI}
import org.http4s.dsl.Http4sDsl
import org.http4s.implicits._
import org.http4s.metrics.prometheus.{Prometheus, PrometheusExportService}
import org.http4s.server.Router
import org.http4s.server.blaze.BlazeServerBuilder
import org.http4s.server.middleware.{CORS, Metrics}
import org.http4s.{HttpRoutes, StaticFile}
import scalacache.Cache
import zio.interop.catz._
import zio.{Queue, Semaphore, Task, ZEnv, ZIO}

import scala.concurrent.duration._
import scala.reflect.runtime.universe.TypeTag

trait Http4sServer extends Http4sDsl[EtlFlowTask] with GqlImplementation {

  val otherRoutes:HttpRoutes[EtlFlowTask] = HttpRoutes.of[EtlFlowTask] {
    case _@GET -> Root => Ok(s"Hello, Welcome to EtlFlow API ${BI.version}, Build with scala version ${BI.scalaVersion}")
  }

  def etlFlowWebServer[EJN <: EtlJobPropsMapping[EtlJobProps,CoreEtlJob[EtlJobProps]] : TypeTag](blocker: Blocker, cache: Cache[String],jobSemaphores: Map[String, Semaphore],transactor: HikariTransactor[Task],etl_job_name_package:String,config:Config,jobQueue: Queue[(String,String,String,String)]): ZIO[ZEnv with EtlFlowHas, Throwable, Nothing] =
    ZIO.runtime[ZEnv with EtlFlowHas]
      .flatMap{implicit runtime =>
        (for {
          metricsSvc         <- PrometheusExportService.build[EtlFlowTask].toManagedZIO
          metrics            <- Prometheus.metricsOps[EtlFlowTask](metricsSvc.collectorRegistry, "server").toManagedZIO
          etlFlowInterpreter <- GqlAPI.api.interpreter.toManaged_
          loginInterpreter   <- GqlLoginAPI.api.interpreter.toManaged_
          banner = """
                     |   ________   _________    _____      ________    _____        ___     ____      ____
                     |  |_   __  | |  _   _  |  |_   _|    |_   __  |  |_   _|     .'   `.  |_  _|    |_  _|
                     |    | |_ \_| |_/ | | \_|    | |        | |_ \_|    | |      /  .-.  \   \ \  /\  / /
                     |    |  _| _      | |        | |   _    |  _|       | |   _  | |   | |    \ \/  \/ /
                     |   _| |__/ |    _| |_      _| |__/ |  _| |_       _| |__/ | \  `-'  /     \  /\  /
                     |  |________|   |_____|    |________| |_____|     |________|  `.___.'       \/  \/
                     |
                     |""".stripMargin.split("\n").toList ++ List(" "*75 + s"${BI.version}", "")
          _                  <- BlazeServerBuilder[EtlFlowTask](runtime.platform.executor.asEC)
            .bindHttp(8080, "0.0.0.0")
            .withConnectorPoolSize(20)
            .withBanner(banner)
            .withResponseHeaderTimeout(110.seconds)
            .withIdleTimeout(120.seconds)
            .withServiceErrorHandler(_ => {
              case ex: Throwable => InternalServerError(ex.getMessage)
            })
            .withHttpApp(
              Router[EtlFlowTask](
                "/about" -> otherRoutes,
                "/"               -> Kleisli.liftF(StaticFile.fromResource("static/index.html", blocker, None)),
                "/etlflow"        -> metricsSvc.routes,
                "/assets/js/2.48dc5211.chunk.js"      -> Kleisli.liftF(StaticFile.fromResource("static/assets/js/2.48dc5211.chunk.js", blocker, None)),
                "/assets/js/main.6f9a8990.chunk.js"      -> Kleisli.liftF(StaticFile.fromResource("static/assets/js/main.6f9a8990.chunk.js", blocker, None)),
                "/assets/css/2.83b1b994.chunk.css"      -> Kleisli.liftF(StaticFile.fromResource("static/assets/css/2.83b1b994.chunk.css", blocker, None)),
                "/assets/css/main.025b9fa1.chunk.css"      -> Kleisli.liftF(StaticFile.fromResource("static/assets/css/main.025b9fa1.chunk.css", blocker, None)),
                "/api/etlflow"    -> CORS(Metrics[EtlFlowTask](metrics)(
                  AuthMiddleware(
                    Http4sAdapter.makeHttpService(etlFlowInterpreter),
                    authEnabled = true,
                    cache
                  ))
                ),
                "/api/login"      -> CORS(Http4sAdapter.makeHttpService(loginInterpreter)),
                "/ws/etlflow"     -> CORS(new WebsocketAPI[EtlFlowTask](cache).streamRoutes),
                "/api"     -> AuthMiddleware(
                  CORS(RestAPI.routes[EJN](jobSemaphores,transactor,etl_job_name_package,config,jobQueue)),
                  authEnabled = true,
                  cache),
              ).orNotFound
            ).resource
            .toManagedZIO
        } yield 0).useForever
      }
}