package etlflow.webserver

import caliban.Http4sAdapter
import cats.data.Kleisli
import cats.effect.Blocker
import doobie.hikari.HikariTransactor
import etlflow.utils.Config
import etlflow.utils.EtlFlowHelper._
import etlflow.webserver.api._
import etlflow.{EtlJobName, EtlJobProps, BuildInfo => BI}
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

trait Http4sServer extends Http4sDsl[EtlFlowTask] with EtlFlowService {

  val otherRoutes:HttpRoutes[EtlFlowTask] = HttpRoutes.of[EtlFlowTask] {
    case _@GET -> Root => Ok(s"Hello, Welcome to EtlFlow API ${BI.version}, Build with scala version ${BI.scalaVersion}")
  }

  def etlFlowWebServer[EJN <: EtlJobName[EJP] : TypeTag, EJP <: EtlJobProps : TypeTag](blocker: Blocker, cache: Cache[String],jobSemaphores: Map[String, Semaphore],transactor: HikariTransactor[Task],etl_job_name_package:String,config:Config,jobQueue: Queue[(String,String,String,String)]): ZIO[ZEnv with EtlFlowHas, Throwable, Nothing] =
    ZIO.runtime[ZEnv with EtlFlowHas]
      .flatMap{implicit runtime =>
        (for {
          metricsSvc         <- PrometheusExportService.build[EtlFlowTask].toManagedZIO
          metrics            <- Prometheus.metricsOps[EtlFlowTask](metricsSvc.collectorRegistry, "server").toManagedZIO
          etlFlowInterpreter <- EtlFlowApi.api.interpreter.toManaged_
          loginInterpreter   <- LoginApi.api.interpreter.toManaged_
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
            .withHttpApp(
              Router[EtlFlowTask](
                "/about" -> otherRoutes,
                "/"               -> Kleisli.liftF(StaticFile.fromResource("static/index.html", blocker, None)),
                "/etlflow"        -> metricsSvc.routes,
                "/assets/jwt-decode.js"      -> Kleisli.liftF(StaticFile.fromResource("static/assets/jwt-decode.js", blocker, None)),
                "/assets/client.js"      -> Kleisli.liftF(StaticFile.fromResource("static/assets/client.js", blocker, None)),
                "/assets/signin.css"      -> Kleisli.liftF(StaticFile.fromResource("static/assets/signin.css", blocker, None)),
                "/assets/icons8-refresh.svg"      -> Kleisli.liftF(StaticFile.fromResource("static/assets/icons8-refresh.svg", blocker, None)),
                "/api/etlflow"    -> CORS(Metrics[EtlFlowTask](metrics)(
                  AuthMiddleware(
                    Http4sAdapter.makeHttpService(etlFlowInterpreter),
                    authEnabled = true,
                    cache
                  ))
                ),
                "/api/login"      -> CORS(Http4sAdapter.makeHttpService(loginInterpreter)),
                "/ws/etlflow"     -> CORS(new StatsStreams[EtlFlowTask](cache).streamRoutes),
                "/api/runjob"     -> AuthMiddleware(
                  CORS(new TriggerJob[EtlFlowTask].triggerEtlJob[EJN,EJP](jobSemaphores,transactor,etl_job_name_package,config,jobQueue)),
                  authEnabled = true,
                  cache),
              ).orNotFound
            ).resource
            .toManagedZIO
        } yield 0).useForever
      }
}