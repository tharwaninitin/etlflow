package etlflow.webserver

import caliban.Http4sAdapter
import cats.data.Kleisli
import cats.effect.Blocker
import etlflow.api.{EtlFlowTask, ServerEnv}
import etlflow.utils.WebServer
import etlflow.{EJPMType, BuildInfo => BI}
import org.http4s.dsl.Http4sDsl
import org.http4s.implicits._
import org.http4s.metrics.prometheus.{Prometheus, PrometheusExportService}
import org.http4s.server.Router
import org.http4s.server.blaze.BlazeServerBuilder
import org.http4s.server.middleware.{CORS, Metrics}
import org.http4s.{HttpRoutes, StaticFile}
import scalacache.Cache
import zio.blocking.Blocking
import zio.interop.catz._
import zio.{ZIO, ZManaged}
import scala.concurrent.duration._
import scala.reflect.runtime.universe.TypeTag

trait Http4sServer extends Http4sDsl[EtlFlowTask] {

  val otherRoutes: HttpRoutes[EtlFlowTask] = HttpRoutes.of[EtlFlowTask] {
    case _@GET -> Root => Ok(s"Hello, Welcome to EtlFlow API ${BI.version}, Build with scala version ${BI.scalaVersion}")
  }

  def allRoutes[EJN <: EJPMType : TypeTag](cache: Cache[String], config: Option[WebServer]): ZManaged[ServerEnv, Throwable, HttpRoutes[EtlFlowTask]] = {
    for {
      blocker            <- ZIO.access[Blocking](_.get.blockingExecutor.asEC).map(Blocker.liftExecutionContext).toManaged_
      metricsSvc         <- PrometheusExportService.build[EtlFlowTask].toManagedZIO
      metrics            <- Prometheus.metricsOps[EtlFlowTask](metricsSvc.collectorRegistry, "server").toManagedZIO
      etlFlowInterpreter <- GqlAPI.api.interpreter.toManaged_
      loginInterpreter   <- GqlLoginAPI.api.interpreter.toManaged_
      routes = Router[EtlFlowTask](
                 "/"               -> Kleisli.liftF(StaticFile.fromResource("static/index.html", blocker, None)),
                  "/assets/js/2.800a40b8.chunk.js"      -> Kleisli.liftF(StaticFile.fromResource("static/assets/js/2.800a40b8.chunk.js", blocker, None)),
                  "/assets/js/main.a5addb2c.chunk.js"   -> Kleisli.liftF(StaticFile.fromResource("static/assets/js/main.a5addb2c.chunk.js", blocker, None)),
                  "/assets/css/2.83b1b994.chunk.css"    -> Kleisli.liftF(StaticFile.fromResource("static/assets/css/2.83b1b994.chunk.css", blocker, None)),
                  "/assets/css/main.025b9fa1.chunk.css" -> Kleisli.liftF(StaticFile.fromResource("static/assets/css/main.025b9fa1.chunk.css", blocker, None)),
                  "/about"       -> otherRoutes,
                  "/etlflow"     -> metricsSvc.routes,
                  "/api/etlflow" -> CORS(Metrics[EtlFlowTask](metrics)(Authentication.middleware(Http4sAdapter.makeHttpService(etlFlowInterpreter), authEnabled = true, cache, config))),
                  "/api/login"   -> CORS(Http4sAdapter.makeHttpService(loginInterpreter)),
                  "/ws/etlflow"  -> CORS(new WebsocketAPI[EtlFlowTask](cache).streamRoutes),
                  "/api"         -> CORS(Authentication.middleware(RestAPI.routes, authEnabled = true, cache, config)),
                  "/swagger"     -> RestAPINew.swaggerRoute,
                  "/restapi"     -> CORS(Authentication.middleware(RestAPINew.routes, authEnabled = true, cache, config))
                )
    } yield routes
  }

  def etlFlowWebServer[EJN <: EJPMType : TypeTag](cache: Cache[String], config: Option[WebServer]): ZIO[ServerEnv, Throwable, Nothing] =
    ZIO.runtime[ServerEnv]
      .flatMap{implicit runtime =>
        (for {
          routes <- allRoutes[EJN](cache,config)
          address = config.map(_.ip_address.getOrElse("0.0.0.0")).getOrElse("0.0.0.0")
          port    = config.map(_.port.getOrElse(8080)).getOrElse(8080)
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
            .bindHttp(port, address)
            .withConnectorPoolSize(20)
            .withBanner(banner)
            .withResponseHeaderTimeout(110.seconds)
            .withIdleTimeout(120.seconds)
            .withServiceErrorHandler(_ => {
              case ex: Throwable => InternalServerError(ex.getMessage)
            })
            .withHttpApp(routes.orNotFound)
            .resource
            .toManagedZIO
        } yield 0).useForever
      }
}