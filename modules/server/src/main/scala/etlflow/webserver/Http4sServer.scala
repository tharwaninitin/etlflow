package etlflow.webserver

import caliban.Http4sAdapter
import cats.data.Kleisli
import cats.effect.Blocker
import etlflow.api.{ServerEnv, ServerTask}
import etlflow.utils.{GetCorsConfig, WebServer}
import etlflow.{EJPMType, BuildInfo => BI}
import org.http4s.dsl.Http4sDsl
import org.http4s.implicits._
import org.http4s.metrics.prometheus.{Prometheus, PrometheusExportService}
import org.http4s.server.Router
import org.http4s.server.blaze.BlazeServerBuilder
import org.http4s.server.middleware.{CORS, Metrics}
import org.http4s.{HttpRoutes, StaticFile}
import zio.blocking.Blocking
import zio.interop.catz._
import zio.{ZIO, ZManaged}

import scala.concurrent.duration._
import scala.reflect.runtime.universe.TypeTag

trait Http4sServer extends Http4sDsl[ServerTask] {

  val otherRoutes: HttpRoutes[ServerTask] = HttpRoutes.of[ServerTask] {
    case _@GET -> Root => Ok(s"Hello, Welcome to EtlFlow API ${BI.version}, Build with scala version ${BI.scalaVersion}")
  }

  def allRoutes[EJN <: EJPMType : TypeTag](auth: Authentication, config: Option[WebServer]): ZManaged[ServerEnv, Throwable, HttpRoutes[ServerTask]] = {
    for {
      blocker            <- ZIO.access[Blocking](_.get.blockingExecutor.asEC).map(Blocker.liftExecutionContext).toManaged_
      metricsSvc         <- PrometheusExportService.build[ServerTask].toManagedZIO
      metrics            <- Prometheus.metricsOps[ServerTask](metricsSvc.collectorRegistry, "server").toManagedZIO
      etlFlowInterpreter <- GqlAPI.api.interpreter.toManaged_
      loginInterpreter   <- GqlLoginAPI.api.interpreter.toManaged_
      corsConfig         = GetCorsConfig(config)
      routes = Router[ServerTask](
                 "/"               -> Kleisli.liftF(StaticFile.fromResource("static/index.html", blocker, None)),
                  "/assets/js/2.dd6933f9.chunk.js"      -> Kleisli.liftF(StaticFile.fromResource("static/assets/js/2.dd6933f9.chunk.js", blocker, None)),
                  "/assets/js/main.bf05146d.chunk.js"   -> Kleisli.liftF(StaticFile.fromResource("static/assets/js/main.bf05146d.chunk.js", blocker, None)),
                  "/assets/css/2.83b1b994.chunk.css"    -> Kleisli.liftF(StaticFile.fromResource("static/assets/css/2.83b1b994.chunk.css", blocker, None)),
                  "/assets/css/main.025b9fa1.chunk.css" -> Kleisli.liftF(StaticFile.fromResource("static/assets/css/main.025b9fa1.chunk.css", blocker, None)),
                  "/about"       -> otherRoutes,
                  "/etlflow"     -> metricsSvc.routes,
                  "/api/etlflow" -> CORS(Metrics[ServerTask](metrics)(auth.middleware(Http4sAdapter.makeHttpService(etlFlowInterpreter)))),
                  "/api/login"   -> CORS(Http4sAdapter.makeHttpService(loginInterpreter)),
                  "/ws/etlflow"  -> CORS(WebsocketAPI(auth).streamRoutes),
                  "/api"         -> CORS(auth.middleware(RestAPI.routes)),
                  "/swagger"     -> RestAPINew.swaggerRoute,
                  "/restapi"     -> CORS(auth.middleware(RestAPINew.routes), corsConfig)
                )
    } yield routes
  }

  def etlFlowWebServer[EJN <: EJPMType : TypeTag](auth: Authentication, config: Option[WebServer]): ServerTask[Nothing] =
    ZIO.runtime[ServerEnv]
      .flatMap{implicit runtime =>
        (for {
          routes  <- allRoutes[EJN](auth, config)
          address = config.map(_.ip_address.getOrElse("0.0.0.0")).getOrElse("0.0.0.0")
          port    = config.map(_.port.getOrElse(8080)).getOrElse(8080)
          banner  = """
                     |   ________   _________    _____      ________    _____        ___     ____      ____
                     |  |_   __  | |  _   _  |  |_   _|    |_   __  |  |_   _|     .'   `.  |_  _|    |_  _|
                     |    | |_ \_| |_/ | | \_|    | |        | |_ \_|    | |      /  .-.  \   \ \  /\  / /
                     |    |  _| _      | |        | |   _    |  _|       | |   _  | |   | |    \ \/  \/ /
                     |   _| |__/ |    _| |_      _| |__/ |  _| |_       _| |__/ | \  `-'  /     \  /\  /
                     |  |________|   |_____|    |________| |_____|     |________|  `.___.'       \/  \/
                     |
                     |""".stripMargin.split("\n").toList ++ List(" "*75 + s"${BI.version}", "")
          _                  <- BlazeServerBuilder[ServerTask](runtime.platform.executor.asEC)
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