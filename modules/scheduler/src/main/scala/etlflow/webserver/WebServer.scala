package etlflow.webserver

import caliban.Http4sAdapter
import cats.data.Kleisli
import cats.effect.Blocker
import etlflow.jdbc.DbManager
import etlflow.scheduler.Scheduler
import etlflow.utils.EtlFlowHelper.{CronJob, EtlFlowHas, EtlFlowTask}
import etlflow.utils.{CacheHelper, Configuration}
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
import zio._
import zio.blocking.Blocking
import zio.interop.catz._
import scala.concurrent.duration._
import scala.reflect.runtime.universe.TypeTag

abstract class WebServer[EJN <: EtlJobName[EJP] : TypeTag, EJP <: EtlJobProps : TypeTag]
  extends Scheduler[EJN,EJP]
    with CatsApp
    with DbManager
    with Http4sDsl[EtlFlowTask]
    with EtlFlowService
    with Configuration {

  val otherRoutes:HttpRoutes[EtlFlowTask] = HttpRoutes.of[EtlFlowTask] {
    case _@GET -> Root => Ok(s"Hello, Welcome to EtlFlow API ${BI.version}, Build with scala version ${BI.scalaVersion}")
  }

  def etlFlowWebServer(blocker: Blocker, cache: Cache[String]): ZIO[ZEnv with EtlFlowHas, Throwable, Nothing] =
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
                                  "/joblist"               -> Kleisli.liftF(StaticFile.fromResource("static/jobListPage.html", blocker, None)),
                                  "/etlflow"        -> metricsSvc.routes,
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
                                ).orNotFound
                              ).resource
                              .toManagedZIO
      } yield 0).useForever
    }

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = {
    val finalRunner: ZIO[ZEnv, Throwable, Unit] = for {
      blocker         <- ZIO.access[Blocking](_.get.blockingExecutor.asEC).map(Blocker.liftExecutionContext)
      transactor      <- createDbTransactor(config.dbLog, platform.executor.asEC, blocker, "EtlFlowScheduler-Pool", 10)
      cache           = CacheHelper.createCache[String](24 * 60)
      cronJobs        <- Ref.make(List.empty[CronJob])
      jobs            <- getEtlJobs[EJN,EJP](etl_job_name_package)
      jobSemaphores   <- createSemaphores(jobs)
      _               <- etlFlowScheduler(transactor,cronJobs,jobSemaphores).fork
      _               <- etlFlowWebServer(blocker,cache).provideCustomLayer(liveHttp4s[EJN,EJP](transactor,cache,cronJobs,jobSemaphores,jobs))
    } yield ()

    finalRunner.catchAll{err =>
      UIO {
        logger.error(err.getMessage)
        err.getStackTrace.foreach(x => logger.error(x.toString))
      }
    }.as(ExitCode.failure)
  }
}
