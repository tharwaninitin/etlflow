package etlflow.scheduler

import caliban.Http4sAdapter
import cats.data.Kleisli
import cats.effect.Blocker
import doobie.hikari.HikariTransactor
import etlflow.jdbc.DbManager
import etlflow.scheduler.api.EtlFlowHelper.{CronJob, EtlFlowHas, EtlFlowTask}
import etlflow.scheduler.api._
import etlflow.utils.{GlobalProperties, JDBC, UtilityFunctions => UF}
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
import zio.console.putStrLn
import zio.interop.catz._
import scala.concurrent.duration._
import scala.reflect.runtime.universe.TypeTag

abstract class WebServer[EJN <: EtlJobName[EJP] : TypeTag, EJP <: EtlJobProps : TypeTag, EJGP <: GlobalProperties : TypeTag]
  extends Scheduler[EJN,EJP] with CatsApp with DbManager with Http4sDsl[EtlFlowTask] with EtlFlowService {

  def globalProperties: Option[EJGP]
  val etl_job_name_package: String = UF.getJobNamePackage[EJN] + "$"

  lazy val global_properties: Option[EJGP] = globalProperties
  val DB_DRIVER: String = global_properties.map(x => x.log_db_driver).getOrElse("<not_set>")
  val DB_URL: String  = global_properties.map(x => x.log_db_url).getOrElse("<not_set>")     // connect URL
  val DB_USER: String = global_properties.map(x => x.log_db_user).getOrElse("<not_set>")    // username
  val DB_PASS: String = global_properties.map(x => x.log_db_pwd).getOrElse("<not_set>")    // password
  val credentials: JDBC = JDBC(DB_URL,DB_USER,DB_PASS,DB_DRIVER)

  val otherRoutes:HttpRoutes[EtlFlowTask] = HttpRoutes.of[EtlFlowTask] {
    case _@GET -> Root => Ok(s"Hello, Welcome to EtlFlow API ${BI.version}, Build with scala version ${BI.scalaVersion}")
  }

  def etlFlowWebServer(blocker: Blocker, transactor: HikariTransactor[Task], cache: Cache[String]): ZIO[ZEnv with EtlFlowHas, Throwable, Nothing] =
    ZIO.runtime[ZEnv with EtlFlowHas]
    .flatMap{implicit runtime =>
      (for {
        metricsSvc         <- PrometheusExportService.build[EtlFlowTask].toManagedZIO
        metrics            <- Prometheus.metricsOps[EtlFlowTask](metricsSvc.collectorRegistry, "server").toManagedZIO
        etlFlowInterpreter <- EtlFlowApi.api.interpreter.toManaged_
        loginInterpreter   <- LoginApi.api.interpreter.toManaged_
        _                  <- BlazeServerBuilder[EtlFlowTask]
                              .bindHttp(8080, "0.0.0.0")
                              .withConnectorPoolSize(20)
                              .withResponseHeaderTimeout(110.seconds)
                              .withIdleTimeout(120.seconds)
                              .withExecutionContext(platform.executor.asEC)
                              .withHttpApp(
                                Router[EtlFlowTask](
                                  "/about" -> otherRoutes,
                                  "/"               -> Kleisli.liftF(StaticFile.fromResource("static/index.html", blocker, None)),
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
                                  "/ws/etlflow"     -> CORS(new EtlFlowStreams[EtlFlowTask].streamRoutes),
                                ).orNotFound
                              ).resource
                              .toManagedZIO
      } yield 0).useForever
    }

  override def run(args: List[String]): ZIO[ZEnv, Nothing, Int] = {
    val finalRunner = for {
      _           <- runDbMigration(credentials)
      blocker     <- ZIO.access[Blocking](_.get.blockingExecutor.asEC).map(Blocker.liftExecutionContext)
      transactor  <- createDbTransactorJDBC(credentials, platform.executor.asEC, blocker, "EtlFlowScheduler-Pool", 10)
      cache       = CacheHelper.createCache[String](60)
      cronJobs    <- Ref.make(List.empty[CronJob])
      _           <- etlFlowScheduler(transactor,cronJobs).fork
      _           <- etlFlowWebServer(blocker,transactor,cache).provideCustomLayer(liveHttp4s[EJN,EJP](transactor,cache,cronJobs))
    } yield ()

    finalRunner.catchAll(err => putStrLn(err.toString)).as(1)
  }
}
