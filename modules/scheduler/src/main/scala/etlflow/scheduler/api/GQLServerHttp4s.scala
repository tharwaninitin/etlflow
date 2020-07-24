package etlflow.scheduler.api

import caliban.Http4sAdapter
import cats.data.{Kleisli, OptionT}
import cats.effect.Blocker
import doobie.implicits._
import doobie.hikari.HikariTransactor
import doobie.quill.DoobieContext
import etlflow.jdbc.DbManager
import etlflow.scheduler.api.EtlFlowHelper.EtlFlowHas
import etlflow.utils.JDBC
import etlflow.{BuildInfo => BI}
import io.getquill.{Literal, LowerCase, PostgresJdbcContext}
import io.prometheus.client.CollectorRegistry
import org.http4s.metrics.prometheus.{Prometheus, PrometheusExportService}
import org.http4s.{HttpRoutes, Request, StaticFile}
import org.http4s.dsl.Http4sDsl
import org.http4s.implicits._
import org.http4s.server.Router
import org.http4s.server.blaze.BlazeServerBuilder
import org.http4s.server.middleware.{CORS, Metrics}
import org.http4s.util.CaseInsensitiveString
import org.slf4j.{Logger, LoggerFactory}
import zio.blocking.Blocking
import zio.console.putStrLn
import zio.interop.catz._
import zio.{RIO, ZIO, _}

import scala.concurrent.duration._

private[scheduler] trait GQLServerHttp4s extends CatsApp with DbManager{
  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  val credentials: JDBC
  def etlFlowLayer(transactor: HikariTransactor[Task]): ZLayer[Blocking, Throwable, EtlFlowHas]

  type EtlFlowTask[A] = RIO[ZEnv with EtlFlowHas, A]
  object ioz extends Http4sDsl[EtlFlowTask]
  import ioz._

  val otherRoutes:HttpRoutes[EtlFlowTask] = HttpRoutes.of[EtlFlowTask] {
    case _@GET -> Root => Ok(s"Hello, Welcome to EtlFlow API ${BI.version}, Build with scala version ${BI.scalaVersion}")
  }

  val doobieContext = new DoobieContext.Postgres(Literal)
  import doobieContext._

  case class UserAuthTokens(token: String)

  def AuthMiddleware(service: HttpRoutes[EtlFlowTask],
         authEnabled: Boolean,
         transactor: HikariTransactor[Task]
        ): HttpRoutes[EtlFlowTask] = Kleisli {
      req: Request[EtlFlowTask] =>
        if(authEnabled) {
          req.headers.get(CaseInsensitiveString("Authorization")) match {
            case Some(value) => {
              logger.info("Token: " + value)
              val token = value.value
              val isTokenValid = {
                val q = quote {
                  query[UserAuthTokens].filter(x => x.token == lift(token)).nonEmpty
                }
                runtime.unsafeRun(doobieContext.run(q).transact(transactor))
              }
              logger.info("Is Token Valid => " + isTokenValid)

              if (isTokenValid) {
                //Return response as it it in case of success
                service(req)
              } else {
                //Return forbidden error as request is invalid
                OptionT.liftF(Forbidden())
              }
            }
            case None => {
              logger.info("Header not present. Invalid Requests !! ")
              OptionT.liftF(Forbidden())
            }
          }
        } else{
          //Return response as it is when authentication is disabled
          service(req)
        }
    }

  def etlFlowRunner(blocker: Blocker, transactor: HikariTransactor[Task]): ZIO[ZEnv with EtlFlowHas, Throwable, Nothing] =
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
                                      true,
                                      transactor
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
      blocker     <- ZIO.access[Blocking](_.get.blockingExecutor.asEC).map(Blocker.liftExecutionContext)
      transactor  <- createDbTransactorJDBC(credentials, platform.executor.asEC, blocker, "EtlFlowScheduler-Pool", 10)
      _           <- etlFlowRunner(blocker,transactor).provideCustomLayer(etlFlowLayer(transactor))
    } yield ()

    finalRunner.catchAll(err => putStrLn(err.toString)).as(1)
  }
}
