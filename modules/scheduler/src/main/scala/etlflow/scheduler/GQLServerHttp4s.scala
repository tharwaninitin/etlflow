package etlflow.scheduler

import caliban.{CalibanError, GraphQLInterpreter, Http4sAdapter}
import doobie.hikari.HikariTransactor
import etlflow.jdbc.DbManager
import etlflow.scheduler.EtlFlowHelper.CronJob
import etlflow.utils.JDBC
import etlflow.{BuildInfo => BI}
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl
import org.http4s.implicits._
import org.http4s.server.Router
import org.http4s.server.blaze.BlazeServerBuilder
import org.http4s.server.middleware.CORS
import zio.console.putStrLn
import zio.interop.catz._
import zio.{RIO, ZIO, _}

import scala.concurrent.duration._

private[scheduler] trait GQLServerHttp4s extends CatsApp with DbManager with BootstrapRuntime {
  val credentials: JDBC

  def etlFlowHttp4sInterpreter(transactor: HikariTransactor[Task], cronJobs: Ref[List[CronJob]]): ZIO[Any, CalibanError, GraphQLInterpreter[ZEnv, Throwable]]
  def getCronJobsDB(transactor: HikariTransactor[Task]): Task[List[CronJob]]

  type EtlFlowTask[A] = RIO[ZEnv, A]

  object ioz extends Http4sDsl[EtlFlowTask]
  import ioz._
  val otherRoutes:HttpRoutes[EtlFlowTask] = HttpRoutes.of[EtlFlowTask] {
    case _@GET -> Root => Ok(s"Hello, Welcome to EtlFlow API ${BI.version}, Build with scala version ${BI.scalaVersion}")
  }

  override def run(args: List[String]): ZIO[ZEnv, Nothing, Int] = {
    val serverManaged = for {
      _                    <- runDbMigration(credentials).toManaged_
      dbTransactor         <- createDbTransactorManagedJDBC(credentials, platform.executor.asEC, "EtlFlowScheduler-Pool")
      cronJobs             <- Ref.make(List.empty[CronJob]).toManaged_
      dbCronJobs           <- getCronJobsDB(dbTransactor).toManaged_
      _                    <- cronJobs.update(_ => dbCronJobs).toManaged_
      etlFlowInterpreter   <- etlFlowHttp4sInterpreter(dbTransactor,cronJobs).toManaged_
      server               <- BlazeServerBuilder[EtlFlowTask]
                                 .bindHttp(8080, "0.0.0.0")
                                 .withConnectorPoolSize(2)
                                 .withResponseHeaderTimeout(55.seconds)
                                 .withIdleTimeout(60.seconds)
                                 .withExecutionContext(platform.executor.asEC)
                                 .withHttpApp(
                                   Router[EtlFlowTask](
                                     "/"    -> otherRoutes,
                                     "/api/etlflow"    -> CORS(Http4sAdapter.makeHttpService(etlFlowInterpreter)),
                                     "/ws/etlflow"     -> Http4sAdapter.makeWebSocketService(etlFlowInterpreter),
                                   ).orNotFound
                                 )
                                 .resource
                                 .toManaged

   } yield server
  serverManaged.useForever.as(0).catchAll(err => putStrLn(err.toString).as(1))
  }
}
