package etlflow.scheduler

import caliban.{CalibanError, GraphQLInterpreter, Http4sAdapter}
import doobie.hikari.HikariTransactor
import etlflow.jdbc.DbManager
import etlflow.scheduler.EtlFlowHelper.{CronJob, EtlJob}
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
import zio.interop.catz.implicits._
import eu.timepit.fs2cron.schedule
import zio.{RIO, ZIO, _}
import zio.duration.Duration
import fs2.Stream
import org.slf4j.{Logger, LoggerFactory}
import scala.concurrent.duration._

private[scheduler] trait GQLServerHttp4s extends CatsApp with DbManager with BootstrapRuntime {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  val credentials: JDBC

  def etlFlowHttp4sInterpreter(transactor: HikariTransactor[Task], cronJobs: Ref[List[CronJob]]): ZIO[Any, CalibanError, GraphQLInterpreter[ZEnv, Throwable]]
  def getCronJobsDB(transactor: HikariTransactor[Task]): Task[List[CronJob]]
  def triggerJob(cronJob: CronJob): Task[EtlJob]

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
       _                   <- (for {
                                dbCronJobs      <- getCronJobsDB(dbTransactor)
                                _               <- Task(logger.info(s"Starting CRON JOB SCHEDULING ${dbCronJobs.toString}"))
                                scheduledJobs   = schedule(dbCronJobs.map(cj => (cj.schedule,Stream.eval(triggerJob(cj)))))
                                 x              <- scheduledJobs.compile.drain.fork
                                //schedule      = Schedule.spaced(Duration.fromScala(60.seconds))
                                updateJob       <- cronJobs.update{_ => dbCronJobs}.fork
                                //_             <- updateJob.repeat(schedule).fork
                              } yield ()).toManaged_
      etlFlowInterpreter   <- etlFlowHttp4sInterpreter(dbTransactor,cronJobs).toManaged_
      server               <- BlazeServerBuilder[EtlFlowTask]
                                 .bindHttp(8080, "0.0.0.0")
                                 .withConnectorPoolSize(10)
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
