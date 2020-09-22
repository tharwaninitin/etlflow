package etlflow.webserver

import caliban.CalibanError.ExecutionError
import cron4s.Cron
import doobie.hikari.HikariTransactor
import etlflow.jdbc.DbManager
import etlflow.log.{JobRun, StepRun}
import etlflow.utils.EtlFlowHelper
import etlflow.utils.EtlFlowHelper._
import etlflow.utils.db.Query
import org.slf4j.{Logger, LoggerFactory}
import scalacache.Cache
import zio._
import zio.stream.ZStream

trait TestSchedulerApp extends DbManager with TestSuiteHelper {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  case class UserInfo(user_name: String, password: String, user_active: String)
  case class CronJobDB(job_name: String, schedule: String, failed: Long, success: Long, is_active: Boolean)
  case class CredentialDB(name: String, `type`: String, value: String)

  def testHttp4s(transactor: HikariTransactor[Task], cache: Cache[String]) : ZLayer[Any, Throwable, EtlFlowHas] = ZLayer.fromEffect {
    for {
      _  <- runDbMigration(credentials)
    } yield new EtlFlow.Service() {

        override def updateJobState(args: EtlFlowHelper.EtlJobStateArgs): ZIO[EtlFlowHas, Throwable, Boolean] = {
          Query.updateJobState(args,transactor)
        }

        override def login(args: EtlFlowHelper.UserArgs): ZIO[EtlFlowHas, Throwable, EtlFlowHelper.UserAuth] = {
          Query.login(args,transactor,cache)
        }

        override def addCronJob(args: EtlFlowHelper.CronJobArgs): ZIO[EtlFlowHas, Throwable, EtlFlowHelper.CronJob] = {
          Query.addCronJob(args,transactor)
        }

        override def updateCronJob(args: EtlFlowHelper.CronJobArgs): ZIO[EtlFlowHas, Throwable, EtlFlowHelper.CronJob] = {
          Query.updateCronJob(args,transactor)
        }

        override def getJobs: ZIO[EtlFlowHas, Throwable, List[Job]] = {
          Query.getJobs(transactor)
            .map(y => y.map{x =>
              Job(x.job_name, Map.empty, Cron(x.schedule).toOption,"","", x.failed, x.success, x.is_active)
            })
          }.mapError{ e =>
            logger.error(e.getMessage)
            ExecutionError(e.getMessage)
          }

        override def getDbJobRuns(args: EtlFlowHelper.DbJobRunArgs): ZIO[EtlFlowHas, Throwable, List[JobRun]] = {
          Query.getDbJobRuns(args,transactor)
        }

        override def addCredentials(args: EtlFlowHelper.CredentialsArgs): ZIO[EtlFlowHas, Throwable, EtlFlowHelper.Credentials] = {
          Query.addCredentials(args,transactor)
        }

        override def updateCredentials(args: EtlFlowHelper.CredentialsArgs): ZIO[EtlFlowHas, Throwable, EtlFlowHelper.Credentials] = {
          Query.updateCredentials(args,transactor)
        }

        override def getDbStepRuns(args: DbStepRunArgs): ZIO[EtlFlowHas, Throwable, List[StepRun]] = {
          Query.getDbStepRuns(args,transactor)
        }

        override def runJob(args: EtlFlowHelper.EtlJobArgs): ZIO[EtlFlowHas, Throwable, EtlFlowHelper.EtlJob] = ???
        override def getInfo: ZIO[EtlFlowHas, Throwable, EtlFlowHelper.EtlFlowMetrics] = ???
        override def notifications: ZStream[EtlFlowHas, Nothing, EtlFlowHelper.EtlJobStatus] = ???
        override def getCurrentTime: ZIO[EtlFlowHas, Throwable, CurrentTime] = ???
      }
    }
}
