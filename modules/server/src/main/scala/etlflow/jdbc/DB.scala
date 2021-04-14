package etlflow.jdbc

import java.time.LocalDateTime
import caliban.CalibanError.ExecutionError
import cron4s.lib.javatime._
import etlflow.etljobs.{EtlJob => CoreEtlJob}
import etlflow.utils.{UtilityFunctions => UF}
import cron4s.Cron
import doobie.implicits._
import etlflow.{EtlJobProps, EtlJobPropsMapping}
import etlflow.log.{JobRun, StepRun}
import etlflow.utils.EtlFlowHelper.Creds.{AWS, JDBC}
import etlflow.utils.EtlFlowHelper._
import etlflow.utils.{EtlFlowUtils, JsonJackson}
import org.ocpsoft.prettytime.PrettyTime
import zio.interop.catz._
import zio.{IO, RIO, Task, UIO, ZIO, ZLayer}
import scala.reflect.runtime.universe.TypeTag

object DB extends EtlFlowUtils {
  // Uncomment this to see generated SQL queries in logs
  // implicit val dbLogger = DBLogger()

  trait Service {
    def getUser(user_name: String): IO[ExecutionError, UserInfo]
    def getJob(name: String): IO[ExecutionError, JobDB]
    def getJobs: IO[ExecutionError, List[JobDB1]]
    def getStepRuns(args: DbStepRunArgs): IO[ExecutionError, List[StepRun]]
    def getJobRuns(args: DbJobRunArgs): IO[ExecutionError, List[JobRun]]
    def getJobLogs(args: JobLogsArgs): IO[ExecutionError, List[JobLogs]]
    def getCredentials: IO[ExecutionError, List[UpdateCredentialDB]]
    def updateSuccessJob(job: String, ts: Long): IO[ExecutionError, Long]
    def updateFailedJob(job: String, ts: Long): IO[ExecutionError, Long]
    def updateJobState(args: EtlJobStateArgs): IO[ExecutionError, Boolean]
    def addCredentials(args: CredentialsArgs): IO[ExecutionError, Credentials]
    def updateCredentials(args: CredentialsArgs): IO[ExecutionError, Credentials]
    def refreshJobs(jobs: List[EtlJob]): IO[ExecutionError, List[CronJob]]
    def getJobsFromDb[EJN <: EtlJobPropsMapping[EtlJobProps,CoreEtlJob[EtlJobProps]] : TypeTag](etl_job_name_package: String): Task[List[Job]]
  }

  def getUser(user_name: String): ZIO[DBEnv, ExecutionError, UserInfo] = ZIO.accessM(_.get.getUser(user_name))
  def getJob(name: String): ZIO[DBEnv, ExecutionError, JobDB] = ZIO.accessM(_.get.getJob(name))
  def getJobs: ZIO[DBEnv, ExecutionError, List[JobDB1]] = ZIO.accessM(_.get.getJobs)
  def getStepRuns(args: DbStepRunArgs): ZIO[DBEnv, ExecutionError, List[StepRun]] = ZIO.accessM(_.get.getStepRuns(args))
  def getJobRuns(args: DbJobRunArgs): ZIO[DBEnv, ExecutionError, List[JobRun]] = ZIO.accessM(_.get.getJobRuns(args))
  def getJobLogs(args: JobLogsArgs): ZIO[DBEnv, ExecutionError, List[JobLogs]] = ZIO.accessM(_.get.getJobLogs(args))
  def getCredentials: ZIO[DBEnv, ExecutionError, List[UpdateCredentialDB]] = ZIO.accessM(_.get.getCredentials)
  def updateSuccessJob(job: String, ts: Long): ZIO[DBEnv, ExecutionError, Long] = ZIO.accessM(_.get.updateSuccessJob(job,ts))
  def updateFailedJob(job: String, ts: Long): ZIO[DBEnv, ExecutionError, Long] = ZIO.accessM(_.get.updateFailedJob(job, ts))
  def updateJobState(args: EtlJobStateArgs): ZIO[DBEnv, ExecutionError, Boolean] = ZIO.accessM(_.get.updateJobState(args))
  def addCredentials(args: CredentialsArgs): ZIO[DBEnv, ExecutionError, Credentials] = ZIO.accessM(_.get.addCredentials(args))
  def updateCredentials(args: CredentialsArgs): ZIO[DBEnv, ExecutionError, Credentials] = ZIO.accessM(_.get.updateCredentials(args))
  def refreshJobs(jobs: List[EtlJob]): ZIO[DBEnv, ExecutionError, List[CronJob]] = ZIO.accessM(_.get.refreshJobs(jobs))
  def getJobsFromDb[EJN <: EtlJobPropsMapping[EtlJobProps,CoreEtlJob[EtlJobProps]] : TypeTag](etl_job_name_package: String): RIO[DBEnv ,List[Job]] =
    ZIO.accessM(_.get.getJobsFromDb[EJN](etl_job_name_package))

  val liveDB: ZLayer[TransactorEnv, Throwable, DBEnv] = ZLayer.fromService { transactor =>
    new Service {
      def getUser(name: String): IO[ExecutionError, UserInfo] = {
        SQL.getUser(name)
          .transact(transactor)
          .mapError { e =>
            logger.error(e.getMessage)
            ExecutionError(e.getMessage)
          }
      }
      def getJob(name: String): IO[ExecutionError, JobDB] = {
        SQL.getJob(name)
          .transact(transactor)
          .mapError { e =>
            logger.error(e.getMessage)
            ExecutionError(e.getMessage)
          }
      }
      def getJobs: IO[ExecutionError, List[JobDB1]] = {
        SQL.getJobs
          .transact(transactor)
          .mapError { e =>
            logger.error(e.getMessage)
            ExecutionError(e.getMessage)
          }
      }
      def getStepRuns(args: DbStepRunArgs): IO[ExecutionError, List[StepRun]] = {
        SQL.getStepRuns(args)
          .transact(transactor)
          .mapError { e =>
            logger.error(e.getMessage)
            ExecutionError(e.getMessage)
          }
      }
      def getJobRuns(args: DbJobRunArgs): IO[ExecutionError, List[JobRun]] = {
        SQL.getJobRuns(args)
          .transact(transactor)
          .mapError { e =>
            logger.error(s"Exception ${e.getMessage} occurred for arguments $args")
            ExecutionError(e.getMessage)
          }
      }
      def getJobLogs(args: JobLogsArgs): IO[ExecutionError, List[JobLogs]] = {
        SQL.getJobLogs(args)
          .transact(transactor)
          .mapError { e =>
            logger.error(s"Exception ${e.getMessage} occurred for arguments $args")
            ExecutionError(e.getMessage)
          }
      }
      def getCredentials: IO[ExecutionError, List[UpdateCredentialDB]] = {
        SQL.getCredentials
          .transact(transactor)
          .mapError { e =>
            logger.error(e.getMessage)
            ExecutionError(e.getMessage)
          }
      }
      def updateSuccessJob(job: String, ts: Long): IO[ExecutionError, Long] = {
        SQL.updateSuccessJob(job, ts)
          .transact(transactor)
          .map(_ => 1L)
          .mapError { e =>
            logger.error(e.getMessage)
            ExecutionError(e.getMessage)
          }
      }
      def updateFailedJob(job: String, ts: Long): IO[ExecutionError, Long] = {
        SQL.updateFailedJob(job, ts)
          .transact(transactor)
          .map(_ => 1L)
          .mapError { e =>
            logger.error(e.getMessage)
            ExecutionError(e.getMessage)
          }
      }
      def updateJobState(args: EtlJobStateArgs): IO[ExecutionError, Boolean] = {
        SQL.updateJobState(args)
          .transact(transactor)
          .map(_ => args.state)
          .mapError { e =>
            logger.error(e.getMessage)
            ExecutionError(e.getMessage)
          }
      }
      def addCredentials(args: CredentialsArgs): IO[ExecutionError, Credentials] = {
        val value = JsonString(JsonJackson.convertToJsonByRemovingKeys(args.value.map(x => (x.key, x.value)).toMap, List.empty))
        val credentialsDB = CredentialDB(
          args.name,
          args.`type` match {
            case JDBC => "jdbc"
            case AWS => "aws"
          },
          value
        )
        SQL.addCredentials(credentialsDB)
          .run
          .transact(transactor)
          .map(_ => Credentials(credentialsDB.name, credentialsDB.`type`, credentialsDB.value.str))
          .mapError { e =>
            logger.error(e.getMessage)
            ExecutionError(e.getMessage)
          }
      }
      def updateCredentials(args: CredentialsArgs): IO[ExecutionError, Credentials] = {
        val value = JsonString(JsonJackson.convertToJsonByRemovingKeys(args.value.map(x => (x.key,x.value)).toMap, List.empty))
        val credentialsDB = CredentialDB(
          args.name,
          args.`type` match {
            case JDBC => "jdbc"
            case AWS => "aws"
          },
          value
        )
        SQL.updateCredentials(credentialsDB)
          .transact(transactor)
          .map(_ => Credentials(credentialsDB.name,credentialsDB.`type`,credentialsDB.value.str))
      }.mapError { e =>
        logger.error(e.getMessage)
        ExecutionError(e.getMessage)
      }
      def refreshJobs(jobs: List[EtlJob]): IO[ExecutionError, List[CronJob]] = {
        val jobsDB = jobs.map{x =>
          JobDB(
            x.name,
            job_description =  "",
            x.props.getOrElse("job_schedule",""),
            0,
            0,
            is_active = true
          )
        }
        val singleTran = for {
          _        <- SQL.logCIO(s"Refreshing jobs in database")
          deleted  <- SQL.deleteJobs(jobsDB)
          _        <- SQL.logCIO(s"Deleted jobs => $deleted")
          inserted <- SQL.insertJobs(jobsDB)
          _        <- SQL.logCIO(s"Inserted/Updated jobs => $inserted")
          db_jobs  <- SQL.selectJobs
          jobs     = db_jobs.map(x => CronJob(x.job_name,x.job_description, Cron(x.schedule).toOption, x.failed, x.success))
        } yield jobs
        if (jobsDB.isEmpty)
          UIO{List.empty}
        else
          singleTran
          .transact(transactor)
          .mapError { e =>
            logger.error(e.getMessage)
            ExecutionError(e.getMessage)
          }
      }
      def getJobsFromDb[EJN <: EtlJobPropsMapping[EtlJobProps,CoreEtlJob[EtlJobProps]] : TypeTag](etl_job_name_package: String): Task[List[Job]] = {
        getJobs
          .map(y => y.map{x => {
            val props = getJobPropsMapping[EJN](x.job_name,etl_job_name_package)
            val p = new PrettyTime()
            val lastRunTime = x.last_run_time.map(ts => p.format(UF.getLocalDateTimeFromTimestamp(ts))).getOrElse("")

            if(Cron(x.schedule).toOption.isDefined) {
              val cron = Cron(x.schedule).toOption
              val startTimeMillis: Long =  UF.getCurrentTimestampUsingLocalDateTime
              val endTimeMillis: Option[Long] = cron.get.next(LocalDateTime.now()).map(dt => UF.getTimestampFromLocalDateTime(dt))
              val remTime1 = endTimeMillis.map(ts => UF.getTimeDifferenceAsString(startTimeMillis,ts)).getOrElse("")
              val remTime2 = endTimeMillis.map(ts => p.format(UF.getLocalDateTimeFromTimestamp(ts))).getOrElse("")

              val nextScheduleTime = cron.get.next(LocalDateTime.now()).getOrElse("").toString
              Job(x.job_name, props, cron, nextScheduleTime, s"$remTime2 ($remTime1)", x.failed, x.success, x.is_active,props("job_max_active_runs").toInt, props("job_deploy_mode"),x.last_run_time.getOrElse(0),s"$lastRunTime")
            }else{
              Job(x.job_name, props, None, "", "", x.failed, x.success, x.is_active,props("job_max_active_runs").toInt, props("job_deploy_mode"),x.last_run_time.getOrElse(0),s"$lastRunTime")
            }
          }})
      }.mapError{ e =>
        logger.error(e.getMessage)
        ExecutionError(e.getMessage)
      }
    }
  }
}
