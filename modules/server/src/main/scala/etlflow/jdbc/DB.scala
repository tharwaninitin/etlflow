package etlflow.jdbc

import caliban.CalibanError.ExecutionError
import cron4s.Cron
import cron4s.lib.javatime._
import doobie.implicits._
import etlflow.EJPMType
import etlflow.api.Schema.Creds.{AWS, JDBC}
import etlflow.api.Schema._
import etlflow.utils.{EtlFlowUtils, JsonJackson, UtilityFunctions => UF}
import org.ocpsoft.prettytime.PrettyTime
import zio.interop.catz._
import zio.{IO, RIO, Task, UIO, ZIO, ZLayer}
import java.time.LocalDateTime
import scala.reflect.runtime.universe.TypeTag
import etlflow.DBEnv

object DB extends EtlFlowUtils {
  // Uncomment this to see generated SQL queries in logs
  // implicit val dbLogger = DBLogger()

  trait Service {
    def getUser(user_name: String): IO[ExecutionError, UserDB]
    def getJob(name: String): IO[ExecutionError, JobDB]
    def getJobs[EJN <: EJPMType : TypeTag](ejpm_package: String): Task[List[Job]]
    def getStepRuns(args: DbStepRunArgs): IO[ExecutionError, List[StepRun]]
    def getJobRuns(args: DbJobRunArgs): IO[ExecutionError, List[JobRun]]
    def getJobLogs(args: JobLogsArgs): IO[ExecutionError, List[JobLogs]]
    def getCredentials: IO[ExecutionError, List[GetCredential]]
    def updateSuccessJob(job: String, ts: Long): IO[ExecutionError, Long]
    def updateFailedJob(job: String, ts: Long): IO[ExecutionError, Long]
    def updateJobState(args: EtlJobStateArgs): IO[ExecutionError, Boolean]
    def addCredential(args: CredentialsArgs): IO[ExecutionError, Credentials]
    def updateCredential(args: CredentialsArgs): IO[ExecutionError, Credentials]
    def refreshJobs(jobs: List[EtlJob]): IO[ExecutionError, List[JobDB]]
  }

  def getUser(user_name: String): ZIO[DBServerEnv, ExecutionError, UserDB] = ZIO.accessM(_.get.getUser(user_name))
  def getJob(name: String): ZIO[DBServerEnv, ExecutionError, JobDB] = ZIO.accessM(_.get.getJob(name))
  def getJobs[EJN <: EJPMType : TypeTag](ejpm_package: String): RIO[DBServerEnv ,List[Job]] = ZIO.accessM(_.get.getJobs[EJN](ejpm_package))
  def getStepRuns(args: DbStepRunArgs): ZIO[DBServerEnv, ExecutionError, List[StepRun]] = ZIO.accessM(_.get.getStepRuns(args))
  def getJobRuns(args: DbJobRunArgs): ZIO[DBServerEnv, ExecutionError, List[JobRun]] = ZIO.accessM(_.get.getJobRuns(args))
  def getJobLogs(args: JobLogsArgs): ZIO[DBServerEnv, ExecutionError, List[JobLogs]] = ZIO.accessM(_.get.getJobLogs(args))
  def getCredentials: ZIO[DBServerEnv, ExecutionError, List[GetCredential]] = ZIO.accessM(_.get.getCredentials)
  def updateSuccessJob(job: String, ts: Long): ZIO[DBServerEnv, ExecutionError, Long] = ZIO.accessM(_.get.updateSuccessJob(job,ts))
  def updateFailedJob(job: String, ts: Long): ZIO[DBServerEnv, ExecutionError, Long] = ZIO.accessM(_.get.updateFailedJob(job, ts))
  def updateJobState(args: EtlJobStateArgs): ZIO[DBServerEnv, ExecutionError, Boolean] = ZIO.accessM(_.get.updateJobState(args))
  def addCredential(args: CredentialsArgs): ZIO[DBServerEnv, ExecutionError, Credentials] = ZIO.accessM(_.get.addCredential(args))
  def updateCredential(args: CredentialsArgs): ZIO[DBServerEnv, ExecutionError, Credentials] = ZIO.accessM(_.get.updateCredential(args))
  def refreshJobs(jobs: List[EtlJob]): ZIO[DBServerEnv, ExecutionError, List[JobDB]] = ZIO.accessM(_.get.refreshJobs(jobs))

  val liveDB: ZLayer[DBEnv, Throwable, DBServerEnv] = ZLayer.fromService { transactor =>
    new Service {
      def getUser(name: String): IO[ExecutionError, UserDB] = {
        SQL.getUser(name)
          .unique
          .transact(transactor)
          .mapError { e =>
            logger.error(e.getMessage)
            ExecutionError(e.getMessage)
          }
      }
      def getJob(name: String): IO[ExecutionError, JobDB] = {
        SQL.getJob(name)
          .unique
          .transact(transactor)
          .mapError { e =>
            logger.error(e.getMessage)
            ExecutionError(e.getMessage)
          }
      }
      def getJobs[EJN <: EJPMType : TypeTag](ejpm_package: String): Task[List[Job]] = {
        SQL.getJobs
          .to[List]
          .map(y => y.map { x => {
            val props = getJobPropsMapping[EJN](x.job_name, ejpm_package)
            val p = new PrettyTime()
            val lastRunTime = x.last_run_time.map(ts => p.format(UF.getLocalDateTimeFromTimestamp(ts))).getOrElse("")

            if (Cron(x.schedule).toOption.isDefined) {
              val cron = Cron(x.schedule).toOption
              val startTimeMillis: Long = UF.getCurrentTimestampUsingLocalDateTime
              val endTimeMillis: Option[Long] = cron.get.next(LocalDateTime.now()).map(dt => UF.getTimestampFromLocalDateTime(dt))
              val remTime1 = endTimeMillis.map(ts => UF.getTimeDifferenceAsString(startTimeMillis, ts)).getOrElse("")
              val remTime2 = endTimeMillis.map(ts => p.format(UF.getLocalDateTimeFromTimestamp(ts))).getOrElse("")

              val nextScheduleTime = cron.get.next(LocalDateTime.now()).getOrElse("").toString
              Job(x.job_name, props, cron, nextScheduleTime, s"$remTime2 ($remTime1)", x.failed, x.success, x.is_active, x.last_run_time.getOrElse(0), s"$lastRunTime")
            } else {
              Job(x.job_name, props, None, "", "", x.failed, x.success, x.is_active, x.last_run_time.getOrElse(0), s"$lastRunTime")
            }
          }
          })
          .transact(transactor)
          .mapError { e =>
            logger.error(e.getMessage)
            ExecutionError(e.getMessage)
          }
      }
      def getStepRuns(args: DbStepRunArgs): IO[ExecutionError, List[StepRun]] = {
        SQL.getStepRuns(args)
          .to[List]
          .map(y => y.map { x => {
            StepRun(x.job_run_id, x.step_name, x.properties, x.state, UF.getTimestampAsString(x.inserted_at), x.elapsed_time, x.step_type, x.step_run_id)
          }
          })
          .transact(transactor)
          .mapError { e =>
            logger.error(e.getMessage)
            ExecutionError(e.getMessage)
          }
      }
      def getJobRuns(args: DbJobRunArgs): IO[ExecutionError, List[JobRun]] = {
        SQL.getJobRuns(args)
          .to[List]
          .map(y => y.map { x => {
            JobRun(x.job_run_id, x.job_name, x.properties, x.state, UF.getTimestampAsString(x.inserted_at), x.elapsed_time, x.job_type, x.is_master)
          }
          })
          .transact(transactor)
          .mapError { e =>
            logger.error(s"Exception ${e.getMessage} occurred for arguments $args")
            ExecutionError(e.getMessage)
          }
      }
      def getJobLogs(args: JobLogsArgs): IO[ExecutionError, List[JobLogs]] = {
        SQL.getJobLogs(args)
          .to[List]
          .transact(transactor)
          .mapError { e =>
            logger.error(s"Exception ${e.getMessage} occurred for arguments $args")
            ExecutionError(e.getMessage)
          }
      }
      def getCredentials: IO[ExecutionError, List[GetCredential]] = {
        SQL.getCredentials
          .to[List]
          .transact(transactor)
          .mapError { e =>
            logger.error(e.getMessage)
            ExecutionError(e.getMessage)
          }
      }
      def updateSuccessJob(job: String, ts: Long): IO[ExecutionError, Long] = {
        SQL.updateSuccessJob(job, ts)
          .run
          .transact(transactor)
          .map(_ => 1L)
          .mapError { e =>
            logger.error(e.getMessage)
            ExecutionError(e.getMessage)
          }
      }
      def updateFailedJob(job: String, ts: Long): IO[ExecutionError, Long] = {
        SQL.updateFailedJob(job, ts)
          .run
          .transact(transactor)
          .map(_ => 1L)
          .mapError { e =>
            logger.error(e.getMessage)
            ExecutionError(e.getMessage)
          }
      }
      def updateJobState(args: EtlJobStateArgs): IO[ExecutionError, Boolean] = {
        SQL.updateJobState(args)
          .run
          .transact(transactor)
          .map(_ => args.state)
          .mapError { e =>
            logger.error(e.getMessage)
            ExecutionError(e.getMessage)
          }
      }
      def addCredential(args: CredentialsArgs): IO[ExecutionError, Credentials] = {
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
      def updateCredential(args: CredentialsArgs): IO[ExecutionError, Credentials] = {
        val value = JsonString(JsonJackson.convertToJsonByRemovingKeys(args.value.map(x => (x.key,x.value)).toMap, List.empty))
        val credentialsDB = CredentialDB(
          args.name,
          args.`type` match {
            case JDBC => "jdbc"
            case AWS => "aws"
          },
          value
        )
        SQL.updateCredentialSingleTran(credentialsDB)
          .transact(transactor)
          .map(_ => Credentials(credentialsDB.name,credentialsDB.`type`,credentialsDB.value.str))
      }.mapError { e =>
        logger.error(e.getMessage)
        ExecutionError(e.getMessage)
      }
      def refreshJobs(jobs: List[EtlJob]): IO[ExecutionError, List[JobDB]] = {
        val jobsDB = jobs.map{x =>
          JobDB(x.name, x.props.getOrElse("job_schedule",""), is_active = true)
        }

        if (jobsDB.isEmpty)
          UIO{List.empty}
        else
          SQL.refreshJobsSingleTran(jobsDB)
          .transact(transactor)
          .mapError { e =>
            logger.error(e.getMessage)
            ExecutionError(e.getMessage)
          }
      }
    }
  }
}
