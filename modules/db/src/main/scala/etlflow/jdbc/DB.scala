package etlflow.jdbc

import caliban.CalibanError.ExecutionError
import cron4s.Cron
import cron4s.lib.javatime._
import cats.free.Free
import doobie.free.connection
import doobie.implicits._
import etlflow.jdbc.SQL.getTimestampAsString
import etlflow.schema._
import org.slf4j.{Logger, LoggerFactory}
import zio.interop.catz._
import zio.{IO, RIO, Task, UIO, ZIO, ZLayer}

object DB {
  // Uncomment this to see generated SQL queries in logs
  // implicit val dbLogger = DBLogger()
  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  trait Service {
    def getUser(user_name: String): IO[ExecutionError, UserDB]

    def getJob(name: String): IO[ExecutionError, JobDB]

    def getJobs(args: Free[connection.ConnectionOp, List[Job]]): Task[List[Job]]

    def getStepRuns(args: DbStepRunArgs): IO[ExecutionError, List[StepRun]]

    def getJobRuns(args: DbJobRunArgs): IO[ExecutionError, List[JobRun]]

    def getJobLogs(args: JobLogsArgs): IO[ExecutionError, List[JobLogs]]

    def getCredentials: IO[ExecutionError, List[GetCredential]]

    def updateSuccessJob(job: String, ts: Long): IO[ExecutionError, Long]

    def updateFailedJob(job: String, ts: Long): IO[ExecutionError, Long]

    def updateJobState(args: EtlJobStateArgs): IO[ExecutionError, Boolean]

    def addCredential(credentialsDB: CredentialDB, actualSerializerOutput: JsonString): IO[ExecutionError, Credentials]

    def updateCredential(credentialsDB: CredentialDB, actualSerializerOutput: JsonString): IO[ExecutionError, Credentials]

    def refreshJobs(jobs: List[EtlJob]): IO[ExecutionError, List[JobDB]]

    def updateStepRun(job_run_id: String, step_name: String, props: String, status: String, elapsed_time: String): IO[ExecutionError, Unit]

    def insertStepRun(job_run_id: String, step_name: String, props: String, step_type: String, step_run_id: String, start_time: Long): IO[ExecutionError, Unit]

    def insertJobRun(job_run_id: String, job_name: String, props: String, job_type: String, is_master: String, start_time: Long): IO[ExecutionError, Unit]

    def updateJobRun(job_run_id: String, status: String, elapsed_time: String): IO[ExecutionError, Unit]
  }

  def getUser(user_name: String): ZIO[DBServerEnv, ExecutionError, UserDB] = ZIO.accessM(_.get.getUser(user_name))
  def getJob(name: String): ZIO[DBServerEnv, ExecutionError, JobDB] = ZIO.accessM(_.get.getJob(name))
  def getJobs(args: Free[connection.ConnectionOp, List[Job]]): RIO[DBServerEnv ,List[Job]] = ZIO.accessM(_.get.getJobs(args))
  def getStepRuns(args: DbStepRunArgs): ZIO[DBServerEnv, ExecutionError, List[StepRun]] = ZIO.accessM(_.get.getStepRuns(args))
  def getJobRuns(args: DbJobRunArgs): ZIO[DBServerEnv, ExecutionError, List[JobRun]] = ZIO.accessM(_.get.getJobRuns(args))
  def getJobLogs(args: JobLogsArgs): ZIO[DBServerEnv, ExecutionError, List[JobLogs]] = ZIO.accessM(_.get.getJobLogs(args))
  def getCredentials: ZIO[DBServerEnv, ExecutionError, List[GetCredential]] = ZIO.accessM(_.get.getCredentials)
  def updateSuccessJob(job: String, ts: Long): ZIO[DBServerEnv, ExecutionError, Long] = ZIO.accessM(_.get.updateSuccessJob(job,ts))
  def updateFailedJob(job: String, ts: Long): ZIO[DBServerEnv, ExecutionError, Long] = ZIO.accessM(_.get.updateFailedJob(job, ts))
  def updateJobState(args: EtlJobStateArgs): ZIO[DBServerEnv, ExecutionError, Boolean] = ZIO.accessM(_.get.updateJobState(args))
  def addCredential(credentialsDB: CredentialDB, actualSerializerOutput:JsonString): ZIO[DBServerEnv, ExecutionError, Credentials] = ZIO.accessM(_.get.addCredential(credentialsDB,actualSerializerOutput))
  def updateCredential(credentialsDB: CredentialDB,actualSerializerOutput:JsonString): ZIO[DBServerEnv, ExecutionError, Credentials] = ZIO.accessM(_.get.updateCredential(credentialsDB,actualSerializerOutput))
  def refreshJobs(jobs: List[EtlJob]): ZIO[DBServerEnv, ExecutionError, List[JobDB]] = ZIO.accessM(_.get.refreshJobs(jobs))
  def updateStepRun(job_run_id: String, step_name: String, props: String, status: String, elapsed_time: String): ZIO[DBServerEnv, Throwable, Unit] = ZIO.accessM(_.get.updateStepRun(job_run_id, step_name, props, status, elapsed_time))
  def insertStepRun(job_run_id: String, step_name: String, props: String, step_type: String, step_run_id: String, start_time: Long): ZIO[DBServerEnv, Throwable, Unit] = ZIO.accessM(_.get.insertStepRun(job_run_id, step_name, props, step_type, step_run_id, start_time))
  def insertJobRun(job_run_id: String, job_name: String, props: String, job_type: String, is_master: String, start_time: Long): ZIO[DBServerEnv, Throwable, Unit] = ZIO.accessM(_.get.insertJobRun(job_run_id, job_name, props, job_type, is_master, start_time))
  def updateJobRun(job_run_id: String, status: String, elapsed_time: String): ZIO[DBServerEnv, Throwable, Unit] = ZIO.accessM(_.get.updateJobRun(job_run_id, status, elapsed_time))

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
      def getJobs(args: Free[connection.ConnectionOp, List[Job]]): Task[List[Job]] = {
        args
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
            StepRun(x.job_run_id, x.step_name, x.properties, x.state, getTimestampAsString(x.inserted_at), x.elapsed_time, x.step_type, x.step_run_id)
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
            JobRun(x.job_run_id, x.job_name, x.properties, x.state, getTimestampAsString(x.inserted_at), x.elapsed_time, x.job_type, x.is_master)
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
      def addCredential(credentialsDB: CredentialDB, actualSerializerOutput:JsonString): IO[ExecutionError, Credentials] = {
        SQL.addCredentials(credentialsDB,actualSerializerOutput)
          .run
          .transact(transactor)
          .map(_ => Credentials(credentialsDB.name, credentialsDB.`type`, credentialsDB.value.str))
          .mapError { e =>
            logger.error(e.getMessage)
            ExecutionError(e.getMessage)
          }
      }
      def updateCredential(credentialsDB: CredentialDB,actualSerializerOutput:JsonString): IO[ExecutionError, Credentials] = {
        SQL.updateCredentialSingleTran(credentialsDB,actualSerializerOutput)
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

      def updateStepRun(job_run_id: String, step_name: String, props: String, status: String, elapsed_time: String): IO[ExecutionError, Unit] = {
        SQL
          .updateStepRun(job_run_id, step_name, props, status, elapsed_time)
          .run
          .transact(transactor).unit
          .mapError { e =>
            logger.error(s"failed in logging to db ${e.getMessage}")
            ExecutionError(e.getMessage)
          }
      }

      def insertStepRun(job_run_id: String, step_name: String, props: String, step_type: String, step_run_id: String, start_time: Long): IO[ExecutionError, Unit] = {
        SQL
          .insertStepRun(job_run_id, step_name, props, step_type, step_run_id, start_time)
          .run
          .transact(transactor).unit
          .mapError { e =>
            logger.error(s"failed in logging to db ${e.getMessage}")
            ExecutionError(e.getMessage)
          }
      }

      def insertJobRun(job_run_id: String, job_name: String, props: String, job_type: String, is_master: String, start_time: Long): IO[ExecutionError, Unit] = {
        SQL
          .insertJobRun(job_run_id, job_name, props, job_type, is_master, start_time)
          .run
          .transact(transactor).unit
          .mapError { e =>
            logger.error(s"failed in logging to db ${e.getMessage}")
            ExecutionError(e.getMessage)
          }
      }

      def updateJobRun(job_run_id: String, status: String, elapsed_time: String): IO[ExecutionError, Unit] = {
        SQL
          .updateJobRun(job_run_id, status, elapsed_time)
          .run
          .transact(transactor).unit
          .mapError { e =>
            logger.error(s"failed in logging to db ${e.getMessage}")
            ExecutionError(e.getMessage)
          }
      }
    }
  }
}
