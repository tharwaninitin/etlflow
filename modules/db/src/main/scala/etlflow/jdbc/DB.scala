package etlflow.jdbc

import cats.free.Free
import doobie.free.connection
import doobie.implicits._
import doobie.util.Read
import doobie.util.fragment.Fragment
import etlflow.common.DateTimeFunctions.getTimestampAsString
import etlflow.common.EtlflowError.DBException
import org.slf4j.{Logger, LoggerFactory}
import zio.interop.catz._
import zio.{IO, RIO, Task, UIO, ZIO, ZLayer}

private[etlflow] object DB {
  // Uncomment this to see generated SQL queries in logs
  // implicit val dbLogger = DBLogger()
  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  trait Service {
    def getUser(user_name: String): IO[Throwable, UserDB]
    def getJob(name: String): IO[Throwable, JobDB]
    def getJobs(args: Free[connection.ConnectionOp, List[Job]]): Task[List[Job]]
    def getStepRuns(args: DbStepRunArgs): IO[Throwable, List[StepRun]]
    def getJobRuns(args: DbJobRunArgs): IO[Throwable, List[JobRun]]
    def getJobLogs(args: JobLogsArgs): IO[Throwable, List[JobLogs]]
    def getCredentials: IO[Throwable, List[GetCredential]]
    def updateSuccessJob(job: String, ts: Long): IO[Throwable, Long]
    def updateFailedJob(job: String, ts: Long): IO[Throwable, Long]
    def updateJobState(args: EtlJobStateArgs): IO[Throwable, Boolean]
    def addCredential(credentialsDB: CredentialDB, actualSerializerOutput: JsonString): IO[Throwable, Credentials]
    def updateCredential(credentialsDB: CredentialDB, actualSerializerOutput: JsonString): IO[Throwable, Credentials]
    def refreshJobs(jobs: List[EtlJob]): IO[Throwable, List[JobDB]]
    def updateStepRun(job_run_id: String, step_name: String, props: String, status: String, elapsed_time: String): IO[Throwable, Unit]
    def insertStepRun(job_run_id: String, step_name: String, props: String, step_type: String, step_run_id: String, start_time: Long): IO[Throwable, Unit]
    def insertJobRun(job_run_id: String, job_name: String, props: String, job_type: String, is_master: String, start_time: Long): IO[Throwable, Unit]
    def updateJobRun(job_run_id: String, status: String, elapsed_time: String): IO[Throwable, Unit]
    def executeQueryWithResponse[T <: Product : Read](query: String): IO[Throwable, List[T]]
    def executeQuery(query: String): IO[Throwable, Unit]
    def executeQueryWithSingleResponse[T : Read](query: String): IO[Throwable, T]
  }

  def getUser(user_name: String): ZIO[DBEnv, Throwable, UserDB] = ZIO.accessM(_.get.getUser(user_name))
  def getJob(name: String): ZIO[DBEnv, Throwable, JobDB] = ZIO.accessM(_.get.getJob(name))
  def getJobs(args: Free[connection.ConnectionOp, List[Job]]): RIO[DBEnv ,List[Job]] = ZIO.accessM(_.get.getJobs(args))
  def getStepRuns(args: DbStepRunArgs): ZIO[DBEnv, Throwable, List[StepRun]] = ZIO.accessM(_.get.getStepRuns(args))
  def getJobRuns(args: DbJobRunArgs): ZIO[DBEnv, Throwable, List[JobRun]] = ZIO.accessM(_.get.getJobRuns(args))
  def getJobLogs(args: JobLogsArgs): ZIO[DBEnv, Throwable, List[JobLogs]] = ZIO.accessM(_.get.getJobLogs(args))
  def getCredentials: ZIO[DBEnv, Throwable, List[GetCredential]] = ZIO.accessM(_.get.getCredentials)
  def updateSuccessJob(job: String, ts: Long): ZIO[DBEnv, Throwable, Long] = ZIO.accessM(_.get.updateSuccessJob(job,ts))
  def updateFailedJob(job: String, ts: Long): ZIO[DBEnv, Throwable, Long] = ZIO.accessM(_.get.updateFailedJob(job, ts))
  def updateJobState(args: EtlJobStateArgs): ZIO[DBEnv, Throwable, Boolean] = ZIO.accessM(_.get.updateJobState(args))
  def addCredential(credentialsDB: CredentialDB, actualSerializerOutput:JsonString): ZIO[DBEnv, Throwable, Credentials] = ZIO.accessM(_.get.addCredential(credentialsDB,actualSerializerOutput))
  def updateCredential(credentialsDB: CredentialDB,actualSerializerOutput:JsonString): ZIO[DBEnv, Throwable, Credentials] = ZIO.accessM(_.get.updateCredential(credentialsDB,actualSerializerOutput))
  def refreshJobs(jobs: List[EtlJob]): ZIO[DBEnv, Throwable, List[JobDB]] = ZIO.accessM(_.get.refreshJobs(jobs))
  def updateStepRun(job_run_id: String, step_name: String, props: String, status: String, elapsed_time: String): ZIO[DBEnv, Throwable, Unit] = ZIO.accessM(_.get.updateStepRun(job_run_id, step_name, props, status, elapsed_time))
  def insertStepRun(job_run_id: String, step_name: String, props: String, step_type: String, step_run_id: String, start_time: Long): ZIO[DBEnv, Throwable, Unit] = ZIO.accessM(_.get.insertStepRun(job_run_id, step_name, props, step_type, step_run_id, start_time))
  def insertJobRun(job_run_id: String, job_name: String, props: String, job_type: String, is_master: String, start_time: Long): ZIO[DBEnv, Throwable, Unit] = ZIO.accessM(_.get.insertJobRun(job_run_id, job_name, props, job_type, is_master, start_time))
  def updateJobRun(job_run_id: String, status: String, elapsed_time: String): ZIO[DBEnv, Throwable, Unit] = ZIO.accessM(_.get.updateJobRun(job_run_id, status, elapsed_time))
  def executeQueryWithResponse[T <: Product : Read](query: String): ZIO[DBEnv, Throwable, List[T]] = ZIO.accessM(_.get.executeQueryWithResponse(query))
  def executeQuery(query: String): ZIO[DBEnv, Throwable, Unit] = ZIO.accessM(_.get.executeQuery(query))
  def executeQueryWithSingleResponse[T : Read](query: String):ZIO[DBEnv, Throwable, T] = ZIO.accessM(_.get.executeQueryWithSingleResponse(query))

  val liveDB: ZLayer[TransactorEnv, Throwable, DBEnv] = ZLayer.fromService { transactor =>
    new Service {
      def getUser(name: String): IO[Throwable, UserDB] = {
        SQL.getUser(name)
          .unique
          .transact(transactor)
          .mapError { e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
          }
      }
      def getJob(name: String): IO[Throwable, JobDB] = {
        SQL.getJob(name)
          .unique
          .transact(transactor)
          .mapError { e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
          }
      }
      def getJobs(args: Free[connection.ConnectionOp, List[Job]]): Task[List[Job]] = {
        args
          .transact(transactor)
          .mapError { e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
          }
      }
      def getStepRuns(args: DbStepRunArgs): IO[Throwable, List[StepRun]] = {
        SQL.getStepRuns(args)
          .to[List]
          .map(y => y.map { x => {
            StepRun(x.job_run_id, x.step_name, x.properties, x.state, getTimestampAsString(x.inserted_at), x.elapsed_time, x.step_type, x.step_run_id)
          }
          })
          .transact(transactor)
          .mapError { e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
          }
      }
      def getJobRuns(args: DbJobRunArgs): IO[Throwable, List[JobRun]] = {
        SQL.getJobRuns(args)
          .to[List]
          .map(y => y.map { x => {
            JobRun(x.job_run_id, x.job_name, x.properties, x.state, getTimestampAsString(x.inserted_at), x.elapsed_time, x.job_type, x.is_master)
          }
          })
          .transact(transactor)
          .mapError { e =>
            logger.error(s"Exception ${e.getMessage} occurred for arguments $args")
            DBException(e.getMessage)
          }
      }
      def getJobLogs(args: JobLogsArgs): IO[Throwable, List[JobLogs]] = {
        SQL.getJobLogs(args)
          .to[List]
          .transact(transactor)
          .mapError { e =>
            logger.error(s"Exception ${e.getMessage} occurred for arguments $args")
            DBException(e.getMessage)
          }
      }
      def getCredentials: IO[Throwable, List[GetCredential]] = {
        SQL.getCredentials
          .to[List]
          .transact(transactor)
          .mapError { e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
          }
      }
      def updateSuccessJob(job: String, ts: Long): IO[Throwable, Long] = {
        SQL.updateSuccessJob(job, ts)
          .run
          .transact(transactor)
          .map(_ => 1L)
          .mapError { e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
          }
      }
      def updateFailedJob(job: String, ts: Long): IO[Throwable, Long] = {
        SQL.updateFailedJob(job, ts)
          .run
          .transact(transactor)
          .map(_ => 1L)
          .mapError { e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
          }
      }
      def updateJobState(args: EtlJobStateArgs): IO[Throwable, Boolean] = {
        SQL.updateJobState(args)
          .run
          .transact(transactor)
          .map(_ => args.state)
          .mapError { e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
          }
      }
      def addCredential(credentialsDB: CredentialDB, actualSerializerOutput:JsonString): IO[Throwable, Credentials] = {
        try {
          SQL.addCredentials(credentialsDB,actualSerializerOutput)
            .run
            .transact(transactor)
            .map(_ => Credentials(credentialsDB.name, credentialsDB.`type`, credentialsDB.value.str))
        } catch {
          case  ex =>
            logger.error(ex.getMessage)
            throw new RuntimeException(ex.getMessage)
        }
      }
      def updateCredential(credentialsDB: CredentialDB,actualSerializerOutput:JsonString): IO[Throwable, Credentials] = {
        SQL.updateCredentialSingleTran(credentialsDB,actualSerializerOutput)
          .transact(transactor)
          .map(_ => Credentials(credentialsDB.name,credentialsDB.`type`,credentialsDB.value.str))
        }.mapError { e =>
          logger.error(e.getMessage)
          DBException(e.getMessage)
      }
      def refreshJobs(jobs: List[EtlJob]): IO[Throwable, List[JobDB]] = {
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
            DBException(e.getMessage)
          }
      }

      def updateStepRun(job_run_id: String, step_name: String, props: String, status: String, elapsed_time: String): IO[Throwable, Unit] = {
        SQL
          .updateStepRun(job_run_id, step_name, props, status, elapsed_time)
          .run
          .transact(transactor).unit
          .mapError { e =>
            logger.error(s"failed in logging to db ${e.getMessage}")
            DBException(e.getMessage)
          }
      }

      def insertStepRun(job_run_id: String, step_name: String, props: String, step_type: String, step_run_id: String, start_time: Long):
      IO[Throwable, Unit] = {
        SQL
          .insertStepRun(job_run_id, step_name, props, step_type, step_run_id, start_time)
          .run
          .transact(transactor).unit
          .mapError { e =>
            logger.error(s"failed in logging to db ${e.getMessage}")
            DBException(e.getMessage)
          }
      }

      def insertJobRun(job_run_id: String, job_name: String, props: String, job_type: String, is_master: String, start_time: Long):
      IO[Throwable, Unit] = {
        SQL
          .insertJobRun(job_run_id, job_name, props, job_type, is_master, start_time)
          .run
          .transact(transactor).unit
          .mapError { e =>
            logger.error(s"failed in logging to db ${e.getMessage}")
            DBException(e.getMessage)
          }
      }

      def updateJobRun(job_run_id: String, status: String, elapsed_time: String): IO[Throwable, Unit] = {
        SQL
          .updateJobRun(job_run_id, status, elapsed_time)
          .run
          .transact(transactor).unit
          .mapError { e =>
            logger.error(s"failed in logging to db ${e.getMessage}")
            DBException(e.getMessage)
          }
      }

      def executeQueryWithResponse[T <: Product : Read](query: String): IO[Throwable, List[T]] = {
        Fragment.const(query)
          .query[T]
          .to[List]
          .transact(transactor)
          .mapError { e =>
            logger.error(s"failed in logging to db ${e.getMessage}")
            DBException(e.getMessage)
          }
      }

      override def executeQuery(query: String): IO[Throwable, Unit] = {
        Fragment.const(query)
          .update
          .run
          .transact(transactor).unit
          .mapError { e =>
            logger.error(s"failed in logging to db ${e.getMessage}")
            DBException(e.getMessage)
          }
      }

      override def executeQueryWithSingleResponse[T: Read](query: String): IO[Throwable, T] = {
        Fragment.const(query)
          .query[T]
          .unique
          .transact(transactor)
          .mapError { e =>
            logger.error(s"failed in logging to db ${e.getMessage}")
            DBException(e.getMessage)
          }
      }
    }
  }
}
