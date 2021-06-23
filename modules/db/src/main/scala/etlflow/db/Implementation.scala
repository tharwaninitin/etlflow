package etlflow.db

import doobie.implicits._
import doobie.util.Read
import doobie.util.fragment.Fragment
import etlflow.common.DateTimeFunctions.getTimestampAsString
import etlflow.common.EtlflowError.DBException
import etlflow.db.DBApi.Service
import org.slf4j.{Logger, LoggerFactory}
import zio.interop.catz._
import zio.{IO, Task, UIO, ZLayer}

private[db] object Implementation {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

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
      def getJobs: Task[List[JobDBAll]] = {
        SQL.getJobs
          .to[List]
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
        SQL.addCredentials(credentialsDB,actualSerializerOutput)
          .run
          .transact(transactor)
          .map(_ => Credentials(credentialsDB.name, credentialsDB.`type`, credentialsDB.value.str))
          .mapError { e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
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
