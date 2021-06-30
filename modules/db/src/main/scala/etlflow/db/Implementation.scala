package etlflow.db

import doobie.implicits._
import doobie.util.Read
import doobie.util.fragment.Fragment
import etlflow.db.DBApi.Service
import etlflow.utils.ApplicationLogger
import etlflow.utils.DateTimeFunctions.getTimestampAsString
import etlflow.utils.EtlflowError.DBException
import zio.interop.catz._
import zio.{IO, UIO, ZLayer}

private[db] object Implementation extends  ApplicationLogger {

  val liveDB: ZLayer[TransactorEnv, Throwable, DBEnv] = ZLayer.fromService { transactor =>
    new Service {
      def getUser(name: String): IO[DBException, UserDB] = {
        SQL.getUser(name)
          .unique
          .transact(transactor)
          .mapError { e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
          }
      }
      def getJob(name: String): IO[DBException, JobDB] = {
        SQL.getJob(name)
          .unique
          .transact(transactor)
          .mapError { e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
          }
      }
      def getJobs: IO[DBException, List[JobDBAll]] = {
        SQL.getJobs
          .to[List]
          .transact(transactor)
          .mapError { e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
          }
      }

      def getStepRuns(args: DbStepRunArgs): IO[DBException, List[StepRun]] = {
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
      def getJobRuns(args: DbJobRunArgs): IO[DBException, List[JobRun]] = {
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
      def getJobLogs(args: JobLogsArgs): IO[DBException, List[JobLogs]] = {
        SQL.getJobLogs(args)
          .to[List]
          .transact(transactor)
          .mapError { e =>
            logger.error(s"Exception ${e.getMessage} occurred for arguments $args")
            DBException(e.getMessage)
          }
      }
      def getCredentials: IO[DBException, List[GetCredential]] = {
        SQL.getCredentials
          .to[List]
          .transact(transactor)
          .mapError { e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
          }
      }
      def updateSuccessJob(job: String, ts: Long): IO[DBException, Long] = {
        SQL.updateSuccessJob(job, ts)
          .run
          .transact(transactor)
          .bimap({
            e =>
              logger.error(e.getMessage)
              DBException(e.getMessage)
            },
            _ => 1L
          )
      }
      def updateFailedJob(job: String, ts: Long): IO[DBException, Long] = {
        SQL.updateFailedJob(job, ts)
          .run
          .transact(transactor)
          .bimap({
            e =>
              logger.error(e.getMessage)
              DBException(e.getMessage)
            },
            _ => 1L
          )
      }
      def updateJobState(args: EtlJobStateArgs): IO[DBException, Boolean] = {
        SQL.updateJobState(args)
          .run
          .transact(transactor)
          .bimap({
            e =>
              logger.error(e.getMessage)
              DBException(e.getMessage)
            },
            _ => args.state
          )
      }
      def addCredential(credentialsDB: CredentialDB, actualSerializerOutput:JsonString): IO[DBException, Credentials] = {
        SQL.addCredentials(credentialsDB, actualSerializerOutput)
          .run
          .transact(transactor)
          .bimap({
            e =>
              logger.error(e.getMessage)
              DBException(e.getMessage)
            },
            _ => Credentials(credentialsDB.name, credentialsDB.`type`, credentialsDB.value.str)
          )
      }
      def updateCredential(credentialsDB: CredentialDB,actualSerializerOutput:JsonString): IO[DBException, Credentials] = SQL.updateCredentialSingleTran(credentialsDB, actualSerializerOutput)
        .transact(transactor)
        .bimap({ e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
          },
          _ => Credentials(credentialsDB.name, credentialsDB.`type`, credentialsDB.value.str)
        )
      def refreshJobs(jobs: List[EtlJob]): IO[DBException, List[JobDB]] = {
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

      def updateStepRun(job_run_id: String, step_name: String, props: String, status: String, elapsed_time: String): IO[DBException, Unit] = {
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
      IO[DBException, Unit] = {
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
      IO[DBException, Unit] = {
        SQL
          .insertJobRun(job_run_id, job_name, props, job_type, is_master, start_time)
          .run
          .transact(transactor).unit
          .mapError { e =>
            logger.error(s"failed in logging to db ${e.getMessage}")
            DBException(e.getMessage)
          }
      }

      def updateJobRun(job_run_id: String, status: String, elapsed_time: String): IO[DBException, Unit] = {
        SQL
          .updateJobRun(job_run_id, status, elapsed_time)
          .run
          .transact(transactor).unit
          .mapError { e =>
            logger.error(s"failed in logging to db ${e.getMessage}")
            DBException(e.getMessage)
          }
      }

      def executeQueryWithResponse[T <: Product : Read](query: String): IO[DBException, List[T]] = {
        Fragment.const(query)
          .query[T]
          .to[List]
          .transact(transactor)
          .mapError { e =>
            logger.error(s"failed in logging to db ${e.getMessage}")
            DBException(e.getMessage)
          }
      }

      override def executeQuery(query: String): IO[DBException, Unit] = {
        Fragment.const(query)
          .update
          .run
          .transact(transactor).unit
          .mapError { e =>
            logger.error(s"failed in logging to db ${e.getMessage}")
            DBException(e.getMessage)
          }
      }

      override def executeQueryWithSingleResponse[T: Read](query: String): IO[DBException, T] = {
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
