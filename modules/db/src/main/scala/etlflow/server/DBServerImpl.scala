package etlflow.server

import etlflow.db._
import etlflow.utils.ApplicationLogger
import etlflow.utils.DateTimeApi.getTimestampAsString
import etlflow.utils.EtlflowError.DBException
import scalikejdbc.NamedDB
import zio.{Has, IO, Task, UIO, ZLayer}

private[etlflow] object DBServerImpl extends ApplicationLogger {
  val live: ZLayer[Has[String], Throwable, DBServerEnv] = ZLayer.fromService { pool_name =>
    new DBServerApi.Service {
      override def getUser(name: String): IO[DBException, UserDB] = {
        Task(
          NamedDB(pool_name) readOnly { implicit s =>
            Sql.getUser(name)
              .map(UserDB(_))
              .single()
              .get
          }
        ).mapError { e =>
          logger.error(e.getMessage)
          DBException(e.getMessage)
        }
      }

      override def getJob(name: String): IO[DBException, JobDB] = {
        Task(
          NamedDB(pool_name) readOnly { implicit s =>
            Sql.getJob(name)
              .map(JobDB(_))
              .single()
              .get
          }
        ).mapError { e =>
          logger.error(e.getMessage)
          DBException(e.getMessage)
        }
      }

      override def getJobs: IO[DBException, List[JobDBAll]] = {
        Task(
          NamedDB(pool_name) readOnly { implicit s =>
            Sql.getJobs
              .map(JobDBAll(_))
              .list
              .apply()
          }).mapError { e =>
          logger.error(e.getMessage)
          DBException(e.getMessage)
        }
      }

      override def getStepRuns(args: DbStepRunArgs): IO[DBException, List[StepRun]] = {
        Task(
          NamedDB(pool_name) readOnly { implicit s =>
            Sql.getStepRuns(args.job_run_id)
              .map(rs => {
                val res = StepRunDB(rs)
                StepRun(res.job_run_id, res.step_name, res.properties, res.status, getTimestampAsString(res.inserted_at), res.elapsed_time, res.step_type, res.step_run_id)
              })
              .list
              .apply()
          }).mapError { e =>
          logger.error(e.getMessage)
          DBException(e.getMessage)
        }
      }

      override def getJobRuns(args: DbJobRunArgs): IO[DBException, List[JobRun]] = {
        Task(
          NamedDB(pool_name) readOnly { implicit s =>
            Sql.getJobRuns(args)
              .map(rs => {
                val res = JobRunDB(rs)
                JobRun(res.job_run_id, res.job_name, res.properties, res.status, getTimestampAsString(res.inserted_at), res.elapsed_time, res.job_type, res.is_master)
              })
              .list
              .apply()
          }).mapError { e =>
          logger.error(e.getMessage)
          DBException(e.getMessage)
        }
      }

      override def getJobLogs(args: JobLogsArgs): IO[DBException, List[JobLogs]] = {
        Task(
          NamedDB(pool_name) readOnly { implicit s =>
            Sql.getJobLogs(args)
              .map(JobLogs(_))
              .list
              .apply()
          }).mapError { e =>
          logger.error(e.getMessage)
          DBException(e.getMessage)
        }
      }

      override def getCredentials: IO[DBException, List[GetCredential]] = {
        Task(
          NamedDB(pool_name) readOnly { implicit s =>
            Sql.getCredentials
              .map(GetCredential(_))
              .list
              .apply()
          }).mapError { e =>
          logger.error(e.getMessage)
          DBException(e.getMessage)
        }
      }

      override def updateSuccessJob(job: String, ts: Long): IO[DBException, Long] = {
        Task(
          NamedDB(pool_name) localTx { implicit s =>
            Sql.updateSuccessJob(job, ts)
              .update()
          }).mapBoth({
          e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
        }, _ => 1L
        )
      }

      override def updateFailedJob(job: String, ts: Long): IO[DBException, Long] = {
        Task(
          NamedDB(pool_name) localTx { implicit s =>
            Sql.updateFailedJob(job, ts)
              .update()
          }).mapBoth({
          e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
        },
          _ => 1L
        )
      }

      override def updateJobState(args: EtlJobStateArgs): IO[DBException, Boolean] = {
        Task(
          NamedDB(pool_name) localTx { implicit s =>
            Sql.updateJobState(args)
              .update()
          }).mapBoth({
          e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
        },
          _ => args.state
        )
      }

      override def addCredential(cred: Credential): IO[DBException, Credential] = {
        Task(
          NamedDB(pool_name) localTx { implicit s =>
            Sql.addCredentials(cred)
              .update()
          }).mapBoth({
          e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
        },
          _ => cred
        )
      }

      override def updateCredential(cred: Credential): IO[DBException, Credential] = {
        Task(
          NamedDB(pool_name) localTx { implicit s =>
            // --- transaction scope start ---
            Sql.updateCredentials(cred).update()
            Sql.addCredentials(cred).update()
            // --- transaction scope end ---
          }).mapBoth({
          e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
        }, _ => cred
        )
      }

      override def refreshJobs(jobs: List[EtlJob]): IO[DBException, List[JobDB]] = {
        val jobsDB = jobs.map { x =>
          JobDB(x.name, x.props.getOrElse("job_schedule", ""), is_active = true)
        }
        val seq = jobsDB.map(data =>
          Seq(data.job_name, "", data.schedule, 0, 0, data.is_active)
        )
        if (jobsDB.isEmpty)
          UIO {
            List.empty
          }
        else
          Task(
            NamedDB(pool_name) localTx { implicit s =>
              // --- transaction scope start ---
              Sql.deleteJobs(jobsDB).update()
              Sql.insertJobs(seq).update()
              Sql.selectJobs.map(JobDB(_)).list.apply()
              // --- transaction scope end ---
            }).mapError { e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
          }
      }
    }
  }
}
