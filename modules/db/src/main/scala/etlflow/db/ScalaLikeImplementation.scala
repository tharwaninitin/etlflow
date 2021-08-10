package etlflow.db

import doobie.util.Read
import etlflow.db.DBApi.Service
import etlflow.schema.Credential.JDBC
import etlflow.utils.ApplicationLogger
import etlflow.utils.DateTimeApi.getTimestampAsString
import etlflow.utils.EtlflowError.DBException
import scalikejdbc._
import zio._

private[db] object ScalaLikeImplementation extends  ApplicationLogger {

  type CPEnv = Has[Unit]

  def createConnectionPool(db: JDBC, pool_size: Int = 2): Managed[Throwable, Unit] = 
    Managed.make(Task{
      logger.info("Creating connection pool")
      Class.forName(db.driver)
      ConnectionPool.singleton(db.url, db.user, db.password, ConnectionPoolSettings(maxSize = pool_size))
    })(cp => UIO{
      logger.info("Closing connection pool")
      ConnectionPool.close()
    })

  def createConnectionPoolLayer(db: JDBC, pool_size: Int = 2): Layer[Throwable,CPEnv] = 
    ZLayer.fromManaged(createConnectionPool(db, pool_size))

  val liveDB: ZLayer[CPEnv, Throwable, DBEnv] = ZLayer.fromService { _ =>
    new Service {
      
      // ad-hoc session provider on the REPL
      implicit val session: DBSession = AutoSession

      override def getUser(name: String): IO[DBException, UserDB] = {
        Task(ScalaLikeSQL.getUser(name)
          .map(UserDB(_))
          .single()
          .apply()
          .get
        ).mapError { e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
        }
      }

      override def getJob(name: String): IO[DBException, JobDB] = {
        Task(ScalaLikeSQL.getJob(name)
          .map(JobDB(_))
          .single()
          .apply()
          .get
        ).mapError { e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
          }
      }

      override def getJobs: IO[DBException, List[JobDBAll]] = {
        Task(ScalaLikeSQL.getJobs
          .map(JobDBAll(_))
          .list
          .apply()
        ).mapError { e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
         }
      }

      override def getStepRuns(args: DbStepRunArgs): IO[DBException, List[StepRun]] = {
        Task(ScalaLikeSQL.getStepRuns(args.job_run_id)
          .map(rs => {
            val res = StepRunDB(rs)
            StepRun(res.job_run_id, res.step_name, res.properties, res.state, getTimestampAsString(res.inserted_at), res.elapsed_time, res.step_type, res.step_run_id)
          })
          .list
          .apply()
        ).mapError { e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
          }
      }

      override def getJobRuns(args: DbJobRunArgs): IO[DBException, List[JobRun]] = {
        Task(ScalaLikeSQL.getJobRuns(args)
          .map(rs => {
            val res = JobRunDB(rs)
            JobRun(res.job_run_id, res.job_name, res.properties, res.state, getTimestampAsString(res.inserted_at), res.elapsed_time, res.job_type, res.is_master)
          })
          .list
          .apply()
        ).mapError { e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
          }
      }

      override def getJobLogs(args: JobLogsArgs): IO[DBException, List[JobLogs]] = {
        Task(ScalaLikeSQL.getJobLogs(args)
          .map(JobLogs(_))
          .list
          .apply()
        ).mapError { e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
          }
      }

      override def getCredentials: IO[DBException, List[GetCredential]] = {
        Task(ScalaLikeSQL.getCredentials
          .map(GetCredential(_))
          .list
          .apply()
        ).mapError { e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
          }
      }

      override def updateSuccessJob(job: String, ts: Long): IO[DBException, Long] = {
        Task(ScalaLikeSQL.updateSuccessJob(job, ts)
          .update()
          .apply()
        ).mapBoth({
            e =>
              logger.error(e.getMessage)
              DBException(e.getMessage)
          }, _ => 1L
          )
      }

      override def updateFailedJob(job: String, ts: Long): IO[DBException, Long] = {
        Task(ScalaLikeSQL.updateFailedJob(job, ts)
          .update()
          .apply()
        ).mapBoth({
            e =>
              logger.error(e.getMessage)
              DBException(e.getMessage)
          },
            _ => 1L
          )
      }

      override def updateJobState(args: EtlJobStateArgs): IO[DBException, Boolean] = {
        Task(ScalaLikeSQL.updateJobState(args)
          .update()
          .apply()
        ).mapBoth({
            e =>
              logger.error(e.getMessage)
              DBException(e.getMessage)
          },
            _ => args.state
          )
      }

      override def addCredential(credentialsDB: CredentialDB, actualSerializerOutput:JsonString): IO[DBException, Credentials] = {
        Task(ScalaLikeSQL.addCredentials(credentialsDB, actualSerializerOutput)
          .update()
          .apply()
        ).mapBoth({
            e =>
              logger.error(e.getMessage)
              DBException(e.getMessage)
          },
            _ => Credentials(credentialsDB.name, credentialsDB.`type`, credentialsDB.value.str)
          )
      }

      override def updateCredential(credentialsDB: CredentialDB,actualSerializerOutput:JsonString): IO[DBException, Credentials] = {
        Task(DB.localTx { _ =>
          ScalaLikeSQL.updateCredentials(credentialsDB)
            .update()
            .apply()
          ScalaLikeSQL.addCredentials(credentialsDB, actualSerializerOutput)
            .update()
            .apply()
        }).mapBoth({
          e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
        }, _ => Credentials(credentialsDB.name, credentialsDB.`type`, credentialsDB.value.str)
        )
      }

      override def refreshJobs(jobs: List[EtlJob]): IO[DBException, List[JobDB]] = {
        val jobsDB = jobs.map{x =>
          JobDB(x.name, x.props.getOrElse("job_schedule",""), is_active = true)
        }
        if (jobsDB.isEmpty)
          UIO{List.empty}
        else
          Task(DB.localTx { _ =>
            ScalaLikeSQL.deleteJobs(jobsDB)
              .update()
              .apply()
            jobsDB.foreach(data => ScalaLikeSQL.insertData(JobDBAll(data.job_name, "", data.schedule, 0,0, data.is_active))
              .update()
              .apply())
            ScalaLikeSQL.selectJobs.map(JobDB(_))
              .list
              .apply()
          }).mapError{ e =>
              logger.error(e.getMessage)
              DBException(e.getMessage)
            }
      }

      override def updateStepRun(job_run_id: String, step_name: String, props: String, status: String, elapsed_time: String): IO[DBException, Unit] = {
        Task(ScalaLikeSQL.updateStepRun(job_run_id, step_name, props, status, elapsed_time)
          .update
          .apply()
        ).mapError({
          e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
        }).unit
      }

      override def insertStepRun(job_run_id: String, step_name: String, props: String, step_type: String, step_run_id: String, start_time: Long): IO[DBException, Unit] = {
        Task(ScalaLikeSQL.insertStepRun(job_run_id, step_name, props, step_type, step_run_id, start_time)
          .update
          .apply()
        ).mapError({
          e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
        }).unit
      }

      override def insertJobRun(job_run_id: String, job_name: String, props: String, job_type: String, is_master: String, start_time: Long): IO[DBException, Unit] = {
        Task(ScalaLikeSQL.insertJobRun(job_run_id, job_name, props, job_type, is_master, start_time)
          .update
          .apply()
        ).mapError({
          e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
        }).unit
      }

      override def updateJobRun(job_run_id: String, status: String, elapsed_time: String): IO[DBException, Unit] = {
        Task(ScalaLikeSQL.updateJobRun(job_run_id, status, elapsed_time)
          .update
          .apply()
        ).mapError({
            e =>
              logger.error(e.getMessage)
              DBException(e.getMessage)
          }).unit
      }
      
      override def executeQueryWithSingleResponse[T : Read](query: String): IO[Throwable, T] = ???
      
      override def executeQueryWithResponse[T <: Product : Read](query: String): IO[DBException, List[T]] = ???
      
      override def executeQuery(query: String): IO[DBException, Unit] = 
        Task(DB readOnly { _ =>
          scalikejdbc.SQL(query)
          .update()
          .apply()
        }).mapError({
            e =>
              logger.error(e.getMessage)
              DBException(e.getMessage)
          }).unit
          
      override def executeQuerySingleOutput[T](query: String)(fn: WrappedResultSet => T): IO[DBException, T] = 
        Task(DB readOnly { _ =>
          scalikejdbc.SQL(query)
          .map(fn)
          .single()
          .apply()
          .get
        }).mapError({
            e =>
              logger.error(e.getMessage)
              DBException(e.getMessage)
          })
    }
  }
}
