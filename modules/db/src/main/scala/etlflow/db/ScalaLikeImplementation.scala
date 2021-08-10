package etlflow.db

import doobie.util.Read
import etlflow.db.DBApi.Service
import etlflow.schema.Credential.JDBC
import etlflow.utils.ApplicationLogger
import etlflow.utils.DateTimeApi.getTimestampAsString
import etlflow.utils.EtlflowError.DBException
import scalikejdbc._
import zio._

private[db] object ScalaLikeImplementation extends ApplicationLogger {

  private def createConnectionPool(db: JDBC, pool_name: String = "EtlFlowPool", pool_size: Int = 2): Managed[Throwable, String] = 
    Managed.make(Task{
      logger.info(s"Creating connection pool $pool_name")
      Class.forName(db.driver)
      ConnectionPool.add(pool_name, db.url, db.user, db.password, ConnectionPoolSettings(maxSize = pool_size))
      pool_name
    })(_ => Task{
      logger.info(s"Closing connection pool $pool_name")
      ConnectionPool.close(pool_name)
    }.orDie)

  def cpLayer(db: JDBC, pool_name: String = "EtlFlowPool", pool_size: Int = 2): Layer[Throwable, Has[String]] = 
    ZLayer.fromManaged(createConnectionPool(db, pool_name, pool_size))

  val liveDB: ZLayer[Has[String], Throwable, DBEnv] = ZLayer.fromService { pool_name =>
    new Service {
    
      override def getUser(name: String): IO[DBException, UserDB] = {
        Task(
          NamedDB(pool_name) readOnly { implicit s =>
            ScalaLikeSQL.getUser(name)
            .map(UserDB(_))
            .single()
            .apply()
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
            ScalaLikeSQL.getJob(name)
            .map(JobDB(_))
            .single()
            .apply()
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
            ScalaLikeSQL.getJobs
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
            ScalaLikeSQL.getStepRuns(args.job_run_id)
            .map(rs => {
              val res = StepRunDB(rs)
              StepRun(res.job_run_id, res.step_name, res.properties, res.state, getTimestampAsString(res.inserted_at), res.elapsed_time, res.step_type, res.step_run_id)
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
            ScalaLikeSQL.getJobRuns(args)
            .map(rs => {
              val res = JobRunDB(rs)
              JobRun(res.job_run_id, res.job_name, res.properties, res.state, getTimestampAsString(res.inserted_at), res.elapsed_time, res.job_type, res.is_master)
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
            ScalaLikeSQL.getJobLogs(args)
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
            ScalaLikeSQL.getCredentials
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
            ScalaLikeSQL.updateSuccessJob(job, ts)
            .update()
            .apply()
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
            ScalaLikeSQL.updateFailedJob(job, ts)
            .update()
            .apply()
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
            ScalaLikeSQL.updateJobState(args)
            .update()
            .apply()
          }).mapBoth({
            e =>
              logger.error(e.getMessage)
              DBException(e.getMessage)
          },
            _ => args.state
          )
      }

      override def addCredential(credentialsDB: CredentialDB, actualSerializerOutput:JsonString): IO[DBException, Credentials] = {
        Task(
          NamedDB(pool_name) localTx { implicit s =>   
            ScalaLikeSQL.addCredentials(credentialsDB, actualSerializerOutput)
            .update()
            .apply()
          }).mapBoth({
            e =>
              logger.error(e.getMessage)
              DBException(e.getMessage)
          },
            _ => Credentials(credentialsDB.name, credentialsDB.`type`, credentialsDB.value.str)
          )
      }

      override def updateCredential(credentialsDB: CredentialDB,actualSerializerOutput:JsonString): IO[DBException, Credentials] = {
        Task(
          NamedDB(pool_name) localTx { implicit s =>
            // --- transaction scope start ---
            ScalaLikeSQL.updateCredentials(credentialsDB).update().apply()
            ScalaLikeSQL.addCredentials(credentialsDB, actualSerializerOutput).update().apply()
            // --- transaction scope end ---
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
        val seq = jobsDB.map(data =>
          Seq(data.job_name, "", data.schedule, 0,0, data.is_active)
        )
        if (jobsDB.isEmpty)
          UIO{List.empty}
        else
          Task(
            NamedDB(pool_name) localTx { implicit s =>
              // --- transaction scope start ---
              ScalaLikeSQL.deleteJobs(jobsDB).update().apply()
              ScalaLikeSQL.insertJobs(seq).update().apply()
              ScalaLikeSQL.selectJobs.map(JobDB(_)).list.apply()
              // --- transaction scope end ---
            }).mapError{ e =>
              logger.error(e.getMessage)
              DBException(e.getMessage)
            }
      }

      override def updateStepRun(job_run_id: String, step_name: String, props: String, status: String, elapsed_time: String): IO[DBException, Unit] = {
        Task(
          NamedDB(pool_name) localTx { implicit s =>
            ScalaLikeSQL.updateStepRun(job_run_id, step_name, props, status, elapsed_time)
            .update
            .apply()
          }).mapError({
          e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
          }).unit
      }

      override def insertStepRun(job_run_id: String, step_name: String, props: String, step_type: String, step_run_id: String, start_time: Long): IO[DBException, Unit] = {
        Task(
          NamedDB(pool_name) localTx { implicit s =>
            ScalaLikeSQL.insertStepRun(job_run_id, step_name, props, step_type, step_run_id, start_time)
            .update
            .apply()
          }).mapError({
          e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
          }).unit
      }

      override def insertJobRun(job_run_id: String, job_name: String, props: String, job_type: String, is_master: String, start_time: Long): IO[DBException, Unit] = {
        Task(
          NamedDB(pool_name) localTx { implicit s =>
            ScalaLikeSQL.insertJobRun(job_run_id, job_name, props, job_type, is_master, start_time)
            .update
            .apply()
          }).mapError({
          e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
          }).unit
      }

      override def updateJobRun(job_run_id: String, status: String, elapsed_time: String): IO[DBException, Unit] = {
        Task(
          NamedDB(pool_name) localTx { implicit s =>
            ScalaLikeSQL.updateJobRun(job_run_id, status, elapsed_time)
            .update
            .apply()
          }).mapError({
            e =>
              logger.error(e.getMessage)
              DBException(e.getMessage)
          }).unit
      }
      
      override def executeQueryWithSingleResponse[T : Read](query: String): IO[Throwable, T] = ???
      
      override def executeQueryWithResponse[T <: Product : Read](query: String): IO[DBException, List[T]] = ???
      
      override def executeQuery(query: String): IO[DBException, Unit] = 
        Task(
          NamedDB(pool_name) localTx { implicit s =>
            scalikejdbc.SQL(query)
            .update()
            .apply()
          }).mapError({
            e =>
              logger.error(e.getMessage)
              DBException(e.getMessage)
          }).unit
          
      override def executeQuerySingleOutput[T](query: String)(fn: WrappedResultSet => T): IO[DBException, T] = 
        Task(
          NamedDB(pool_name) localTx { implicit s =>
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
