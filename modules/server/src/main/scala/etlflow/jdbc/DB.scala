package etlflow.jdbc

import caliban.CalibanError.ExecutionError
import doobie.implicits._
import etlflow.log.{JobRun, StepRun}
import etlflow.utils.EtlFlowHelper._
import zio.interop.catz._
import zio.macros.accessible
import zio.{IO, ZLayer}

@accessible
object DB {
  trait Service {
    def getUser(user_name: String): IO[ExecutionError, UserInfo]
    def getJob(name: String): IO[ExecutionError, JobDB]
    def getJobs: IO[ExecutionError, List[JobDB1]]
    def getStepRuns(args: DbStepRunArgs): IO[ExecutionError, List[StepRun]]
    def getJobRuns(args: DbJobRunArgs): IO[ExecutionError, List[JobRun]]
    def getJobLogs(args: JobLogsArgs): IO[ExecutionError, List[JobLogs]]
    def getCredentials: IO[ExecutionError, List[UpdateCredentialDB]]
  }
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
    }
  }
}
