package etlflow.api

import etlflow.api.Schema._
import etlflow.executor.Executor
import etlflow.jdbc.{DB, DBServerEnv}
import etlflow.log.{ApplicationLogger, JobRun, StepRun}
import etlflow.utils.{CacheHelper, EtlFlowUtils, QueueHelper, UtilityFunctions => UF}
import etlflow.webserver.Authentication
import etlflow.{EJPMType, BuildInfo => BI}
import org.ocpsoft.prettytime.PrettyTime
import zio._
import zio.blocking.Blocking
import zio.stream.ZStream
import scala.reflect.runtime.universe.TypeTag

object Implementation extends EtlFlowUtils with ApplicationLogger {

  def live[EJN <: EJPMType : TypeTag](auth: Authentication, executor: Executor[EJN], jobs: List[EtlJob], ejpm_package: String): ZLayer[Blocking, Throwable, APIEnv] = {
    for {
      subscribers <- Ref.make(List.empty[Queue[EtlJobStatus]])
      activeJobs  <- Ref.make(0)
      pt          = new PrettyTime()
    } yield new Service {

      override def getJobs: ZIO[APIEnv with DBServerEnv, Throwable, List[Job]] = DB.getJobs[EJN](ejpm_package)

      override def getCacheStats: ZIO[APIEnv, Throwable, List[CacheDetails]] = {
        val job_props = CacheHelper.getCacheStats(jobPropsMappingCache, "JobProps")
        val login     = CacheHelper.getCacheStats(auth.cache, "Login")
        UIO(List(login,job_props))
      }

      override def getQueueStats: ZIO[APIEnv, Throwable, List[QueueDetails]] = QueueHelper.takeAll(executor.job_queue)

      override def getJobLogs(args: JobLogsArgs): ZIO[APIEnv with DBServerEnv, Throwable, List[JobLogs]] = DB.getJobLogs(args)

      override def getCredentials: ZIO[APIEnv with DBServerEnv, Throwable, List[GetCredential]] = DB.getCredentials

      override def runJob(args: EtlJobArgs, submitter: String): ServerTask[EtlJob] = executor.runActiveEtlJob(args, submitter)

      override def getDbStepRuns(args: DbStepRunArgs): ZIO[APIEnv with DBServerEnv, Throwable, List[StepRun]] = DB.getStepRuns(args)

      override def getDbJobRuns(args: DbJobRunArgs): ZIO[APIEnv with DBServerEnv, Throwable, List[JobRun]] = DB.getJobRuns(args)

      override def updateJobState(args: EtlJobStateArgs): ZIO[APIEnv with DBServerEnv, Throwable, Boolean] = DB.updateJobState(args)

      override def login(args: UserArgs): ZIO[APIEnv with DBServerEnv, Throwable, UserAuth] = auth.login(args)

      override def getInfo: ZIO[APIEnv, Throwable, EtlFlowMetrics] = {
        for {
          x <- activeJobs.get
          y <- subscribers.get
          dt = UF.getLocalDateTimeFromTimestamp(BI.builtAtMillis)
        } yield EtlFlowMetrics(
          x,
          y.length,
          jobs.length,
          jobs.length,
          build_time = s"${dt.toString.take(16)} ${pt.format(dt)}"
        )
      }

      override def getCurrentTime: ZIO[APIEnv, Throwable, CurrentTime] = UIO(CurrentTime(current_time = UF.getCurrentTimestampAsString()))

      override def addCredentials(args: CredentialsArgs): ZIO[APIEnv with DBServerEnv, Throwable, Credentials] = DB.addCredential(args)

      override def updateCredentials(args: CredentialsArgs): ZIO[APIEnv with DBServerEnv, Throwable, Credentials] = DB.updateCredential(args)

      override def notifications: ZStream[APIEnv, Nothing, EtlJobStatus] = ZStream.unwrap {
        for {
          queue <- Queue.unbounded[EtlJobStatus]
          _     <- UIO(logger.info(s"Starting new subscriber"))
          _     <- subscribers.update(queue :: _)
        } yield ZStream.fromQueue(queue).ensuring(queue.shutdown)
      }
    }
  }.toLayer
}
