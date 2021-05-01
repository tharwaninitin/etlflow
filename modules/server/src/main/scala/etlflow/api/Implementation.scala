package etlflow.api

import etlflow.api.Schema._
import etlflow.executor.Executor
import etlflow.jdbc.{DB, DBServerEnv}
import etlflow.log.ApplicationLogger
import etlflow.utils.{CacheHelper, EtlFlowUtils, UtilityFunctions => UF}
import etlflow.webserver.Authentication
import etlflow.{EJPMType, BuildInfo => BI}
import org.ocpsoft.prettytime.PrettyTime
import scalacache.caffeine.CaffeineCache
import zio.Fiber.Status.{Running, Suspended}
import zio._
import zio.blocking.Blocking
import scala.reflect.runtime.universe.TypeTag

object Implementation extends EtlFlowUtils with ApplicationLogger {

  def live[EJN <: EJPMType : TypeTag](auth: Authentication, executor: Executor[EJN], jobs: List[EtlJob], ejpm_package: String, supervisor: Supervisor[Chunk[Fiber.Runtime[Any, Any]]], cache: CaffeineCache[QueueDetails]): ZLayer[Blocking, Throwable, APIEnv] = {
    ZLayer.succeed(new Service {

      val pt = new PrettyTime()

      final private def monitorFibers(supervisor: Supervisor[Chunk[Fiber.Runtime[Any, Any]]]): UIO[List[EtlJobStatus]] = for {
        uio_status <- supervisor.value.map(_.map(_.dump))
        status     <- ZIO.collectAll(uio_status.toList)
        op = status.map{x =>
              val status = x.status match {
                case Running(_) => "Running"
                case Suspended(previous, interruptible, epoch, blockingOn, asyncTrace) => s"Suspended($asyncTrace)"
              }
              EtlJobStatus(x.fiberId.seqNumber.toString, x.fiberName.getOrElse(""), UF.getTimestampAsString(x.fiberId.startTimeMillis), "Scheduled", status)
             }
      } yield op

      override def getJobs: ZIO[APIEnv with DBServerEnv, Throwable, List[Job]] = DB.getJobs[EJN](ejpm_package)

      override def getCacheStats: ZIO[APIEnv, Throwable, List[CacheDetails]] = {
        val job_props = CacheHelper.getCacheStats(jobPropsMappingCache, "JobProps")
        val login     = CacheHelper.getCacheStats(auth.cache, "Login")
        UIO(List(login,job_props))
      }

      override def getQueueStats: ZIO[APIEnv, Throwable, List[QueueDetails]] = UIO(CacheHelper.getValues(cache))

      override def getJobStats: ZIO[APIEnv, Throwable, List[EtlJobStatus]] = monitorFibers(supervisor)

      override def getJobLogs(args: JobLogsArgs): ZIO[APIEnv with DBServerEnv, Throwable, List[JobLogs]] = DB.getJobLogs(args)

      override def getCredentials: ZIO[APIEnv with DBServerEnv, Throwable, List[GetCredential]] = DB.getCredentials

      override def runJob(args: EtlJobArgs, submitter: String): ServerTask[EtlJob] = executor.runActiveEtlJob(args, submitter)

      override def getDbStepRuns(args: DbStepRunArgs): ZIO[APIEnv with DBServerEnv, Throwable, List[StepRun]] = DB.getStepRuns(args)

      override def getDbJobRuns(args: DbJobRunArgs): ZIO[APIEnv with DBServerEnv, Throwable, List[JobRun]] = DB.getJobRuns(args)

      override def updateJobState(args: EtlJobStateArgs): ZIO[APIEnv with DBServerEnv, Throwable, Boolean] = DB.updateJobState(args)

      override def login(args: UserArgs): ZIO[APIEnv with DBServerEnv, Throwable, UserAuth] = auth.login(args)

      override def getInfo: ZIO[APIEnv, Throwable, EtlFlowMetrics] = Task {
        val dt = UF.getLocalDateTimeFromTimestamp(BI.builtAtMillis)
        EtlFlowMetrics(
          0,
          0,
          jobs.length,
          jobs.length,
          build_time = s"${dt.toString.take(16)} ${pt.format(dt)}"
        )
      }

      override def getCurrentTime: ZIO[APIEnv, Throwable, CurrentTime] = UIO(CurrentTime(current_time = UF.getCurrentTimestampAsString()))

      override def addCredentials(args: CredentialsArgs): ZIO[APIEnv with DBServerEnv, Throwable, Credentials] = DB.addCredential(args)

      override def updateCredentials(args: CredentialsArgs): ZIO[APIEnv with DBServerEnv, Throwable, Credentials] = DB.updateCredential(args)
    })
  }
}
