package etlflow.utils

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import caliban.CalibanError.ExecutionError
import cron4s.Cron
import cron4s.lib.javatime._
import doobie.hikari.HikariTransactor
import etlflow.log.ApplicationLogger
import etlflow.utils.EtlFlowHelper.{CacheInfo, EtlJob, EtlJobArgs, Job}
import etlflow.utils.db.Query
import etlflow.utils.{UtilityFunctions => UF}
import etlflow.{EtlJobName, EtlJobProps}
import scalacache.Mode
import scalacache.memoization.memoizeF
import zio.interop.catz._
import zio.{Queue, Semaphore, Task}
import scala.concurrent.duration._
import scala.reflect.runtime.universe.TypeTag

trait EtlFlowUtils  extends  etlflow.executor.Executor with ApplicationLogger {

  implicit val jobPropsCache = CacheHelper.createCache[Map[String, String]]
  implicit val mode: Mode[Task] = scalacache.CatsEffect.modes.async

  def getPropsCacheStats = {
    val data:Map[String,String] = CacheHelper.toMap(jobPropsCache)
    CacheInfo("JobProps",
      jobPropsCache.underlying.stats.hitCount(),
      jobPropsCache.underlying.stats.hitRate(),
      jobPropsCache.underlying.asMap().size(),
      jobPropsCache.underlying.stats.missCount(),
      jobPropsCache.underlying.stats.missRate(),
      jobPropsCache.underlying.stats.requestCount(),
      data
    )
  }

  def getJobsFromDb[EJN <: EtlJobName[EJP] : TypeTag, EJP <: EtlJobProps : TypeTag](transactor: HikariTransactor[Task], etl_job_name_package: String): Task[List[Job]] = {
    for {
      rt    <- Task.runtime
      jobs  <- Query.getJobs(transactor)
        .map(y => y.map{x => {
          val props = zio.Runtime.default.unsafeRun(getJobActualProps[EJN,EJP](x.job_name,etl_job_name_package))
          if(Cron(x.schedule).toOption.isDefined) {
            val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm")
            val cron = Cron(x.schedule).toOption
            val endTime = sdf.parse(cron.get.next(LocalDateTime.now()).getOrElse("").toString).getTime
            val startTime = sdf.parse(LocalDateTime.now().toString).getTime
            val nextScheduleTime = cron.get.next(LocalDateTime.now()).getOrElse("").toString
            Job(x.job_name, props, cron, nextScheduleTime, UF.getTimeDifferenceAsString(startTime,endTime), x.failed, x.success, x.is_active,props.get("job_max_active_runs").get.toInt, props.get("job_deploy_mode").get)
          }else{
            Job(x.job_name, props, None, "", "", x.failed, x.success, x.is_active,props.get("job_max_active_runs").get.toInt, props.get("job_deploy_mode").get)
          }
        }})
    } yield jobs
  }.mapError{ e =>
    logger.error(e.getMessage)
    ExecutionError(e.getMessage)
  }
  def getEtlJobs[EJN <: EtlJobName[EJP] : TypeTag, EJP <: EtlJobProps : TypeTag](etl_job_name_package: String): Task[List[EtlJob]] = {
    Task{
      UF.getEtlJobs[EJN].map(x => EtlJob(x,zio.Runtime.default.unsafeRun(getJobActualProps[EJN,EJP](x,etl_job_name_package)))).toList
    }.mapError{ e =>
      logger.error(e.getMessage)
      ExecutionError(e.getMessage)
    }
  }
  def getJobActualProps[EJN <: EtlJobName[EJP] : TypeTag, EJP <: EtlJobProps : TypeTag](jobName: String, etl_job_name_package: String): Task[Map[String, String]] = memoizeF[Task, Map[String, String]](Some(3600.second)){
    val name = UF.getEtlJobName[EJN](jobName, etl_job_name_package)
    val exclude_keys = List("job_run_id","job_description","job_properties")
    Task(JsonJackson.convertToJsonByRemovingKeysAsMap(name.getActualProperties(Map.empty), exclude_keys).map(x => (x._1, x._2.toString)))
  }

  def createSemaphores(jobs: List[EtlJob]): Task[Map[String, Semaphore]] = {
    for {
      rt          <- Task.runtime
      semaphores  = jobs.map(job => (job.name, rt.unsafeRun(Semaphore.make(permits = job.props("job_max_active_runs").toLong)))).toMap
    } yield semaphores
  }

  def runEtlJobsFromApi[EJN <: EtlJobName[EJP] : TypeTag, EJP <: EtlJobProps : TypeTag](args: EtlJobArgs,transactor: HikariTransactor[Task],sem: Semaphore,config: Config, etl_job_name_package: String,jobQueue: Queue[(String,String)]): Task[Option[EtlJob]] ={
    runActiveEtlJob[EJN,EJP](args,transactor,sem,config,etl_job_name_package,"Rest-Api",jobQueue)
  }
}
