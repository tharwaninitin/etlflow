package etlflow.utils

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import caliban.CalibanError.ExecutionError
import cron4s.Cron
import cron4s.lib.javatime._
import doobie.hikari.HikariTransactor
import etlflow.log.ApplicationLogger
import etlflow.etljobs.{EtlJob => CoreEtlJob}
import etlflow.utils.EtlFlowHelper.{CacheDetails, CacheInfo, EtlJob, EtlJobArgs, Job}
import etlflow.utils.db.Query
import etlflow.utils.{UtilityFunctions => UF}
import etlflow.{EtlJobProps, EtlJobPropsMapping}
import scalacache.memoization.memoizeSync
import scalacache.modes.sync._
import zio.{Queue, Semaphore, Task}

import java.io.{PrintWriter, StringWriter}
import scala.reflect.runtime.universe.TypeTag

trait EtlFlowUtils  extends  ApplicationLogger {

  implicit val jobPropsCache = CacheHelper.createCache[Map[String, String]]

  def getPropsCacheStats = {
    val data:Map[String,String] = CacheHelper.toMap(jobPropsCache)
    val cacheInfo = CacheInfo("JobProps",
      jobPropsCache.underlying.stats.hitCount(),
      jobPropsCache.underlying.stats.hitRate(),
      jobPropsCache.underlying.asMap().size(),
      jobPropsCache.underlying.stats.missCount(),
      jobPropsCache.underlying.stats.missRate(),
      jobPropsCache.underlying.stats.requestCount(),
      data
    )

    CacheDetails("JobProps",JsonJackson.convertToJsonByRemovingKeysAsMap(cacheInfo,List("data")).mapValues(x => (x.toString)))

  }

  def getJobsFromDb[EJN <: EtlJobPropsMapping[EtlJobProps,CoreEtlJob[EtlJobProps]] : TypeTag](transactor: HikariTransactor[Task], etl_job_name_package: String): Task[List[Job]] = {
    for {
      rt    <- Task.runtime
      jobs  <- Query.getJobs(transactor)
        .map(y => y.map{x => {
          val props = getJobActualProps[EJN](x.job_name,etl_job_name_package)
          if(Cron(x.schedule).toOption.isDefined) {
            val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm")
            val cron = Cron(x.schedule).toOption
            val endTime = sdf.parse(cron.get.next(LocalDateTime.now()).getOrElse("").toString).getTime
            val startTime = sdf.parse(LocalDateTime.now().toString).getTime
            val nextScheduleTime = cron.get.next(LocalDateTime.now()).getOrElse("").toString
            Job(x.job_name, props, cron, nextScheduleTime, UF.getTimeDifferenceAsString(startTime,endTime), x.failed, x.success, x.is_active,props("job_max_active_runs").toInt, props("job_deploy_mode"))
          }else{
            Job(x.job_name, props, None, "", "", x.failed, x.success, x.is_active,props("job_max_active_runs").toInt, props("job_deploy_mode"))
          }
        }})
    } yield jobs
  }.mapError{ e =>
    logger.error(e.getMessage)
    ExecutionError(e.getMessage)
  }
  def getEtlJobs[EJN <: EtlJobPropsMapping[EtlJobProps,CoreEtlJob[EtlJobProps]] : TypeTag](etl_job_name_package: String): Task[List[EtlJob]] = {
      Task {
        UF.getEtlJobs[EJN].map(x => EtlJob(x, getJobActualProps[EJN](x, etl_job_name_package))).toList
        }.mapError{ e =>
        logger.error(e.getMessage)
        ExecutionError(e.getMessage)
      }
  }

  def getJobActualProps[EJN <: EtlJobPropsMapping[EtlJobProps,CoreEtlJob[EtlJobProps]] : TypeTag](jobName: String, etl_job_name_package: String): Map[String, String] = memoizeSync[Map[String, String]](None){
    val name = UF.getEtlJobName[EJN](jobName, etl_job_name_package)
    val exclude_keys = List("job_run_id","job_description","job_properties")
    JsonJackson.convertToJsonByRemovingKeysAsMap(
        try {
          name.getActualProperties(Map.empty)
        }
        catch{
            case ex: Exception => logger.info(ex.toString)
              val errors = new StringWriter();
              ex.printStackTrace(new PrintWriter(errors))
              Map("job_max_active_runs" -> 10,"job_deploy_mode" -> "NA", "Error" -> errors.toString())
          }, exclude_keys)
      .map(x => (x._1, x._2.toString))
  }

  def createSemaphores(jobs: List[EtlJob]): Task[Map[String, Semaphore]] = {
    for {
      rt          <- Task.runtime
      semaphores  = jobs.map(job => (job.name, rt.unsafeRun(Semaphore.make(permits = job.props("job_max_active_runs").toLong)))).toMap
    } yield semaphores
  }
}
