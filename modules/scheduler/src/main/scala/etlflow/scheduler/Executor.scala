package etlflow.scheduler

import caliban.CalibanError.ExecutionError
import doobie.hikari.HikariTransactor
import etlflow.etljobs.{EtlJob => EtlFlowEtlJob}
import etlflow.gcp.{DP, DPService}
import etlflow.scheduler.api.EtlFlowHelper._
import etlflow.scheduler.db.Update
import etlflow.utils.Executor.DATAPROC
import etlflow.utils.{Config, JsonJackson, UtilityFunctions => UF}
import etlflow.{EtlJobName, EtlJobProps}
import zio._
import scala.reflect.runtime.universe.TypeTag

abstract class Executor[EJN <: EtlJobName[EJP] : TypeTag, EJP <: EtlJobProps : TypeTag]
  extends WebServer[EJN,EJP] {

  val main_class: String
  val dp_libs: List[String]

  def toEtlJob(job_name: EJN): (EJP,Config) => EtlFlowEtlJob

  final override def runEtlJobDataProc(args: EtlJobArgs, transactor: HikariTransactor[Task], config: DATAPROC): Task[EtlJob] = {

    val etlJobDetails: Task[(EJN, Map[String, String])] = Task {
      val job_name      = UF.getEtlJobName[EJN](args.name, etl_job_name_package)
      val props_map     = args.props.map(x => (x.key,x.value)).toMap
      (job_name,props_map)
    }.mapError{ e =>
      logger.error(e.getMessage)
      ExecutionError(e.getMessage)
    }

    for {
      (job_name,props_map) <- etlJobDetails
      execution_props  <- Task {
        JsonJackson.convertToJsonByRemovingKeysAsMap(job_name.getActualProperties(props_map), List.empty)
          .map(x => (x._1, x._2.toString))
      }.mapError{ e =>
        logger.error(e.getMessage)
        ExecutionError(e.getMessage)
      }
      _  <- DPService.executeSparkJob(job_name.toString,props_map,main_class,dp_libs).provideLayer(DP.live(config)).foldM(
        ex => UIO(logger.error(ex.getMessage)) *> Update.updateFailedJob(job_name.toString,transactor),
        _  => Update.updateSuccessJob(job_name.toString,transactor)
      ).forkDaemon
    } yield EtlJob(args.name,execution_props)
  }

  final override def runEtlJobLocal(args: EtlJobArgs, transactor: HikariTransactor[Task]): Task[EtlJob] = {

    val etlJobDetails: Task[(EJN, EtlFlowEtlJob, Map[String, String])] = Task {
      val job_name      = UF.getEtlJobName[EJN](args.name, etl_job_name_package)
      val props_map     = args.props.map(x => (x.key,x.value)).toMap
      val etl_job       = toEtlJob(job_name)(job_name.getActualProperties(props_map),globalProperties)
      etl_job.job_name  = job_name.toString
      (job_name,etl_job,props_map)
    }.mapError{ e =>
      logger.error(e.getMessage)
      ExecutionError(e.getMessage)
    }

    for {
      (job_name,etl_job,props_map) <- etlJobDetails
      execution_props   <- Task {
        JsonJackson.convertToJsonByRemovingKeysAsMap(job_name.getActualProperties(props_map), List.empty)
          .map(x => (x._1, x._2.toString))
      }.mapError{ e =>
        logger.error(e.getMessage)
        ExecutionError(e.getMessage)
      }
      _                  <- etl_job.execute().foldM(
        ex => Update.updateFailedJob(job_name.toString,transactor),
        _  => Update.updateSuccessJob(job_name.toString,transactor)
      ).forkDaemon
    } yield EtlJob(args.name,execution_props)
  }
}
