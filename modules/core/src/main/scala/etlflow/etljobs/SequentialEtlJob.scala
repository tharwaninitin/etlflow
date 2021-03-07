package etlflow.etljobs

import etlflow.{EtlJobProps, LoggerResource}
import etlflow.etlsteps._
import etlflow.utils.LoggingLevel
import zio.{Has, Task, ZEnv, ZIO}

trait SequentialEtlJob[EJP <: EtlJobProps] extends GenericEtlJob[EJP] {

  def etlStepList: List[EtlStep[Unit,Unit]]
  override val job_type: String =  "SequentialEtlJob"

  final override val job: ZIO[Has[LoggerResource] with ZEnv, Throwable, Unit] = for {
    step_list <- Task.succeed(etlStepList.map(_.execute()))
    job       <- ZIO.collectAll(step_list) *> ZIO.unit
  } yield job

  final override def printJobInfo(level: LoggingLevel = LoggingLevel.INFO): Unit = {
    etlStepList.foreach{ etl =>
      etl.getStepProperties(level).foreach(println)
    }
  }

  final override def getJobInfo(level: LoggingLevel = LoggingLevel.INFO): List[(String,Map[String,String])] = {
    etlStepList.flatMap{ etl_step =>
      Map(etl_step.name -> etl_step.getStepProperties(level))
    }
  }
}
