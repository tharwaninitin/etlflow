package etlflow.etljobs

import etlflow.LoggerResource
import etlflow.etlsteps._
import zio.{Task, ZIO}

trait SequentialEtlJobWithLogging extends GenericEtlJobWithLogging {

    def etlStepList: List[EtlStep[Unit,Unit]]

    final override val job: ZIO[LoggerResource, Throwable, Unit] = for {
      step_list <- Task.succeed {
                      etlStepList.map(_.execute())
                    }
      job       <- ZIO.collectAll(step_list) *> ZIO.unit
    } yield job

    final override def printJobInfo(level: String = "info"): Unit = {
      etlStepList.foreach{ etl =>
        etl.getStepProperties(level).foreach(println)
      }
    }

    final override def getJobInfo(level: String = "info"): List[(String,Map[String,String])] = {
      etlStepList.flatMap{ etl_step =>
        Map(etl_step.name -> etl_step.getStepProperties(level))
      }
    }
}
