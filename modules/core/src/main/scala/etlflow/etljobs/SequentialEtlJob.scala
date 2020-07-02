package etlflow.etljobs

import etlflow.etlsteps._
import zio.{Task, ZIO}

trait SequentialEtlJob extends GenericEtlJob {

    def etlStepList: List[EtlStep[Unit,Unit]]

    final override val job: Task[Unit] = for {
      step_list <- Task.succeed(etlStepList.map(_.process()))
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
