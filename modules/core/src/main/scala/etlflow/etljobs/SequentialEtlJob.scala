package etlflow.etljobs

import etlflow.LoggerResource
import etlflow.etlsteps._
import zio.{Task, ZIO}

trait SequentialEtlJob extends EtlJob {

    def etlStepList: List[EtlStep[_,_]]

    final override def etlJob(implicit resource: LoggerResource): Task[Unit] = for {
      step_list <- Task.succeed {
                      etlStepList.map {
                        case step: BQLoadStep[_] => step.execute(bq)(resource)
                        case step: BQQueryStep => step.execute(bq)(resource)
                        case step: SparkETLStep => step.execute(spark)(resource)
                        case step: SparkReadWriteStep[_, _] => step.execute(spark)(resource)
                        case step: DBQueryStep => step.execute()(resource)
                      }
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
