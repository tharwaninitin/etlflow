package etljobs.etljob

import etljobs.etlsteps._
import etljobs.log.{DbManager, SlackManager}
import etljobs.utils.{UtilityFunctions => UF}
import zio.{UIO, ZIO}

trait SequentialEtlJob extends EtlJob {

    final override val db: Option[DbManager]       = createDbLogger(job_name, job_properties, global_properties, platform.executor.asEC)
    final override val slack: Option[SlackManager] = createSlackLogger(job_name, job_properties, global_properties)

    def etl_step_list: List[EtlStep[_,_]]

    def printJobInfo(level: String = "info"): Unit = {
      etl_step_list.foreach{ etl =>
        etl.getStepProperties(level).foreach(println)
      }
    }

    def getJobInfo(level: String = "info"): List[(String,Map[String,String])] = {
      etl_step_list.flatMap{ etl_step =>
        Map(etl_step.name -> etl_step.getStepProperties(level))
      }
    }

    def execute(): Unit = {
      val job = for {
        job_start_time  <- UIO.succeed(UF.getCurrentTimestamp)
        _               <- logJobInit(db)
        step_list       = etl_step_list.map {
                            case step: BQLoadStep[_]            => step.execute(bq)(db,slack)
                            case step: BQQueryStep              => step.execute(bq)(db,slack)
                            case step: SparkETLStep             => step.execute(spark)(db,slack)
                            case step: SparkReadWriteStep[_, _] => step.execute(spark)(db,slack)
                          }
        _               <- ZIO.collectAll(step_list).mapError(e => logError(e,job_start_time)(db))
        _               <- logSuccess(job_start_time)(db)
      } yield ()
      runtime.unsafeRun(job)
    }
}
