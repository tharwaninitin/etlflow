package etljobs

import com.google.cloud.bigquery.BigQuery
import etljobs.etlsteps.EtlStep
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import scala.util.{Failure, Success, Try}

trait EtlJob {
  val etl_job_logger : Logger = Logger.getLogger(getClass.getName)
  var job_state : Map[String, Map[String,String]] = Map.empty
  val spark : SparkSession
  val bq : BigQuery
  val job_properties : Map[String,String]

  def apply() : List[EtlStep[Unit,Unit]]

  def printJobInfo() : Unit = {
    println("Here 00")
    apply().foreach{ etl =>
      etl.getStepProperties.foreach(println)
    }

  }

  def execute() : Unit = {
    val t0 = System.nanoTime()
    apply().foreach { etl =>

      val results = etl.process()

      results match {
        case Success(_) =>
          etl_job_logger.info(s"Step ${etl.name} ran successfully")
          etl_job_logger.info("#################################################################################################")
        case Failure(exception) => {
          etl_job_logger.error(s"Step ${etl.name} failed with exception $exception")
          if (job_properties.getOrElse("throw_exception_on_error","true") == "true")
            throw exception
        }
      }

      job_state ++= etl.getExecutionMetrics
    }
    val t1 = System.nanoTime()

    etl_job_logger.info("Job completed in : " + (t1 - t0) / 1000000000.0 / 60.0 + " mins")
    etl_job_logger.info("Steps details are as belows: ")
    job_state.foreach(mp => etl_job_logger.info(s"Step name => ${mp._1} Step properties => ${mp._2}"))
  }
}
