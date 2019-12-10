package etljobs.etlsteps

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

class SparkReadWriteStep[T <: Product: TypeTag](
          val name : String
          ,val etl_metadata : Map[String, String] = Map[String, String]()
          ,source_dataset : => Dataset[T]
          ,transform_function : Option[Dataset[T] => Dataset[Row]] = None
          ,write_function : (Dataset[Row],SparkSession) => Unit
        )(implicit spark : SparkSession)
extends EtlStep[Unit,Unit]
{
  lazy val dataset : Dataset[Row] = transform_function match {
    case Some(tf) => source_dataset.transform(tf)
    case None => source_dataset.toDF()
  }
  private var recordsWrittenCount = 0L
  spark.sparkContext.addSparkListener(new SparkListener() {
    override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
      synchronized {
        recordsWrittenCount += taskEnd.taskMetrics.outputMetrics.recordsWritten
      }
    }
  })

  def process(input_state : Unit): Try[Unit] = {
    Try{
      etl_logger.info("#################################################################################################")
      etl_logger.info(s"Starting ETL Step : $name")
      write_function(dataset,spark)
      etl_logger.info("#################################################################################################")
    }
  }

  override def getExecutionMetrics : Map[String, Map[String,String]] = {
    etl_logger.info(s"Number of records written: $recordsWrittenCount")
    Map(name ->
      Map("Number of records written" -> recordsWrittenCount.toString)
    )
    /*
    spark.sharedState.statusStore
      .executionsList
      .foreach { execution =>
        println("Job ID: " + execution.executionId)
        println("Job Stages: " + execution.stages)
        println("Job Details: ")
        val mp = execution.metricValues
        val joined_metrics = execution.metrics.flatMap{ case SQLPlanMetric(name, accumulatorId, metricType) =>
          mp.get(accumulatorId).map(value => (accumulatorId,name,value.trim,metricType))
        }
        joined_metrics.sortBy(_._1).foreach(println(_))
      }
    */
  }

  def showPlan(): Unit = {
    etl_logger.info(s"Plan for job $name is as below:")
    dataset.explain()
  }

  def showSchema(): Unit = {
    etl_logger.info(s"Schema for job $name is as below:")
    etl_logger.info(dataset.schema.toDDL.split(",").mkString(",\n  "))
    //etl_logger.info(s"Schema for this job is: ${dataset.schema.foreach(println(_))}")
    //etl_logger.info(dataset.schema.prettyJson)
  }

  def showSampleData(): Unit = {
    etl_logger.info(s"Sample data for job $name:")
    dataset.show(10, truncate = false)
  }

  def showCorruptedData(): Unit = {
    etl_logger.info(s"Corrupted data for job $name:")
    source_dataset.filter("_corrupt_record is not null").show(100,truncate = false)
  }
}
