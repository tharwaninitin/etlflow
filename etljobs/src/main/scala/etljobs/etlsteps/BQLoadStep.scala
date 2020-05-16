package etljobs.etlsteps

import java.util
import com.google.cloud.bigquery.{BigQuery, Field, JobInfo, LegacySQLTypeName, Schema}
import etljobs.bigquery.LoadApi
import etljobs.utils._
import org.apache.spark.sql.Encoders
import zio.Task
import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

class BQLoadStep[T <: Product : TypeTag] private[etljobs] (
            val name: String
            , input_location: => Either[String, Seq[(String, String)]]
            , input_type: IOType
            , input_file_system: FSType = GCS
            , output_dataset: String
            , output_table: String
            , output_write_disposition: JobInfo.WriteDisposition = JobInfo.WriteDisposition.WRITE_TRUNCATE
            , output_create_disposition: JobInfo.CreateDisposition = JobInfo.CreateDisposition.CREATE_NEVER
       )
  extends EtlStep[BigQuery,Unit] {
  var row_count: Map[String, Long] = Map.empty

  def process(bq: BigQuery): Task[Unit] = Task {
    etl_logger.info("#################################################################################################")
    etl_logger.info(s"Starting BQ Data Load Step : $name")

     def getBQType(sp_type: String): LegacySQLTypeName = sp_type match {
      case "StringType"   => LegacySQLTypeName.STRING
      case "IntegerType"  => LegacySQLTypeName.INTEGER
      case "LongType"     => LegacySQLTypeName.INTEGER
      case "DoubleType"   => LegacySQLTypeName.FLOAT
      case "DateType"     => LegacySQLTypeName.DATE
      case "BooleanType"  => LegacySQLTypeName.BOOLEAN
      case _              => LegacySQLTypeName.STRING
    }

    val schema: Option[Schema] = Try{
      val fields = new util.ArrayList[Field]
      Encoders.product[T].schema.map(x => fields.add(Field.of(x.name, getBQType(x.dataType.toString))))
      val s = Schema.of(fields)
      etl_logger.info(s"Schema provided: ${s.getFields.asScala.map(x => (x.getName,x.getType))}")
      s
    }.toOption

    if (input_file_system == LOCAL) {
      etl_logger.info(s"FileSystem: $input_file_system")
      LoadApi.loadIntoBQFromLocalFile(
        input_location,input_type,output_dataset,output_table,output_write_disposition,output_create_disposition
      )
    }
    else if (input_file_system == GCS) {
      input_location match {
        case Left(value) =>
          etl_logger.info(s"FileSystem: $input_file_system")
          row_count = LoadApi.loadIntoBQTable(
            bq, value, input_type, output_dataset, output_table, output_write_disposition, output_create_disposition, schema
          )
        case Right(value) =>
          etl_logger.info(s"FileSystem: $input_file_system")
          row_count = LoadApi.loadIntoPartitionedBQTable(
            bq, value, input_type, output_dataset, output_table, output_write_disposition, output_create_disposition, schema
          )
      }
    }
    etl_logger.info("#################################################################################################")
  }

  override def getExecutionMetrics: Map[String, Map[String, String]] = {
    Map(name ->
      Map(
        "total_rows" -> row_count.foldLeft(0L)((a, b) => a + b._2).toString
        // "total_size" -> destinationTable.map(x => s"${x.getNumBytes / 1000000.0} MB").getOrElse("error in getting size")
      )
    )
  }

  override def getStepProperties(level: String): Map[String, String] = {
    if (level.equalsIgnoreCase("info"))
    {
      Map(
        "input_type" -> input_type.toString
        ,"input_location" -> input_location.fold(
          source_path => source_path,
          source_paths_partitions => source_paths_partitions.length.toString
        )
        ,"output_dataset" -> output_dataset
        ,"output_table" -> output_table
        ,"output_table_write_disposition" -> output_write_disposition.toString
        ,"output_table_create_disposition" -> output_create_disposition.toString
        ,"output_rows" -> row_count.foldLeft(0L)((a, b) => a + b._2).toString
      )
    } else
    {
      Map(
        "input_type" -> input_type.toString
        ,"input_location" -> input_location.fold(
          source_path => source_path,
          source_paths_partitions => source_paths_partitions.mkString(",")
        )
        ,"input_class" -> Try(Encoders.product[T].schema.toDDL).toOption.getOrElse("No Class Provided")
        ,"output_dataset" -> output_dataset
        ,"output_table" -> output_table
        ,"output_table_write_disposition" -> output_write_disposition.toString
        ,"output_table_create_disposition" -> output_create_disposition.toString
        ,"output_rows" -> row_count.map(x => x._1 + "<==>" + x._2.toString).mkString(",")
      )
    }
  }
}

object BQLoadStep {
  def apply[T <: Product : TypeTag]
      (name: String
      , input_location: => Either[String, Seq[(String, String)]]
      , input_type: IOType
      , input_file_system: FSType = GCS
      , output_dataset: String
      , output_table: String
      , output_write_disposition: JobInfo.WriteDisposition = JobInfo.WriteDisposition.WRITE_TRUNCATE
      , output_create_disposition: JobInfo.CreateDisposition = JobInfo.CreateDisposition.CREATE_NEVER
     ): BQLoadStep[T] = {
    new BQLoadStep[T](name, input_location, input_type, input_file_system
      , output_dataset, output_table, output_write_disposition, output_create_disposition)
  }
}