package etljobs.etlsteps

import java.util.ArrayList
import com.google.cloud.bigquery.{BigQuery, Field, JobInfo, LegacySQLTypeName, Schema, StandardTableDefinition, TableId}
import etljobs.bigquery.LoadApi
import etljobs.utils._
import org.apache.spark.sql.Encoders
import scala.reflect.runtime.universe.TypeTag
import scala.util.Try
import collection.JavaConverters._

class BQLoadStep[T <: Product : TypeTag] private (
            val name: String
            , input_location: => Either[String, Seq[(String, String)]]
            , input_type: IOType
            , input_file_system: FSType = GCS
            , output_dataset: String
            , output_table: String
            , output_write_disposition: JobInfo.WriteDisposition = JobInfo.WriteDisposition.WRITE_TRUNCATE
            , output_create_disposition: JobInfo.CreateDisposition = JobInfo.CreateDisposition.CREATE_NEVER
       )(bq: => BigQuery)
  extends EtlStep[Unit, Unit] {
  var row_count: Map[String, Long] = Map.empty

  def process(input_state: Unit): Try[Unit] = Try {
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
      val fields = new ArrayList[Field]
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
    else if (input_location.isRight && input_file_system == GCS) {
      etl_logger.info(s"FileSystem: $input_file_system")
      row_count = LoadApi.loadIntoPartitionedBQTable(
        bq, input_location.right.get, input_type
        , output_dataset, output_table, output_write_disposition, output_create_disposition, schema
      )
    }
    else if (input_location.isLeft && input_file_system == GCS) {
      etl_logger.info(s"FileSystem: $input_file_system")
      row_count = LoadApi.loadIntoBQTable(
        bq, input_location.left.get, input_type
        , output_dataset, output_table, output_write_disposition, output_create_disposition, schema
      )
    }
    etl_logger.info("#################################################################################################")
  }

  override def getExecutionMetrics: Map[String, Map[String, String]] = {
    val destinationTable = Try {
      val tableId = TableId.of(output_dataset, output_table)
      bq.getTable(tableId).getDefinition[StandardTableDefinition]
    }.toOption

    Map(name ->
      Map(
        "total_rows" -> destinationTable.map(x => x.getNumRows.toString).getOrElse("error in getting number of rows"),
        "total_size" -> destinationTable.map(x => s"${x.getNumBytes / 1000000.0} MB").getOrElse("error in getting size")
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
     )(bq: => BigQuery): BQLoadStep[T] = {
    new BQLoadStep[T](name, input_location, input_type, input_file_system
      , output_dataset, output_table, output_write_disposition, output_create_disposition)(bq)
  }
}