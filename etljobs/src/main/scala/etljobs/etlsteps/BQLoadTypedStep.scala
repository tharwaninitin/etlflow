package etljobs.etlsteps

import java.util
import com.google.cloud.bigquery.{BigQuery, CsvOptions, Field, FormatOptions, JobInfo, LegacySQLTypeName, Schema, StandardTableDefinition, TableId}
import etljobs.bigquery.LoadApi
import etljobs.utils._
import org.apache.spark.sql.Encoders
import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

class BQLoadTypedStep[T <: Product : TypeTag](
                                               val name: String
                                               , source_path: => String = ""
                                               , source_paths_partitions: => Seq[(String, String)] = Seq()
                                               , source_format: IOType
                                               , source_file_system: FSType = GCS
                                               , destination_dataset: String
                                               , destination_table: String
                                               , write_disposition: JobInfo.WriteDisposition = JobInfo.WriteDisposition.WRITE_TRUNCATE
                                               , create_disposition: JobInfo.CreateDisposition = JobInfo.CreateDisposition.CREATE_NEVER
                                             )(bq: => BigQuery)
  extends EtlStep[Unit, Unit] {
  var row_count: Map[String, Long] = Map.empty

  def process(input_state: Unit): Try[Unit] = Try {
    etl_logger.info("#################################################################################################")
    etl_logger.info(s"Starting BQ Data Load Step : $name")

    val source_format_bq: FormatOptions = source_format match {
      case PARQUET => FormatOptions.parquet
      case ORC => FormatOptions.orc
      case CSV(field_delimiter, header_present, _, _) => CsvOptions.newBuilder()
        .setSkipLeadingRows(if (header_present) 1 else 0)
        .setFieldDelimiter(field_delimiter)
        .build()
      case _ => FormatOptions.parquet
    }

    def getBQType(sp_type: String): LegacySQLTypeName = sp_type match {
      case "StringType" => LegacySQLTypeName.STRING
      case "IntegerType" => LegacySQLTypeName.INTEGER
      case "LongType" => LegacySQLTypeName.INTEGER
      case "DoubleType" => LegacySQLTypeName.FLOAT
      case "DateType" => LegacySQLTypeName.DATE
      case "BooleanType" => LegacySQLTypeName.BOOLEAN
      case _ => LegacySQLTypeName.STRING
    }

    val fields = new util.ArrayList[Field]
    Encoders.product[T].schema.map(x => fields.add(Field.of(x.name, getBQType(x.dataType.toString))))
    val schema: Schema = Schema.of(fields)

    if (source_paths_partitions.nonEmpty && source_file_system == GCS) {
      etl_logger.info(s"FileSystem: $source_file_system")
      row_count = LoadApi.loadIntoPartitionedBQTableFromGCS(
        bq, source_paths_partitions, source_format_bq
        , destination_dataset, destination_table, write_disposition, create_disposition, Some(schema)
      )
    }
    else if (source_path != "" && source_file_system == GCS) {
      etl_logger.info(s"FileSystem: $source_file_system")
      row_count = LoadApi.loadIntoUnpartitionedBQTableFromGCS(
        bq, source_path, source_format_bq
        , destination_dataset, destination_table, write_disposition, create_disposition, Some(schema)
      )
    }
    etl_logger.info("#################################################################################################")
  }

  override def getExecutionMetrics: Map[String, Map[String, String]] = {
    val tableId = TableId.of(destination_dataset, destination_table)
    val destinationTable = bq.getTable(tableId).getDefinition[StandardTableDefinition]
    Map(name ->
      Map(
        "total_rows" -> destinationTable.getNumRows.toString,
        "total_size" -> f"${destinationTable.getNumBytes / 1000000.0} MB"
      )
    )
  }

  override def getStepProperties(level: String): Map[String, String] = {
    if (level.equalsIgnoreCase("info"))
    {
      Map(
        "source_format" -> source_format.toString
        ,"source_path" -> source_path
        ,"source_dirs_count" -> source_paths_partitions.length.toString
        ,"destination_dataset" -> destination_dataset
        ,"destination_table" -> destination_table
        ,"write_disposition" -> write_disposition.toString
        ,"create_disposition" -> create_disposition.toString
      ) ++ row_count.map(x => (x._1, x._2.toString))
    } else
    {
      Map(
        "source_format" -> source_format.toString
        , "source_dirs" -> source_paths_partitions.mkString(",")
        , "source_path" -> source_path
        , "destination_dataset" -> destination_dataset
        , "destination_table" -> destination_table
        , "write_disposition" -> write_disposition.toString
        , "create_disposition" -> create_disposition.toString
      ) ++ row_count.map(x => (x._1, x._2.toString))
    }
  }
}

object BQLoadTypedStep {
  def apply[T <: Product : TypeTag]
      (name: String
      , source_path: => String = ""
      , source_paths_partitions: => Seq[(String, String)] = Seq()
      , source_format: IOType
      , source_file_system: FSType = GCS
      , destination_dataset: String
      , destination_table: String
      , write_disposition: JobInfo.WriteDisposition = JobInfo.WriteDisposition.WRITE_TRUNCATE
      , create_disposition: JobInfo.CreateDisposition = JobInfo.CreateDisposition.CREATE_NEVER
     )(bq: => BigQuery): BQLoadTypedStep[T] = {
    new BQLoadTypedStep[T](name, source_path, source_paths_partitions, source_format, source_file_system
      , destination_dataset, destination_table, write_disposition, create_disposition)(bq)
  }
}
