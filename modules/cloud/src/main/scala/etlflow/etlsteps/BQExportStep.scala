package etlflow.etlsteps

import com.google.cloud.bigquery.{Field, JobInfo, LegacySQLTypeName, Schema}
import etlflow.Credential
import etlflow.gcp._
import etlflow.utils.{LoggingLevel, UtilityFunctions => UF}
import zio.{Task, UIO}

import java.util
import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

class BQExportStep[T <: Product : TypeTag] private[etlflow](
     val name: String
     , source_project: Option[String] = None
     , source_dataset: String
     , source_table: String
     , destination_path: String
     , destination_file_name:Option[String] = None
     , destination_format:BQInputType
     , destination_compression_type:String = "gzip"
     , credentials: Option[Credential.GCP] = None
     )
  extends EtlStep[Unit, Unit] {
  var row_count: Map[String, Long] = Map.empty

  final def process(input: => Unit): Task[Unit] = {
    etl_logger.info("#" * 50)
    etl_logger.info(s"Starting BQ Data Export Step : $name")

    def getBQType(sp_type: String): LegacySQLTypeName = sp_type match {
      case "String" => LegacySQLTypeName.STRING
      case "Int" => LegacySQLTypeName.INTEGER
      case "Long" => LegacySQLTypeName.INTEGER
      case "Double" => LegacySQLTypeName.FLOAT
      case "java.sql.Date" => LegacySQLTypeName.DATE
      case "java.util.Date" => LegacySQLTypeName.DATE
      case "Boolean" => LegacySQLTypeName.BOOLEAN
      case _ => LegacySQLTypeName.STRING
    }

    val schema: Option[Schema] = Try {
      val fields = new util.ArrayList[Field]
      val ccFields = UF.getFields[T].reverse
      if (ccFields.isEmpty)
        throw new RuntimeException("Schema not provided")
      ccFields.map(x => fields.add(Field.of(x._1, getBQType(x._2))))
      val s = Schema.of(fields)
      etl_logger.info(s"Schema provided: ${s.getFields.asScala.map(x => (x.getName, x.getType))}")
      s
    }.toOption

    val env = BQ.live(credentials)

    val program: Task[Unit] = {
      BQService.exportFromBQTable(
        source_project,source_dataset,
        source_table,destination_path,destination_file_name,destination_format,destination_compression_type
      ).provideLayer(env)
    }
    program *> UIO(etl_logger.info("#" * 50))
  }

  override def getExecutionMetrics: Map[String, Map[String, String]] = {
    Map(name ->
      Map(
        "total_rows" -> row_count.foldLeft(0L)((a, b) => a + b._2).toString
        // "total_size" -> destinationTable.map(x => s"${x.getNumBytes / 1000000.0} MB").getOrElse("error in getting size")
      )
    )
  }

  override def getStepProperties(level: LoggingLevel): Map[String, String] = {
//    if (level == LoggingLevel.INFO) {
//      Map(
//          "input_type" -> input_type.toString
//        , "input_location" -> input_location.fold(
//          source_path => source_path,
//          source_paths_partitions => source_paths_partitions.length.toString
//        )
//        , "output_dataset" -> output_dataset
//        , "output_table" -> output_table
//        , "output_table_write_disposition" -> output_write_disposition.toString
//        , "output_table_create_disposition" -> output_create_disposition.toString
//        , "output_rows" -> row_count.foldLeft(0L)((a, b) => a + b._2).toString
//      )
//    } else {
//      Map(
//        "input_type" -> input_type.toString
//        , "input_location" -> input_location.fold(
//          source_path => source_path,
//          source_paths_partitions => source_paths_partitions.mkString(",")
//        )
//        , "input_class" -> Try(UF.getFields[T].mkString(", ")).toOption.getOrElse("No Class Provided")
//        , "output_dataset" -> output_dataset
//        , "output_table" -> output_table
//        , "output_table_write_disposition" -> output_write_disposition.toString
//        , "output_table_create_disposition" -> output_create_disposition.toString
//        , "output_rows" -> row_count.map(x => x._1 + "<==>" + x._2.toString).mkString(",")
//      )
//    }
    Map.empty
  }
}

object BQExportStep {
  def apply[T <: Product : TypeTag]
  (name: String
   , source_project: Option[String] = None
   , source_dataset: String
   , source_table: String
   , destination_path: String
   , destination_file_name:Option[String] = None
   , destination_format:BQInputType
   , destination_compression_type:String = "gzip"
   , credentials: Option[Credential.GCP] = None
  ): BQExportStep[T] = {
    new BQExportStep[T](name, source_project, source_dataset, source_table, destination_path,destination_file_name
      ,destination_format,destination_compression_type, credentials)
  }
}

