package etlflow.etlsteps

import com.google.cloud.bigquery.{Field, JobInfo, LegacySQLTypeName, Schema}
import etlflow.gcp._
import etlflow.schema.{Credential, LoggingLevel}
import etlflow.utils.{ReflectAPI => RF}
import zio.{Tag, Task, UIO}

import java.util
import scala.collection.JavaConverters._
import scala.util.Try

class BQLoadStep[T <: Product : Tag] private[etlflow](
                                                       val name: String
                                                       , input_location: => Either[String, Seq[(String, String)]]
                                                       , input_type: BQInputType
                                                       , input_file_system: FSType = FSType.GCS
                                                       , output_project: Option[String] = None
                                                       , output_dataset: String
                                                       , output_table: String
                                                       , output_write_disposition: JobInfo.WriteDisposition = JobInfo.WriteDisposition.WRITE_TRUNCATE
                                                       , output_create_disposition: JobInfo.CreateDisposition = JobInfo.CreateDisposition.CREATE_NEVER
                                                       , credentials: Option[Credential.GCP] = None
                                                     )
  extends EtlStep[Unit, Unit] {
  var row_count: Map[String, Long] = Map.empty

  final def process(input: =>Unit): Task[Unit] = {
    logger.info("#"*50)
    logger.info(s"Starting BQ Data Load Step : $name")

    def getBQType(sp_type: String): LegacySQLTypeName = sp_type match {
      case "String"         => LegacySQLTypeName.STRING
      case "Int"            => LegacySQLTypeName.INTEGER
      case "Long"           => LegacySQLTypeName.INTEGER
      case "Double"         => LegacySQLTypeName.FLOAT
      case "java.sql.Date"  => LegacySQLTypeName.DATE
      case "java.util.Date" => LegacySQLTypeName.DATE
      case "Boolean"        => LegacySQLTypeName.BOOLEAN
      case _                => LegacySQLTypeName.STRING
    }

    val schema: Option[Schema] = Try{
      val fields = new util.ArrayList[Field]
      val ccFields = RF.getFields[T].toSeq.reverse
      if (ccFields.isEmpty)
        throw new RuntimeException("Schema not provided")
      ccFields.map(x => fields.add(Field.of(x._1, getBQType(x._2))))
      val s = Schema.of(fields)
      logger.info(s"Schema provided: ${s.getFields.asScala.map(x => (x.getName,x.getType))}")
      s
    }.toOption

    val env = BQ.live(credentials)

    val program: Task[Unit] = input_file_system match {
      case FSType.LOCAL =>
        logger.info(s"FileSystem: $input_file_system")
        BQService.loadIntoBQFromLocalFile(
          input_location, input_type, output_dataset, output_table,
          output_write_disposition, output_create_disposition
        ).provideLayer(env)
      case FSType.GCS =>
        input_location match {
          case Left(value) =>
            logger.info(s"FileSystem: $input_file_system")
            BQService.loadIntoBQTable(
              value, input_type, output_project, output_dataset, output_table, output_write_disposition,
              output_create_disposition, schema
            ).provideLayer(env).map{x =>
              row_count = x
            }
          case Right(value) =>
            logger.info(s"FileSystem: $input_file_system")
            BQService.loadIntoPartitionedBQTable(
              value, input_type, output_project, output_dataset, output_table, output_write_disposition,
              output_create_disposition, schema, 10).provideLayer(env).map{x =>
              row_count = x
            }
        }
    }
    program *> UIO(logger.info("#"*50))
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
    if (level == LoggingLevel.INFO)
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
        //        ,"input_class" -> Try(RF.getFields[T].mkString(", ")).toOption.getOrElse("No Class Provided")
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
  def apply[T <: Product : Tag]
  (name: String
   , input_location: => Either[String, Seq[(String, String)]]
   , input_type: BQInputType
   , input_file_system: FSType = FSType.GCS
   , output_project: Option[String] = None
   , output_dataset: String
   , output_table: String
   , output_write_disposition: JobInfo.WriteDisposition = JobInfo.WriteDisposition.WRITE_TRUNCATE
   , output_create_disposition: JobInfo.CreateDisposition = JobInfo.CreateDisposition.CREATE_NEVER
   , credentials: Option[Credential.GCP] = None
  ): BQLoadStep[T] = {
    new BQLoadStep[T](name, input_location, input_type, input_file_system, output_project
      , output_dataset, output_table, output_write_disposition, output_create_disposition, credentials)
  }
}