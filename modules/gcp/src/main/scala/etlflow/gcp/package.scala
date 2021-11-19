package etlflow

import com.google.api.gax.paging.Page
import com.google.cloud.bigquery.{Field, FieldValueList, JobInfo, LegacySQLTypeName, Schema}
import com.google.cloud.storage.Blob
import com.google.cloud.storage.Storage.BlobListOption
import etlflow.utils.{ApplicationLogger, GetFields}
import zio.{Has, Tag, ZIO}
import java.util
import scala.jdk.CollectionConverters._
import scala.util.Try

package object gcp extends ApplicationLogger{

  private[etlflow] type GCSService = Has[GCSService.Service]
  sealed trait BQInputType extends Serializable
  object BQInputType {
    case class CSV(delimiter: String = ",", header_present: Boolean = true, parse_mode: String = "FAILFAST", quotechar: String = "\"") extends BQInputType {
      override def toString: String = s"CSV with delimiter => $delimiter header_present => $header_present parse_mode => $parse_mode"
    }
    case class JSON(multi_line: Boolean = false) extends BQInputType {
      override def toString: String = s"Json with multiline  => $multi_line"
    }
    case object BQ extends BQInputType
    case object PARQUET extends BQInputType
    case object ORC extends BQInputType
  }

  sealed trait FSType
  object FSType {
    case object LOCAL extends FSType
    case object GCS extends FSType
  }

  private[etlflow] object GCSService {
    trait Service {
      def listObjects(bucket: String, options: List[BlobListOption]): ZIO[GCSService, Throwable, Page[Blob]]
      def listObjects(bucket: String, prefix: String): ZIO[GCSService, Throwable, List[Blob]]
      def lookupObject(bucket: String, prefix: String, key: String): ZIO[GCSService, Throwable, Boolean]
      def putObject(bucket: String, key: String, file: String): ZIO[GCSService, Throwable, Blob]
    }
    def putObject(bucket: String, key: String, file: String): ZIO[GCSService, Throwable, Blob] = ZIO.accessM(_.get.putObject(bucket,key,file))
    def lookupObject(bucket: String, prefix: String, key: String): ZIO[GCSService, Throwable, Boolean] = ZIO.accessM(_.get.lookupObject(bucket,prefix,key))
    def listObjects(bucket: String, options: List[BlobListOption]): ZIO[GCSService, Throwable, Page[Blob]] = ZIO.accessM(_.get.listObjects(bucket,options))
    def listObjects(bucket: String, prefix: String): ZIO[GCSService, Throwable, List[Blob]] = ZIO.accessM(_.get.listObjects(bucket,prefix))
  }

  private[etlflow] type BQService = Has[BQService.Service]

  private[etlflow] object BQService {
    trait Service {
      def executeQuery(query: String): ZIO[BQService, Throwable, Unit]
      def getDataFromBQ(query: String): ZIO[BQService, Throwable, Iterable[FieldValueList]]
      def loadIntoBQFromLocalFile(
         source_locations: Either[String, Seq[(String, String)]], source_format: BQInputType, destination_dataset: String,
         destination_table: String, write_disposition: JobInfo.WriteDisposition, create_disposition: JobInfo.CreateDisposition
         ): ZIO[BQService, Throwable, Unit]
      def loadIntoBQTable(
          source_path: String, source_format: BQInputType, destination_project: Option[String], destination_dataset: String,
          destination_table: String, write_disposition: JobInfo.WriteDisposition, create_disposition: JobInfo.CreateDisposition,
          schema: Option[Schema] = None): ZIO[BQService, Throwable, Map[String, Long]]
      def loadIntoPartitionedBQTable(
          source_paths_partitions: Seq[(String, String)], source_format: BQInputType, destination_project: Option[String],
          destination_dataset: String, destination_table: String, write_disposition: JobInfo.WriteDisposition,
          create_disposition: JobInfo.CreateDisposition, schema: Option[Schema], parallelism: Int
        ): ZIO[BQService, Throwable, Map[String, Long]]
      def exportFromBQTable(
          source_project: Option[String], source_dataset: String,
          source_table: String, destination_path: String, destination_file_name:Option[String] ,
          destination_format: BQInputType, destination_compression_type:String = "gzip"
        ): ZIO[BQService, Throwable, Unit]

    }
    def getDataFromBQ(query: String): ZIO[BQService, Throwable, Iterable[FieldValueList]] =
      ZIO.accessM(_.get.getDataFromBQ(query))
    def executeQuery(query: String): ZIO[BQService, Throwable, Unit] =
      ZIO.accessM(_.get.executeQuery(query))
    def loadIntoBQFromLocalFile(
       source_locations: Either[String, Seq[(String, String)]], source_format: BQInputType, destination_dataset: String,
       destination_table: String, write_disposition: JobInfo.WriteDisposition, create_disposition: JobInfo.CreateDisposition
     ): ZIO[BQService, Throwable, Unit] =
      ZIO.accessM(_.get.loadIntoBQFromLocalFile(source_locations,source_format,destination_dataset,
        destination_table, write_disposition, create_disposition)
      )
    def loadIntoBQTable(
       source_path: String, source_format: BQInputType, destination_project: Option[String], destination_dataset: String, destination_table: String,
       write_disposition: JobInfo.WriteDisposition, create_disposition: JobInfo.CreateDisposition,
       schema: Option[Schema] = None): ZIO[BQService, Throwable, Map[String, Long]] =
      ZIO.accessM(_.get.loadIntoBQTable(source_path,source_format,destination_project,destination_dataset,
        destination_table,write_disposition,create_disposition,schema)
      )
    def loadIntoPartitionedBQTable(
        source_paths_partitions: Seq[(String, String)], source_format: BQInputType, destination_project: Option[String],
        destination_dataset: String, destination_table: String, write_disposition: JobInfo.WriteDisposition,
        create_disposition: JobInfo.CreateDisposition, schema: Option[Schema], parallelism: Int
      ): ZIO[BQService, Throwable, Map[String, Long]] =
      ZIO.accessM(_.get.loadIntoPartitionedBQTable(source_paths_partitions,source_format,destination_project,destination_dataset,
        destination_table,write_disposition,create_disposition,schema,parallelism)
      )
    def exportFromBQTable(
        source_project: Option[String], source_dataset: String,
        source_table: String, destination_path: String,destination_file_name:Option[String], destination_format: BQInputType,
        destination_compression_type:String = "gzip"
      ): ZIO[BQService, Throwable, Unit] =
      ZIO.accessM(_.get.exportFromBQTable(source_project,source_dataset,
        source_table,destination_path,destination_file_name,destination_format,destination_compression_type)
      )
  }

  private[etlflow] type DPService = Has[DPService.Service]

  case class DataprocProperties (
    bucket_name: String,
    subnet_uri: Option[String] = None,
    network_tags: List[String] = List.empty,
    service_account: Option[String] = None,
    idle_deletion_duration_sec: Option[Long] = Some(1800L),
    master_machine_type_uri: String = "n1-standard-4",
    worker_machine_type_uri: String = "n1-standard-4",
    image_version: String = "1.5.4-debian10",
    boot_disk_type: String = "pd-ssd",
    master_boot_disk_size_gb: Int = 400,
    worker_boot_disk_size_gb: Int = 200,
    master_num_instance: Int = 1,
    worker_num_instance: Int = 3
  )

  private[etlflow] object DPService {
    trait Service {
      def executeSparkJob(name: String, properties: Map[String,String], main_class: String, libs: List[String]): ZIO[DPService, Throwable, Unit]
      def executeHiveJob(query: String): ZIO[DPService, Throwable, Unit]
      def createDataproc(props: DataprocProperties): ZIO[DPService, Throwable, Unit]
      def deleteDataproc(): ZIO[DPService, Throwable, Unit]
    }
    def executeSparkJob(name: String, properties: Map[String,String], main_class: String, libs: List[String]): ZIO[DPService, Throwable, Unit] =
      ZIO.accessM(_.get.executeSparkJob(name, properties, main_class, libs))
    def executeHiveJob(query: String): ZIO[DPService, Throwable, Unit] = ZIO.accessM(_.get.executeHiveJob(query))
    def createDataproc(props: DataprocProperties): ZIO[DPService, Throwable, Unit] = ZIO.accessM(_.get.createDataproc(props))
    def deleteDataproc(): ZIO[DPService, Throwable, Unit] = ZIO.accessM(_.get.deleteDataproc())
  }

  def getBQType(sp_type: String): LegacySQLTypeName = sp_type match {
    case "string"         => LegacySQLTypeName.STRING
    case "int"            => LegacySQLTypeName.INTEGER
    case "long"           => LegacySQLTypeName.INTEGER
    case "double"         => LegacySQLTypeName.FLOAT
    case "java.sql.Date"  => LegacySQLTypeName.DATE
    case "java.util.Date" => LegacySQLTypeName.DATE
    case "boolean"        => LegacySQLTypeName.BOOLEAN
    case _                => LegacySQLTypeName.STRING

  }

  def getBqSchema[T: Tag]: Option[Schema] = {
    Try {
      val fields = new util.ArrayList[Field]
      val ccFields = GetFields[T]
      if (ccFields.isEmpty)
        throw new RuntimeException("Schema not provided")
      ccFields.map(x => fields.add(Field.of(x._1, getBQType(x._2))))
      val s = Schema.of(fields)
      logger.info(s"Schema provided: ${s.getFields.asScala.map(x => (x.getName, x.getType))}")
      s
    }.toOption
  }
}
