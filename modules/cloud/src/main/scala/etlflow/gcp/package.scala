package etlflow

import com.google.api.gax.paging.Page
import com.google.cloud.bigquery.{FieldValueList, JobInfo, Schema}
import com.google.cloud.storage.Blob
import com.google.cloud.storage.Storage.BlobListOption
import etlflow.utils.IOType
import org.slf4j.{Logger, LoggerFactory}
import zio.{Has, ZIO}

package object gcp {
  val gcp_logger: Logger = LoggerFactory.getLogger(getClass.getName)

  type GCSService = Has[GCSService.Service]

  object GCSService {
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

  type BQService = Has[BQService.Service]

  object BQService {
    trait Service {
      def executeQuery(query: String): ZIO[BQService, Throwable, Unit]
      def getDataFromBQ(query: String): ZIO[BQService, Throwable, Iterable[FieldValueList]]
      def loadIntoBQFromLocalFile(
                                   source_locations: Either[String, Seq[(String, String)]], source_format: IOType, destination_dataset: String,
                                   destination_table: String, write_disposition: JobInfo.WriteDisposition, create_disposition: JobInfo.CreateDisposition
                                 ): ZIO[BQService, Throwable, Unit]
      def loadIntoBQTable(
                           source_path: String, source_format: IOType,destination_project: Option[String], destination_dataset: String, destination_table: String,
                           write_disposition: JobInfo.WriteDisposition, create_disposition: JobInfo.CreateDisposition,
                           schema: Option[Schema] = None): ZIO[BQService, Throwable, Map[String, Long]]
      def loadIntoPartitionedBQTable(
                                      source_paths_partitions: Seq[(String, String)], source_format: IOType,destination_project: Option[String],
                                      destination_dataset: String, destination_table: String, write_disposition: JobInfo.WriteDisposition,
                                      create_disposition: JobInfo.CreateDisposition, schema: Option[Schema], parallelism: Int
                                    ): ZIO[BQService, Throwable, Map[String, Long]]
    }
    def getDataFromBQ(query: String): ZIO[BQService, Throwable, Iterable[FieldValueList]] =
      ZIO.accessM(_.get.getDataFromBQ(query))
    def executeQuery(query: String): ZIO[BQService, Throwable, Unit] =
      ZIO.accessM(_.get.executeQuery(query))
    def loadIntoBQFromLocalFile(
                                 source_locations: Either[String, Seq[(String, String)]], source_format: IOType, destination_dataset: String,
                                 destination_table: String, write_disposition: JobInfo.WriteDisposition, create_disposition: JobInfo.CreateDisposition
                               ): ZIO[BQService, Throwable, Unit] =
      ZIO.accessM(_.get.loadIntoBQFromLocalFile(source_locations,source_format,destination_dataset,
        destination_table, write_disposition, create_disposition)
      )
    def loadIntoBQTable(
                         source_path: String, source_format: IOType,destination_project: Option[String], destination_dataset: String, destination_table: String,
                         write_disposition: JobInfo.WriteDisposition, create_disposition: JobInfo.CreateDisposition,
                         schema: Option[Schema] = None): ZIO[BQService, Throwable, Map[String, Long]] =
      ZIO.accessM(_.get.loadIntoBQTable(source_path,source_format,destination_project,destination_dataset,
        destination_table,write_disposition,create_disposition,schema)
      )
    def loadIntoPartitionedBQTable(
                                    source_paths_partitions: Seq[(String, String)], source_format: IOType,destination_project: Option[String],
                                    destination_dataset: String, destination_table: String, write_disposition: JobInfo.WriteDisposition,
                                    create_disposition: JobInfo.CreateDisposition, schema: Option[Schema], parallelism: Int
                                  ): ZIO[BQService, Throwable, Map[String, Long]] =
      ZIO.accessM(_.get.loadIntoPartitionedBQTable(source_paths_partitions,source_format,destination_project,destination_dataset,
        destination_table,write_disposition,create_disposition,schema,parallelism)
      )
  }

  type DPService = Has[DPService.Service]

  object DPService {
    trait Service {
      def executeSparkJob(name: String, properties: Map[String,String], main_class: String, libs: List[String]): ZIO[DPService, Throwable, Unit]
      def executeHiveJob(query: String): ZIO[DPService, Throwable, Unit]
    }
    def executeSparkJob(name: String, properties: Map[String,String], main_class: String, libs: List[String]): ZIO[DPService, Throwable, Unit] =
      ZIO.accessM(_.get.executeSparkJob(name, properties, main_class, libs))
    def executeHiveJob(query: String): ZIO[DPService, Throwable, Unit] = ZIO.accessM(_.get.executeHiveJob(query))
  }
}
