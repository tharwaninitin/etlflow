package etlflow.gcp

import com.google.cloud.bigquery.{FieldValueList, JobInfo, Schema}
import zio.ZIO

object BQApi {
  trait Service[F[_]] {
    def executeQuery(query: String): F[Unit]
    def getDataFromBQ(query: String): F[Iterable[FieldValueList]]
    def loadIntoBQFromLocalFile(
        source_locations: Either[String, Seq[(String, String)]],
        source_format: BQInputType,
        destination_dataset: String,
        destination_table: String,
        write_disposition: JobInfo.WriteDisposition,
        create_disposition: JobInfo.CreateDisposition
    ): F[Unit]
    def loadIntoBQTable(
        source_path: String,
        source_format: BQInputType,
        destination_project: Option[String],
        destination_dataset: String,
        destination_table: String,
        write_disposition: JobInfo.WriteDisposition,
        create_disposition: JobInfo.CreateDisposition,
        schema: Option[Schema] = None
    ): F[Map[String, Long]]
    def loadIntoPartitionedBQTable(
        source_paths_partitions: Seq[(String, String)],
        source_format: BQInputType,
        destination_project: Option[String],
        destination_dataset: String,
        destination_table: String,
        write_disposition: JobInfo.WriteDisposition,
        create_disposition: JobInfo.CreateDisposition,
        schema: Option[Schema],
        parallelism: Int
    ): F[Map[String, Long]]
    def exportFromBQTable(
        source_project: Option[String],
        source_dataset: String,
        source_table: String,
        destination_path: String,
        destination_file_name: Option[String],
        destination_format: BQInputType,
        destination_compression_type: String = "gzip"
    ): F[Unit]

  }
  def getDataFromBQ(query: String): ZIO[BQEnv, Throwable, Iterable[FieldValueList]] =
    ZIO.accessM(_.get.getDataFromBQ(query))
  def executeQuery(query: String): ZIO[BQEnv, Throwable, Unit] = ZIO.accessM(_.get.executeQuery(query))
  def loadIntoBQFromLocalFile(
      source_locations: Either[String, Seq[(String, String)]],
      source_format: BQInputType,
      destination_dataset: String,
      destination_table: String,
      write_disposition: JobInfo.WriteDisposition,
      create_disposition: JobInfo.CreateDisposition
  ): ZIO[BQEnv, Throwable, Unit] =
    ZIO.accessM(
      _.get.loadIntoBQFromLocalFile(
        source_locations,
        source_format,
        destination_dataset,
        destination_table,
        write_disposition,
        create_disposition
      )
    )
  def loadIntoBQTable(
      source_path: String,
      source_format: BQInputType,
      destination_project: Option[String],
      destination_dataset: String,
      destination_table: String,
      write_disposition: JobInfo.WriteDisposition,
      create_disposition: JobInfo.CreateDisposition,
      schema: Option[Schema] = None
  ): ZIO[BQEnv, Throwable, Map[String, Long]] =
    ZIO.accessM(
      _.get.loadIntoBQTable(
        source_path,
        source_format,
        destination_project,
        destination_dataset,
        destination_table,
        write_disposition,
        create_disposition,
        schema
      )
    )
  def loadIntoPartitionedBQTable(
      source_paths_partitions: Seq[(String, String)],
      source_format: BQInputType,
      destination_project: Option[String],
      destination_dataset: String,
      destination_table: String,
      write_disposition: JobInfo.WriteDisposition,
      create_disposition: JobInfo.CreateDisposition,
      schema: Option[Schema],
      parallelism: Int
  ): ZIO[BQEnv, Throwable, Map[String, Long]] =
    ZIO.accessM(
      _.get.loadIntoPartitionedBQTable(
        source_paths_partitions,
        source_format,
        destination_project,
        destination_dataset,
        destination_table,
        write_disposition,
        create_disposition,
        schema,
        parallelism
      )
    )
  def exportFromBQTable(
      source_project: Option[String],
      source_dataset: String,
      source_table: String,
      destination_path: String,
      destination_file_name: Option[String],
      destination_format: BQInputType,
      destination_compression_type: String = "gzip"
  ): ZIO[BQEnv, Throwable, Unit] =
    ZIO.accessM(
      _.get.exportFromBQTable(
        source_project,
        source_dataset,
        source_table,
        destination_path,
        destination_file_name,
        destination_format,
        destination_compression_type
      )
    )
}
