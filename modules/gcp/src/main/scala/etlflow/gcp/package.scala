package etlflow

import com.google.cloud.bigquery.{Field, LegacySQLTypeName, Schema}
import etlflow.utils.ApplicationLogger
import zio.{Has, Tag, Task}
import java.util
import scala.jdk.CollectionConverters._
import scala.util.Try

package object gcp extends ApplicationLogger {

  type GCSEnv   = Has[GCSApi.Service[Task]]
  type BQEnv    = Has[BQApi.Service[Task]]
  type DPEnv    = Has[DPApi.Service[Task]]
  type DPJobEnv = Has[DPJobApi.Service[Task]]

  case class BQLoadException(msg: String) extends RuntimeException(msg)

  case class DataprocProperties(
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

  sealed trait BQInputType extends Serializable
  object BQInputType {
    case class CSV(
        delimiter: String = ",",
        header_present: Boolean = true,
        parse_mode: String = "FAILFAST",
        quotechar: String = "\""
    ) extends BQInputType {
      override def toString: String =
        s"CSV with delimiter => $delimiter header_present => $header_present parse_mode => $parse_mode"
    }
    case class JSON(multi_line: Boolean = false) extends BQInputType {
      override def toString: String = s"Json with multiline  => $multi_line"
    }
    case object BQ      extends BQInputType
    case object PARQUET extends BQInputType
    case object ORC     extends BQInputType
  }

  sealed trait FSType
  object FSType {
    case object LOCAL extends FSType
    case object GCS   extends FSType
  }

  sealed trait Location
  object Location {
    case class LOCAL(path: String)               extends Location
    case class GCS(bucket: String, path: String) extends Location
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

  def getFields[T: Tag]: Array[(String, String)] =
    implicitly[Tag[T]].closestClass.getDeclaredFields.map(f => (f.getName, f.getType.getName))

  def getBqSchema[T: Tag]: Option[Schema] =
    Try {
      val fields   = new util.ArrayList[Field]
      val ccFields = getFields[T]
      if (ccFields.isEmpty)
        throw new RuntimeException("Schema not provided")
      ccFields.map(x => fields.add(Field.of(x._1, getBQType(x._2))))
      val s = Schema.of(fields)
      logger.info(s"Schema provided: ${s.getFields.asScala.map(x => (x.getName, x.getType))}")
      s
    }.toOption
}
