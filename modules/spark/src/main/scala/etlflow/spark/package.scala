package etlflow

import etlflow.model.Credential.JDBC
import zio.{Has, Task}

package object spark {
  type SparkEnv = Has[SparkApi.Service[Task]]
  sealed trait IOType
  object IOType {
    final case class CSV(
        delimiter: String = ",",
        header_present: Boolean = true,
        parse_mode: String = "FAILFAST",
        quotechar: String = "\""
    ) extends IOType {
      override def toString: String =
        s"CSV with delimiter => $delimiter header_present => $header_present parse_mode => $parse_mode"
    }
    final case class MCSV(delimiter: String, no_of_columns: Int) extends IOType
    final case class JSON(multi_line: Boolean = false) extends IOType {
      override def toString: String = s"Json with multiline  => $multi_line"
    }
    final case class RDB(jdbc: JDBC, partition: Option[Partition] = None) extends IOType {
      override def toString: String = s"RDB with url => ${jdbc.url}"
    }
    final case class Partition(num_partition: Int, partition_column: String, lower_bound: String, upper_bound: String)
    final case class BQ(temp_dataset: String = "temp", operation_type: String = "table") extends IOType
    final case object PARQUET                                                            extends IOType
    final case object ORC                                                                extends IOType
    final case object TEXT                                                               extends IOType
  }

  sealed trait Environment
  object Environment {
    final case class GCP(service_account_key_path: String, project_id: String = "") extends Environment {
      override def toString: String = "****service_account_key_path****"
    }
    final case class AWS(access_key: String, secret_key: String) extends Environment {
      override def toString: String = "****access_key****secret_key****"
    }
    case object LOCAL extends Environment
  }
}
