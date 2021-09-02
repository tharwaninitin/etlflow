package examples.schema

import etlflow.EtlJobProps
import etlflow.schema.Executor.{DATAPROC, KUBERNETES, LOCAL_SUBPROCESS}

sealed trait MyEtlJobProps extends EtlJobProps

object MyEtlJobProps {

  val local_subprocess = LOCAL_SUBPROCESS("examples/target/docker/stage/opt/docker/bin/load-data",heap_min_memory = "-Xms100m",heap_max_memory = "-Xms100m")

  val dataproc   = DATAPROC("project-name","region","endpoint","cluster-name")

  case class EtlJob1Props (
                            ratings_input_path: List[String] = List(""),
                            ratings_intermediate_path: String = "",
                            ratings_output_file_name: Option[String] = Some("ratings.orc"),
                           ) extends MyEtlJobProps

  case class EtlJob2Props (ratings_input_path: String = "") extends MyEtlJobProps

  case class EtlJob3Props (
                             ratings_input_path: String = "",
                             ratings_output_dataset: String = "",
                             ratings_output_table_name: String = "",
                           ) extends MyEtlJobProps

  case class EtlJob4Props(
                            ratings_input_path: List[String] = List(""),
                            ratings_output_table: String = "",
                         ) extends MyEtlJobProps
}
