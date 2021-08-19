package examples.schema

import etlflow.EtlJobProps
import etlflow.schema.Executor.{DATAPROC, KUBERNETES, LOCAL_SUBPROCESS}

sealed trait MyEtlJobProps extends EtlJobProps

object MyEtlJobProps {

  val local_subprocess = LOCAL_SUBPROCESS("examples/target/docker/stage/opt/docker/bin/load-data",heap_min_memory = "-Xms100m",heap_max_memory = "-Xms100m")

  val dataproc   = DATAPROC("project-name","region","endpoint","cluster-name")

  val kubernetes = KUBERNETES(
    "etlflow:0.7.19",
    "default",
    Map(
      "GOOGLE_APPLICATION_CREDENTIALS"-> Option("<cred_file>"),
      "LOG_DB_URL"-> Option("jdbc:postgresql://host.docker.internal:5432/postgres"),
      "LOG_DB_USER"-> Option("<username>"),
      "LOG_DB_PWD"-> Option("<pwd>"),
      "LOG_DB_DRIVER"-> Option("org.postgresql.Driver")
    )
  )

  case class SampleProps() extends MyEtlJobProps
  case class LocalSampleProps() extends MyEtlJobProps
  case class EtlJob1Props (
                            ratings_input_path: List[String] = List(""),
                            ratings_intermediate_path: String = "",
                            ratings_output_dataset: String = "",
                            ratings_output_table_name: String = "",
                            ratings_output_file_name: Option[String] = Some("ratings.orc"),
                           ) extends MyEtlJobProps
  case class EtlJob23Props (
                             ratings_input_path: String = "",
                             ratings_output_dataset: String = "",
                             ratings_output_table_name: String = "",
                           ) extends MyEtlJobProps

  case class EtlJob4Props() extends MyEtlJobProps


  case class EtlJob5Props(
                            ratings_input_path: List[String] = List(""),
                            ratings_output_table: String = "",
                         ) extends MyEtlJobProps

  case class EtlJob6Props() extends MyEtlJobProps
}
