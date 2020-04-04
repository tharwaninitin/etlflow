package etljobs.schema

import etljobs.EtlJobProps

object MyEtlJobProps {
  sealed trait MyEtlJobProps extends EtlJobProps
  case class EtlJob1Props (
                            job_properties: Map[String,String] = Map.empty,
                            ratings_input_path: List[String] = List(""),
                            ratings_output_path: String = "",
                            ratings_output_dataset: String = "test",
                            ratings_output_table_name: String = "ratings",
                            ratings_output_file_name: Option[String] = Some("ratings.orc")
                          ) extends MyEtlJobProps
  case class EtlJob23Props (
                            job_properties: Map[String,String] = Map.empty,
                            ratings_input_path: String = "",
                            ratings_output_path: String = "",
                            ratings_output_dataset: String = "",
                            ratings_output_table_name: String = ""
                          ) extends MyEtlJobProps
  case class EtlJob4Props (
                           job_properties: Map[String,String] = Map.empty
                          ) extends MyEtlJobProps
  case class EtlJob5Props (
                            job_properties: Map[String,String] = Map.empty,
                            ratings_input_path: List[String] = List(""),
                            ratings_output_table: String = "ratings",
                            jdbc_user: String     = "",
                            jdbc_password: String = "",
                            jdbc_url: String      = "",
                            jdbc_driver: String   = ""
                          ) extends MyEtlJobProps
}
