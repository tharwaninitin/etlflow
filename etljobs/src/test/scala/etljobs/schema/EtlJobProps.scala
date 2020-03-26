package etljobs.schema

import etljobs.utils.{UtilityFunctions => UF}
import etljobs.{EtlJobName, EtlJobProps}

object EtlJobProps {
  sealed trait MyEtlJobProps extends EtlJobProps
  case class EtlJob1Props (
                            job_name: EtlJobName,
                            job_properties: Map[String,String] = Map.empty,
                            ratings_input_path: List[String] = List(""),
                            ratings_output_path: String = "",
                            ratings_output_dataset: String = "",
                            ratings_output_table_name: String = "",
                            ratings_output_file_name: Option[String] = Some("ratings.orc")
                          ) extends MyEtlJobProps
  case class EtlJob23Props (
                            job_name: EtlJobName,
                            job_properties: Map[String,String] = Map.empty,
                            ratings_input_path: String = "",
                            ratings_output_path: String = "",
                            ratings_output_dataset: String = "",
                            ratings_output_table_name: String = ""
                          ) extends MyEtlJobProps

  val catOnePropsList  =  UF.getEtlJobProps[EtlJob1Props]()
  val catTwoPropsList  =  UF.getEtlJobProps[EtlJob23Props]()
}
