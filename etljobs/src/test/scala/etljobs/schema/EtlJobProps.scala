package etljobs.schema

import etljobs.{EtlJobName, EtlProps}

object EtlJobProps {
  sealed trait MyEtlProps extends EtlProps
  case class EtlJob1Props (
                            job_run_id: String,
                            job_name: EtlJobName,
                            ratings_input_path: String,
                            ratings_output_path: String,
                            ratings_output_dataset: String,
                            ratings_output_table_name: String,
                            ratings_output_file_name: String
                          ) extends MyEtlProps
  case class EtlJob23Props (
                            job_run_id: String,
                            job_name: EtlJobName,
                            ratings_input_path: String,
                            ratings_output_path: String,
                            ratings_output_dataset: String,
                            ratings_output_table_name: String
                          ) extends MyEtlProps
}
