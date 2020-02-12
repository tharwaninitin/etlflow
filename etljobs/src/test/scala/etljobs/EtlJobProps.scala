package etljobs

object EtlJobProps {
  case class EtlJob3Props (
      job_run_id: String,
      job_name: EtlJobName,
      ratings_input_path: String,
      ratings_output_path: String,
      ratings_output_dataset: String,
      ratings_output_table_name: String
    ) extends EtlProps
}
