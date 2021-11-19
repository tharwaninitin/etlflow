package etlflow.jobtests

import etlflow.EtlJobProps

object MyEtlJobProps {

  private val canonical_path = new java.io.File(".").getCanonicalPath

  case class EtlJob1Props() extends EtlJobProps

  private val delta_input_file_path = s"$canonical_path/modules/core/src/test/resources/delta_op"
  private val input_file_path = s"$canonical_path/modules/core/src/test/resources/input/movies/ratings_parquet/ratings.parquet"
  case class EtlJob2Props (
    ratings_input_path: List[String] = List(input_file_path),
    ratings_output_table_name: String = "ratings",
  ) extends EtlJobProps


  case class EtlJob23Props (
                             ratings_input_path: String = input_file_path,
                             ratings_output_dataset: String = "test",
                             ratings_output_table_name: String = "ratings"
                           ) extends EtlJobProps

  case class EtlJob3Props() extends EtlJobProps
  case class EtlJob4Props() extends EtlJobProps

  case class EtlJob5Props() extends EtlJobProps

  case class EtlJob6Props (
                            ratings_input_dataset: String = "test",
                            ratings_input_table_name: String = "ratings",
                            ratings_intermediate_bucket: String = s"gs://${sys.env("GCS_BUCKET")}/intermediate/ratings",
                            ratings_output_bucket_1: String = s"gs://${sys.env("GCS_BUCKET")}/output/ratings/csv",
                            ratings_output_bucket_2: String = s"s3a://${sys.env("S3_BUCKET")}/temp/output/ratings/parquet",
                            ratings_output_bucket_3: String = s"gs://${sys.env("GCS_BUCKET")}/output/ratings/json",
                            ratings_output_file_name: Option[String] = Some("ratings.csv"),
                          ) extends EtlJobProps

  case class EtlJobDeltaLake (
                              input_path: String = delta_input_file_path
                             ) extends EtlJobProps
}
