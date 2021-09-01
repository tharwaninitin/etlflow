package examples.schema

import etlflow.{EtlJobProps, EtlJobPropsMapping}
import etlflow.etljobs.EtlJob
import examples.jobs._
import examples.schema.MyEtlJobProps._
import io.circe.generic.auto._

sealed trait MyEtlJobPropsMapping[EJP <: EtlJobProps, EJ <: EtlJob[EJP]] extends EtlJobPropsMapping[EJP,EJ]

object MyEtlJobPropsMapping {
  val default_ratings_input_path = "examplespark/src/main/data/movies/ratings_parquet/ratings.parquet"
  val default_ratings_intermediate_path = "examplespark/src/main/data/movies/output"
  val default_ratings_input_path_csv = "examplespark/src/main/data/movies/ratings/*"
  val default_output_dataset = "dev"

  case object PARQUETtoORCJob extends MyEtlJobPropsMapping[EtlJob1Props,EtlJobParquetToOrc] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob1Props = EtlJob1Props(
      ratings_input_path = List(job_properties.getOrElse("ratings_input_path", default_ratings_input_path)),
      ratings_intermediate_path = job_properties.getOrElse("ratings_intermediate_path", default_ratings_intermediate_path),
      ratings_output_file_name = Some(job_properties.getOrElse("ratings_output_file_name", "ratings.orc"))
    )
  }
  case object CSVtoPARQUETJob extends MyEtlJobPropsMapping[EtlJob2Props,EtlJobCsvToParquet] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob2Props = EtlJob2Props(
      ratings_input_path = job_properties.getOrElse("ratings_input_pat",default_ratings_input_path_csv),
    )

  }
  case object CSVtoCSVGcsJob extends MyEtlJobPropsMapping[EtlJob3Props,EtlJobCsvToCsvGcs] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob3Props = EtlJob3Props(
      ratings_input_path = job_properties.getOrElse("ratings_input_path",default_ratings_input_path_csv),
      ratings_output_dataset = job_properties.getOrElse("ratings_output_dataset",default_output_dataset),
      ratings_output_table_name = job_properties.getOrElse("ratings_output_table_name","ratings_par")
    )

  }
  case object PARQUETtoJDBCJob extends MyEtlJobPropsMapping[EtlJob4Props,EtlJobParquetToJdbc] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob4Props = EtlJob4Props(
      ratings_input_path = List(job_properties.getOrElse("ratings_input_path",default_ratings_input_path)),
      ratings_output_table = job_properties.getOrElse("ratings_output_table_name","ratings")
    )
  }
}
