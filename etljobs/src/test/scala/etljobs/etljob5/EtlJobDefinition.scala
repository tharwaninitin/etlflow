package etljobs.etljob5

// Spark Imports
import etljobs.spark.SparkManager
import etljobs.utils.BQ
import org.apache.spark.sql.SaveMode
// EtlJob library specific Imports
import etljobs.EtlJob
import etljobs.{EtlJobProps, EtlStepList}
import etljobs.etlsteps.SparkReadWriteStep
import etljobs.utils.{JDBC, PARQUET, GlobalProperties}
// Job specific imports
import etljobs.schema.EtlJobProps.EtlJob5Props
import etljobs.schema.EtlJobSchemas.{RatingBQ,Rating}

case class EtlJobDefinition(job_properties: EtlJobProps, global_properties: Option[GlobalProperties] = None)
  extends EtlJob with SparkManager {

  val job_props = job_properties.asInstanceOf[EtlJob5Props]

  val step1 = SparkReadWriteStep[Rating](
    name             = "LoadRatingsParquetToJdbc",
    input_location   = job_props.ratings_input_path,
    input_columns    = Seq("user_id","movie_id","rating","timestamp"),
    input_type       = PARQUET,
    output_type      = JDBC(job_props.jdbc_url, job_props.jdbc_user, job_props.jdbc_password, job_props.jdbc_driver),
    output_location  = job_props.ratings_output_table,
    output_save_mode = SaveMode.Overwrite
  )(spark)

  val step2 = SparkReadWriteStep[RatingBQ](
    name             = "LoadRatingsBqToJdbc",
    input_location   = Seq("test.ratings"),
    input_columns    = Seq("user_id","movie_id","rating"),
    input_type       = BQ,
    output_type      = JDBC(job_props.jdbc_url, job_props.jdbc_user, job_props.jdbc_password, job_props.jdbc_driver),
    output_location  = job_props.ratings_output_table,
    output_save_mode = SaveMode.Overwrite
  )(spark)

  val etl_step_list = EtlStepList(step1,step2)
}

