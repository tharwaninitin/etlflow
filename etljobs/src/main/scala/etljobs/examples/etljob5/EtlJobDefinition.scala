package etljobs.examples.etljob5

// Spark Imports
import etljobs.EtlJobName
import etljobs.examples.MyGlobalProperties
import etljobs.examples.schema.MyEtlJobProps
import etljobs.spark.SparkManager
import etljobs.utils.BQ
import org.apache.spark.sql.SaveMode
// EtlJob library specific Imports
import etljobs.EtlJob
import etljobs.{EtlJobProps, EtlStepList}
import etljobs.etlsteps.SparkReadWriteStep
import etljobs.utils.{JDBC, PARQUET, GlobalProperties}
// Job specific imports
import etljobs.examples.schema.MyEtlJobProps.EtlJob5Props
import etljobs.examples.schema.MyEtlJobSchema.{RatingBQ,Rating}

case class EtlJobDefinition(job_properties: MyEtlJobProps, global_properties: Option[MyGlobalProperties]) extends EtlJob with SparkManager {

  private val global_props = global_properties.get
  private val job_props = job_properties.asInstanceOf[EtlJob5Props]

  private val step1 = SparkReadWriteStep[Rating](
    name             = "LoadRatingsParquetToJdbc",
    input_location   = job_props.ratings_input_path,
    input_columns    = Seq("user_id","movie_id","rating","timestamp"),
    input_type       = PARQUET,
    output_type      = JDBC(global_props.jdbc_url, global_props.jdbc_user, global_props.jdbc_pwd, global_props.jdbc_driver),
    output_location  = job_props.ratings_output_table,
    output_save_mode = SaveMode.Overwrite
  )(spark)

  private val step2 = SparkReadWriteStep[RatingBQ](
    name             = "LoadRatingsBqToJdbc",
    input_location   = Seq("test.ratings"),
    input_columns    = Seq("user_id","movie_id","rating"),
    input_type       = BQ,
    output_type      = JDBC(global_props.jdbc_url, global_props.jdbc_user, global_props.jdbc_pwd, global_props.jdbc_driver),
    output_location  = job_props.ratings_output_table,
    output_save_mode = SaveMode.Overwrite
  )(spark)

  val etl_step_list = EtlStepList(step1,step2)
}

