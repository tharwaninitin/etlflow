package examples.jobs

import etljobs.EtlStepList
import etljobs.etljob.SequentialEtlJob
import etljobs.etlsteps.{EtlStep, SparkReadWriteStep}
import etljobs.utils.{BQ, JDBC, PARQUET}
import examples.MyGlobalProperties
import examples.schema.MyEtlJobProps
import examples.schema.MyEtlJobProps.EtlJob5Props
import examples.schema.MyEtlJobSchema.{Rating, RatingBQ}
import org.apache.spark.sql.SaveMode

case class EtlJob5Definition(job_properties: MyEtlJobProps, global_properties: Option[MyGlobalProperties]) extends SequentialEtlJob {

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
  )

  private val step2 = SparkReadWriteStep[RatingBQ](
    name             = "LoadRatingsBqToJdbc",
    input_location   = Seq("test.ratings"),
    input_columns    = Seq("user_id","movie_id","rating"),
    input_type       = BQ,
    output_type      = JDBC(global_props.jdbc_url, global_props.jdbc_user, global_props.jdbc_pwd, global_props.jdbc_driver),
    output_location  = job_props.ratings_output_table,
    output_save_mode = SaveMode.Overwrite
  )

  val etl_step_list: List[EtlStep[_,_]] = EtlStepList(step1,step2)
}
