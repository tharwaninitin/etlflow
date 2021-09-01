package examples.jobs

import etlflow.etljobs.GenericEtlJob
import etlflow.etlsteps.SparkReadTransformWriteStep
import etlflow.spark.Environment.{GCP, LOCAL}
import etlflow.spark.{IOType, SparkManager}
import examples.schema.MyEtlJobProps.EtlJob3Props
import examples.schema.MyEtlJobSchema.{Rating, RatingOutput}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, from_unixtime, unix_timestamp}
import org.apache.spark.sql.types.DateType

case class EtlJobCsvToCsvGcs(job_properties: EtlJob3Props) extends GenericEtlJob[EtlJob3Props] {

    private val gcs_output_path = f"gs://${sys.env("GCS_BUCKET")}/output/ratings1"
  private var output_date_paths : Seq[(String,String)] = Seq()
  private val temp_date_col = "temp_date_col"
  private val job_props:EtlJob3Props  = job_properties

  private implicit val spark: SparkSession = SparkManager.createSparkSession(Set(LOCAL,GCP(sys.env("GOOGLE_APPLICATION_CREDENTIALS"), sys.env("GCP_PROJECT_ID"))), hive_support = false)

  val get_formatted_date: (String,String,String) => Column
  = (ColumnName:String,ExistingFormat:String,NewFormat:String) => {
    from_unixtime(unix_timestamp(col(ColumnName), ExistingFormat), NewFormat)
  }

  private def enrichRatingData(spark: SparkSession, in: Dataset[Rating]): Dataset[RatingOutput] = {

    import spark.implicits._

    val ratings_df = in
        .withColumn("date", from_unixtime(col("timestamp"), "yyyy-MM-dd").cast(DateType))
        .withColumn(temp_date_col, get_formatted_date("date","yyyy-MM-dd","yyyyMMdd"))
        .where(f"$temp_date_col in ('20160101', '20160102')")

    output_date_paths = ratings_df
        .select(f"$temp_date_col")
        .distinct()
        .as[String]
        .collect()
        .map(date => (gcs_output_path + f"/$temp_date_col=" + date + "/part*", date))

    ratings_df.drop(f"$temp_date_col")

    val mapping = Encoders.product[RatingOutput]
    val ratings_ds = ratings_df.as[RatingOutput](mapping)
    ratings_ds
  }

  private val step1 = SparkReadTransformWriteStep[Rating, RatingOutput](
    name                  = "LoadRatingsParquet",
    input_location        = Seq(job_props.ratings_input_path),
    input_type            = IOType.CSV(),
    transform_function    = enrichRatingData,
    output_type           = IOType.CSV(),
    output_location       = gcs_output_path,
    output_partition_col  = Seq(f"$temp_date_col"),
    output_save_mode      = SaveMode.Overwrite
  )

  val job = for {
    - <- step1.execute()
   } yield ()
}
