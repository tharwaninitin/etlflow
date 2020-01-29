package etljob3

import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.sql.{Dataset,Row}
import etljobs.spark.ReadApi
import etljobs.utils.SessionManager
import etljobs.utils.{CSV, PARQUET, GlobalProperties, SessionManager}
import etljobs.etlsteps.SparkReadWriteStateStep
import etljobs.etlsteps.SparkReadWriteStateStep.{Input,Output}
import etljobs.bigquery.QueryApi
import EtlJobSchemas.{Rating,RatingOutput}

class EtlJobTestSuite extends FlatSpec with Matchers {

  // STEP 1: Initialize job properties and create BQ tables required for jobs 
  val canonical_path = new java.io.File(".").getCanonicalPath
  val global_properties = new MyGlobalProperties(canonical_path + "/etljobs/src/test/resources/loaddata.properties")
  val props : Map[String,String] = Map(
    "job_name" -> "EtlJob3CSVtoPARQUETtoBQGcsWith2Steps",
    "ratings_input_path" -> f"$canonical_path/etljobs/src/test/resources/input/movies/ratings/*",
    "ratings_output_path" -> f"gs://${global_properties.gcs_output_bucket}/output/ratings",
    "ratings_output_dataset" -> "test",
    "ratings_output_table_name" -> "ratings_par"
  )
  val create_table_script = """
  CREATE TABLE test.ratings_par (
    user_id INT64
  , movie_id INT64
  , rating FLOAT64
  , timestamp INT64
  , date DATE
  ) PARTITION BY date
  """

  // STEP 2: Execute JOB
  val etljob = new EtlJobDefinition(props, Some(global_properties))
  val state = etljob.execute(send_slack_notification = true, slack_notification_level = "info")
  println(state)

  // STEP 3: Run tests
  val sm = new SessionManager(Some(global_properties)) {}
  val raw : Dataset[Rating] = ReadApi.LoadDS[Rating](
                                  Seq(props("ratings_input_path")), 
                                  CSV(",", true, props.getOrElse("parse_mode","FAILFAST"))
                                )(sm.spark)
  val op : Output[RatingOutput,Unit] = etljob.enrichRatingData(sm.spark, props)(Input[Rating,Unit](raw,()))
  val Row(sum_ratings: Double, count_ratings: Long) = op.ds.selectExpr("sum(rating)","count(*)").first()

  val destination_dataset = props("ratings_output_dataset")
  val destination_table = props("ratings_output_table_name")

  val query:String = s""" select count(*) as count,sum(rating) sum_ratings from $destination_dataset.$destination_table """.stripMargin
  val result = QueryApi.getDataFromBQ(sm.bq, query)
  val count_records_bq:Long = result.head.get("count").getLongValue
  val sum_ratings_bq:Double = result.head.get("sum_ratings").getDoubleValue

  "Record counts" should "be matching in transformed DF and BQ table " in {
    assert(count_ratings==count_records_bq)
  }

  "Sum of ratings" should "be matching in transformed DF and BQ table " in {
    assert(sum_ratings==sum_ratings_bq)
  }

}


