package etlflow.etljob2

import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.sql.{Dataset, Row}
import etlflow.spark.{ReadApi, SparkManager}
import etlflow.utils.{AppLogger, CSV, GlobalProperties}
import etlflow.bigquery.{BigQueryManager, QueryApi}
import examples.MyGlobalProperties
import examples.schema.MyEtlJobName.EtlJob2CSVtoPARQUETtoBQLocalWith3Steps
import examples.schema.MyEtlJobProps.EtlJob23Props
import examples.schema.MyEtlJobSchema.{Rating, RatingOutput}

class SequentialEtlJobTestSuite extends FlatSpec with Matchers {
//  AppLogger.initialize()
  // STEP 1: Initialize job properties and create BQ tables required for jobs 
  private val canonical_path = new java.io.File(".").getCanonicalPath
  private val global_props = new MyGlobalProperties(canonical_path + "/etljobs/src/test/resources/loaddata.properties") {}
  val job_props = EtlJob23Props(
    job_properties = Map.empty,
    ratings_input_path = s"$canonical_path/etljobs/src/test/resources/input/movies/ratings/*",
    ratings_output_dataset = "test",
    ratings_output_table_name = "ratings_par"
  )
  job_props.job_notification_level = "info"
  job_props.job_send_slack_notification = true

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
  val etljob = EtlJobDefinition(EtlJob2CSVtoPARQUETtoBQLocalWith3Steps.toString,job_properties=job_props, global_properties=Some(global_props))
  val state = etljob.execute
  println(state)

  // STEP 3: Run tests
  private val sm = new SparkManager {
    val global_properties: Option[GlobalProperties] = Some(global_props)
  }
  val raw: Dataset[Rating] = ReadApi.LoadDS[Rating](
                                  Seq(job_props.ratings_input_path),
                                  CSV()
                                )(sm.spark)
  val op: Dataset[RatingOutput] = etljob.enrichRatingData(sm.spark)(raw)
  val Row(sum_ratings: Double, count_ratings: Long) = op.selectExpr("sum(rating)","count(*)").first()

  val destination_dataset = job_props.ratings_output_dataset
  val destination_table = job_props.ratings_output_table_name

  val bqm = new BigQueryManager {
    val global_properties: Option[GlobalProperties] = Some(global_props)
  }
  val query:String = s""" SELECT count(*) as count,sum(rating) sum_ratings 
                          FROM $destination_dataset.$destination_table """.stripMargin
  val result = QueryApi.getDataFromBQ(bqm.bq, query)
  val count_records_bq:Long = result.head.get("count").getLongValue
  val sum_ratings_bq:Double = result.head.get("sum_ratings").getDoubleValue

  "Record counts" should "be matching in transformed DF and BQ table " in {
    assert(count_ratings==count_records_bq)
  }

  "Sum of ratings" should "be matching in transformed DF and BQ table " in {
    assert(sum_ratings==sum_ratings_bq)
  }

}


