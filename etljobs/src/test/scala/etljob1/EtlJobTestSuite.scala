package etljob1

import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.sql.{Dataset,Row}
import etljobs.spark.ReadApi
import etljobs.utils.SessionManager
import etljobs.utils.{CSV, ORC, PARQUET, GlobalProperties}
import etljobs.etlsteps.SparkReadWriteStateStep
import etljobs.bigquery.QueryApi
import EtlJobSchemas.RatingOutput

class EtlJobTestSuite extends FlatSpec with Matchers {

  // STEP 1: Initialize job properties and create BQ tables required for jobs 
  val canonical_path = new java.io.File(".").getCanonicalPath
  val global_properties = new GlobalProperties(canonical_path + "/etljobs/src/test/resources/loaddata.properties") {}
  val props : Map[String,String] = Map(
    "job_name" -> "EtlJob1PARQUETtoORCtoBQLocalWith2StepsWithSlack",
    "ratings_input_path" -> s"$canonical_path/etljobs/src/test/resources/input/movies/ratings_parquet/*",
    "ratings_output_path" -> s"$canonical_path/etljobs/src/test/resources/output/movies/ratings",
    "ratings_output_dataset" -> "test",
    "ratings_output_table_name" -> "ratings",
    "ratings_output_file_name" -> "ratings.parquet"
    //,"aggregate_error" -> "true"
    //"parse_mode" -> "PERMISSIVE"
  )

  // STEP 2: Execute JOB
  val etljob = new EtlJobDefinition(props, global_properties)
  etljob.execute(send_slack_notification = true)

  // Could use Hlist here for getting single step out of job
  // To get errors in CSV if any, run single step like this
  // etljob.apply().filter(etl => etl.name == "LoadRatingsParquet").foreach{ etl =>
  //    etl.asInstanceOf[SparkReadWriteStep[Rating , Unit, RatingOutput, Unit]].showCorruptedData()
  //  }

  // STEP 3: Run tests
  val sm = new SessionManager(global_properties) {}
  val raw: Dataset[RatingOutput] = ReadApi.LoadDS[RatingOutput](
                                  Seq(props("ratings_input_path")), 
                                  PARQUET
                                )(sm.spark)
  val Row(sum_ratings: Double, count_ratings: Long) = raw.selectExpr("sum(rating)","count(*)").first()

  val destination_dataset = props("ratings_output_dataset")
  val destination_table = props("ratings_output_table_name")

  val query: String = s""" SELECT count(*) as count,sum(rating) sum_ratings 
                          FROM $destination_dataset.$destination_table """.stripMargin
  val result = QueryApi.getDataFromBQ(sm.bq, query)
  val count_records_bq: Long = result.head.get("count").getLongValue
  val sum_ratings_bq: Double = result.head.get("sum_ratings").getDoubleValue

  "Record counts" should "be matching in transformed DF and BQ table " in {
    assert(count_ratings==count_records_bq)
  }

  "Sum of ratings" should "be matching in transformed DF and BQ table " in {
    assert(sum_ratings==sum_ratings_bq)
  }

}


