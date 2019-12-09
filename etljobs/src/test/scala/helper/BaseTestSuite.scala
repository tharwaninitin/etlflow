package helper

import com.google.cloud.bigquery.BigQueryOptions
import org.apache.spark.sql.SparkSession
import etljobs.utils.Settings
import org.apache.log4j.{Level, Logger}

trait BaseTestSuite {
  Logger.getLogger("org").setLevel(Level.WARN)
  lazy val spark  = SparkSession.builder().master("local[*]").getOrCreate()
  lazy val bq = BigQueryOptions.getDefaultInstance.getService
  val canonical_path = new java.io.File(".").getCanonicalPath
  val prop_path = canonical_path + "/etljobs/src/test/resources/loaddata_test.properties"
  val ss_test =  new Settings(prop_path)
}
