package etlflow.spark

import org.apache.spark.sql.SparkSession

object SparkRuntimeConf {
  def apply(spark: SparkSession): Map[String, String] = {
    val runtimeConf = spark.sparkContext.getConf
    Map(
      "executor_memory"    -> runtimeConf.getOption("spark.executor.memory").getOrElse("NA"),
      "executor_cores"     -> runtimeConf.getOption("spark.executor.cores").getOrElse("NA"),
      "executor_instances" -> runtimeConf.getOption("spark.executor.instances").getOrElse("NA"),
      "driver_memory"      -> runtimeConf.getOption("spark.driver.memory").getOrElse("NA"),
      "yarn_memory"        -> runtimeConf.getOption("spark.yarn.am.memory").getOrElse("NA"),
      "parallelism"        -> runtimeConf.getOption("spark.default.parallelism").getOrElse("NA")
    )
  }
}
