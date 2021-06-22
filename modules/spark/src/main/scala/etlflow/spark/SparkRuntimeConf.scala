package etlflow.spark

import org.apache.spark.sql.SparkSession

object SparkRuntimeConf {
  def apply(spark: SparkSession):  Map[String,String] = {
    val runtimeConf = spark.sparkContext.getConf
    Map("executor_memory"-> runtimeConf.get("spark.executor.memory")
      , "executor_cores"-> runtimeConf.get("spark.executor.cores")
      , "executor_instances" -> runtimeConf.get("spark.executor.instances")
      , "driver_memory" -> runtimeConf.get("spark.driver.memory")
      , "yarn_memory" -> runtimeConf.get("spark.yarn.am.memory")
      , "parallelism" -> runtimeConf.get("spark.default.parallelism")
    )
  }
}
