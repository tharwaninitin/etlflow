package etlflow.spark

import org.apache.spark.sql.SparkSession

object SparkMemoryConf {
  def apply(spark: SparkSession):  Map[String,String] = {
    val memoryConf = spark.sparkContext.getConf
    Map("executor_memory"-> memoryConf.get("spark.executor.memory")
      , "executor_cores"-> memoryConf.get("spark.executor.cores")
      , "executor_instances" -> memoryConf.get("spark.executor.instances")
      , "driver_memory" -> memoryConf.get("spark.driver.memory")
      , "yarn_memory" -> memoryConf.get("spark.yarn.am.memory")
      , "parallelism" -> memoryConf.get("spark.default.parallelism")
    )
  }
}
