package etlflow.task

import etlflow.utils.ApplicationLogger

trait EtlTask extends ApplicationLogger {
  val name: String
  val taskType: String = this.getClass.getSimpleName

  def getExecutionMetrics: Map[String, String] = Map.empty[String, String]
  def getTaskProperties: Map[String, String]   = Map.empty[String, String]
}
