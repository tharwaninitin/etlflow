package etlflow.task

import etlflow.utils.ApplicationLogger

trait EtlTask extends ApplicationLogger {
  val name: String
  val stepType: String = this.getClass.getSimpleName

  def getExecutionMetrics: Map[String, String] = Map.empty[String, String]
  def getStepProperties: Map[String, String]   = Map.empty[String, String]
}
