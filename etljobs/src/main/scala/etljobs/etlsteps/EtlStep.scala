package etljobs.etlsteps

import org.apache.log4j.Logger
import scala.util.Try

trait EtlStep[IPSTATE,OPSTATE] {
  val name: String
  val etl_logger : Logger = Logger.getLogger(getClass.getName)

  def process(input_state: IPSTATE) : Try[OPSTATE]
  def getExecutionMetrics: Map[String,Map[String,String]] = Map()
  def getStepProperties(level: String = "info"): Map[String,String] = Map()
}
