package etljobs.etlsteps

import org.apache.spark.sql.SparkSession
import scala.util.Try

class SparkETLStep(
                  val name : String
                  ,transform_function : () => Unit
                  )(spark : => SparkSession, etl_metadata : Map[String, String])
extends EtlStep[Unit,Unit]
{
  def process(input_state : Unit): Try[Unit] = {
    Try{
      etl_logger.info("#################################################################################################")
      etl_logger.info(s"Starting ETL Step : $name")
      transform_function()
      etl_logger.info("#################################################################################################")
    }
  }
}
