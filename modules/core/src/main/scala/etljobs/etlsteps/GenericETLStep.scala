package etljobs.etlsteps

import zio.Task

class GenericETLStep (
                  val name: String
                  ,transform_function: () => Unit
                  )
extends EtlStep[Unit,Unit]
{
  def process(in: Unit): Task[Unit] = Task {
    etl_logger.info("#################################################################################################")
    etl_logger.info(s"Starting Generic ETL Step : $name")
    transform_function()
    etl_logger.info("#################################################################################################")
  }
}


