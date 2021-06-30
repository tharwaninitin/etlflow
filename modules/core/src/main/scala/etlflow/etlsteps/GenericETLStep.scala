package etlflow.etlsteps

import zio.Task

case class GenericETLStep[IP,OP](name: String, transform_function: IP => OP) extends EtlStep[IP,OP]
{
  final def process(input: =>IP): Task[OP] = Task {
    logger.info("#################################################################################################")
    logger.info(s"Starting Generic ETL Step: $name")
    val op = transform_function(input)
    logger.info("#################################################################################################")
    op
  }
}


