package etlflow.utils

import etlflow.utils.EtlFlowHelper.QueueInfo
import org.slf4j.{Logger, LoggerFactory}
import zio.{Queue, Task}

object QueueHelper {

  lazy val Queue_logger: Logger = LoggerFactory.getLogger(getClass.getName)

  def takeAll(jobQueue: Queue[(String,String)]): Task[List[QueueInfo]]  = {
    for {
      list        <- jobQueue.takeAll
      queue_list  = list.map(x => QueueInfo(x._1,x._2))
      _           <- jobQueue.offerAll(list)
    } yield queue_list
  }
}
