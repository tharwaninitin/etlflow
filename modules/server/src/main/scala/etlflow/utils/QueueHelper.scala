package etlflow.utils

import etlflow.utils.EtlFlowHelper.{QueueDetails, QueueInfo}
import org.slf4j.{Logger, LoggerFactory}
import zio.{Queue, Task}

object QueueHelper {

  lazy val Queue_logger: Logger = LoggerFactory.getLogger(getClass.getName)

  def takeAll(jobQueue: Queue[(String,String,String,String)]): Task[List[QueueDetails]]  = {
    for {
      list        <- jobQueue.takeAll
      queue_list  = list.map(x => {
        val queue_info = QueueInfo(x._1,x._2,x._3,x._4)
        var queue_list:List[QueueDetails] = List.empty
        queue_list = queue_list :+ QueueDetails(x._1,queue_info.props,x._2 )
        queue_list.head
      })
      _           <- jobQueue.offerAll(list)
    } yield queue_list
  }
}
