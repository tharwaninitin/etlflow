package etlflow.utils

import etlflow.api.Schema.QueueDetails
import zio.{Queue, Task}

private [etlflow] object QueueHelper {

  def takeAll(jobQueue: Queue[(String,String,String,String)]): Task[List[QueueDetails]]  = {
    for {
      list        <- jobQueue.takeAll
      queue_list  = list.map(x => QueueDetails(x._1,x._3,x._2,x._4)).reverse
      _           <- jobQueue.offerAll(list)
    } yield queue_list
  }
}
