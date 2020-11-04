package etlflow.utils

import etlflow.utils.EtlFlowHelper.QueueInfo
import org.slf4j.{Logger, LoggerFactory}
import zio.{Queue, Runtime, Task}

object QueueHelper {

  lazy val Queue_logger: Logger = LoggerFactory.getLogger(getClass.getName)

  def takeAll(jobQueue: Queue[(String,String)]): Task[List[QueueInfo]]  = {
    val queue = Runtime.default.unsafeRun(jobQueue.takeAll)
    var queue_list : List[QueueInfo] = List.empty
    queue.map(x => {queue_list =  QueueInfo(x._1,x._2) +: queue_list})
    //        jobQueue.offerAll(queue_list.map(x => (x.job_name,x.submitted_from)))
    //        queue_list.map( values => jobQueue.offer((values.job_name,values.submitted_from)) )
    Task(queue_list)
  }
}
