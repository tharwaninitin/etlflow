package etlflow.utils

import etlflow.utils.EtlFlowHelper.{EtlJobArgs, Props}
import org.slf4j.{Logger, LoggerFactory}

object RequestValidator {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  def validator(data:String) : Either[String,EtlJobArgs] = {
    logger.info("Requested stream is :" + data)
    var job_name = ""
    var props = ""
    var props_list: List[Props] = List.empty
    var request_output = ""
    val split_data = data.split("&")

    if (data != "") {
      if (split_data(0).split("=")(0) == "job_name") {
        job_name = split_data(0).split("=")(1)
        if(split_data.contains("props"))
          props = split_data(1).split("=")(1)
      } else if (split_data(0).split("=")(0) == "props") {
        props = split_data(0).split("=")(1)
        job_name = split_data(1).split("=")(1)
      } else {
        logger.warn("Invalid Request")
        request_output = "Invalid Request"
      }
    } else {
      logger.warn("Invalid Request")
      request_output = "Invalid Request"
    }

    if (request_output != "Invalid Request") {
      //convert recieved json into Map[String,String]
      if(split_data.contains("props")) {
        val expected_props = JsonCirce.convertToObject[Map[String, String]](props)

        expected_props foreach {
          case (k, v) => {
            props_list = Props(k, v) :: props_list
          }
        }
        Right(EtlJobArgs(job_name, props_list))
      }else{
        Right(EtlJobArgs(job_name, List.empty))
      }
    }else{
      Left(request_output)
    }
  }
}
