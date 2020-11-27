package etlflow.utils

import etlflow.utils.EtlFlowHelper.{EtlJobArgs, Props}
import org.slf4j.{Logger, LoggerFactory}

object RequestValidator {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  def validator(job_name:String,props:Option[String]) : Either[String,EtlJobArgs] = {

    var props_list: List[Props] = List.empty
    if(props.isDefined) {
      val expected_props = JsonCirce.convertToObject[Map[String, String]](props.get.replaceAll("\\(","\\{").replaceAll("\\)","\\}"))
      expected_props foreach {
        case (k, v) => {
          props_list = Props(k, v) :: props_list
        }
      }
      Right(EtlJobArgs(job_name, props_list))
    }else{
      Right(EtlJobArgs(job_name, List.empty))
    }
  }
}

