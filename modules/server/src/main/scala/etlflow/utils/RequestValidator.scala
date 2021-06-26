package etlflow.utils

import etlflow.api.Schema.{EtlJobArgs, Props}
import etlflow.json.{JsonApi, JsonEnv}
import zio.{RIO, Task}

private [etlflow] object RequestValidator {

  def apply(job_name: String, props: Option[String]): RIO[JsonEnv, EtlJobArgs] = {
    if(props.isDefined) {
      val expected_props = JsonApi.convertToObject[Map[String, String]](props.get.replaceAll("\\(","\\{").replaceAll("\\)","\\}"))
      expected_props.map{props =>
          EtlJobArgs(job_name, Some(props.map{ case (k, v) => Props(k, v) }.toList))
      }
    } else {
      Task(EtlJobArgs(job_name))
    }
  }
}

