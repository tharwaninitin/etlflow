package etlflow.utils

import etlflow.api.Schema.EtlJobArgs
import etlflow.schema.Props

object RequestValidator {
  def apply(job_name: String, props: Option[String]): Either[String, EtlJobArgs] = {
    if(props.isDefined) {
      val expected_props = JsonCirce.convertToObjectEither[Map[String, String]](props.get.replaceAll("\\(","\\{").replaceAll("\\)","\\}"))
      expected_props match {
        case Left(e) => Left(e.getMessage)
        case Right(props) =>
          val output = EtlJobArgs(job_name, Some(props.map{ case (k, v) => Props(k, v) }.toList))
          Right(output)
      }
    } else {
      val output = EtlJobArgs(job_name)
      Right(output)
    }
  }
}

