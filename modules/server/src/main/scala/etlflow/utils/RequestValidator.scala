package etlflow.utils

import etlflow.api.Schema.{EtlJobArgs, Props}
import etlflow.json.{Implementation, JsonService}
import io.circe.generic.auto._
import zio.Runtime.default.unsafeRun

private [etlflow] object RequestValidator {
  def apply(job_name: String, props: Option[String]): Either[String, EtlJobArgs] = {
    if(props.isDefined) {
      val expected_props = unsafeRun(JsonService.convertToObjectEither[Map[String, String]](props.get.replaceAll("\\(","\\{").replaceAll("\\)","\\}")).provideLayer(Implementation.live))
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

