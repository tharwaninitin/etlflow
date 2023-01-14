package etlflow.model

import zio.json.{DeriveJsonCodec, JsonCodec}
import java.time.ZonedDateTime

case class JobRun(
    id: String,
    name: String,
    props: String,
    status: String,
    createdAt: ZonedDateTime,
    modifiedAt: ZonedDateTime
)

object JobRun {
  implicit val codecJR: JsonCodec[JobRun] = DeriveJsonCodec.gen[JobRun]
}
