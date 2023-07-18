package etlflow.model

import zio.json.{DeriveJsonCodec, JsonCodec}
import java.time.ZonedDateTime

case class JobRun(
    id: String,
    name: String,
    metadata: String,
    status: String,
    createdAt: ZonedDateTime,
    modifiedAt: ZonedDateTime
)

@SuppressWarnings(Array("org.wartremover.warts.SeqApply"))
object JobRun {
  implicit val codecJR: JsonCodec[JobRun] = DeriveJsonCodec.gen[JobRun]
}
