package etlflow.model

import zio.json.{DeriveJsonCodec, JsonCodec}
import java.time.ZonedDateTime

case class TaskRun(
    id: String,
    jobRunId: String,
    name: String,
    taskType: String,
    metadata: String,
    status: String,
    createdAt: ZonedDateTime,
    modifiedAt: ZonedDateTime
)

@SuppressWarnings(Array("org.wartremover.warts.SeqApply"))
object TaskRun {
  implicit val codecTR: JsonCodec[TaskRun] = DeriveJsonCodec.gen[TaskRun]
}
