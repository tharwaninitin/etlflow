package etlflow.task

import com.google.cloud.bigquery.FieldValueList
import etlflow.model.EtlFlowException.RetryException
import etlflow.utils.RetrySchedule
import gcp4zio.bq.BQ
import zio.{RIO, ZIO}
import scala.concurrent.duration.{Duration, DurationInt}

/** BQ Sensor on SQL query based on output rows satisfying condition defined by sensor function
  * @param name
  *   Task name
  * @param query
  *   BigQuery SQL query
  * @param sensor
  *   Function which takes Iterable[FieldValueList] and return bool, which on returning true exits the poll
  * @param spaced
  *   Specifies duration each repetition should be spaced from the last run
  * @param retry
  *   Number of times we need to check query the BQ table before this effect terminates
  */
case class BQSensorTask(
    name: String,
    query: String,
    sensor: Iterable[FieldValueList] => Boolean,
    spaced: Duration = 1.minute,
    retry: Option[Int] = None
) extends EtlTask[BQ, Unit] {

  override protected def process: RIO[BQ, Unit] = {
    val program: RIO[BQ, Unit] = for {
      rows <- BQ.fetchResults(query)(identity)
      bool <- ZIO.attempt(sensor(rows))
      _    <- ZIO.when(!bool)(ZIO.fail(RetryException("Condition not satisfied. Polling again")))
      _    <- ZIO.logInfo("Condition satisfied. Finished Polling")
    } yield ()

    for {
      _ <- ZIO.logInfo("#" * 50)
      _ <- ZIO.logInfo("Started Polling BQ Table")
      _ <- retry.map(r => program.retry(RetrySchedule.recurs(r, spaced))).getOrElse(program.retry(RetrySchedule.forever(spaced)))
      _ <- ZIO.logInfo("#" * 50)
    } yield ()
  }

  override val metadata: Map[String, String] = Map("query" -> query)
}
