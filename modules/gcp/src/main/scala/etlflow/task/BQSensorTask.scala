package etlflow.task

import com.google.cloud.bigquery.FieldValueList
import etlflow.model.EtlFlowException.RetryException
import etlflow.utils.RetrySchedule
import gcp4zio.bq.BQ
import zio.{RIO, ZIO}
import scala.concurrent.duration.{Duration, DurationInt}

/** Track BQ Table by querying and applying condition on result
  * @param name
  *   Task name
  * @param query
  *   BQ DQL(Select) query
  * @param sensor
  *   Function which takes Iterable[FieldValueList] and return bool, which on true exits the poll task
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
      _    <- ZIO.logInfo(s"Polling BQ Table")
      rows <- BQ.getData(query)
      bool <- ZIO.attempt(sensor(rows))
      _    <- ZIO.when(!bool)(ZIO.fail(RetryException("Condition not satisfied. Polling again")))
      _    <- ZIO.logInfo("Condition satisfied. Finished Polling")
    } yield ()

    val runnable: RIO[BQ, Unit] = for {
      _ <- ZIO.logInfo("#" * 50)
      _ <- ZIO.logInfo("Started Polling BQ Table")
      _ <- retry.map(r => program.retry(RetrySchedule.recurs(r, spaced))).getOrElse(program.retry(RetrySchedule.forever(spaced)))
      _ <- ZIO.logInfo("#" * 50)
    } yield ()

    runnable
  }

  override def getTaskProperties: Map[String, String] = Map("query" -> query)
}
