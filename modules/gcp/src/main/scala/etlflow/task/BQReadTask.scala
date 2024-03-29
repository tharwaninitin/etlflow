package etlflow.task

import com.google.cloud.bigquery.FieldValueList
import gcp4zio.bq._
import zio.{RIO, ZIO}

case class BQReadTask[T](name: String, query: String)(fn: FieldValueList => T) extends EtlTask[BQ, Iterable[T]] {
  override protected def process: RIO[BQ, Iterable[T]] = for {
    _   <- ZIO.logInfo("#" * 100)
    _   <- ZIO.logInfo(s"Starting BQ Read Task: $name")
    _   <- ZIO.logInfo(s"Query: $query")
    out <- BQ.fetchResults[T](query)(fn)
  } yield out
  override val metadata: Map[String, String] = Map("query" -> query)
}
