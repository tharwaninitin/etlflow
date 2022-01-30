package etlflow

import com.google.cloud.bigquery.{LegacySQLTypeName, Schema}
import etlflow.gcp.getBqSchema
import zio.test._
import java.sql.Date
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

object BQSchemaMappingTestSuite extends DefaultRunnableSpec {

  case class RatingCSV(
      userId: String,
      movieId: Int,
      rating: Long,
      timestamp: Double,
      start_date: Date,
      end_date: java.util.Date,
      is_active: Boolean
  )

  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("getFields Test Suites")(
      test("getFields[RatingCSV] should return map of field names and field types") {
        val schema: Option[Schema] = getBqSchema[RatingCSV]
        val op = ArrayBuffer(
          ("userId", LegacySQLTypeName.STRING),
          ("movieId", LegacySQLTypeName.INTEGER),
          ("rating", LegacySQLTypeName.INTEGER),
          ("timestamp", LegacySQLTypeName.FLOAT),
          ("start_date", LegacySQLTypeName.DATE),
          ("end_date", LegacySQLTypeName.DATE),
          ("is_active", LegacySQLTypeName.BOOLEAN)
        )
        assertTrue(schema.get.getFields.asScala.map(x => (x.getName, x.getType)) == op)
      }
    )
}
