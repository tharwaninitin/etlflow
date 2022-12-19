package etlflow

import etlflow.model.Credential.JDBC
import scalikejdbc.{NoExtractor, SQL}

trait DbSuiteHelper {
  val credentials: JDBC = JDBC(
    sys.env.getOrElse("DB_URL", "localhost"),
    sys.env.getOrElse("DB_USER", "root"),
    sys.env.getOrElse("DB_PWD", "root"),
    sys.env.getOrElse("DB_DRIVER", "org.postgresql.Driver")
  )

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  def getSqlQueryAsString[T](sqlQuery: SQL[T, NoExtractor]): String = {
    val statement = sqlQuery.statement
    val params = sqlQuery.parameters.map { value =>
      if (value == null) "null" else value.toString
    }
    params.foldLeft(statement) { (text, param) =>
      text.replaceFirst("\\?", param)
    }
  }
}
