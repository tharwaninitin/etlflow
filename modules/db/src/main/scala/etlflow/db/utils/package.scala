package etlflow.db

import scalikejdbc.{NoExtractor, SQL}

package object utils {
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
