package etlflow.coretests

import etlflow.db.liveDBWithTransactor
import etlflow.utils.{ApplicationLogger, Configuration}

trait TestSuiteHelper extends Configuration with ApplicationLogger {
  val canonical_path = new java.io.File(".").getCanonicalPath
  val file           = s"$canonical_path/modules/core/src/test/resources/input/movies/ratings_parquet/ratings.parquet"
  val fullLayer      = liveDBWithTransactor(config.db) ++ etlflow.json.Implementation.live
}
