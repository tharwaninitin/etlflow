package etlflow.utils

object MapToJson {
  def apply(map: Map[String, String]): String = "{" + map.map(kv => s""""${kv._1}":"${kv._2}"""").mkString(",") + "}"
}
