package etlflow.schema

sealed trait LoggingLevel
object LoggingLevel {
  case object JOB extends LoggingLevel
  case object DEBUG extends LoggingLevel
  case object INFO extends LoggingLevel
}