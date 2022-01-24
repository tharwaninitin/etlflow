package etlflow.server.model

case class EtlJobArgs(name: String, props: Option[List[Props]] = None)
