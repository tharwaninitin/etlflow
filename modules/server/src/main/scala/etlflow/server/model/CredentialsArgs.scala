package etlflow.server.model

case class CredentialsArgs(name: String, `type`: Creds, value: List[Props])
