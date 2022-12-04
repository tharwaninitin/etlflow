package etlflow

package object k8s {
  case class Secret(name: String, mountPath: String)
}
