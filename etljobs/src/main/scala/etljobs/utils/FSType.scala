package etljobs.utils

sealed trait FSType

case object LOCAL extends FSType
case object GCS extends FSType