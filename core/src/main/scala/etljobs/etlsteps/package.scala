package etljobs

import scala.reflect.runtime.universe.TypeTag
import org.apache.spark.sql.Dataset

package object etlsteps {
  type StateLessEtlStep = EtlStep[Unit,Unit]

  case class Input[T, S](ds: Dataset[T], ips: S = Unit)
  case class Output[T, S](ds: Dataset[T], ops: S = Unit)

  type InputDataset[T] = Input[T, Unit]
  type OutputDataset[T] = Output[T, Unit]

  implicit def convertInputToDataset[T <: Product: TypeTag](in: Input[T, Unit]): Dataset[T] = in.ds
  implicit def convertOutputToDataset[T <: Product: TypeTag](ot: Output[T, Unit]): Dataset[T] = ot.ds

  implicit def convertDatasetToInput[T <: Product: TypeTag](ds: Dataset[T]): InputDataset[T] = Input(ds)
  implicit def convertOutputToDataset[T <: Product: TypeTag](ds: Dataset[T]): OutputDataset[T] = Output(ds)
}
