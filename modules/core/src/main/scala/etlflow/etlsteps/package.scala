package etlflow

import org.apache.spark.sql.Dataset
// import scala.reflect.runtime.universe.TypeTag
// import monocle.Iso

package object etlsteps {
  case class DatasetWithState[T, S](ds: Dataset[T], state: S = Unit)
//  type DatasetWoState[T] = DatasetWithState[T, Unit]
//  def change[T]:Iso[DatasetWoState[T], Dataset[T]] = Iso[DatasetWoState[T], Dataset[T]](_.ds){ds => DatasetWithState(ds)}
//  implicit def convertToDataset[T <: Product: TypeTag](in: DatasetWithState[T, Unit]): Dataset[T] = in.ds
//  implicit def convertToDatasetWoState[T <: Product: TypeTag](ds: Dataset[T]): DatasetWoState[T] = DatasetWithState(ds)
}
