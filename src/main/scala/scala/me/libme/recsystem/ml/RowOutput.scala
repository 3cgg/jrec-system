package scala.me.libme.recsystem.ml

import org.apache.spark.sql.DataFrame

/**
  * Created by J on 2018/1/8.
  */
trait RowOutput {

  def write(dataFrame: DataFrame):Unit

}
