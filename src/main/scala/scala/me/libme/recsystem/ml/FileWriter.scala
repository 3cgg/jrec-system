package scala.me.libme.recsystem.ml
import java.io.File

import me.libme.kernel._c.util.JIOUtils
import org.apache.spark.sql.DataFrame

/**
  * Created by J on 2018/1/8.
  */
class FileWriter extends RowOutput{

  /**
    * "D:\\result_movielens_ratings.txt"
    */
  var filePath:String=_

  var count:Int=10

  override def write(dataFrame: DataFrame): Unit = {

    // write schema / struct
    val header=new StringBuffer()

    val schema=dataFrame.schema
    schema.foreach(field=>{
      header.append(field.name+"::")
    })

    JIOUtils.write(new File(filePath),header.toString.getBytes("utf-8"))


    dataFrame.take(count).foreach(row=>{

      row.

    })


















  }


}
