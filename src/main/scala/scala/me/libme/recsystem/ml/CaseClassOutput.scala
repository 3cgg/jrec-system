package scala.me.libme.recsystem.ml
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer



case class UserItemRating(userId:Int,itemId:Int,rating:Float, timestamp: Long)

case class UserItemStruct(userId:Int,itemRatings:ArrayBuffer[UserItemRating]){


  def addUserItemRating(userItemRating: UserItemRating):UserItemStruct={
    this.itemRatings+=userItemRating
    this
  }


}


/**
  * Created by J on 2018/1/9.
  */
object CaseClassOutput extends RowOutput[mutable.Map[Int,UserItemStruct]]{

  var count:Int=10

  def appendChar(str: String):Unit={

    println(str)
  }

  override def write(dataFrame: DataFrame): mutable.Map[Int,UserItemStruct] = {

    // write schema / struct
    val header=new StringBuffer()

    val schema=dataFrame.schema
    schema.foreach(field=>{
      header.append(field.name+"::")
    })

    appendChar(header.toString+"\n")


    val userItemRatings:mutable.Map[Int,UserItemStruct]=new mutable.HashMap[Int,UserItemStruct]()

    dataFrame.take(count).foreach(row=>{

      val userId=row.getInt(0);

      val userItemStruct:UserItemStruct=UserItemStruct(userId,new ArrayBuffer[UserItemRating]())
      userItemRatings.put(userId,userItemStruct)

      row.getSeq[GenericRowWithSchema](1).foreach(grs=>{
        val userItemRating=UserItemRating(userId,grs.getInt(0),grs.getFloat(1),0)
        userItemStruct.addUserItemRating(userItemRating)
      })
    })

    return userItemRatings

  }


}
