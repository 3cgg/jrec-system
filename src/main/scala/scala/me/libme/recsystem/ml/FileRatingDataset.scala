package scala.me.libme.recsystem.ml

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by J on 2018/1/8.
  */
class FileRatingDataset extends RatingDataset{

  /**
    * "D:\\java_\\spark-2.2.1-bin-hadoop2.7/data/mllib/als/sample_movielens_ratings.txt"
    */
  var filePath:String=_

  override def ratingDataset(): DataFrame = {

    val ratings = SparkSession.getActiveSession.get.read.textFile(filePath)
      .map(parseRating)
      .toDF()
    ratings
  }



  def parseRating(str: String): Rating = {
    val fields = str.split("::")
    assert(fields.size == 4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }




}
