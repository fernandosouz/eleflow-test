package com.eleflow.br.setup

import io.circe.generic.auto._
import io.circe.parser
import com.eleflow.br.models.Configuration
import org.apache.spark.sql.SparkSession
import com.eleflow.br.services.FuelAnalysis

object ApplicationLoader {

  def loadAndSetup(sparkSession: SparkSession, args: Array[String]) = {

    val configurationsFileContent = sparkSession.sparkContext.textFile(args(0)).collect().mkString

    val configuration: Configuration = parser.decode[Configuration](configurationsFileContent) match {
      case Right(value) => value
      case Left(_) => throw new IllegalArgumentException("Error on converting configuration file")
    }

    ApplicationContext.setup(sparkSession, configuration)
    FuelAnalysis
  }

}
