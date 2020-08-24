package com.eleflow.br.setup

import com.eleflow.br.models.Configuration
import org.apache.spark.sql.SparkSession

object ApplicationContext {

  var SPARK: SparkSession = _
  var CONFIGURATION: Configuration = _

  def setup(sparkSession: SparkSession, configuration: Configuration) = {
    SPARK = sparkSession
    CONFIGURATION = configuration
  }
}
