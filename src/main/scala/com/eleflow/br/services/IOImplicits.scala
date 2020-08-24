package com.eleflow.br.services

import com.eleflow.br.setup.ApplicationContext.{CONFIGURATION, SPARK}
import org.apache.spark.sql.DataFrame

object IOImplicits {

  class IOImplicits(dataFrame: DataFrame) {

    def save(name: String) =
      dataFrame
        .write
        .csv(if(CONFIGURATION.outputPath.get.takeRight(1) == "/" ) s"${CONFIGURATION.outputPath.get}$name" else s"${CONFIGURATION.outputPath.get}/$name")
  }

  def read =
    SPARK
      .read
      .format(CONFIGURATION.format)
      .option("header", "true")
      .load(CONFIGURATION.inputPath)

  implicit def ioImplicits(dataFrame: DataFrame): IOImplicits = new IOImplicits(dataFrame)
}
