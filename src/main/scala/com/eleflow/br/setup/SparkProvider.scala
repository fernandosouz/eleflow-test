package com.eleflow.br.setup

import org.apache.spark.sql.SparkSession

object SparkProvider {

  private val LOCAL = "local[*]"

  def local(): SparkSession =
    SparkSession
      .builder()
      .config("spark.master", LOCAL)
      .getOrCreate()

  def cluster(): SparkSession = SparkSession.builder().getOrCreate()

}
