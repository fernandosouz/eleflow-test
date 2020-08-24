package com.eleflow.br

import com.eleflow.br.setup.{ApplicationLoader, SparkProvider}

object LocalFuelAnalysisMain extends App {
  ApplicationLoader
    .loadAndSetup(SparkProvider.local(), args)
    .execute
}

object FuelAnalysisMain extends App {
  ApplicationLoader
    .loadAndSetup(SparkProvider.cluster(), args)
    .execute
}
