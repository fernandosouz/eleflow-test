package com.eleflow.br.services
import com.eleflow.br.services.IOImplicits.{ioImplicits, read}
import com.eleflow.br.services.QueriesImplicits.queriesImplicits
import com.eleflow.br.services.StandardizeImplicits.standardizeImplicits
import com.eleflow.br.setup.ApplicationContext._

object FuelAnalysis {

  def execute = {

    val standardizeDf = read.standardize

    val avgProductsByCityAndMonthDf = standardizeDf.avgProductsByCityAndMonth
    val avgProductsByStateAndRegionDf = standardizeDf.avgProductsByStateAndRegion
    val minAndMaxVarianceAndVariation = standardizeDf.minAndMaxVarianceAndVariation
    val citiesWithMoreDiffBetweenProductsDf = standardizeDf.citiesWithMoreDiffBetweenProducts

    if (CONFIGURATION.show) {
      avgProductsByCityAndMonthDf.show()
      avgProductsByStateAndRegionDf.show()
      minAndMaxVarianceAndVariation.show()
      citiesWithMoreDiffBetweenProductsDf.show()
    }

    if (CONFIGURATION.outputPath.isDefined) {
      avgProductsByCityAndMonthDf.save("avgProductsByCityAndMonthDf")
      avgProductsByStateAndRegionDf.save("avgProductsByStateAndRegionDf")
      minAndMaxVarianceAndVariation.save("minAndMaxVarianceAndVariation")
      citiesWithMoreDiffBetweenProductsDf.save("citiesWithMoreDiffBetweenProductsDf")
    }
  }
}
