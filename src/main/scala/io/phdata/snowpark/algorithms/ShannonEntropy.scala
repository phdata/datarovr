package io.phdata.snowpark.algorithms

object ShannonEntropy {
  def entropy(values: Array[AnyVal]): Double = {
    val m: Int = values.length
    val bases: Map[AnyVal, Int] = values.groupBy(identity).mapValues(_.length)
    var shannon_entropy_value: Double = 0
    bases.foreach{case (_, v) => {
      // number of residues
      val n_i: Int = v
      val p_i: Double = n_i / m.toDouble
      val entropy_i = p_i * (Math.log(p_i)/ Math.log(2))
      shannon_entropy_value += entropy_i
    }}
    shannon_entropy_value * -1
  }
}
