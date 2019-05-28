val priceCoffeeByDay: Map[String,Double] = Map(
  "Sunday" -> 2.45,
  "Monday" -> 3.54,
  "Tuesday" -> 4.50,
  "Wednesday" -> 3.21,
  "Thursday" -> 5.11,
  "Friday" -> 2.39,
  "Saturday" -> 5.87
)

val totalSpendOnCoffeeUsingSum = priceCoffeeByDay.values.sum
val minSpetOncoffeeusingMin = priceCoffeeByDay.values.min

val cofeesumusingreduce = priceCoffeeByDay.values
                          .reduce((a,b) => a+b)

val totalSpentOnCoffee = priceCoffeeByDay.values.reduce(_+_)

val minSpentOnCofee = priceCoffeeByDay.values.reduce(_ min _)

