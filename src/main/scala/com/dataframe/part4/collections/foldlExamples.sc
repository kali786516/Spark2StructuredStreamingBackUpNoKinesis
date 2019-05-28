val numbers = List(1,4,5,6,7)

/*fold left*/
val SumL = numbers.foldLeft(0)((b,a) => b + a)
val minL = numbers.foldLeft(numbers.head)((b,a) => b min a)
val maxL = numbers.foldLeft(numbers.head)((b,a) => b max a)
val productL = numbers.foldLeft(1)((b,a) => b * a)

/*fold right*/
val SumR = numbers.foldRight(0)((b,a) => b + a)
val minR = numbers.foldRight(numbers.head)((b,a) => b min a)
val maxR = numbers.foldRight(numbers.head)((b,a) => b max a)
val productR = numbers.foldRight(1)((b,a) => b * a)

/*fold */
val Sum = numbers.fold(0)((b,a) => b + a)
val min = numbers.fold(numbers.head)((b,a) => b min a)
val max = numbers.fold(numbers.head)((b,a) => b max a)
val product = numbers.fold(1)((b,a) => b * a)

// use Nil for list concatenation



