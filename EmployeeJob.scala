import com.twitter.scalding._
class EmployeeJob(args:Args) extends Job(args) {

  val data = Tsv(args("filename"), ('id, 'salary, 'year, 'name))
  .map(('year, 'salary) -> 'incr){
    x:(Short, Double) =>
    val (year, salary) = x
    0.9*((2014 - year)*365 + salary*0.001)
  }

  data.filter('id){
    id:Long => id%2 == 0
  }.groupAll {
    _.sum[Double]('incr -> 'incrTotal)
  }.write(Tsv("evenincr"))

  data.filter('id){
    id:Long => id%2 != 0
  }.groupAll {
    _.sum[Double]('incr -> 'incrTotal)
  }.write(Tsv("oddincr"))
}
