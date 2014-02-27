object TestMapReduce extends App {
  import TrivialHadoop._

  case class Employee(id:Long, salary:Double, year:Short, name:String)
  case class EmployeeIncrement(emp:Employee, incr:Double)
  case class Totals(num:Double)

  // every employee gets an increment
  // $1 for each day since the year they joined the company + 0.1% of their existing salary
  // 10% taxes, so 0.9 * above
  def mymapper(e:Employee) = EmployeeIncrement(e, 0.9*((2014 - e.year)*365 + e.salary*0.001))

  // odd numbered employees paid on different schedule
  def evenfilter(ei:EmployeeIncrement) = ei.emp.id % 2 == 0
  def oddfilter(ei:EmployeeIncrement) = ei.emp.id % 2 != 0

  // add up the employee increments
  def myreducer(ei:Iterator[EmployeeIncrement]) = Totals(ei.foldLeft(0.0)((a,ei)=>a+ei.incr))

  // make the employee DB
  val totalRecords = args(0).toInt
  val filename = args(1)
  toTsv(filename,
    (e:Employee) => (e.id + TAB + e.salary + TAB + e.year + TAB + e.name),
    (i:Int) => Employee(i, 70000 + rnd.nextDouble + rnd.nextInt(23456), (1960 + rnd.nextInt(30)).toShort, "Employee:" + i),
    totalRecords)

  val init = timer
  // read from Tsv
  def rows = fromTsv (filename,
     (s:String) => {
      val arr = s.split(TAB)
      Employee(arr(0).toLong, arr(1).toDouble, arr(2).toShort, arr(3))
  })

  val evenIncr = reduce(filter(map(rows, mymapper), evenfilter), myreducer).num
  val oddIncr =  reduce(filter(map(rows, mymapper), oddfilter ), myreducer).num
  val total = sum(map(rows, mymapper), (r:EmployeeIncrement) => r.incr)
  assert( math.abs(total - (evenIncr + oddIncr)) < 1e-3)
  val time = timer - init
  printf("Total %.3f\tOdd Increment: %.3f\tEven Increment: %.3f\tTime in ms: %d\tTotal Records: %d\n", total, oddIncr, evenIncr, time, totalRecords )
}
