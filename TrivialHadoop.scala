object TrivialHadoop {

  def TAB = "\t"
  def timer = System.currentTimeMillis
  def rnd = util.Random

  trait Mapper[R,S] { def map(r:R):S }

  trait Reducer[R,S] { def reduce(r:Iterator[R]):S }

  case class mkMapper[R,S](mapper:R=>S) extends Mapper[R,S] {
    override def map(r:R) = mapper(r)
  }

  case class mkReducer[R,S](reducer : Iterator[R]=>S) extends Reducer[R,S] {
    override def reduce(r:Iterator[R]) = reducer(r)
  }

  def map[R,S](rows:Iterator[R], mapper : R=>S):Iterator[S] = map(rows, mkMapper(mapper))

  def map[R,S](rows:Iterator[R], mr : Mapper[R,S]):Iterator[S] = rows.map{row => mr.map(row)}

  def filter[R](rows:Iterator[R], predicate:R=>Boolean) =
    rows.flatMap{ row => if (predicate(row)) Some(row) else None }

  def reduce[R,S](rows:Iterator[R], reducer:Iterator[R]=>S):S = reduce(rows, mkReducer(reducer))

  def reduce[R,S](rows:Iterator[R], mr:Reducer[R,S]):S = mr.reduce(rows)

  def sum[R](rows:Iterator[R], func:R=>Int) =
    rows.foldLeft(0)((a,r) => a + func(r))

  def sum[R](rows:Iterator[R], func:R=>Double) =
    rows.foldLeft(0.0)((a,r) => a + func(r))

  def group[R](rows:Iterator[R]):Stream[(R,Int)] = {
    rows
    .toIterable
    .groupBy(x=>x)
    .map(kv=>kv._1->kv._2.size)
    .toStream
    .sortBy(kv => kv._2)
    .reverse
  }

  def summary[R](rows:Stream[(R,Int)], mk:R=>String) {
    val count = rows.foldLeft(0)((a,r)=>a + r._2)
    printf("\nTotal Records: %d\n\n", count)
    rows.foreach(row => printf("%s\t%d\n", mk(row._1), row._2))
  }

  def fromTsv[R](filename:String, func:String=>R, n:Int = Integer.MAX_VALUE) = {
    val iter = io.Source
    .fromFile(filename)
    .getLines
    if (n == Integer.MAX_VALUE)
      iter.map{str:String => func(str)}
    else
      iter.slice(0,n).map{str:String => func(str)}
  }

  def fromTsv[R](filename:String, func:String=>Option[R]) = {
    io.Source
    .fromFile(filename)
    .getLines
    .flatMap{str:String => func(str)}
  }

  def toTsv[R](filename:String, func:R=>String, mkR:Int=>R, n:Int) = {
    val pw = new java.io.PrintWriter(filename)
    for( i<- 0 until n) pw.println(func(mkR(i)))
    pw.flush
    pw.close
  }
}
