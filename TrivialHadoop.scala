object TrivialHadoop {

  def TAB = "\t"
  def timer = System.currentTimeMillis
  def rnd = util.Random

  trait Mapper[R,S] {
    def map(r:R):S
  }

  trait Reducer[R,S] {
    def reduce(r:Iterator[R]):S
  }

  case class mkMapper[R,S](mapper:R=>S) extends Mapper[R,S] {
    override def map(r:R) = mapper(r)
  }

  case class mkReducer[R,S](reducer : Iterator[R]=>S) extends Reducer[R,S] {
    override def reduce(r:Iterator[R]) = reducer(r)
  }

  def map[R,S](rows:Iterator[R], mapper : R=>S) = {
    val mr = mkMapper(mapper)
    rows.map{row => mr.map(row)}
  }

  def filter[R](rows:Iterator[R], predicate:R=>Boolean) = {
    rows.flatMap{ row => if (predicate(row)) Some(row) else None }
  }

  def reduce[R,S](rows:Iterator[R], reducer:Iterator[R]=>S) = {
    val mr = mkReducer(reducer)
    mr.reduce(rows)
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

  def toTsv[R](filename:String, func:R=>String, mkR:Int=>R, n:Int) = {
    val pw = new java.io.PrintWriter(filename)
    for( i<- 0 until n) {
      pw.println(func(mkR(i)))
      //if (i%10000 == 0) printf("\nSaved %d rows to %s\n", i, filename)
    }
    pw.flush
    pw.close
    //printf("\nSaved %d rows to %s\n", n, filename)
  }
}