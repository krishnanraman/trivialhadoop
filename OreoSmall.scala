object OreoSmall extends App {
  import TrivialHadoop._

  case class OreoRec(handle:String, tweet:String,date:String,hour:Byte)
  case class DateHour(date:String, hour:Byte)

  def validate(s:String) = {
    val arr = s.split(TAB)
    val valid = (arr.size > 3) && arr(2).startsWith("2014")
    if (!valid) None else Some(OreoRec(arr(0), arr(1), arr(2), arr(3).toByte))
  }

  val filename = args(0)
  def rows = fromTsv (filename,validate _)
  def mapper(o:OreoRec) = DateHour(o.date.split(" ").head, o.hour)
  def mkString(dh:DateHour) = dh.date + "T" + dh.hour

  summary(group(map(rows, mapper)), mkString)
}
