// ./pants goal binary src/scala/com/twitter/observability:observability-deploy --binary-deployjar -vx
//scald.rb --jar observability --host hadoopnest1.smf1.twitter.com --hdfs com.twitter.observability.jobs.observability.Oreo --date 2014-01-24 2014-01-31 --tz UTC --output oreo
// HADOOP_CLASSPATH=/usr/share/java/hadoop-lzo-0.4.15.jar:observability-deploy.jar hadoop jar observability-deploy.jar -libjars observability-deploy.jar -Dmapred.min.split.size=1073741824 -Dmapred.reduce.tasks=300 -Dcascading.spill.map.threshold=500000 -Dcascading.spill.list.threshold=500000 -Dcascading.aggregateby.threshold=500000 com.twitter.observability.jobs.observability.UniqueMetrics --hdfs --date 2013-02-17 2014-02-24 --tz UTC --tool.partialok

package com.twitter.observability.jobs.observability
import com.twitter.pluck.job.TwitterDateJob
import com.twitter.pluck.source.{StatusSource, UserTableSource}
import com.twitter.scalding._

class Oreo(args : Args) extends TwitterDateJob(args) {

  val HOUR_REGEX = """\d+-\d+-\d+ (\d+):\d+:\d+""".r
  val OREO = "#sendmeoreo"

  val tweets =
    StatusSource()
      .mapTo('tweeterId, 'createdAt, 'text) { s =>
        (s.getUserId.toLong, getHour(s.getCreatedAt), s.getText.toLowerCase)
      }
      .filter('text) {text:String => text.contains(OREO)}

  val now = RichDate(new java.util.Date().getTime())
  val yesterday = now - new Days(1)
  val users =
    UserTableSource()(DateRange(yesterday, now))
    .mapTo('userId, 'location, 'timeZone, 'handle) {
      s => (s.getId.toLong, s.getLocation, s.getTimeZone, s.getScreenName)
    }

  tweets
  .joinWithLarger('tweeterId -> 'userId, users)
  .project('handle, 'text, 'createdAt, 'hour, 'location, 'timeZone)
  .write(Tsv(args("output")))

  def getHour(date : String):Option[Int] = {
    HOUR_REGEX.findFirstMatchIn(date).map{ _.subgroups }.map{ subgroups => subgroups(0).toInt }
  }
}
