trivialhadoop
=============

Scalding vs TrivialHadoop:  
 
Scalding is an industrial-strength Map Reduce framework for Scala, 
built atop Cascading, with numerous other dependencies. 
TrivialHadoop is a trivial Map-Reduce framework I whipped out over the weekend. 
 
The intent is to demonstrate that all the mystery, magic and power of Map Reduce 
comes from the distributed (hDfs) computing aspect over billions of rows. 
So long as you operate in a "BILLION ROWS OR LESS" space, it is rather easy to code up a basic 
Map Reduce framework that runs on your local filesystems, that is very competitive with Scalding. 
Without parallelization, TrivialHadoop is atleast 2-3 times faster than Scalding.
Once you include Akka & Actors into the mix, you can get a 5x quite easily. 
 
That said, Scalding is a battle-tested framework with a rather *nice* DSL that makes 
writing MR jobs a very pleasant experience, with tons of tooling & community built around it.
 
Key Takeaways:
a. Map-Reduce on small data ( Small == "BILLION ROWS OR LESS") is a valuable abstraction. 
Rather than write python/ruby scripts each time to solve specific problems on file-based sources, 
it is better to stick with the Map-Reduce abstraction regardless of the number of rows.
 
b. Apart from the sheer size ( BILLIONS OF ROWS), the Distributed aspect of hDfs is the cause 
for both the power of Map-Reduce & the concomitant complexity. On a local filesystem with 
no built-in failover, one can build a custom Map-Reduce framework that easily outperforms 
the existing industrial-strength HDFS-based Map-Reduce solutions, so long as you operate in the "small data" space.
 
c. The majority of ML work is on "small data", though the majority of ETL work is big data. 
Put another way, the usecase for crunching matrices with millions of rows is remote, 
much more likely to crunch matrices with few thousand rows. Most ML jobs originate as big data ETL jobs, 
that are then reduced to a small data space & run through liblinear/Breeze. 
It is easier to couple ML libraries such as liblinear/Breeze/COLT with homegrown Map-Reduce 
frameworks that operate on small data, than use bigdata ML frameworks like Mahout.
 
d. Ultimately, subjective values like company culture, tooling, OSS community, "niceness" of the DSL triumph raw speed.
