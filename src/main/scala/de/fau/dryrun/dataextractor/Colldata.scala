
package de.fau.dryrun.dataextractor

import scala.io.Source
import java.io.File
import scala.collection.mutable.Buffer
import org.slf4j.LoggerFactory
import org.apache.log4j.Logger
import org.apache.log4j.ConsoleAppender
import org.apache.log4j.PatternLayout
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration




class Experiment(dir: File) {
	val log = LoggerFactory.getLogger(this.getClass)
	//log.debug("Parsing " + dir)
	
	
	val extractors = 0 to 3 
	def extractorFactory(id:Int):DataExtractor = {
		id match {
			case 0 => new DEuip1
			case 1 => new DEuip1 with DEuipSim1
			case 2 => new DEuip1_rcv
			case 3 => new DEuip1_rcv with DEuipSim1 with DEuip1_rcvSim 
		}
	}
	
	val config = Source.fromFile(dir.toString +"/conf.txt").getLines.map(_.split("=", 2)).map(e => e(0) -> e(1)).toMap		
	
	val resultsf = new File(dir.toString + "/results")
	val data = extractors.aggregate(Vector[Data]())(_ ++ extractorFactory(_).extractDir(dir), _ ++ _)
	
	
	
	lazy val results:Vector[Result] =  Await.result(resultFuture, Duration.Inf)
	val resultFuture = future {	data.filter(_.isInstanceOf[Result]).map(_.asInstanceOf[Result])}
	
	lazy val resultsNodeKeyValueMap = Await.result(resultsNodeKeyValueMapFuture, Duration.Inf)
	val resultsNodeKeyValueMapFuture:Future[Map[Int, Map[String, Long]]] = future {
		val rv = collection.mutable.Map[Int, collection.mutable.Map[String, Long]]()
		for(r <- results) {
			val cll = rv.getOrElseUpdate(r.node, collection.mutable.Map[String, Long]())
			cll += (r.k -> r.v)
		}
		//Convert to immutable
		rv.map(x => {x._1 -> x._2.toMap}).toMap
	} 
	
	lazy val resultNodes = {
		results.map(_.node).toSet
	}
	
	
	def snapshots =  data.filter(_.isInstanceOf[Snapshot]).map(_.asInstanceOf[Snapshot])
}






object DataCollector {
	val data = collection.mutable.Map[String, Experiment]()
	val log = LoggerFactory.getLogger(this.getClass)

	val sep = ", "
	
	def main(args: Array[String]): Unit = {
		val DEFAULT_PATTERN_LAYOUT = "%-23d{yyyy-MM-dd HH:mm:ss,SSS} | %-30.30t | %-30.30c{1} | %-5p | %m%n"
		Logger.getRootLogger.addAppender(new ConsoleAppender(new PatternLayout(DEFAULT_PATTERN_LAYOUT)))
		
		val folder = new File(args(0))
		
		
		
		log.info("Reading: " + folder)

		// Using par here does not improve performance		
		//val experiments = folder.listFiles.filter(_.isDirectory).par.map(new Experiment(_))
		val experiments = folder.listFiles.filter(_.isDirectory).map(new Experiment(_))		
		
		
		log.info("Experiments: " + experiments.size)
		log.info("Data: " + experiments.map(_.data.size).sum)
		
		
		//Output
		val configs = experiments.map(_.config.keySet).reduce(_ union _).toList.sortWith(_.compareTo(_) < 0)
		val reskeys = experiments.map(_.results.map(_.k).toSet).reduce(_ union _).toList.sortWith(_.compareTo(_) < 0)
		val nodes = experiments.map(_.results.map(_.node).toSet).reduce(_ union _).toList.sortWith(_ < _)
		
		
		log.info("Configs: " + configs.size)
		log.info("Keys: " + reskeys.size)
		log.info("Nodes: " + nodes.size)
		
		log.debug("Keys: " +  reskeys.mkString(", "))
		
		val outname = {if(args.length == 2) args(1) else args(0)}

		
		
		//Stacked results
		log.info("Wrinting stacked results");
		
		;{
			val outfile = new java.io.PrintWriter(new File(outname + ".res.stacked"))
			outfile.println((configs ::: List("node", "key", "value")).mkString(sep))
			for(exp <- experiments) {
				val pres = configs.map(exp.config.getOrElse(_, "null")).mkString("", sep, sep)
				for(res <- exp.results) {
					outfile.println(pres + res.nkvList.mkString(sep))
				}
			}
			outfile.close
		}
		//Unstacked results
		log.info("Wrinting unstacked results");
		
		;{
			val outfile = new java.io.PrintWriter(new File(outname + ".res.unstacked"))
			val header =  configs ::: List("node") ::: reskeys 
			outfile.println(header.mkString(sep))
			val data = for(exp <- experiments ; (node, dat) <- exp.resultsNodeKeyValueMap) yield {
					val rv = configs.map(exp.config.getOrElse(_, "null")) ::: 
							List(node.toString)	:::
							reskeys.map(dat.getOrElse(_, "null"))
					if(rv.size != header.size) {
						log.error("Wring Size!")
						log.error("RV"+rv.size +"H: " +header.size + " C: " + configs.size + " R: " + reskeys.size)
					}
							
					rv
			}
			
			
			outfile.print(data.map(_.mkString(sep)).mkString("\n"))			
			outfile.close
		}
		
		log.info("Export Information to read in R")
		
		;{
			val outfile = new java.io.PrintWriter(new File(outname + ".res.r_inputs"))
			outfile.println({configs ::: List("node")}.mkString(" ") )
			outfile.close
			
		}
		
		log.info("Done")
		
		
	}

}
