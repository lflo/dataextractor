
package de.fau.dryrun.dataextractor

import scala.io.Source
import java.io.File
import scala.collection.mutable.Buffer
import org.slf4j.LoggerFactory
import org.apache.log4j.Logger
import org.apache.log4j.ConsoleAppender
import org.apache.log4j.PatternLayout






class Experiment(dir: File) {
	// Get Config
	val log = LoggerFactory.getLogger(this.getClass)
	//log.debug("Parsing " + dir)
	
	val extractors = List[DataExtractor](new DEuip1)
	val config =  Source.fromFile(dir.toString +"/conf.txt").getLines.map(_.split("=")).map(e => e(0) -> e(1)).toMap
	
	
	
	val resultsf = new File(dir.toString + "/results")
	val data = extractors.aggregate(List[Data]())(_ ::: _.extractDir(dir), _ ::: _)
	
	
	def results = data.filter(_.isInstanceOf[Result]).map(_.asInstanceOf[Result])
	def snapshots =  data.filter(_.isInstanceOf[Snapshot]).map(_.asInstanceOf[Snapshot])
}






object DataCollector {
	val data = collection.mutable.Map[String, Experiment]()
	val log = LoggerFactory.getLogger(this.getClass)
	
	def main(args: Array[String]): Unit = {
	    val DEFAULT_PATTERN_LAYOUT = "%-23d{yyyy-MM-dd HH:mm:ss,SSS} | %-30.30t | %-30.30c{1} | %-5p | %m%n"				
		Logger.getRootLogger.addAppender(new ConsoleAppender(new PatternLayout(DEFAULT_PATTERN_LAYOUT)))
		
		
		val folder = new File(args(0))
		val outfile = new java.io.PrintWriter(new File(args(1)))
		
		log.info("Reading: " + folder)

		// Using par here does not improve performance		
		//val experiments = folder.listFiles.filter(_.isDirectory).par.map(new Experiment(_))
		val experiments = folder.listFiles.filter(_.isDirectory).map(new Experiment(_))		
		val configs = experiments.map(_.config.keys).flatten.toSet.toList
		
		
		log.info("Experiments: " + experiments.size)
		log.info("Data: " + experiments.map(_.data).flatten.size)
		
		outfile.println({configs :+ "node" :+ "key" :+ "value"}.mkString(" "))
		for(exp <- experiments) {
			val pres = configs.map(exp.config.getOrElse(_, "null")).mkString(""," ", " ")
			exp.results.map(_.nkvList.mkString(" ")).foreach(x => {outfile.println(pres + x)})
		}
		outfile.close		

	}

}