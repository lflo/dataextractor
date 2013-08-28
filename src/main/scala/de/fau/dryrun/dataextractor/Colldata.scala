
package de.fau.dryrun.dataextractor

import scala.io.Source
import java.io.File
import scala.collection.mutable.Buffer
import org.slf4j.LoggerFactory
import org.apache.log4j.Logger
import org.apache.log4j.ConsoleAppender
import org.apache.log4j.PatternLayout






class Experiment(dir: File) {
	val log = LoggerFactory.getLogger(this.getClass)
	//log.debug("Parsing " + dir)
	

	val extractors = List[DataExtractor](new DEuip1, new DEuipSim1)
	val config = Source.fromFile(dir.toString +"/conf.txt").getLines.map(_.split("=", 2)).map(e => e(0) -> e(1)).toMap		
	
	val resultsf = new File(dir.toString + "/results")
	val data = extractors.aggregate(Vector[Data]())(_ ++ _.extractDir(dir), _ ++ _)
	
	
	def results = data.filter(_.isInstanceOf[Result]).map(_.asInstanceOf[Result])
	def snapshots =  data.filter(_.isInstanceOf[Snapshot]).map(_.asInstanceOf[Snapshot])
}






object DataCollector {
	val data = collection.mutable.Map[String, Experiment]()
	val log = LoggerFactory.getLogger(this.getClass)

	val sep = " "
	
	def main(args: Array[String]): Unit = {
		val DEFAULT_PATTERN_LAYOUT = "%-23d{yyyy-MM-dd HH:mm:ss,SSS} | %-30.30t | %-30.30c{1} | %-5p | %m%n"
		Logger.getRootLogger.addAppender(new ConsoleAppender(new PatternLayout(DEFAULT_PATTERN_LAYOUT)))
		
		val folder = new File(args(0))
		val outfile = new java.io.PrintWriter(new File(args(1)))
		
		log.info("Reading: " + folder)

		// Using par here does not improve performance		
		//val experiments = folder.listFiles.filter(_.isDirectory).par.map(new Experiment(_))
		val experiments = folder.listFiles.filter(_.isDirectory).map(new Experiment(_))		
		val configs = experiments.map(_.config.keySet).reduce(_ union _).toList
		
		
		log.info("Experiments: " + experiments.size)
		log.info("Data: " + experiments.map(_.data.size).sum)
		
		outfile.println((configs ::: List("node", "key", "value")).mkString(sep))
		for(exp <- experiments) {
			val pres = configs.map(exp.config.getOrElse(_, "null")).mkString("", sep, sep)
			for(res <- exp.results) {
				outfile.println(pres + res.nkvList.mkString(sep))
			}
		}

		outfile.close
	}

}
