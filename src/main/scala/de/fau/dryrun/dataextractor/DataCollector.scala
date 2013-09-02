
package de.fau.dryrun.dataextractor

import scala.io.Source
import java.io.File
import scala.collection.mutable.Buffer
import org.slf4j.LoggerFactory
import org.apache.log4j.Logger
import org.apache.log4j.ConsoleAppender
import org.apache.log4j.PatternLayout







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
		
		val resKeys = experiments.map(_.results.map(_.k).toSet).reduce(_ union _).toList.sortWith(_.compareTo(_) < 0)
		val nodes = experiments.map(_.results.map(_.node).toSet).reduce(_ union _).toList.sortWith(_ < _)
		
		val expResKeys = experiments.map(_.expResults.map(_.k).toSet).reduce(_ union _).toList.sortWith(_.compareTo(_) < 0)
		
		
		log.info("Configs: " + configs.size)
		log.info("Keys: " + resKeys.size)
		log.info("Nodes: " + nodes.size)
		log.info("ExpResults: " + experiments.map(_.expResults.size).sum)
		log.info("ExpKeys: " + expResKeys.size);
		//log.debug("Keys: " +  resKeys.mkString(", "))
		
		
		val outname = {if(args.length == 2) args(1) else args(0)}

		
		
		
		//Stacked results
		log.info("Wrinting stacked results");
		;{

			val oLines = for(exp <- experiments) yield{
				val pres = configs.map(exp.config.getOrElse(_, "null")).mkString("", sep, sep)
				for(res <- exp.results) yield {
					pres + res.stackList.mkString(sep)
				}
			}
			if(oLines.size > 0) {
				val outfile = new java.io.PrintWriter(new File(outname + ".res.stacked"))
				outfile.println(oLines.mkString("\n"))
				outfile.close
			} else {
				log.info("No Stacked results")
			}
		}
		
		//Stacked experiment results
		log.info("Wrinting stacked expriment results");
		;{
			val oLines = for(exp <- experiments) yield {
				val pres = configs.map(exp.config.getOrElse(_, "null")).mkString("", sep, sep)
				for(res <- exp.expResults) yield {
					pres + res.stackList.mkString(sep)
				}
			}
			if(oLines.size > 0) {
				val outfile = new java.io.PrintWriter(new File(outname + ".res.exp_stacked"))
				outfile.println(oLines.mkString("\n"))
				outfile.close
			}else {
				log.info("No Stacked experiment results")
			}
		}
		
		//Unstacked results
		log.info("Wrinting unstacked results");
		;{
			val header =  configs ::: List("node") ::: resKeys
			val olines = for(exp <- experiments ; (node, dat) <- exp.resultsNodeKeyValueMap) yield {
					val rv = configs.map(exp.config.getOrElse(_, "null")) ::: 
							List(node.toString)	:::
							resKeys.map(dat.getOrElse(_, "null"))
					if(rv.size != header.size) {
						log.error("Wrong Size!")
						log.error("RV: " + rv.size +"H: " +header.size + " C: " + configs.size + " R: " + resKeys.size)
					}
							
					rv
			}
			if(olines.size > 0) {
				val outfile = new java.io.PrintWriter(new File(outname + ".res.unstacked"))				 
				outfile.println(header.mkString(sep))
				outfile.print(olines.map(_.mkString(sep)).mkString("\n"))			
				outfile.close
			}else {
				log.info("No unstacked results")
			}
		}
		
		//Unstacked results
		log.info("Wrinting unstacked Experiment results");
		;{
			val header =  configs ::: expResKeys
			val olines = for(exp <- experiments ) yield {
					val dat = exp.expResultsKeyValueMap
					val rv = configs.map(exp.config.getOrElse(_, "null")) ::: 
							expResKeys.map(dat.getOrElse(_, "null"))
					if(rv.size != header.size) {
						log.error("Wrong Size!")
						log.error("RV"+rv.size +"H: " +header.size + " C: " + configs.size + " R: " + resKeys.size)
					}
							
					rv
			}
			if(olines.size > 0) {
				val outfile = new java.io.PrintWriter(new File(outname + ".res.exp_unstacked"))				 
				outfile.println(header.mkString(sep))
				outfile.print(olines.map(_.mkString(sep)).mkString("\n"))			
				outfile.close
			}else {
				log.info("No unstacked results")
			}
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
