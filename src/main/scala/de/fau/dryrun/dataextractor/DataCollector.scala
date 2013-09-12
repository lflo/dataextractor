package de.fau.dryrun.dataextractor

import scala.io.Source
import java.io.File
import scala.collection.mutable.Buffer
import org.slf4j.LoggerFactory
import org.apache.log4j.Logger
import org.apache.log4j.ConsoleAppender
import org.apache.log4j.PatternLayout
import scopt.mutable.OptionParser
import scopt.mutable.OptionParser._
import java.text.SimpleDateFormat
import java.util.Date

object DataCollector {
	val data = collection.mutable.Map[String, Experiment]()
	val log = LoggerFactory.getLogger(this.getClass)

	val sep = ", "
	val nodeStr = "node"
		
		
	private def getStartEnd(experiments:Array[Experiment]) = {
		var s = new Date(Long.MaxValue)
		var e = new Date(0)
		for(exp <- experiments) {
			if(s.after(exp.end)) s = exp.end
			if(e.before(exp.end)) e = exp.end
			
		}
		s -> e
	}
	
	def main(args: Array[String]): Unit = {
		val DEFAULT_PATTERN_LAYOUT = "%-23d{yyyy-MM-dd HH:mm:ss,SSS} | %-30.30t | %-30.30c{1} | %-5p | %m%n"
		Logger.getRootLogger.addAppender(new ConsoleAppender(new PatternLayout(DEFAULT_PATTERN_LAYOUT)))
		
		val dp=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");	
		var infolder = "" 
		var outfile = ""
		var startDate:Date = null
		var endDate:Date = null
		var dryRun = false
		val parser = new OptionParser("scopt") {
		  arg("<infolder>", "<infolder> input file", { v: String => infolder = v })
		  argOpt("[<outfile>]", "<outfile> output file", { v: String => outfile = v })
		  opt("s","start", "When to start collecting data in yyyy-MM-ddTHH:mm:ss",  {v: String => startDate = dp.parse(v)})
		  opt("e", "end", "When to stop collecting data in yyyy-MM-ddTHH:mm:ss",  {v: String => endDate = dp.parse(v)})
		  opt("n", "dryrun", "Dont write to file", {dryRun = true} )
		  help("h", "help", "prints this usage text")
		  
		  // arglist("<file>...", "arglist allows variable number of arguments",
		  //   { v: String => config.files = (v :: config.files).reverse })
		}
		
		if(!parser.parse(args)) sys.exit(1)
		
		
		
		val folder = new File(infolder)
				
		log.info("Reading: " + folder)

		// Using par here does not improve performance		
		//val experiments = folder.listFiles.filter(_.isDirectory).par.map(new Experiment(_))
		val experiments = folder.listFiles.filter(_.isDirectory).map(new Experiment(_))		
		
		
		log.info("Tatal Experiments: " + experiments.size)
		log.info("Total Data: " + experiments.map(_.data.size).sum)
		
		val (tsDate, teDate ) = getStartEnd(experiments)
		log.info("Total timeframe: " + dp.format(tsDate) + " - " + dp.format(teDate))
		
		
		
		val selexp = if(startDate != null || endDate != null) {
			if(startDate == null) startDate = new Date(0)
			if(endDate == null) endDate = new Date(Long.MaxValue)
			log.info("Limiting selection to " + dp.format(startDate) + " - " + dp.format(endDate))
			experiments.filter(x => {x.end.after(startDate) && x.end.before(endDate)})
		} else experiments
		
		
		//Output
		val configs = selexp.map(_.config.keySet).reduce(_ union _).toList.sortWith(_.compareTo(_) < 0)
		val resKeys = selexp.map(_.results.map(_.k).toSet).reduce(_ union _).toList.sortWith(_.compareTo(_) < 0)
		val nodes = selexp.map(_.results.map(_.node).toSet).reduce(_ union _).toList.sortWith(_ < _)
		val expResKeys = selexp.map(_.expResults.map(_.k).toSet).reduce(_ union _).toList.sortWith(_.compareTo(_) < 0)
		
		
		log.info("Experiments: " + selexp.size)
		log.info("Data: " + selexp.map(_.data.size).sum)
		log.info("Configs: " + configs.size)
		log.info("Keys: " + resKeys.size)
		log.info("Nodes: " + nodes.size)
		log.info("ExpResults: " + selexp.map(_.expResults.size).sum)
		log.info("ExpKeys: " + expResKeys.size);
		
		
		val (sDate, eDate) = getStartEnd(selexp)
		log.info("Timeframe: " + dp.format(sDate) + " - " + dp.format(eDate))
		//log.debug("Keys: " +  resKeys.mkString(", "))
		

		if(dryRun) {
			log.info("No output")
			sys.exit(0)
		}
		
		val outname = if(outfile.length > 1) outfile else folder.toString
		

		// write output file containing results from given function	
		def output(name: String, fileSuffix: String, columns: Seq[String])
		          (f: Experiment => Iterable[Iterable[String]]) = {
			log.info("Wrinting " + name)

			val header = configs ++ columns
			val ColCount = header.size

			val oLines = for(exp <- experiments) yield {
				val conf = configs map { c => exp.config get c getOrElse "null" }
				f apply exp map { rv => (rv.size + conf.size) match {
					case ColCount => conf ++ rv
					case s @ _ => {	log.error("Wrong Size: " + s + " != " + ColCount + " !"); Nil }
				}}
			}

			if(oLines.size > 0) {
				val ofile = new java.io.PrintWriter(new File(outname + "." + fileSuffix))
				ofile.println(header.mkString(sep)) 
				for(line <- oLines.flatten) ofile.println(line.mkString(sep))
				ofile.close
			} else
				log.info("No " + name)
		}
		
		
		//Stacked results
		output("stacked results", "res.stacked", List(nodeStr, "key", "value")) { exp => 
			for(res <- exp.results) yield res.toList
		}

		//Stacked experiment results
		output("stacked experiment results", "res.exp_stacked", List("key", "value")) { exp => 
			for(res <- exp.expResults) yield res.toList
		}

		//Unstacked results
		output("unstacked results", "res.unstacked", List(nodeStr) ++ resKeys) { exp => 
			for((node, dat) <- exp.resultsNodeKeyValueMap) yield
				Seq(node.toString) ++ resKeys.map(dat.getOrElse(_, "null").toString)
		}

		//Unstacked experiment results
		output("unstacked experiment results", "res.exp_unstacked", expResKeys) { exp => 
			val dat = exp.expResultsKeyValueMap
			Seq(expResKeys.map(dat.getOrElse(_, "null").toString))
		}
		
		//R information
		log.info("Export Information to read in R")
		val ofile = new java.io.PrintWriter(new File(outname + ".res.r_inputs"))
		ofile.println( (configs ++ List(nodeStr)).mkString(" ") )
		ofile.close
		
		log.info("Done")
			
	}

}
