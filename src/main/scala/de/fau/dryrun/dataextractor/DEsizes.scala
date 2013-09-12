package de.fau.dryrun.dataextractor

import org.slf4j.LoggerFactory
import java.io.File
import Data.NilVD

class DEsizes extends LineExtractor with FileExtractor {
	val log = LoggerFactory.getLogger(this.getClass)

	val filenamePattern = """sizes\.txt"""
	
	val dat = collection.mutable.Map[String, Int]()
	
	def parse(lines: Iterator[String]):Vector[Data] = {
		/*
		 *    text	   data	    bss	    dec	    hex	filename
		 *    48892	    214	   8816	  57922	   e242	udp-client.sky
		 *    48470	    214	   8762	  57446	   e066	udp-server.sky
		 * 
		 */
		val data = Vector("text", "data", "bss", "dec", "hex", "filename")
		val idata = lines.next.split("\t").map(_.trim).toVector
		if(idata.isEmpty) {
			log.error("Could not parse file. No content.")
			return NilVD
		}

		if(! (idata.equals(data))) {
			log.debug("LS: " + idata.mkString("-","-","-"))
			log.debug("LI: " + data.mkString("-","-","-"))
			return Vector[Data]()
		}
		val rv = collection.mutable.Buffer[Data]()
		for(l <- lines)  {
			val els = l.split("\t").map(_.trim)
			//log.debug("Ld: " + els.mkString("-","-","-"))
			rv += ExpResult(els(5)+"_txt", els(0).toInt)
			rv += ExpResult(els(5)+"_data", els(1).toInt)
			rv += ExpResult(els(5)+"_bss", els(2).toInt)
		}
		rv.toVector
		
	}

}
