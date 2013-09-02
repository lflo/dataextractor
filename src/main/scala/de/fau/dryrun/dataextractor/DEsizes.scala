package de.fau.dryrun.dataextractor

import org.slf4j.LoggerFactory
import java.io.File

class DEsizes extends DataExtractor {
	val log = LoggerFactory.getLogger(this.getClass)
	
		override def getFileExtractor(file:File):FileExtractor = {

		
		//log.debug("Checking file " + file + " - Name: " + file.getName)
				
		if(file.getName.equals("sizes.txt")){
			//log.debug("Size")
			new Lines() {
				val dat = collection.mutable.Map[String, Int]()
				override def parse(lines: List[String]) : Vector[Data] = {
					/*
					 *    text	   data	    bss	    dec	    hex	filename
					 *    48892	    214	   8816	  57922	   e242	udp-client.sky
					 *    48470	    214	   8762	  57446	   e066	udp-server.sky
					 * 
					 */
					val data = Vector("text", "data", "bss", "dec", "hex", "filename")
					val idata =lines(0).split("\t").map(_.trim).toVector
					if(! (idata.equals(data))) {
						log.debug("LS: " + idata.mkString("-","-","-"))
						log.debug("LI: " + data.mkString("-","-","-"))
						return Vector[Data]()
					}
					val rv = collection.mutable.Buffer[Data]()
					for(l <- lines.tail)  {
						val els = l.split("\t").map(_.trim)
						log.debug("Ld: " + els.mkString("-","-","-"))
						rv += new ExpResult(els(5)+"_txt", els(0).toInt)
						rv += new ExpResult(els(5)+"_data", els(1).toInt)
						rv += new ExpResult(els(5)+"_bss", els(2).toInt)
					}
					rv.toVector
					
				}
			}
		} else 
			Dont
	}

}