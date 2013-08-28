package de.fau.dryrun.dataextractor


import java.io.File
import DataExtractor._
import org.slf4j.LoggerFactory

class DEuip1 extends DataExtractor {
	val log = LoggerFactory.getLogger(this.getClass)

	val rst = List("tx", "rx", "reliabletx", "reliablerx", "rexmit", "acktx", "noacktx",
		"ackrx", "timedout", "badackrx", "toolong", "tooshort", "badsynch", "badcrc",
		"contentiondrop", "sendingdrop", "lltx", "llrx").map("rime_" + _)
	val ust = List("recv", "sent", "forwarded", "drop", "vhlerr", "hblenerr",
			"lblenerr", "fragerr", "chkerr", "protoerr").map("ip_" + _ ) :::
			List("recv", "sent", "drop", "typeerr", "chkerr").map("icmp_" + _ ) :::
			List("recv", "sent", "drop", "chkerr", "ackerr","rst", "rexmit", "syndrop", 
					"synrst").map("tcp_" + _) :::
			List("drop", "recv", "sent", "chkerr").map("udp_" + _) :::
			List("drop", "recv", "sent").map("nd6_" + _)
			
	val egt = List("CPU", "PM", "TX", "RX").map("ener_" + _)
	
	
	def extract(s:String) = idExtract(s)
	val filename = "wisebed.log"
	
	override def getExtractor(file:File):Extractor = {
		var eg = false
		var us = false
		var rs = false
		
		def ok = eg && us && rs
		
		//log.debug("Checking file " + file + " - Name: " + file.getName)
				
		if(file.getName.equals(filename)){
			//log.debug("New Parallel")
			new Parallel() {
				override def parse(line:String):Vector[Data]  = {
					//log.debug("Parsing " + line)
					val Array(mid, data) = line.split(" ", 2)
					extract(mid) match {
						case None => Vector[Data]()
						case Some(id) => {
							//log.debug("data:  -" + data + "-")
							val z = data.take(4) match {
								case "RS: " => {
									rs = true
									rst }
								case "US: " => {
									us = true
									ust }
								case "EG: " => {
									eg = true
									egt }
								case _ => Nil
							}
							val dataa = data.trim.split(" ").tail
							/*
							if(z != Nil) {
								log.debug("data: -" +data + "-")
								log.debug("dataa: -" + dataa.mkString(" ") + "-")
							
								log.debug("SZ: "  + z.size + "/" + dataa.size)
							}*/
							if(z.size != dataa.size) 
								Vector[Data]() 
							else {
								for((k, v) <- z.to[Vector] zip dataa)
									yield new Result(id, k, v.toInt)
							}
						}
					}
					
				}
			}
		}
		else 
			Dont
	} 
	
	
}
