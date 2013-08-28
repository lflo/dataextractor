package de.fau.dryrun.dataextractor


import java.io.File
import DataExtractor._
import org.slf4j.LoggerFactory

class DEuip1 extends DataExtractor {
	val log = LoggerFactory.getLogger(this.getClass)
	
	val rst = List("rime_tx", "rime_reliabletx", "rime_rexmit", "rime_ackrx", 
			"rime_toolong", "rime_badsxnch", "rime_contentiondrop", "rime_lltx")
	val ust = List("recv", "sent", "forwarded", "drop", "vhlerr", "hblenerr",
			"lblenerr", "fragerr", "chkerr", "protoerr", "chkerr").map("ip_" + _ ) :::
			List("recv", "sent", "drop", "typeerr", "chkerr").map("icmp_" + _ ) :::
			List("recv", "sent", "drop", "chkerr", "ackerr","rst", "rexmit", "syndrop", 
					"synrst").map("tcp_" + _) :::
			List("drop", "recv", "sent", "chkerr").map("udp_" + _) :::
			List("drop", "recv", "sent").map("nd6_" + _)
			
	val egt = List("CPU", "PM", "TX", "RX").map("ener_" + _)
	
	override def getExtractor(file:File):Extractor = {
		var eg = false
		var us = false
		var rs = false
		
		def ok = eg && us && rs
		
		//log.debug("Checking file " + file + " - Name: " + file.getName)
				
		if(file.getName.equals("wisebed.log")){
			//log.debug("New Parallel")
			new Parallel() {
				override def parse(line:String):Vector[Data]  = {
					//log.debug("Parsing " + line)
					val Array(mid, data) = line.split(" ", 2)
					idExtract(mid) match {
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
							for((k, v) <- z.to[Vector] zip data.split(" ").tail)
								yield new Result(id, k, v.toInt)
						}
					}
					
				}
			}
		}
		else 
			Dont
	} 
	
	
}
