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
	
	
	def extract(s:String) = idExtract(s)
	
	override def getExtractor(file:File):Extractor = {
		var eg = false
		var us = false
		var rs = false
		
		def ok = eg && us && rs
		
		//log.debug("Checking file " + file + " - Name: " + file.getName)
				
		if(file.getName.equals("wisebed.log")){
			//log.debug("New Parallel")
			new Parallel() {
				override def parse(line:String):List[Data]  = {
					//log.debug("Parsing " + line)
				val (mid, _data) = line.span(_ != ':')
					lazy val data = _data.dropWhile(_ != ' ').tail
					lazy val vals = data.split(" ")
					extract(mid) match {
						case None => List[Data]()
						case Some(id) => {
							//log.debug("data:  -" + data + "-")
							if(data.startsWith("RS: ") && rst.size == data.size) {								
								//log.debug("Got RS")
								rs = true
								vals.tail.map(_.toInt).zip(rst).map(x => {new Result(id, x._2, x._1)}).toList
							} else if(data.startsWith("US: ") && ust.size == data.size) {
								us = true
								//log.debug("Got US")
								vals.tail.map(_.toInt).zip(ust).map(x => {new Result(id, x._2, x._1)}).toList
							}else if(data.startsWith("EG: ") && egt.size == data.size) {
								eg = true
								vals.tail.map(_.toInt).zip(egt).map(x => {new Result(id, x._2, x._1)}).toList
							} else						
								List[Data]();
						}
					}
					
				}
			}
		}
		else 
			Dont
	} 
	
	
}