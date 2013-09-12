package de.fau.dryrun.dataextractor


import java.io.File
import DataExtractor._
import org.slf4j.LoggerFactory
import scala.util.Try
import scala.util.Failure
import scala.util.Success

case class Line(time: Long, mote: String, msg: String)
object Line {
	val sep = "\t"
	def fromString(line: String)= Try {
		val Array(time, mote, msg) = line.split(sep, 3)
		Line(time.toLong, mote, msg)
	}.toOption
}

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
	
	private def zipToRes(id:Int, k:List[String], dat:String):(Boolean, Vector[Data])  = {
		val v = dat.split(" ")
		if(k.size != v.size) {
			log.warn("Does not match:\n" + k.mkString(", ") + "\n" + v.mkString(", "))
			return(false -> Vector[Result]())
		}
		val vInt = Try(v.map(_.toInt))
		
		if(vInt.isFailure) {
			log.warn("Could not convert to Int: " + v.mkString(", "))
			return(false -> Vector[Result]())
		}
		
		true -> k.toVector.zip(vInt.get).map(x => {new Result(id, x._1, x._2)})
	}
	
	override def getFileExtractor(file:File):FileExtractor = {

		
		//log.debug("Checking file " + file + " - Name: " + file.getName)
				
		if(file.getName.equals(filename)){
			//log.debug("New Parallel")
			new Parallel() {
				
				var eg = false
				var us = false
				var rs = false
				
				override def ok =  eg && us && rs
				override def parse(line:String):Vector[Data]  = {
					//log.debug("Parsing " + line)
					Line.fromString(line).collect {
						case Line(time, mote, msg) =>
							Some((extract(mote), msg.splitAt(3))) collect {
								case (Some(id), ("RS:", d)) => val(a, b) = zipToRes(id, rst, d); if(a) rs = true; b
								case (Some(id), ("US:", d)) => val(a, b) = zipToRes(id, ust, d); if(a) us = true; b
								case (Some(id), ("EG ", d)) => val(a, b) = zipToRes(id, egt, d); if(a) eg = true; b
							}
					}.flatten getOrElse Vector[Data]()
				}
			}
		}
		else 
			Dont
	} 
	
	override def apply() = new DEuip1
}

trait DEuipSim1 extends DEuip1 {

	override val filename = "motes.log"
		
	override def extract(s:String) = Try(s.dropRight(1).toInt).toOption
	
}

