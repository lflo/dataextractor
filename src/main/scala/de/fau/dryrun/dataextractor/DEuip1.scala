package de.fau.dryrun.dataextractor

import java.io.File
import DataExtractor._
import org.slf4j.LoggerFactory
import scala.util.Try
import scala.util.Failure
import scala.util.Success
import Data.NilVD

/** Provides hex string to Int conversion */
object HexStrings {
	implicit class HexString(val s: String) {
		def hex:Int = {
			if(s.startsWith("0x")) {
				Integer.parseInt(s.drop(2), 16)	
			} else {
				Integer.parseInt(s, 16)
			}
		}
	}
}
import HexStrings._


/** Mote ID */
case class MoteID(id: String)


/** Timestamped mote log message */
case class Message(time: Long, mote: String, msg: String)
object Message {
	val sep = "\t"
	def fromString(line: String) = Try {
		val Array(time, mote, msg) = line.split(sep, 3)
		Message(time.toLong, mote, msg)
	}.toOption
}


/** Extraction of MoteID from log */
trait MotesExtractor {
	def extract(s:String): Option[Int]
}

/** MoteID extraction for Wisebed */
trait WisebedExtractor extends MotesExtractor {
	val filenamePattern = """wisebed\.log"""	

	def extract(id:String) = if(id.startsWith("urn:")) Some(id.split(":").last.hex) else None
}

/** MoteID extraction for COOJA */
trait SimulationExtractor extends MotesExtractor {
	val filenamePattern = """motes\.log"""	

	def extract(s:String) = Try(s.toInt).toOption
}

/** Extraction of mote messages */
trait MessagesExtractor extends LinearLineExtractor { self: MotesExtractor =>
	def parseMessage(time: Long, id: Int, msg: String): Vector[Data]

	def parseLine(line: String) = Message.fromString(line).collect {
		case Message(time, mote, msg) => extract(mote).map(id => parseMessage(time, id, msg))
	}.flatten getOrElse NilVD
}

/** UIP statistics extractor */
abstract class DEuip1 extends MessagesExtractor 
		with ParallelLineExtractor with MotesExtractor with FinishExtractor with FileExtractor {
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
			
	val egt = List("CPU", "LPM", "TX", "RX").map("ener_" + _)

	private def zipToRes(id:Int, keys:List[String], dat:String):Vector[Data] = {
		val v = dat.trim.split(" ")
		if(keys.size != v.size) {
			log.warn("Does not match:\n" + keys.mkString(", ") + "\n" + v.mkString(", "))
			return NilVD
		}

		val vInt = Try(v.map(_.toInt)).getOrElse {
			log.warn("Could not convert to Int: " + v.mkString(", "))
			return NilVD
		}
		
		keys.toVector zip vInt map { case (k,v) => Result(id, k, v) }
	}

	def failed(res: Vector[Data]) = (rst ++ ust ++ egt) exists { x => res.find(_.asInstanceOf[Result].k == x).isEmpty && { println(x);true} }
	def finish(res: Vector[Data]) = if(failed(res)) NilVD else res

	def parseMessage(time: Long, id: Int, msg: String) = msg.splitAt(3) match {
		case ("RS:", d) => zipToRes(id, rst, d)
		case ("US:", d) => zipToRes(id, ust, d)
		case ("EG:", d) => zipToRes(id, egt, d)
		case ("EG ", d) => zipToRes(id, egt, d)
		case _ => NilVD
	}
}

/** Wisebed UIP stat extractor */
class DEuipWB1 extends DEuip1 with WisebedExtractor 

/** COOJA UIP stat extractor */
class DEuipSim1 extends DEuip1 with SimulationExtractor

