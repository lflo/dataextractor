package de.fau.dryrun.dataextractor

import java.io.File
import org.slf4j.LoggerFactory
import Data.NilVD
import HexStrings._

/** RPL-UDP packet reception count extractor */
trait DEuip1_rcv extends LineExtractor with MotesExtractor with FileExtractor {
	val log = LoggerFactory.getLogger(this.getClass)
    
	def getBackup(id:Int):Option[Int] = DEuip1_rcv.backupMap.get(id)
	
	def parse(lines: Iterator[String]): Vector[Data] = {
		val splitlines = lines.map(Message.fromString).flatten.toList
		
		var missKeyReported = false 

		//1234	urn:fau:0x000a	MAC 00:12:74:00:0e:d5:30:12 Contiki 2.6 started. Node id is set to 10.
		var idMap = {
			for(Message(time, smote, msg) <- splitlines;
		                  splitmsg = msg.split(" ");
				  if splitmsg.size == 11;
				  if splitmsg(0) == "MAC";
				  mid <- extract(smote);
				  id = splitmsg(1).split(":").last.hex
			) yield id -> mid
		}.toMap.withDefault { key => 
			getBackup(key) match {
				case Some(b) => {
					if(!missKeyReported) {
						missKeyReported = true
						log.debug("Missing key  \""  + key + "\", No more missing keys will be reported for this file")
					}
					b }
				case None => 
					{ log.error("Missing key  \""  + key + "\" Could not Recover"); return NilVD } 
			}
		}
		
		DEuip1_rcv.backupMap ++= idMap
		
		//log.debug("IDmap: " + idMap.map(x => {x._1 + " " + x._2}).mkString(", ") )
		
		val rcvMap = collection.mutable.Map[Int, Int]().withDefaultValue(0)
		val parselines = for(Message(time, smote, msg) <- splitlines;
		                  splitmsg = msg.split(" ");
				  if splitmsg.size == 8;
				  if splitmsg(1) == "recv"
			      ) rcvMap(splitmsg(7).toInt) += 1
		
		rcvMap.map { case (id, cnt) => new Result(idMap(id), "meta_packrcv", cnt) }.toVector
		// TODO
		//val results = for((id, mid) <- DEuip1_rcv.backupMap) yield Result(mid, "meta_packrcv", rcvMap.getOrElse(mid, 0).toLong)
		//results.toVector
	}

}

object DEuip1_rcv{
	import collection.JavaConversions._
	val backupMap:collection.concurrent.Map[Int, Int] = new java.util.concurrent.ConcurrentHashMap[Int, Int]()
}

class DEuip1_rcvWB extends DEuip1_rcv with WisebedExtractor
class DEuip1_rcvSim extends DEuip1_rcv with SimulationExtractor {
	override def getBackup(id:Int) = Some(id)
}
