package de.fau.dryrun.dataextractor

import java.io.File
import org.slf4j.LoggerFactory

class DEuip1_rcv extends DEuip1 {
    
	def getBackup(id:Int):Option[Int] = DEuip1_rcv.backupMap.get(id)
	
	override def getFileExtractor(file:File):FileExtractor = {		
		//log.debug("Checking file " + file + " - Name: " + file.getName)
				
		if(file.getName.equals(filename)){
			//log.debug("New Parallel")
			new Lines() {
				val dat = collection.mutable.Map[String, Int]()
				override def parse(lines: List[String]) : Vector[Data] = {
					
					val splitlines = lines.map(_.split(" "))
					//urn:fau:0x000a: MAC 00:12:74:00:0e:d5:30:12 Contiki 2.6 started. Node id is set to 10.
					val idlines = splitlines.filter(x => {x.size == 12 && x(1).equals("MAC")})
					var idmap = {for(l <- idlines) yield {
						import DataExtractor._
						val mid = extract(l(0)).get
						val id = l(2).split(":").last.hex
						id -> mid
					}}.toMap
					
					DEuip1_rcv.backupMap ++= idmap
					
					//log.debug("IDmap: " + idmap.map(x => {x._1 + " " + x._2}).mkString(", ") )
					
					val parselines = splitlines.filter(x => {x.size == 10 && x(2).equals("recv")})
					
					val rcvMap = collection.mutable.Map[Int, Int]()
					for(l <- parselines)  {
						val src = l(9).toInt
						val ctr = rcvMap.getOrElse(src, 0)
						rcvMap += src -> (ctr + 1) 
					}
					for(key <- rcvMap.keys) if(!idmap.contains(key)) {
						val b = getBackup(key)
						if(b.isDefined) {
							log.debug("Missing key  \""  + key + "\" in " + filename)
							idmap = idmap + (key -> b.get)
						} else {
							log.error("Missing key  \""  + key + "\" in " + filename + " Could not Recover")
							return Vector[Data]()
						}
					}
					rcvMap.map(x => new Result(idmap(x._1),"meta_packrcv", x._2)).toVector
				}
			}
				
		}
		else 
			Dont
	} 
}

object DEuip1_rcv{
	val backupMap = collection.mutable.Map[Int, Int]()
}

trait DEuip1_rcvSim extends DEuip1_rcv {
	override def getBackup(id:Int):Option[Int] = Some(id)
}
