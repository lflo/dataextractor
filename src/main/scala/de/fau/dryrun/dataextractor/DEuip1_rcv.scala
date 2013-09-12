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
				override def parse(lines: List[String]) : Vector[Data] = {
					
					val splitlines = lines.map(Line.fromString).collect { case Some(l:Line) => l }
					//1234\turn:fau:0x000a\tMAC 00:12:74:00:0e:d5:30:12 Contiki 2.6 started. Node id is set to 10.
					var idmap = {
						import DataExtractor._
						for(Line(time, smote, msg) <- splitlines;
					                  splitmsg = msg.split(" ");
							  if splitmsg.size == 11;
							  if splitmsg(0) == "MAC";
							  mid <- extract(smote);
							  id = splitmsg(1).split(":").last.hex
						) yield id -> mid
					}.toMap
					
					DEuip1_rcv.backupMap ++= idmap
					
					//log.debug("IDmap: " + idmap.map(x => {x._1 + " " + x._2}).mkString(", ") )
					
					val rcvMap = collection.mutable.Map[Int, Int]().withDefaultValue(0)
					val parselines = for(Line(time, smote, msg) <- splitlines;
					                  splitmsg = msg.split(" ");
							  if splitmsg.size == 9;
							  if splitmsg(1) == "recv"
						      ) rcvMap(splitmsg(8).toInt) += 1

					for(key <- rcvMap.keys if(!idmap.contains(key)))
						getBackup(key) match {
							case Some(b) => {
								log.debug("Missing key  \""  + key + "\" in " + filename)
								idmap = idmap + (key -> b)
							}
							case None => {
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
