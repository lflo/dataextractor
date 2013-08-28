
/**
 * Extract Data
 */

package de.fau.dryrun.dataextractor

import java.io.File
import scala.io.Source
import org.slf4j.LoggerFactory

sealed abstract class Data
class ExpResult(val k:String, val v:Long) extends Data
class Result(val node:Int, val k:String, val v:Long) extends Data {
	def nkvList= List[String](node.toString, k, v.toString)
}
class ExpSnapshot(val time:Long, val k:String, val v:Long) extends Data
class Snapshot(val node:Int, val time:Long, val k:String, val v:Long) extends Data



abstract class DataExtractor {	
	class Extractor
	abstract case class Lines() extends Extractor { //Pass all lines
		def parse(lines: List[String]) : List[Data]
	} 
	abstract case class Linear() extends Extractor { //Pass lines linear
		def parse(line:String):List[Data]
	}
	abstract case class Parallel() extends Extractor { // Pass lines in parallel
		def parse(line:String):List[Data]
	}
	case object Dont extends Extractor //Dont parse file
		
	protected def getExtractor(file:File):Extractor = Dont
	
	/*
	 * This function allows to parse the file first and then decide whether this was a valid extractor
	 */
	protected def ok = true
	
	def extractFile(file:File):List[Data] = {
		getExtractor(file) match {
			case ec:Lines => ec.parse(Source.fromFile(file).getLines.toList)
			case ec:Linear => Source.fromFile(file).getLines.foldLeft(List[Data]())(_ ::: ec.parse(_))
			case ec:Parallel => Source.fromFile(file).getLines.aggregate(List[Data]())(_ ::: ec.parse(_), _ ::: _)
			case Dont =>  List[Data]()
		}
		
	}
	
	def extractDir(dir:File):List[Data] = {
		val rv = dir.listFiles.aggregate(List[Data]())( (list, file) => {
			list ::: {
				if(file.isDirectory) {
					extractDir(file)
				} else {
					extractFile(file)
				}
			}
		}, _ ::: _)
		
		if(ok) rv else List[Data]()
	}
}

object DataExtractor{
	import scala.language.implicitConversions
	
	val log = LoggerFactory.getLogger(this.getClass)
	
	class HexString(val s: String) {
		def hex:Int = {
			if(s.startsWith("0x")) {
				Integer.parseInt(s.splitAt(2)._2, 16)	
			} else {
				Integer.parseInt(s, 16)
			}
		}
	}
	
	implicit def str2hex(str: String): HexString = new HexString(str)
	
	class MoteID(val id: String)
	
	/**
	 * Extract the id of the node 
	 */
	def idExtract(id:String):Option[Int] = {
		if(id.startsWith("urn:")) {
			return Some(id.split(":").apply(2).hex)
		} 
		//log.info("Not urn, but -"  + id + "-" )
		None
	}
	
	
}
