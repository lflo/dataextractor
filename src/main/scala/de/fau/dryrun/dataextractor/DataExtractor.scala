
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
	class FileExtractor {
		def ok:Boolean = true
	}
	abstract case class Lines() extends FileExtractor { //Pass all lines
		def parse(lines: List[String]) : Vector[Data]
	} 
	abstract case class Linear() extends FileExtractor { //Pass lines linear
		def parse(line:String):Vector[Data]
	}
	abstract case class Parallel() extends FileExtractor { // Pass lines in parallel
		def parse(line:String):Vector[Data]
	}
	case object Dont extends FileExtractor //Dont parse file
		
	protected def getFileExtractor(file:File):FileExtractor = Dont
	
	/*
	 * This function allows to parse the file first and then decide whether this was a valid extractor
	 */
	def ok = true
	
	def extractFile(file:File):Vector[Data] = {
		val fe = getFileExtractor(file) 
		val rv = fe match {
			case ec:Lines => ec.parse(Source.fromFile(file).getLines.toList); 
			case ec:Linear => Source.fromFile(file).getLines.foldLeft(Vector[Data]())(_ ++ ec.parse(_))
			case ec:Parallel => Source.fromFile(file).getLines.aggregate(Vector[Data]())(_ ++ ec.parse(_), _ ++ _)
			case Dont => Vector[Data]() 
		}
		if(fe.ok == true) rv else Vector[Data]()
		
	}
	
	def extractDir(dir:File):Vector[Data] = {
		val rv = dir.listFiles.aggregate(Vector[Data]())( (list, file) => {
			list ++ {
				if(file.isDirectory) {
					extractDir(file)
				} else {
					extractFile(file)
				}
			}
		}, _ ++ _)
		
		if(ok) rv else Vector[Data]() 
	}
	def apply():DataExtractor =  throw new Exception("DataExtractor has no Factory")
}

object DataExtractor{
	import scala.language.implicitConversions
	
	val log = LoggerFactory.getLogger(this.getClass)
	
	class HexString(val s: String) {
		def hex:Int = {
			if(s.startsWith("0x")) {
				Integer.parseInt(s.drop(2), 16)	
			} else {
				Integer.parseInt(s, 16)
			}
		}
	}
	
	implicit def str2hex(str: String): HexString = new HexString(str)
	
	class MoteID(val id: String) extends AnyVal
	
	/**
	 * Extract the id of the node 
	 */
	def idExtract(id:String):Option[Int] = {
		if(id.startsWith("urn:")) {
			return Some(id.split(":").last.hex)
		} 
		//log.info("Not urn, but -"  + id + "-" )
		None
	}
	
	def apply():DataExtractor =  throw new Exception("DataExtractor has no Factory")
}
