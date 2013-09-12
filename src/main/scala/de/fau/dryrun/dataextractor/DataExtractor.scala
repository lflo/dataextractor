/**
 * Extract Data
 */

package de.fau.dryrun.dataextractor

import java.io.File
import scala.io.Source
import org.slf4j.LoggerFactory
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.collection.mutable.Buffer
import collection.JavaConversions._
import java.util.Date



sealed abstract class Data extends Product {
	def toList = productIterator.map(_.toString).toList
	var timeStamp:Option[Date] = None
	def withTimeStamp(time: Date) = {
		timeStamp = Some(time)
		this
	}
}
case class ExpResult(k:String, v:Long) extends Data
case class Result(node:Int, k:String, v:Long) extends Data
//case class ExpSnapshot(time:Long, k:String, v:Long) extends Data
//case class Snapshot(node:Int, time:Long, k:String, val v:Long) extends Data

// Provide a Nil for Vector[Data] for convenience
object Data {
	val NilVD = Vector[Data]()
}
import Data.NilVD


/** Abstract data extractor */
abstract class DataExtractor {	
	/** Extract given file */
	def extractFile(file:File):Vector[Data]
	
	/** Extract file or folder and combine results */
	def extract(file:File):Vector[Data] = {
		if(DataExtractor.isDir(file)) {
			DataExtractor.getDirList(file).par.aggregate(NilVD)( (list, f) =>
				list ++ extract(f)
			, _ ++ _)
		} else {
			val modified = new Date(file.lastModified)
			// TODO: IF there is a timestamp from the raw data we should use that!
			extractFile(file) map { _.withTimeStamp(modified) } 
		}
	}
}

/** Mixin to only extract files matching given pattern */
trait FileExtractor extends DataExtractor {
	def filenamePattern: String

	abstract override def extractFile(file:File) =
		if(file.getName matches filenamePattern)
			super.extractFile(file)
		else
			NilVD
}

/** Mixin to modify/check results after reading the file */
trait FinishExtractor extends DataExtractor {
	def finish(res: Vector[Data]): Vector[Data]

	abstract override def extractFile(file:File) = 
		finish(super.extractFile(file))
}

/** Extract file as a Iterator of lines */
trait LineExtractor extends DataExtractor {
	def parse(lines: Iterator[String]): Vector[Data]

	def extractFile(file:File) = parse(Source.fromFile(file).getLines)
}

/** Extract file by extracting individual lines */
trait LinearLineExtractor extends LineExtractor {
	def parseLine(line: String): Vector[Data]

	def parse(lines: Iterator[String]) = lines.foldLeft(NilVD)(_ ++ parseLine(_))
}

/** Extract file by extracting individual lines in parallel */
trait ParallelLineExtractor extends LinearLineExtractor {
	override def parse(lines: Iterator[String]) = lines.toStream.par.aggregate(NilVD)(_ ++ parseLine(_), _ ++ _)
}


// Filesystem caching
object DataExtractor{
	val listMap: collection.concurrent.Map[File, Array[File]] = new java.util.concurrent.ConcurrentHashMap[File,Array[File]]
	def getDirList(dir:File) = listMap.getOrElseUpdate(dir, dir.listFiles)

	val dirMap: collection.concurrent.Map[File, Boolean] = new java.util.concurrent.ConcurrentHashMap[File,Boolean]
	def isDir(file:File) = dirMap.getOrElseUpdate(file, file.isDirectory)
}


/** Experiment contained in subdirectory */
class Experiment(dir: File) {
	val log = LoggerFactory.getLogger(this.getClass)
	//log.debug("Parsing " + dir)
	 
	
	
	val extractors:List[Unit => DataExtractor] = List(
			Unit => {new DEuipWB1},
			Unit => {new DEuipSim1},
			Unit => {new DEuip1_rcvWB},
			Unit => {new DEuip1_rcvSim}, 
			Unit => {new DEsizes}
	)
	
	val config = Source.fromFile(dir.toString +"/conf.txt").getLines.map(_.split("=", 2)).map(e => e(0) -> e(1)).toMap		
	
	//Handle dir with every extractor
	val data = extractors.par.aggregate(NilVD)(_ ++ _().extract(dir), _ ++ _)
	
	val (start, end) = data.map(_.timeStamp).foldLeft(new Date(Long.MaxValue), new Date(0)) {
		case ((min, max), Some(ts)) => (if(ts before min) ts else min, if(ts after max) ts else max)
		case ((min, max), None) => (min, max)
	}
	
	//Prepare results
	val results:Vector[Result] =  data.view.filter(_.isInstanceOf[Result]).map(_.asInstanceOf[Result]).toVector
	val resultsNodeKeyValueMap = {
		val rv = collection.mutable.Map[Int, collection.mutable.Map[String, Long]]()
		for(r <- results) {
			val cll = rv.getOrElseUpdate(r.node, collection.mutable.Map[String, Long]())
			cll += (r.k -> r.v)
		}
		//Convert to immutable
		rv.map(x => {x._1 -> x._2.toMap}).toMap
	}
	val resultNodes = {
		results.map(_.node).toSet
	}
	
	//Prepare experiment Results
	val expResults:Vector[ExpResult] =  data.view.filter(_.isInstanceOf[ExpResult]).map(_.asInstanceOf[ExpResult]).toVector
	val expResultsKeyValueMap = expResults.map(x =>  {x.k -> x.v}).toMap
		
	
	//def snapshots =  data.filter(_.isInstanceOf[Snapshot]).map(_.asInstanceOf[Snapshot])
}
