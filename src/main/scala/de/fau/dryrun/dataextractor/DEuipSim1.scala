package de.fau.dryrun.dataextractor


import java.io.File
import DataExtractor._
import org.slf4j.LoggerFactory
import scala.util.Try
import scala.util.Success
import scala.util.Failure

class DEuipSim1 extends DEuip1 {
	override val filename = "motes.log"
		
	override def extract(s:String) = {
		
		Try(s.dropRight(1).toInt) match {
			case Failure(_) => None
			case Success(id) => Some(id)
		}
	}	
	
	override def apply() = new DEuipSim1
}

object DEuipSim1{
	def apply() = new DEuipSim1
}
