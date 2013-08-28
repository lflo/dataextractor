package de.fau.dryrun.dataextractor


import java.io.File
import DataExtractor._
import org.slf4j.LoggerFactory
import scala.util.Try
import scala.util.Success
import scala.util.Failure

class DEuipSim1 extends DEuip1 {
	
	def extract(s:String) = {		
		Try(s.toInt) match {
			case Failure(_) => None
			case Success(id) => Some(id)
		}
	}	
	
}