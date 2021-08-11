package org.example

import org.apache.log4j.{LogManager, Logger}

trait Logging {

  val logger: Logger = LogManager.getLogger(getClass)

}
