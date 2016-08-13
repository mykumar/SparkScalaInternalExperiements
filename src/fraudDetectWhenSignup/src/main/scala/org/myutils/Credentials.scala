package org.myutils

import java.io.File
import org.ini4j.Ini

class Credentials(file: String)
{
  val ini = file

  def handler(): Ini = {
    new Ini(new File(ini))
  }
}
