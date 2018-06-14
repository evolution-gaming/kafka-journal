package akka.persistence.kafka.journal

import java.io.File

trait TmpDir {
  def file: File
  def delete(): Unit
}

object TmpDir {

  def apply(name: String): TmpDir = new TmpDir {
    val file: File = {
      val file = File.createTempFile(name, null)
      file.deleteOnExit()
      file.delete()
      file.mkdirs()
      file
    }

    def delete(): Unit = file.deleteRecursively()
  }

  implicit class FileOps(val self: File) extends AnyVal {

    def deleteRecursively(): Unit = {
      if (self.isDirectory) {
        val files = Option(self.listFiles())
        for {
          files <- files
          file <- files
        } file.deleteRecursively()
      }
      self.delete()
    }
  }
}
