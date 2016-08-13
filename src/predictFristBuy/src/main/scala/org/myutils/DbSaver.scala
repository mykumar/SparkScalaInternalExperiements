package org.myutils

import java.util.Properties
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils

class DbSaver(_url: String, _username: String, _password: String, _driver: String)
{
  val url = _url
  val username = _username
  val password = _password
  val driver = _driver

  private def getProps = {
    val props = new Properties()
    props.setProperty("user", username)
    props.setProperty("password", password)
    props.setProperty("driver", driver)

    props
  }

  def createAndSave(df: DataFrame, table: String): Unit = {
    df.write.jdbc(url, table, getProps)
  }

  def append(df: DataFrame, table: String): Unit = {
    JdbcUtils.saveTable(df, url, table, getProps)
  }
}
