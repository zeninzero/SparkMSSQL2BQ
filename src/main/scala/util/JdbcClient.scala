package com.zero.util

import java.sql.DriverManager
import java.sql.Connection

object JdbcClient {

    def getTableDetails(driver:String,url:String,username: String,password: String,dbTable: String,whereClause: String) = {

      var connection:Connection = null
      var colName : String = ""
      var upperBound = 0L

      try {
        // make the connection
        Class.forName(driver)
        println(driver)

        connection = DriverManager.getConnection(url, username, password)

        // create the statement, and run the select query
        val statement = connection.createStatement()
        val resultSetCol = statement.executeQuery("SELECT top 10 * FROM "+dbTable+" ")

        println("resultSetCol::"+resultSetCol)

        colName = resultSetCol.getMetaData().getColumnName(1)

        val resultSetCount = statement.executeQuery("SELECT count(1) as upperBound FROM "+dbTable+whereClause)
        resultSetCount.next()
        upperBound = resultSetCount.getLong("upperBound")

      } catch {
        case e => e.printStackTrace
      }
      connection.close()
      (colName,upperBound)

    }

}
