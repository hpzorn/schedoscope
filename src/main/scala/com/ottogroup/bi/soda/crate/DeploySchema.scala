package com.ottogroup.bi.soda.crate
import com.ottogroup.bi.soda.crate.ddl.HiveQl
import com.ottogroup.bi.soda.dsl.View
import java.sql.DriverManager
import java.sql.Connection
import java.security.MessageDigest
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import scala.collection.mutable.HashSet
import scala.collection.JavaConversions._
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.fs.FileSystem
import java.security.PrivilegedAction
import scala.Array.canBuildFrom
import com.ottogroup.bi.soda.crate.ddl.HiveQl
import org.apache.hadoop.hive.metastore.api.Partition
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException
import org.apache.hadoop.hive.metastore.api.Database
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException
import java.io.InvalidObjectException

class DeploySchema(val metastoreClient: IMetaStoreClient, val connection: Connection) {

  val md5 = MessageDigest.getInstance("MD5")
  val existingSchemas = collection.mutable.Set[String]()
  def digest(string: String): String = md5.digest(string.toCharArray().map(_.toByte)).map("%02X" format _).mkString

  def alterTable(dbName: String, tableName: String, key: String, value: String): Unit = {
    val table = metastoreClient.getTable(dbName, tableName)
    table.putToParameters(key, value)
    metastoreClient.alter_table(dbName, tableName, table)
  }

  def dropAndCreateTableSchema(dbName: String, tableName: String, sql: String): Unit = {
    val stmt = connection.createStatement()
    if (!metastoreClient.getAllDatabases.contains(dbName)) {
      stmt.execute(s"CREATE DATABASE ${dbName}")
    }
    if (metastoreClient.tableExists(dbName, tableName)) {
      metastoreClient.dropTable(dbName, tableName, false, true)
    }
    stmt.execute(sql)
    alterTable(dbName, tableName, "hash", digest(sql))
  }

  def schemaExists(dbname: String, tableName: String, sql: String): Boolean = {

    val d = digest(sql)
    if (existingSchemas.contains(d))
      return true;
    if (!metastoreClient.tableExists(dbname, tableName)) {
      false
    } else {
      val table = metastoreClient.getTable(dbname, tableName)
      val props = table.getParameters()
      if (!props.containsKey("hash"))
        false
      else if (d == props.get("hash").toString()) {
        existingSchemas += d
        true
      } else
        false
    }
  }

  def createPartition(view: View): Partition = {
    if (!schemaExists(view.dbName, view.n, HiveQl.ddl(view)))
      dropAndCreateTableSchema(view.dbName, view.n, HiveQl.ddl(view))
    try {
      metastoreClient.appendPartition(view.dbName, view.n, view.partitionPathBuilder.apply)
    } catch {
      case e: AlreadyExistsException => metastoreClient.getPartition(view.dbName, view.n, view.partitionPathBuilder.apply)
      case e: InvalidObjectException => println(view.partitionPathBuilder.apply); throw (e)
    }

  }

  def removeObsoleteTables(dbname: String, validTables: List[String]) = {
    val tables = metastoreClient.getTables(dbname, "*")
    tables.diff(validTables).foreach { tableName =>
      {
        val table = metastoreClient.getTable(dbname, tableName)
        if (table.getParameters().containsKey("hash"))
          metastoreClient.dropTable(dbname, tableName, false, true)
      }
    }
  }

  def deploySchemataForViews(views: Seq[View]): Unit = {
    val hashSet = HashSet[String]()
    views.filter(view => { if (hashSet.contains(HiveQl.ddl(view))) { false } else { hashSet.add(HiveQl.ddl(view)); true } }).foreach { view =>
      {
        if (!schemaExists(view.dbName, view.n, HiveQl.ddl(view)))
          dropAndCreateTableSchema(view.dbName, view.n, HiveQl.ddl(view))
      }
    }
  }

}

object DeploySchema {
  def apply(jdbcUrl: String, metaStoreUri: String, serverKerberosPrincipal: String) = {
    val hadoopConfiguration = new Configuration(false)
    hadoopConfiguration.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"))
    hadoopConfiguration.addResource(new Path("/etc/hadoop/conf/core-site.xml"))
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    UserGroupInformation.setConfiguration(hadoopConfiguration)
    val user = UserGroupInformation.getLoginUser()
    val hdfs = FileSystem.get(hadoopConfiguration)
    val connection =
      user.doAs(new PrivilegedAction[Connection]() {
        def run(): Connection = {
          DriverManager.getConnection(jdbcUrl)
        }
      })

    val conf = new HiveConf()
    conf.set("hive.metastore.local", "false");
    println(metaStoreUri.trim())
    conf.setVar(HiveConf.ConfVars.METASTOREURIS, metaStoreUri.trim());
    if (serverKerberosPrincipal != null) {
      conf.setBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL,
        true);
      conf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL,
        serverKerberosPrincipal);
    }
    //conf.getAllProperties().store(System.out, "")
    val metastoreClient = new HiveMetaStoreClient(conf)
    new DeploySchema(metastoreClient, connection)
  }

  def apply(metastoreClient: IMetaStoreClient, connection: Connection) = {
    new DeploySchema(metastoreClient, connection)
  }

  def main(args: Array[String]) = {

  }
}