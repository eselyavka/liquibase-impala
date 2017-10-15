# Table of contents
1. [About liquibase-impala](#about-liquibase-impala)
2. [Notes on compatibility](#notes-on-compatibility)
3. [How to install](#how-to-install)
4. [How to use](#how-to-use)
    - [with a Maven plugin](#with-a-maven-plugin)
    - [with a standalone liquibase binary](#with-a-standalone-liquibase-binary)
    - [Liquibase-impala specific configuration](#liquibase-impala-specific-configuration)
5. [How to test locally](#how-to-test-locally)

# About liquibase-impala
Liquibase-impala is a [Liquibase](http://www.liquibase.org/) [extension](https://liquibase.jira.com/wiki/spaces/CONTRIB/overview), which adds support for Impala and Hive.

# Notes on compatibility
As of version 1.1.x the plugin was tested and should work with the following versions of external dependencies:

| Dependency          | Versions                     |
| :-----------------: | :--------------------------: |
| Liquibase           | 3.5.2, 3.5.3                 |
| Impala JDBC driver  | Cloudera Impala JDBC 2.5.32  |
| Hive JDBC driver    | Cloudera Impala Hive 2.5.18  |

**Other configurations are likely to work too** so you are encouraged to test with your versions. Let us know when you do!

# How to install
As of version 1.1.x liquibase-impala depends on proprietary Cloudera connectors for Impala and Hive. These are not present in any public Maven repositories.
Therefore, to build and install the plugin, you must do the following:
1. Download Impala JDBC driver and its dependencies from http://www.cloudera.com/downloads/connectors/impala/jdbc/2-5-32.html
2. Download Hive JDBC driver from http://www.cloudera.com/downloads/connectors/hive/jdbc/2-5-18.html
3. Unpack and install the following dependencies in your local Maven repository, using standard Maven command: 
```mvn install:install-file -Dfile=${file} -DgroupId=${groupId} -DartifactId=${artifactId} -Dversion=${version} -Dpackaging=jar```

| file                     | groupId                   | artifactId             | version |
| ------------------------ | ------------------------- | ---------------------- | ------- |
| ql.jar                   | com.cloudera.impala.jdbc  | ql                     | 2.5.32  |
| hive_metastore.jar       | com.cloudera.impala.jdbc  | hive_metastore         | 2.5.32  |
| hive_service.jar         | com.cloudera.impala.jdbc  | hive_metastore         | 2.5.32  |
| ImpalaJDBC41.jar         | com.cloudera.impala.jdbc  | ImpalaJDBC41.jar       | 2.5.32  |
| TCLIServiceClient.jar    | com.cloudera.impala.jdbc  | TCLIServiceClient.jar  | 2.5.32  |
| HiveJDBC41.jar           | com.cloudera.hive.jdbc    | HiveJDBC41.jar         | 2.5.18  |

4. _(optional, but recommended)_ Deploy the above artifacts to an internal, private Maven repository such as [Nexus](https://www.sonatype.com/nexus-repository-sonatype)
or [Artifactory](https://www.jfrog.com/artifactory/), for subsequent use.
5. Build liquibase-impala by executing ```mvn clean install```. This will install liquibase-impala in your local Maven repo and create a _liquibase-impala.jar_ fat-jar in the _target/_ directory.
6. _(optional, but recommended)_ Deploy liquibase-impala to your internal, private Maven repository.

# How to use
There are two distinct ways liquibase-impala can be used to manage your Impala or Hive database.

## with a Maven plugin
To use liquibase-impala in concert with ```liquibase-maven-plugin```:
1. Make sure liquibase-impala is present in your local or remote (internal) Maven repo.
2. Add the following to your ```pom.xml``` file:
```xml
<build>
  <plugins>
    <!-- (...) -->
    <plugin>
      <groupId>org.liquibase</groupId>
      <artifactId>liquibase-maven-plugin</artifactId>
      <version>${liquibase.version}</version>
      <dependencies>
        <!-- (...) -->
        <dependency>
          <groupId>org.liquibase.ext.impala</groupId>
          <artifactId>liquibase-impala</artifactId>
          <version>${liquibase.impala.version}</version>
        </dependency>
      </dependencies>
    </plugin>
  </plugins>
</build>
```

3. Run Liquibase as you normally would using Maven plugin, for example:
```
mvn liquibase:update \
  -Dliquibase.changeLogFile=changelog/changelog.xml \
  -Dliquibase.driver=com.cloudera.hive.jdbc41.HS2Driver \
  -Dliquibase.username=<user>
  -Dliquibase.password=<password>
  -Dliquibase.url=jdbc:hive2://<host>:<port>/<database>;UID=<user>;UseNativeQuery=1
```

## with a standalone liquibase binary
1. Make sure that ```liquibase``` is on your ```$PATH```
2. Modify ```liquibase.properties``` according to your Impala/Hive endpoint
3. Put liquibase-impala fat-jar on your classpath, f.e. under the ```${LIQUIBASE_HOME}/lib```
4. Start migration, f.e.: ```liquibase update```

## Liquibase-impala specific configuration
Liquibase-impala provides a number of additional configuration parameters that can be used to influence its behaviour:

| parameter         | values                | description                                       |
| ----------------- | --------------------- | ------------------------------------------------- |
| liquibase.lock    | true (default), false | enables/disables locking facility for a given job |
| liquibase.syncDDL | true (default), false | wraps every statement with [SYNC_DDL](http://www.cloudera.com/documentation/cdh/5-1-x/Impala/Installing-and-Using-Impala/ciiu_sync_ddl.html) |

# How to test locally
Script ```examples/run.sh``` performs basic integration testing of Impala and Hive, which includes:
* update execution
* tag execution
* rollback execution

The script can be executed with the command ```./run.sh <both|hive|impala> PATH_TO_LIQUIBASE_HOME```
