## Cross-building

CabinDB can be built as a single self contained cross-platform JAR. The cross-platform jar can be used on any 64-bit OSX system, 32-bit Linux system, or 64-bit Linux system.

Building a cross-platform JAR requires:

 * [Docker](https://www.docker.com/docker-community)
 * A Mac OSX machine that can compile CabinDB.
 * Java 7 set as JAVA_HOME.

Once you have these items, run this make command from CabinDB's root source directory:

    make jclean clean cabindbjavastaticreleasedocker

This command will build CabinDB natively on OSX, and will then spin up docker containers to build CabinDB for 32-bit and 64-bit Linux with glibc, and 32-bit and 64-bit Linux with musl libc.

You can find all native binaries and JARs in the java/target directory upon completion:

    libcabindbjni-linux32.so
    libcabindbjni-linux64.so
    libcabindbjni-linux64-musl.so
    libcabindbjni-linux32-musl.so
    libcabindbjni-osx.jnilib
    cabindbjni-x.y.z-javadoc.jar
    cabindbjni-x.y.z-linux32.jar
    cabindbjni-x.y.z-linux64.jar
    cabindbjni-x.y.z-linux64-musl.jar
    cabindbjni-x.y.z-linux32-musl.jar
    cabindbjni-x.y.z-osx.jar
    cabindbjni-x.y.z-sources.jar
    cabindbjni-x.y.z.jar

Where x.y.z is the built version number of CabinDB.

## Maven publication

Set ~/.m2/settings.xml to contain:

    <settings xmlns="http://maven.apache.org/SETTINGS/1.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 http://maven.apache.org/xsd/settings-1.0.0.xsd">
      <servers>
        <server>
          <id>sonatype-nexus-staging</id>
          <username>your-sonatype-jira-username</username>
          <password>your-sonatype-jira-password</password>
        </server>
      </servers>
    </settings>

From CabinDB's root directory, first build the Java static JARs:

    make jclean clean cabindbjavastaticpublish

This command will [stage the JAR artifacts on the Sonatype staging repository](http://central.sonatype.org/pages/manual-staging-bundle-creation-and-deployment.html). To release the staged artifacts.

1. Go to [https://oss.sonatype.org/#stagingRepositories](https://oss.sonatype.org/#stagingRepositories) and search for "cabindb" in the upper right hand search box.
2. Select the cabindb staging repository, and inspect its contents.
3. If all is well, follow [these steps](https://oss.sonatype.org/#stagingRepositories) to close the repository and release it.

After the release has occurred, the artifacts will be synced to Maven central within 24-48 hours.
