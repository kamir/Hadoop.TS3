#--------------------------------------------------------------
# deploy required artifacts into local maven repository
#

mvn install:install-file -Dfile=/GITHUB/Hadoop.TS.NG/dist/Hadoop.TS.NG.jar -DgroupId=com.cloudera -DartifactId=hadoop-ts-core -Dversion=1.2.3 -Dpackaging=jar -DskipTests
mvn install:install-file -Dfile=artifacts/infodynamics-dist-1.3/infodynamics.jar -DgroupId=gpl3 -DartifactId=infodyn -Dversion=1.3 -Dpackaging=jar -DskipTests
mvn install:install-file -Dfile=artifacts/gephi-toolkit/gephi-toolkit.jar -DgroupId=gpl3 -DartifactId=gephitk -Dversion=0.8.2 -Dpackaging=jar -DskipTests
mvn install:install-file -Dfile=artifacts/candidates/PolySolve.jar -DgroupId=gpl3 -DartifactId=polysolve -Dversion=1.0.0 -Dpackaging=jar -DskipTests
