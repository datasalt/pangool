#!/bin/bash

# 
# You can change PISAE_HOME to another name and allow it to be set externally
#
if [ "$PISAE_HOME" = "" ]; then
	export PISAE_HOME=`pwd`
fi

#
# Prepend conf.local/ to the classpath for custom configuration (developers)
#
if [ -d $PISAE_HOME/conf.local ]; then
	export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:$PISAE_HOME/conf.local;
fi

#
# Add configuration to the classpath
#
export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:$PISAE_HOME/conf;

#
# Add libs to the classpath
#
for f in $PISAE_HOME/lib/*.jar; do
	export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:$f;
done

export HADOOP_CLIENT_OPTS="-Djavax.xml.parsers.DocumentBuilderFactory=com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl"

hadoop $*
