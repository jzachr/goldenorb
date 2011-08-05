#!/bin/sh

 # Licensed to Ravel, Inc. under one
 # or more contributor license agreements.  See the NOTICE file
 # distributed with this work for additional information
 # regarding copyright ownership.  Ravel, Inc. licenses this file
 # to you under the Apache License, Version 2.0 (the
 # "License"); you may not use this file except in compliance
 # with the License.  You may obtain a copy of the License at
 # 
 #     http://www.apache.org/licenses/LICENSE-2.0
 # 
 # Unless required by applicable law or agreed to in writing, software
 # distributed under the License is distributed on an "AS IS" BASIS,
 # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 # See the License for the specific language governing permissions and
 # limitations under the License

# ORB_HOME          Path to GoldenOrb on this machine
#export ORB_HOME=


ORB_JAR="org.goldenorb.core-0.1.1-SNAPSHOT.jar"

if [ "x$ORB_HOME" = "x" ]
then
	ORB_HOME=".."
fi

if [ "x$ORB_LIBS" = "x" ]
then
	ORB_LIBS="$ORB_HOME"/lib
fi

if [ "x$ORB_CONF" = "x" ]
then
	ORB_CONF="$ORB_HOME"/conf
fi

if [ ! -d "$ORB_HOME"/logs ]
then
	mkdir "$ORB_HOME"/logs
fi

ORB_LOGF="$ORB_HOME"/logs/orb-tracker.`date +%Y-%m-%d.%H%M-%Z`.out
ORB_PIDFILE="$ORB_HOME"/orbtracker.pid
ORB_CLASSPATH="$ORB_CONF"/.:"$ORB_HOME"/"$ORB_JAR":"$ORB_LIBS"/\*
case $1 in
start)
	echo "using ORB_HOME=$ORB_HOME"
	echo "using ORB_LIBS=$ORB_LIBS"
	echo "starting OrbTracker"
	java -cp "$ORB_CLASSPATH" org.goldenorb.OrbTracker >> "$ORB_LOGF" 2>&1 &
	/bin/echo -n $! > "$ORB_PIDFILE"
	echo STARTED
	;;
stop)
	echo "stopping OrbTracker"
	if [ ! -f "$ORB_PIDFILE" ]
	then
		echo "error: could not find $ORB_PIDFILE"
		exit 1
	else
		kill -9 $(cat "$ORB_PIDFILE")
		rm "$ORB_PIDFILE"
		echo STOPPED
	fi
	;;
*)
	echo "usage: $0 {start|stop}"
esac