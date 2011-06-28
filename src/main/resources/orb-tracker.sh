#!/bin/sh

ORB_JAR="org.goldenorb.core-0.1.0-SNAPSHOT.jar"

if [ "x$ORB_HOME" = "x" ]
then
	ORB_HOME="../../.."
fi

if [ "x$ORB_LIBS" = "x" ]
then
	ORB_LIBS="$ORB_HOME"/lib
fi

ORB_PIDFILE="$ORB_HOME"/orbtracker.pid
ORB_CLASSPATH="$ORB_LIBS"/.:"$ORB_LIBS"/\*:"$ORB_HOME"/target/"$ORB_JAR":`cat $ORB_HOME/classpath.txt`

case $1 in
start)
	echo "using ORB_HOME=$ORB_HOME"
	echo "using ORB_LIBS=$ORB_LIBS"
	echo "starting OrbTracker"
	java -cp "$ORB_CLASSPATH" org.goldenorb.OrbTracker &
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