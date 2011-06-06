package org.goldenorb.net;

import java.net.UnknownHostException;

import org.apache.hadoop.net.DNS;
import org.goldenorb.conf.OrbConfiguration;

public class OrbDNS {
	public static String getDefaultHost(OrbConfiguration orbConf) throws UnknownHostException{
		return DNS.getDefaultHost(orbConf.getNetworkInterface(), "default");
	}
}
