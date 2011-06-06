package org.goldenorb.conf;


/** Something that may be configured with a {@link OrbConfiguration}. */
public interface OrbConfigurable {
	  /** Set the configuration to be used by this object. */
	  void setOrbConf(OrbConfiguration orbConf);

	  /** Return the configuration used by this object. */
	  OrbConfiguration getOrbConf();
}
