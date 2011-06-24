package org.goldenorb;

import org.goldenorb.conf.OrbConfiguration;

public class TestRunner {
  public static void main(String[] args) {
    OrbRunner runner = new OrbRunner();
    runner.runJob(new OrbConfiguration(true));
  }
}
