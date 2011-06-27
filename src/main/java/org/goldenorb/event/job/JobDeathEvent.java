/**
 * Licensed to Ravel, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Ravel, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */
package org.goldenorb.event.job;

import org.goldenorb.event.OrbEvent;

/**
 * An OrbEvent that signifies that a job has died.
 */
public class JobDeathEvent extends OrbEvent{
  private String jobNumber;
/**
 * Constructor
 *
 * @param  String jobNumber
 */
  public JobDeathEvent(String jobNumber) {
    super(OrbEvent.JOB_DEATH);
    this.jobNumber = jobNumber;
  }
/**
 * Set the jobNumber
 * @param  String jobNumber
 */
  public void setJobNumber(String jobNumber) {
    this.jobNumber = jobNumber;
  }
/**
 * Return the jobNumber
 */
  public String getJobNumber() {
    return jobNumber;
  }
}
