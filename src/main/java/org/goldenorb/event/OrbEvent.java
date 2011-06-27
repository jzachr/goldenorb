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
package org.goldenorb.event;

/**
 * OrbEvents are passed through OrbCallback interfaces in order to notify the group
 * when certain events have happened.
 */

public class OrbEvent {
  public final static int LOST_MEMBER = 1;
  public final static int NEW_MEMBER = 2;
  public final static int LEADERSHIP_CHANGE = 3;
  public final static int NEW_JOB = 4;
  public final static int JOB_COMPLETE = 5;
  public final static int JOB_DEATH = 6;
  public final static int ORB_EXCEPTION = 8;
  public final static int MEMBER_DATA_CHANGE = 9;
  public final static int JOB_START = 10;

  int type;

/**
 * Constructor
 *
 * @param  int type - The type of event this OrbEvent is to signify.
 */
  public OrbEvent(int type){
    this.type = type;
  }
   
/**
 * Return the type of event that it is
 */
  public int getType(){
    return type;
  }
/**
 * Set the type of event that it is.
 * @param  int type
 */
  public void setType(int type){
    this.type = type;
  }
}
