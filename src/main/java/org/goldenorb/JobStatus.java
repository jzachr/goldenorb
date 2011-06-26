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
package org.goldenorb;

public class JobStatus {
  public final static int ACTIVE = 1;
  public final static int COMPLETE = 2;
  public final static int DEAD = 3;
  
  private int status;
  
/**
 * Constructor
 *
 * @param  int status
 */
  public JobStatus(int status){
    this.status = status;
  }
  
/**
 * Return the ctive
 */
  public boolean isActive(){
    return status == ACTIVE;
  }
  
/**
 * Return the omplete
 */
  public boolean isComplete(){
    return status == COMPLETE;
  }
  
/**
 * Return the ead
 */
  public boolean isDead(){
    return status == DEAD;
  }
  
/**
 * Set the active
 */
  public void setActive(){
    status = ACTIVE;
  }
  
/**
 * Set the complete
 */
  public void setComplete(){
    status = COMPLETE;
  }
  
/**
 * Set the dead
 */
  public void setDead(){
    status = DEAD;
  }
}
