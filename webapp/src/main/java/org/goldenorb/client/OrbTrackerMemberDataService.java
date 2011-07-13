package org.goldenorb.client;

import com.google.gwt.core.client.GWT;
import com.google.gwt.user.client.rpc.RemoteService;
import com.google.gwt.user.client.rpc.RemoteServiceRelativePath;

@RemoteServiceRelativePath("OrbTrackerMemberDataService")
public interface OrbTrackerMemberDataService extends RemoteService {
  
  OrbTrackerMemberData[] getOrbTrackerMemberData() throws ZooKeeperConnectionException,
                                                  WatcherException,
                                                  NodeDoesNotExistException;
  
  String[] getJobsInQueue() throws NodeDoesNotExistException, ZooKeeperConnectionException, WatcherException;
  
  String[] getJobsInProgress() throws NodeDoesNotExistException,
                              ZooKeeperConnectionException,
                              WatcherException;
  
  /**
   * Utility class for simplifying access to the instance of async service.
   */
  public static class Util {
    private static OrbTrackerMemberDataServiceAsync instance;
    
    public static OrbTrackerMemberDataServiceAsync getInstance() {
      if (instance == null) {
        instance = GWT.create(OrbTrackerMemberDataService.class);
      }
      return instance;
    }
  }
}
