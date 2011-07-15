package org.goldenorb.client;

//import java.util.List;

import java.util.Date;

import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.core.client.GWT;
import com.google.gwt.i18n.client.DateTimeFormat;
import com.google.gwt.user.client.Timer;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.Image;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.RootPanel;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.HorizontalPanel;

/**
 * Entry point classes define <code>onModuleLoad()</code>.
 */
public class OrbTrackerStatus implements EntryPoint {
  
  private FlexTable orbTrackerFlexTable;
  private Label lastUpdatedLabel;
  private Label errorLabelOTM;
  private Label errorLabelJQ;
  private Label errorLabelJIP;
  private static final int REFRESH_INTERVAL = 5000;
  private OrbTrackerMemberDataServiceAsync dataService = GWT.create(OrbTrackerMemberDataService.class);
  private VerticalPanel mainPanel;
  private HorizontalPanel jobsGroupedPanel;
  private FlexTable jobsInProgressPanel;
  private FlexTable jobsInQueuePanel;
  
  @Override
  public void onModuleLoad() {
    RootPanel rootPanel = RootPanel.get();
    mainPanel = new VerticalPanel();
    rootPanel.add(mainPanel);
    
    Image image = new Image("images/logo-full.jpg");
    mainPanel.add(image);
    
    // Label titleLabel = new Label("GoldenOrb");
    // mainPanel.add(titleLabel);
    
    lastUpdatedLabel = new Label("Last Updated : ");
    mainPanel.add(lastUpdatedLabel);
    
    Label lblOrbtrackermembers = new Label("OrbTrackerMembers");
    mainPanel.add(lblOrbtrackermembers);
    
    orbTrackerFlexTable = new FlexTable();
    mainPanel.add(orbTrackerFlexTable);
    orbTrackerFlexTable.setSize("761px", "116px");
    orbTrackerFlexTable.setText(0, 0, "Node Name");
    orbTrackerFlexTable.setText(0, 1, "Partition Capacity");
    orbTrackerFlexTable.setText(0, 2, "Available Partitions");
    orbTrackerFlexTable.setText(0, 3, "Reserved Partitions");
    orbTrackerFlexTable.setText(0, 4, "In Use Partitions");
    orbTrackerFlexTable.setText(0, 5, "Host Name");
    orbTrackerFlexTable.setText(0, 6, "Leader");
    orbTrackerFlexTable.setText(0, 7, "Port");
    
    jobsGroupedPanel = new HorizontalPanel();
    mainPanel.add(jobsGroupedPanel);
    jobsGroupedPanel.setSize("258px", "100px");
    
    jobsInProgressPanel = new FlexTable();
    jobsInProgressPanel.setText(0, 0, "Jobs In Progress");
    jobsGroupedPanel.add(jobsInProgressPanel);
    
    jobsInQueuePanel = new FlexTable();
    jobsInQueuePanel.setText(0, 0, "Jobs In Queue");
    jobsGroupedPanel.add(jobsInQueuePanel);
    
    jobsInProgressPanel.setTitle("Jobs In Progress");
    jobsInQueuePanel.setTitle("Jobs in Queue");
    
    errorLabelOTM = new Label("");
    mainPanel.add(errorLabelOTM);
    
    errorLabelJIP = new Label("");
    mainPanel.add(errorLabelJIP);
    
    errorLabelJQ = new Label("");
    mainPanel.add(errorLabelJQ);
    
    Timer refreshTimer = new Timer() {
      public void run() {
        refreshWatchDataList();
        refreshJobsInProgress();
        refreshJobsInQueue();
      }
      
    };
    refreshTimer.scheduleRepeating(REFRESH_INTERVAL);
    
  }
  
  private void refreshJobsInProgress() {
    // initialize service
    if (dataService == null) {
      dataService = GWT.create(OrbTrackerMemberDataService.class);
    }
    
    // set up callback
    AsyncCallback<String[]> callback = new AsyncCallback<String[]>() {
      @SuppressWarnings("deprecation")
      public void onFailure(Throwable caught) {
        // If error occurred while getting updates
        String details = caught.getMessage();
        if (caught instanceof WatcherException) {
          details = ((WatcherException) caught).getErrorMessage();
        } else if (caught instanceof ZooKeeperConnectionException) {
          details = ((ZooKeeperConnectionException) caught).getErrorMessage();
        } else if (caught instanceof NodeDoesNotExistException) {
          details = ((NodeDoesNotExistException) caught).getErrorMessage();
        }
        errorLabelJIP.setText(details);
        errorLabelJIP.setVisible(true);
        lastUpdatedLabel.setText("Last Updated : "
                                 + DateTimeFormat.getMediumDateTimeFormat().format(new Date()));
        clearTables(jobsInProgressPanel);
      }
      
      @SuppressWarnings("deprecation")
      public void onSuccess(String[] result) {
        updateJobTable(result, jobsInProgressPanel);
        lastUpdatedLabel.setText("Last Updated : "
                                 + DateTimeFormat.getMediumDateTimeFormat().format(new Date()));
        errorLabelJIP.setVisible(false);
      }
    };
    dataService.getJobsInProgress(callback);
    
  }
  
  private void refreshJobsInQueue() {
    // initialize service
    if (dataService == null) {
      dataService = GWT.create(OrbTrackerMemberDataService.class);
    }
    // set up callback
    AsyncCallback<String[]> callback = new AsyncCallback<String[]>() {
      @SuppressWarnings("deprecation")
      public void onFailure(Throwable caught) {
        // If error occurred while getting updates
        String details = caught.getMessage();
        if (caught instanceof WatcherException) {
          details = ((WatcherException) caught).getErrorMessage();
        } else if (caught instanceof ZooKeeperConnectionException) {
          details = ((ZooKeeperConnectionException) caught).getErrorMessage();
        } else if (caught instanceof NodeDoesNotExistException) {
          details = ((NodeDoesNotExistException) caught).getErrorMessage();
        }
        errorLabelJQ.setText(details);
        errorLabelJQ.setVisible(true);
        lastUpdatedLabel.setText("Last Updated : "
                                 + DateTimeFormat.getMediumDateTimeFormat().format(new Date()));
        clearTables(jobsInQueuePanel);
      }
      
      @SuppressWarnings("deprecation")
      public void onSuccess(String[] result) {
        updateJobTable(result, jobsInQueuePanel);
        lastUpdatedLabel.setText("Last Updated : "
                                 + DateTimeFormat.getMediumDateTimeFormat().format(new Date()));
        errorLabelJQ.setVisible(false);
      }
    };
    dataService.getJobsInQueue(callback);
  }
  
  private void refreshWatchDataList() {
    // initialize service
    if (dataService == null) {
      dataService = GWT.create(OrbTrackerMemberDataService.class);
    }
    // set up callback
    AsyncCallback<OrbTrackerMemberData[]> callback = new AsyncCallback<OrbTrackerMemberData[]>() {
      @SuppressWarnings("deprecation")
      public void onFailure(Throwable caught) {
        // If error occurred while getting updates
        String details = caught.getMessage();
        if (caught instanceof WatcherException) {
          details = ((WatcherException) caught).getErrorMessage();
        } else if (caught instanceof ZooKeeperConnectionException) {
          details = ((ZooKeeperConnectionException) caught).getErrorMessage();
        } else if (caught instanceof NodeDoesNotExistException) {
          details = ((NodeDoesNotExistException) caught).getErrorMessage();
        }
        errorLabelOTM.setText(details);
        errorLabelOTM.setVisible(true);
        lastUpdatedLabel.setText("Last Updated : "
                                 + DateTimeFormat.getMediumDateTimeFormat().format(new Date()));
        clearTables(orbTrackerFlexTable);
      }
      
      @SuppressWarnings("deprecation")
      public void onSuccess(OrbTrackerMemberData[] result) {
        updateTable(result);
        lastUpdatedLabel.setText("Last Updated : "
                                 + DateTimeFormat.getMediumDateTimeFormat().format(new Date()));
        errorLabelOTM.setVisible(false);
      }
      
    };
    
    // Make the call to the OrbTrackerMemberData service.
    dataService.getOrbTrackerMemberData(callback);
    
  }
  
  private void updateJobTable(String[] jobs, FlexTable tableToUpdate) {
    // clear table
    int num_rows = tableToUpdate.getRowCount();
    for (int r = num_rows - 1; r > 0; r--) {
      tableToUpdate.removeRow(r);
    }
    int row = 1;
    for (String job : jobs) {
      tableToUpdate.setText(row, 0, job);
      row++;
    }
  }
  
  private void updateTable(OrbTrackerMemberData[] update) {
    // clear table
    int num_rows = orbTrackerFlexTable.getRowCount();
    for (int r = num_rows - 1; r > 0; r--) {
      orbTrackerFlexTable.removeRow(r);
    }
    int row = 1;
    for (OrbTrackerMemberData data : update) {
      orbTrackerFlexTable.setSize("761px", "116px");
      orbTrackerFlexTable.setText(row, 0, data.getName());
      orbTrackerFlexTable.setText(row, 1, "" + data.getPartitionCapacity());
      orbTrackerFlexTable.setText(row, 2, "" + data.getAvailablePartitions());
      orbTrackerFlexTable.setText(row, 3, "" + data.getReservedPartitions());
      orbTrackerFlexTable.setText(row, 4, "" + data.getInUsePartitions());
      orbTrackerFlexTable.setText(row, 5, "" + data.getHostname());
      orbTrackerFlexTable.setText(row, 6, "" + data.getLeaderStatus());
      orbTrackerFlexTable.setText(row, 7, "" + data.getPort());
      row++;
    }
  }
  
  private void clearTables(FlexTable table) {
    int num_rows = table.getRowCount();
    for (int r = num_rows - 1; r > 0; r--) {
      table.removeRow(r);
    }
  }
}
