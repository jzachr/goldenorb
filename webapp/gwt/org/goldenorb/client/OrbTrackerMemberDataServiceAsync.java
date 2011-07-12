package org.goldenorb.client;

import com.google.gwt.core.client.GWT;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.rpc.ServiceDefTarget;

public interface OrbTrackerMemberDataServiceAsync
{

    /**
     * GWT-RPC service  asynchronous (client-side) interface
     * @see org.goldenorb.client.OrbTrackerMemberDataService
     */
    void getOrbTrackerMemberData( AsyncCallback<org.goldenorb.client.OrbTrackerMemberData[]> callback );


    /**
     * GWT-RPC service  asynchronous (client-side) interface
     * @see org.goldenorb.client.OrbTrackerMemberDataService
     */
    void getJobsInQueue( AsyncCallback<java.lang.String[]> callback );


    /**
     * GWT-RPC service  asynchronous (client-side) interface
     * @see org.goldenorb.client.OrbTrackerMemberDataService
     */
    void getJobsInProgress( AsyncCallback<java.lang.String[]> callback );


    /**
     * Utility class to get the RPC Async interface from client-side code
     */
    public static final class Util 
    { 
        private static OrbTrackerMemberDataServiceAsync instance;

        public static final OrbTrackerMemberDataServiceAsync getInstance()
        {
            if ( instance == null )
            {
                instance = (OrbTrackerMemberDataServiceAsync) GWT.create( OrbTrackerMemberDataService.class );
                ServiceDefTarget target = (ServiceDefTarget) instance;
                target.setServiceEntryPoint( GWT.getModuleBaseURL() + "OrbTrackerMemberDataService" );
            }
            return instance;
        }

        private Util()
        {
            // Utility class should not be instanciated
        }
    }
}
