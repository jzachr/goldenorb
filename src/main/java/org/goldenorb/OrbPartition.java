package org.goldenorb;

//This is the minimal amount stub for this to not break the build.
public class OrbPartition implements Runnable{
	
  public OrbPartition(){}
  
  public static void main(String[] args[]){
    new OrbPartition();
  }
  
  public class OrbCommunicationInterface {
		public int superStep() {
			return 0;
		}

		public void voteToHalt(String vertexID) {
		}
		
		public void sendMessage(Message message){
		}	
	}

  @Override
  public void run() {
    synchronized(this){
      while(true){
        try {
          this.wait(120000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
