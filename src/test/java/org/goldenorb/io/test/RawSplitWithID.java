package org.goldenorb.io.test;

import org.goldenorb.io.input.RawSplit;

public class RawSplitWithID extends RawSplit{
  
  private String ID;
  
  public RawSplitWithID(String ID, String[] locations){
    this.ID = ID;
    setLocations(locations);
  }

  public void setID(String iD) {
    ID = iD;
  }

  public String getID() {
    return ID;
  }
  
  @Override
  public String toString(){
    StringBuilder locationString = new StringBuilder();
    for(String location: getLocations()){
      locationString.append(location);
      locationString.append(", ");
    }
    return ID + " : " + locationString.toString();
  }
}
