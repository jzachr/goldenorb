package org.goldenorb.io.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class RawSplit implements Writable {
    private String splitClass;
    private BytesWritable bytes = new BytesWritable();
    private String[] locations;
    long dataLength;

    public void setBytes(byte[] data, int offset, int length) {
      bytes.set(data, offset, length);
    }

    public void setClassName(String className) {
      splitClass = className;
    }
      
    public String getClassName() {
      return splitClass;
    }
      
    public BytesWritable getBytes() {
      return bytes;
    }

    public void clearBytes() {
      bytes = null;
    }
      
    public void setLocations(String[] locations) {
      this.locations = locations;
    }
      
    public String[] getLocations() {
      return locations;
    }
      
    public void readFields(DataInput in) throws IOException {
      splitClass = Text.readString(in);
      dataLength = in.readLong();
      bytes.readFields(in);
      int len = WritableUtils.readVInt(in);
      locations = new String[len];
      for(int i=0; i < len; ++i) {
        locations[i] = Text.readString(in);
      }
    }
      
    public void write(DataOutput out) throws IOException {
      Text.writeString(out, splitClass);
      out.writeLong(dataLength);
      bytes.write(out);
      WritableUtils.writeVInt(out, locations.length);
      for(int i = 0; i < locations.length; i++) {
        Text.writeString(out, locations[i]);
      }        
    }

    public long getDataLength() {
      return dataLength;
    }
    public void setDataLength(long l) {
      dataLength = l;
    }
    
  }