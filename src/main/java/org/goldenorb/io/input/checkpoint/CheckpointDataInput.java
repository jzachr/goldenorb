package org.goldenorb.io.input.checkpoint;

import java.io.Closeable;
import java.io.DataInput;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.goldenorb.conf.OrbConfigurable;
import org.goldenorb.conf.OrbConfiguration;

public class CheckpointDataInput implements DataInput, Closeable, OrbConfigurable {
  
  private OrbConfiguration orbConf;
  private FSDataInputStream instream;

  
  public CheckpointDataInput(OrbConfiguration orbconf, int super_step, int partition) throws IOException {
    this.setOrbConf(orbconf);
    String inpath = this.orbConf.getFileOutputPath()+"/"+ this.orbConf.getJobNumber()+"/"+super_step+
                    "/"+partition+"/SS"+super_step+"Part"+partition;
    
    FileSystem fs = FileSystem.get(URI.create(inpath), orbconf);
    this.instream = fs.open(new Path(inpath));
  }
  
  
  public void readFully(byte[] b) throws IOException {
    instream.readFully(b);
    
  }
  
  public void readFully(byte[] b, int off, int len) throws IOException {
    instream.readFully(b, off, len);
  }
  
  public int skipBytes(int n) throws IOException {
    return instream.skipBytes(n);
  }
  
  public boolean readBoolean() throws IOException {
    return instream.readBoolean();
  }
  
  public byte readByte() throws IOException {
     return instream.readByte();
  }
  
  public int readUnsignedByte() throws IOException {
     return instream.readUnsignedByte();
  }
  
  public short readShort() throws IOException {
    return instream.readShort();
  }
  
  public int readUnsignedShort() throws IOException {
    return instream.readUnsignedShort();
  }
  
  public char readChar() throws IOException {
    return instream.readChar();
  }
  
  public int readInt() throws IOException {
    return instream.readInt();
  }
  
  public long readLong() throws IOException {
    return instream.readLong();
  }
  
  public float readFloat() throws IOException {
    return instream.readFloat();
  }
  
  public double readDouble() throws IOException {
    return instream.readDouble();
  }
  
  @SuppressWarnings("deprecation")
  public String readLine() throws IOException {
    return instream.readLine();
  }
  
  public String readUTF() throws IOException {
    return instream.readUTF();
  }
  
  public void close () throws IOException {
    instream.close();
  }

  @Override
  public void setOrbConf(OrbConfiguration orbConf) {
    this.orbConf = orbConf;
  }

  @Override
  public OrbConfiguration getOrbConf() {
    return orbConf;
  }
  
}
