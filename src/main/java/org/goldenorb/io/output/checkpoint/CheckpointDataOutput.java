package org.goldenorb.io.output.checkpoint;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.URI;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.goldenorb.conf.OrbConfiguration;
import org.omg.CORBA.Any;
import org.omg.CORBA.DataOutputStream;
import org.omg.CORBA.Object;
import org.omg.CORBA.TypeCode;

public class CheckpointDataOutput implements DataOutput {
  
  private OrbConfiguration orbconf;
  private FSDataOutputStream outstream;
  private String outpath;
  
  //@SuppressWarnings("deprecation")
  public CheckpointDataOutput(OrbConfiguration orbconf, int super_step, int partition) throws IOException {
    this.orbconf     = orbconf;
    outpath     = this.orbconf.
    getFileOutputPath()+"/"+this.orbconf.getJobNumber()+
                       "/"+super_step+"/"+partition+"/SS"+super_step+"Part"+partition;
//    System.out.println("outpath= " + this.orbconf.getFileOutputPath()+"/"+this.orbconf.getJobNumber());
    
    FileSystem fs    = FileSystem.get(URI.create(outpath), orbconf);
    OutputStream out = fs.create(new Path(outpath), true);
    this.outstream   = new FSDataOutputStream(out, FileSystem.getStatistics(outpath, FileSystem.class));
    
        
  }
  
  public String getOutpath() {
    return this.outpath;
  }
  
  public void write(int b) throws IOException {
    outstream.write(b);
  }
  
  public void write(byte[] b) throws IOException {
    outstream.write(b);
  }
  
  public void write(byte[] b, int off, int len) throws IOException {
    outstream.write(b, off, len);
  }
  
  public void writeBoolean(boolean v) throws IOException {
    outstream.writeBoolean(v);
  }
  
  public void writeByte(int v) throws IOException {
    outstream.writeByte(v);
  }
  
  public void writeShort(int v) throws IOException {
    outstream.writeShort(v);
  }
  
  public void writeChar(int v) throws IOException {
    outstream.writeChar(v);
  }
  
  public void writeInt(int v) throws IOException {
    outstream.writeInt(v);
  }
  
  public void writeLong(long v) throws IOException {
    outstream.writeLong(v);
  }
  
  public void writeFloat(float v) throws IOException {
    outstream.writeFloat(v);
  }
  
  public void writeDouble(double v) throws IOException {
    outstream.writeDouble(v);
  }
  
  public void writeBytes(String s) throws IOException {
    outstream.writeBytes(s);
  }
  
  public void writeChars(String s) throws IOException {
    outstream.writeChars(s);
  }
  
  public void writeUTF(String s) throws IOException {
    outstream.writeUTF(s);
  }
  
  public void close() throws IOException{
    outstream.close();
  }

  
  
}
