/**
 * Copyright 2011 Twitter, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.pycascading;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.Iterator;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;



import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.DataOutputStream;
import java.io.BufferedOutputStream;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;


/**
 * Wrapper for a Cascading BaseOperation that prepares the input tuples for a
 * Python function. It can convert between tuples and Python lists and dicts.
 * 
 * @author Gabor Szabo
 */
@SuppressWarnings({ "rawtypes", "deprecation" })
public class CascadingBaseStreamingOperationWrapper extends BaseOperation implements Serializable {
  private static final long serialVersionUID = -535185466322890691L;

  protected String commandLine;

  /**
   * This is necessary for the deserialization.
   */
  public CascadingBaseStreamingOperationWrapper() {
    super();
  }

  public CascadingBaseStreamingOperationWrapper(Fields fieldDeclaration) {
    super(fieldDeclaration);
  }

  public CascadingBaseStreamingOperationWrapper(int numArgs) {
    super(numArgs);
  }

  public CascadingBaseStreamingOperationWrapper(int numArgs, Fields fieldDeclaration) {
    super(numArgs, fieldDeclaration);
  }

  private final static int BUFFER_SIZE = 128 * 1024;

  DataOutputStream stdoutStream = null;
  BufferedReader stdinStream = null;
  Process childProcess = null;
  BlockingQueue<String> childProcessOutputQueue;
  ChildOutputReader child;
  // We need to delay the deserialization of the Python functions up to this
  // point, since the sources are in the distributed cache, whose location is in
  // the jobconf, and we get access to the jobconf only at this point for the
  // first time.
  @Override
  public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
    JobConf jobConf = ((HadoopFlowProcess) flowProcess).getJobConf();
    
    try {
      ProcessBuilder builder = new ProcessBuilder(this.commandLine.split(" "));
      builder.redirectErrorStream(true);
      this.childProcess = builder.start();
    } catch (IOException e) {
        throw new RuntimeException(e);
    }
    this.stdoutStream = new DataOutputStream(new BufferedOutputStream(
                                                       childProcess.getOutputStream(),
                                                       BUFFER_SIZE));
    this.stdinStream = new BufferedReader(new InputStreamReader(childProcess.getInputStream()));
    childProcessOutputQueue = new ArrayBlockingQueue<String>(10000);
    child = new ChildOutputReader(stdinStream, childProcessOutputQueue);
    child.start();
  }

  /**
   * We assume that the Python functions (map and reduce) are always called with
   * the same number of arguments. Override this to return the number of
   * arguments we will be passing in all the time.
   * 
   * @return the number of arguments the wrapper is passing in
   */
  public int getNumParameters() {
    return 0;
  }


  /**
   * This handles dealing with the stdin/stdout streams for dealing with the child process
   * 
   * @return the standard out line we got from the child process for the stdin line
   */
  public void callFunction(String currentLine) {
    try {
      stdoutStream.write(currentLine.getBytes("UTF-8"));
      stdoutStream.write('\n');
      stdoutStream.flush();
    } catch (IOException e) {
        throw new RuntimeException(e);
    }
  }

  public void flushOutput(TupleEntryCollector outputCollector) {
    String current = childProcessOutputQueue.poll();
    while(current != null) {
        String[] strTuple = current.split("\t");
        Tuple result = new Tuple();
        for (String s : strTuple) {
          result.add(s);
        }
        outputCollector.add(result);
        current = childProcessOutputQueue.poll();
    }
  }
  
  public void finishOutput(TupleEntryCollector outputCollector) {
    try {
      stdoutStream.close();
    } catch (IOException e){}
    try {
      childProcess.waitFor();
    } catch (InterruptedException e) {}
    try {
      child.join();
    } catch (InterruptedException e) {}
    flushOutput(outputCollector);
  }
  /**
   * Setter for the command line args to be called.
   * 
   * @param String
   *          the commandline 
   */
  public void setFunction(String commandLine) {
    this.commandLine = commandLine;
  }
  /**
   * The Python callback function to call to get the source of a PyFunction. We
   * better do it in Python using the inspect module, than hack it around in
   * Java.
   * 
   * @param callBack
   *          the PyFunction that is called to get the source of a Python
   *          function
   */
  public void setWriteObjectCallBack(Object callBack) {
   
  }

  
  class ChildOutputReader extends Thread {
    private BufferedReader outReader;
    private BlockingQueue<String> outCollector;
    ChildOutputReader(BufferedReader outReader, BlockingQueue<String> outCollector) {
      setDaemon(true);
      this.outReader = outReader;
      this.outCollector = outCollector;
    }

    public void run() {
      try {
        String inputLine;// = outReader.readLine();
        // 3/4 Tool to Hadoop
        while ((inputLine = outReader.readLine()) != null) {
          outCollector.add(inputLine);
          //inputLine = outReader.readLine();
        }
      } catch (IOException ex) {

      }
    }

  }


}
