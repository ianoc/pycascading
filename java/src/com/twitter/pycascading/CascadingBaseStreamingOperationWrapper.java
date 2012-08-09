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


import java.io.File;
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
 * Wrapper for a Cascading BaseOperation that:
 *  * Launches and controls the sub process that streaming communicates with
 *  * Prepares the input tuples for a the streaming client process.
 *  * Parses the output of the client process for the generating of resulting tuples.
 * 
 * @author Ian O Connell
 */

@SuppressWarnings({ "rawtypes", "deprecation" })
public class CascadingBaseStreamingOperationWrapper extends BaseOperation implements Serializable {
  private static final long serialVersionUID = -535185466322890691L;
  
  // Buffer size used when dealing with the sub process
  private final static int BUFFER_SIZE = 128 * 1024; 

  // Streams for interacting with stdin/stdout of the streaming subprocess
  DataOutputStream stdoutStream = null;
  BufferedReader stdinStream = null;
  
  // streaming sub process object
  Process childProcess = null;
  
  // blocking queue to allow passing back stdout from the monitor thread
  BlockingQueue<String> childProcessOutputQueue;
  
  // Thread that reads from the stdout of the subprocess
  ChildOutputReader child;
  
  // Command line string passed in that will give the streaming process
  protected String commandLine;

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

  
  /**
   * The init steps on each node, here we need to start the subprocess
   * and hook up the pipes so we can communicate with it
   * 
   */
  @Override
  public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
    JobConf jobConf = ((HadoopFlowProcess) flowProcess).getJobConf();
    
    
    String sourceDir = null;
    if ("hadoop".equals(jobConf.get("pycascading.running_mode"))) {
      try {
        Path[] archives = DistributedCache.getLocalCacheArchives(jobConf);
        sourceDir = archives[1].toString() + "/";
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } 
    
    
    try {
      ProcessBuilder builder = new ProcessBuilder(this.commandLine.split(" "));
      builder.redirectErrorStream(true);
      if(sourceDir != null) {
        builder.directory(new File(sourceDir));
      }
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

  /**
   * This flushes the in memory thread safe queue of data we have that came back from the process
   * We bring it back to the main thread and then add it to the outputCollector
   * Here we presume the subprocess's output is tab seperated(bad)
   * We check to see if we are missing tuples at the end, if so we add null's
   *
   */
  public void flushOutput(TupleEntryCollector outputCollector) {
    String current = childProcessOutputQueue.poll();
    while(current != null) {
        String[] strTuple = current.split("\t");
        Tuple result = new Tuple();
        for (String s : strTuple) {
          result.add(s);
        }
        while(result.size() < fieldDeclaration.size()) {
          result.add(null);
        }
        
        outputCollector.add(result);
        current = childProcessOutputQueue.poll();
    }
  }
  
  /**
   * We call this when we have stopped passing data to the child process.
   * We close off the stdin pipe which should cause the process to gracefully exit.
   * After that we wait for thread to finish up and then flush the buffers one last time
   */
   
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
   * This Thread is launched to block on the stdout from the child process much as how the Hadoop Streaming class works.
   * This code is originally based on this with modifications as the Cascading collector cannot be presumed to be thread safe
   * unlike the Hadoop one. As such this thread writes to a thread safe queue which must be fetched from the main thread
   * and then written to the collector
   */
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
        String inputLine;
        while ((inputLine = outReader.readLine()) != null) {
          outCollector.add(inputLine);
        }
      } catch (IOException ex) {

      }
    }

  }


}
