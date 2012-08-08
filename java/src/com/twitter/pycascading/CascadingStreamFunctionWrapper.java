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

import java.io.ObjectInputStream;
import java.io.Serializable;

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;

/**
 * Wrapper for a Cascading Function that calls a Python function.
 * 
 * @author Gabor Szabo, Ian O Connell
 */
@SuppressWarnings("rawtypes")
public class CascadingStreamFunctionWrapper extends CascadingStreamingRecordProducerWrapper implements Function,
        Serializable {
  private static final long serialVersionUID = -3512295576396796360L;

  public CascadingStreamFunctionWrapper() {
    super();
  }

  public CascadingStreamFunctionWrapper(Fields fieldDeclaration) {
    super(fieldDeclaration);
  }

  public CascadingStreamFunctionWrapper(int numArgs) {
    super(numArgs);
  }

  public CascadingStreamFunctionWrapper(int numArgs, Fields fieldDeclaration) {
    super(numArgs, fieldDeclaration);
  }

  
  private TupleEntryCollector outputCollector = null;
  @Override
  public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
    String inputTuple = functionCall.getArguments().getTuple().toString("\t");
    this.outputCollector = functionCall.getOutputCollector();
    callFunction(inputTuple);
    flushOutput(this.outputCollector);
  }
  
  @Override
  public void flush( FlowProcess flowProcess, OperationCall operationCall )
  {
     flushOutput(this.outputCollector);
  }
  
  @Override
  public void cleanup( FlowProcess flowProcess, OperationCall operationCall )
  {
      if(this.outputCollector != null) {
        finishOutput(this.outputCollector);
      }
  }

}
