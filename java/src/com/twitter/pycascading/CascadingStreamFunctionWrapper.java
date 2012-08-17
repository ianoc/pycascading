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
 * Wrapper function which the python streaming calls use for execution.
 * 
 * @author Ian O Connell
 */
@SuppressWarnings("rawtypes")
public class CascadingStreamFunctionWrapper extends CascadingBaseStreamingOperationWrapper implements Function,
        Serializable {
  private static final long serialVersionUID = -3512295576396796360L;

  public CascadingStreamFunctionWrapper() {
    super();
  }

  public CascadingStreamFunctionWrapper(Fields fieldDeclaration) {
    super(fieldDeclaration);
  }

  public CascadingStreamFunctionWrapper(Fields fieldDeclaration, boolean skipOffset) {
    super(fieldDeclaration, skipOffset);
  }

  
  public CascadingStreamFunctionWrapper(int numArgs) {
    super(numArgs);
  }

  public CascadingStreamFunctionWrapper(int numArgs, Fields fieldDeclaration, boolean skipOffset) {
    super(numArgs, fieldDeclaration, skipOffset);
  }

  
  private TupleEntryCollector outputCollector = null;
  @Override
  public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
    // This will turn all the tuples into a single tab seperated string
    // TODO: Do we need to escape tuples which have strings in them here?
    String inputTuple = functionCall.getArguments().getTuple().get(0).toString();
    callFunction(inputTuple);
    // We currently flush after operating on each tuple,
    // we may want to reduce this in future, but this is low enough overhead that
    // we don't mind for now.
    flushOutput(functionCall.getOutputCollector());
  }
  
  @Override
  public void flush( FlowProcess flowProcess, OperationCall operationCall )
  {
    FunctionCall functionCall = (FunctionCall) operationCall;
    finishOutput(functionCall.getOutputCollector());
  }
  

}
