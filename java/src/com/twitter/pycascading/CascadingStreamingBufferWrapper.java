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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

/**
 * 
 * @author Ian O Connell
 */
@SuppressWarnings("rawtypes")
public class CascadingStreamingBufferWrapper extends CascadingBaseStreamingOperationWrapper implements Buffer,
        Serializable {
  private static final long serialVersionUID = -3512295576396796360L;

  public CascadingStreamingBufferWrapper() {
    super();
  }

  public CascadingStreamingBufferWrapper(Fields fieldDeclaration) {
    super(fieldDeclaration, false);
  }

  public CascadingStreamingBufferWrapper(int numArgs) {
    super(numArgs);
  }

  public CascadingStreamingBufferWrapper(int numArgs, Fields fieldDeclaration) {
    super(numArgs, fieldDeclaration);
  }


  public int getNumParameters() {
    return super.getNumParameters() + 1;
  }

 @Override
  public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
    // TODO: if the Python buffer expects Python dicts or lists, then we need to
    // convert the Iterator
    @SuppressWarnings("unchecked")
    Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();

    // This gets called even when there are no tuples in the group after
    // a GroupBy (see the Buffer javadoc). So we need to check if there are any
    // valid tuples returned in the group.
    if (arguments.hasNext()) {
      TupleEntry group = bufferCall.getGroup();
      TupleEntryCollector outputCollector = bufferCall.getOutputCollector();
      callFunction(bufferCall.getOutputCollector(), group, arguments);
      flushOutput(bufferCall.getOutputCollector());
    }
  }
}
