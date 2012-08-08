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

import java.io.Serializable;


import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

/**
 * This class is the parent class for Cascading Functions and Buffers. It
 * essetially converts records coming from the Python function to tuples.
 * 
 * @author Gabor Szabo
 */
public class CascadingStreamingRecordProducerWrapper extends CascadingBaseStreamingOperationWrapper implements
        Serializable {
  private static final long serialVersionUID = -1198203231681047370L;


  public CascadingStreamingRecordProducerWrapper() {
    super();
  }

  public CascadingStreamingRecordProducerWrapper(Fields fieldDeclaration) {
    super(fieldDeclaration);
  }

  public CascadingStreamingRecordProducerWrapper(int numArgs) {
    super(numArgs);
  }

  public CascadingStreamingRecordProducerWrapper(int numArgs, Fields fieldDeclaration) {
    super(numArgs, fieldDeclaration);
  }

  public int getNumParameters() {
    return 1;
  }

  

  protected void collectOutput(TupleEntryCollector outputCollector, String ret) {

    // The result will be a string of the entire line, convert this over to be
    // a tuple object
    

  }

}
