package com.twitter.pycascading.expression;

import java.beans.ConstructorProperties;

import cascading.flow.FlowProcess;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.python.core.Py;
import org.python.core.PySequenceList;

public class ExpressionFilter extends ExpressionOperation implements Filter<ExpressionOperation.Context>
  {
  /**
   * Constructor ExpressionFilter creates a new ExpressionFilter instance.
   *
   * @param expression    of type String
   */
  @ConstructorProperties({"expression"})
  public ExpressionFilter( String expression )
    {
    super( expression );
    }

   @ConstructorProperties({"fieldDeclaration", "expression"})
  protected ExpressionFilter( Fields fieldDeclaration, String expression )
    {
    super( fieldDeclaration, expression );
    }

  @Override
  public boolean isRemove( FlowProcess flowProcess, FilterCall<Context> filterCall )
    {
    return !Py.py2boolean(evaluate( filterCall.getContext(), filterCall.getArguments() , false));
    }
  }
