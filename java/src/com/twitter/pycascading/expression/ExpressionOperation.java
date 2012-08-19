package com.twitter.pycascading.expression;

import java.beans.ConstructorProperties;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.python.core.Py;
import org.python.core.PyCode;
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;

/**
 * Class ExpressionOperation is the base class for {@link ExpressionFunction}, {@link ExpressionFilter},
 * {@link cascading.operation.assertion.AssertExpression}.
 */
public class ExpressionOperation extends BaseOperation<ExpressionOperation.Context>
  {
  /** Field expression */
  protected final String expression;

  public static class Context
    {
    private PythonInterpreter expressionEvaluator;
    private Fields parameterFields;
    protected Tuple tuple;
        private PyCode func;
    }

    @ConstructorProperties({"expression"})
  protected ExpressionOperation( String expression )
    {
    this.expression = expression;
    }
    
  @ConstructorProperties({"fieldDeclaration", "expression"})
  protected ExpressionOperation( Fields fieldDeclaration, String expression )
    {
    super( fieldDeclaration );
    this.expression = expression;
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Context> operationCall ) {    
    PythonInterpreter interpreter = new PythonInterpreter();
    if( operationCall.getContext() == null ) {
      operationCall.setContext( new Context() );
    }
    Py.initPython();
    Context context = operationCall.getContext();
    context.parameterFields = operationCall.getArgumentFields();
    context.expressionEvaluator = interpreter;
    context.func = interpreter.compile(this.expression);
    context.tuple = Tuple.size( 1 );
    }

  /**
   * Performs the actual expression evaluation.
   *
   * @param context
   * @param input   of type TupleEntry 
   * @return PyObject either the return value, or the local environment it executed in.
   */
  protected PyObject evaluate( Context context, TupleEntry input , Boolean wantContext)
    {
        Fields f = input.getFields();
        Tuple t = input.getTuple();
        PythonInterpreter interpreter = context.expressionEvaluator;
        PyObject locals = Py.newStringMap();
        for(int i = 0; i < input.size(); i++) {
            Comparable fieldDefn = f.get(i); // Get the field name or position
            if(fieldDefn instanceof String) {
                locals.__setitem__((String)fieldDefn, Py.java2py(t.getObject(i)));
            } else {
                String fieldName = String.format("$%d", (Integer)fieldDefn);
                locals.__setitem__(fieldName, Py.java2py(t.getObject(i)));
            }
        }
        locals.__setitem__("tuple", Py.java2py(input));
        interpreter.setLocals(locals);
        if(wantContext) {
            interpreter.exec(context.func);
            return locals;
        } else {
            return interpreter.eval(context.func);
        }
    }

  @Override
  public boolean equals( Object object )
    {
        if (this == object) {
            return true;
        }
        if (!(object instanceof ExpressionOperation)) {
            return false;
        }
        if (!super.equals(object)) {
            return false;
        }

        ExpressionOperation that = (ExpressionOperation) object;

        if (expression != null ? !expression.equals(that.expression) : that.expression != null) {
            return false;
        }

        return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( expression != null ? expression.hashCode() : 0 );
    return result;
    }
  }
