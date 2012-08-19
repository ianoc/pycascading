package com.twitter.pycascading.expression;

import java.beans.ConstructorProperties;

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.python.core.Py;
import org.python.core.PyObject;

public class ExpressionFunction extends ExpressionOperation implements Function<ExpressionOperation.Context> {

    @ConstructorProperties({"fieldDeclaration", "expression"})
    public ExpressionFunction(Fields fieldDeclaration, String expression) {
        super(fieldDeclaration, expression);

        verify(fieldDeclaration);
    }
    
    private void verify(Fields fieldDeclaration) {
        if (!fieldDeclaration.isSubstitution() && fieldDeclaration.size() != 1) {
            throw new IllegalArgumentException("fieldDeclaration may only declare one field, was " + fieldDeclaration.print());
        }
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall<ExpressionOperation.Context> functionCall) {
        if (fieldDeclaration.isArguments()) {
            PyObject context = evaluate(functionCall.getContext(), functionCall.getArguments(), true);
            Tuple result = new Tuple();
            Fields f = functionCall.getArguments().getFields();
            for (int i = 0; i < f.size(); i++) {
                Comparable fieldDefn = f.get(i); // Get the field name or position
                PyObject val;
                if (fieldDefn instanceof String) {
                    val = context.__getitem__(Py.java2py((String) fieldDefn));
                } else {
                    String fieldName = String.format("$%d", (Integer) fieldDefn);
                    val = context.__getitem__(Py.java2py((String) fieldName));
                }
                result.add(val);
            }
            functionCall.getOutputCollector().add(result);
        } else {
            PyObject ret = evaluate(functionCall.getContext(), functionCall.getArguments(), false);
            // 1 Field, we are adding one on the end now
            functionCall.getOutputCollector().add(new Tuple(ret));
        }
    }
}
