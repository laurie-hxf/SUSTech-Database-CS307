package edu.sustech.cs307.tuple;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.meta.TabCol;
import edu.sustech.cs307.value.Value;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AggregateTuple extends Tuple {
    private final List<TabCol> schema;
    private final Value[] inputValue;
    private Tuple tuple;


    public AggregateTuple(List<Value> inputValue, List<TabCol> schema,Tuple tuple) {
        this.schema = schema;
        this.inputValue = inputValue.toArray(new Value[0]);
        this.tuple = tuple;
    }
    public Value getValue(TabCol tabCol) throws DBException{
//        for (TabCol projectColumn : schema) {
//            if (projectColumn.equals(tabCol)) {
//                return inputTuple.getValue(tabCol); // Get value from input tuple
//            }
//        }
        for(int i = 0; i < schema.size(); i++){
            if(schema.get(i).equals(tabCol)){
                return inputValue[i];
            }
        }
        return null;
//        return inputValue[0]; // Column not in projection list
    }

    public Tuple getTuple(){
        return tuple;
    }

    public TabCol[] getTupleSchema(){return schema.toArray(new TabCol[0]);}

    public Value[] getValues() throws DBException{
        ArrayList<Value> values = new ArrayList<>();
        values.addAll(Arrays.asList(inputValue));
        values.addAll(Arrays.asList(tuple.getValues()));
        return values.toArray(new Value[0]);
//        return inputValue;
    }

}
