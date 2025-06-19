package edu.sustech.cs307.physicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.meta.TabCol;
import edu.sustech.cs307.tuple.AggregateTuple;
import edu.sustech.cs307.tuple.ProjectTuple;
import edu.sustech.cs307.tuple.Tuple;
import edu.sustech.cs307.value.Value;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static edu.sustech.cs307.value.ValueType.INTEGER;

public class AggregateOperator implements PhysicalOperator {
    private PhysicalOperator child;
    private List<TabCol> outputSchema = new ArrayList<>(); // Use bounded wildcard
    private  TabCol currentTabCol;
    private String Aggregate;
    private List<Pair<String,TabCol>> pairs;
    private Tuple finalAggregatedTuple;
    private boolean inputProcessedAndResultCalculated; // 标记是否已处理所有输入并计算了结果
    private boolean outputEmitted;

    public AggregateOperator(PhysicalOperator child, List<Pair<String,TabCol>> pairs) { // Use bounded wildcard
        this.child = child;
        this.pairs = pairs;
        for(Pair<String,TabCol> pair:pairs){
            outputSchema.add(pair.getRight());
        }
//        this.outputSchema = pair.getRight();
//        this.Aggregate = pair.getLeft().toUpperCase();
//        if (this.outputSchema.size() == 1 && this.outputSchema.get(0).getTableName().equals("*")) {
//            List<TabCol> newOutputSchema = new ArrayList<>();
//            for (ColumnMeta tabCol : child.outputSchema()) {
//                newOutputSchema.add(new TabCol(tabCol.tableName, tabCol.name));
//            }
//            this.outputSchema = newOutputSchema;
//        }
    }
    public boolean hasNext() throws DBException{return !outputEmitted;}

    public void Begin() throws DBException{child.Begin();}

    public void Next() throws DBException{
        if (outputEmitted) {
            // 如果已经输出过结果，那么就没有 "下一个" 了
            this.finalAggregatedTuple = null; // 清空，确保 Current() 返回 null
            return;
        }
        List<Value> values;
        if (!inputProcessedAndResultCalculated) {
            values = calculateAggregateValue();
            Tuple tuple = null;
            if(child instanceof GroupByOperator groupBy) {
                tuple = groupBy.CurrentGroupTuples().get(0);
            }else {
                tuple = child.Current();
            }
            this.finalAggregatedTuple = new AggregateTuple(values,outputSchema,tuple);
            if(!child.hasNext()){
                this.inputProcessedAndResultCalculated = true;
                this.outputEmitted = true;
            }

        }
    }

    private List<Value> calculateAggregateValue() throws DBException {
        List<Value> values = new ArrayList<>();
        if(child instanceof GroupByOperator groupBy) {
            if (child.hasNext()) {
                Value res;
                long sum = 0;
                child.Next();
                Tuple inputTuple;
                for(Pair<String,TabCol> pair:pairs){
                    res = null;
                    currentTabCol = pair.getRight();
                    Aggregate = pair.getLeft().toUpperCase();
                    List<Tuple> tuples = groupBy.CurrentGroupTuples();
                    for (Tuple tuple : tuples) {
                        inputTuple = tuple;
                        Value value = inputTuple.getValue(currentTabCol);
                        if (value != null| Objects.equals(Aggregate, "COUNT")) {
                            switch (Aggregate) {
                                case "COUNT" -> {
                                    res = new Value(Long.valueOf(tuples.size()));
                                }
                                case "MAX" -> {
                                    if (res == null) {
                                        res = value;
                                    }
                                    if (value.compareTo(res) > 0) {
                                        res = value;
                                    }
                                }
                                case "MIN" -> {
                                    if (res == null) {
                                        res = value;
                                    }
                                    if (value.compareTo(res) < 0) {
                                        res = value;
                                    }
                                }
                                case "SUM" ->{
                                    if (res == null) {
                                        res = value;
                                    }
                                    else {
                                        if (value != null) {
                                            res=res.add(value);
                                        }
                                    }
                                }
                            }

                        }
                    }
                    values.add(res);
                }
            }
        }
        else {
            for (Pair<String, TabCol> pair : pairs) {
                long result = 0;
                Value res = null;
                child.Begin();
                while (child.hasNext()) {
                    child.Next();
                    Tuple inputTuple = child.Current();
                    if (inputTuple == null) {
                        return null;
                    }
                    currentTabCol = pair.getRight();
                    Aggregate = pair.getLeft().toUpperCase();
                    Value value = inputTuple.getValue(currentTabCol);
                    if (value != null| Objects.equals(Aggregate, "COUNT")) {
                        switch (Aggregate) {
                            case "COUNT" -> {
                                result += 1;
                                res = new Value(result);
                            }
                            case "MAX" -> {
                                if (res == null) {
                                    res = value;
                                }
                                if (value.compareTo(res) > 0) {
                                    res = value;
                                }
                            }
                            case "MIN" -> {
                                if (res == null) {
                                    res = value;
                                }
                                if (value.compareTo(res) < 0) {
                                    res = value;
                                }
                            }
                            case "SUM" ->{
                                if (res == null) {
                                    res = value;
                                }
                                else {
                                    if (value != null) {
                                        res=res.add(value);
                                    }
                                }
                            }
                        }
                    }
                }
                values.add(res);
            }
        }
        return values;
    }

    public Tuple Current(){return finalAggregatedTuple;}

    public void Close(){
        child.Close();
        finalAggregatedTuple = null;
    }

    public ArrayList<ColumnMeta> outputSchema(){
        ArrayList<ColumnMeta> result = new ArrayList<>();
        ColumnMeta columnMeta = null;
        for (TabCol tabCol : outputSchema) {
            columnMeta = new ColumnMeta(tabCol.getTableName(), tabCol.getColumnName(), INTEGER, 1, 0);
            result.add(columnMeta);
        }
        result.addAll(child.outputSchema());
        return result;
    }
}

