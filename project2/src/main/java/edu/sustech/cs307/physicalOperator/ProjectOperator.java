package edu.sustech.cs307.physicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.tuple.ProjectTuple;
import edu.sustech.cs307.tuple.Tuple;
import edu.sustech.cs307.meta.TabCol;
import edu.sustech.cs307.value.ValueType;

import java.util.ArrayList;
import java.util.List;

public class ProjectOperator implements PhysicalOperator {
    private PhysicalOperator child;
    private List<TabCol> outputSchema; // Use bounded wildcard
    private Tuple currentTuple;

    public ProjectOperator(PhysicalOperator child, List<TabCol> outputSchema) {
        this.child = child;
        this.outputSchema = outputSchema;
        if (this.outputSchema.size() == 1 && this.outputSchema.get(0).getTableName().equals("*")) {
            List<TabCol> newOutputSchema = new ArrayList<>();
            for (ColumnMeta tabCol : child.outputSchema()) {
                newOutputSchema.add(new TabCol(tabCol.tableName, tabCol.name));
            }
            this.outputSchema = newOutputSchema;
        }
    }

    @Override
    public boolean hasNext() throws DBException {
        return child.hasNext();
    }

    @Override
    public void Begin() throws DBException {
        child.Begin();
    }

    @Override
    public void Next() throws DBException {
        Tuple inputTuple = null;
        if(child instanceof GroupByOperator groupBy){
            if (hasNext()) {
                child.Next();
                List<Tuple> tuples = groupBy.CurrentGroupTuples();
                inputTuple = tuples.get(0);
                ArrayList<ColumnMeta> newOutputSchema = groupBy.outputSchema();
                for (int i = 0; i < outputSchema.size(); i++) {
                    boolean found = false;
                    for (int j = 0; j < newOutputSchema.size(); j++) {
                        if ((newOutputSchema.get(j).name.equals(outputSchema.get(i).getColumnName())&&newOutputSchema.get(j).tableName.equals(outputSchema.get(i).getTableName()))) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        throw new DBException(ExceptionTypes.ColumnDoseNotExist(outputSchema.get(i).getColumnName()));
                    }
                }
                currentTuple = new ProjectTuple(inputTuple, outputSchema);
            }
        }
        else if (child instanceof AggregateOperator aggregateOperator){
            if(hasNext()){
                child.Next();
//                ArrayList<ColumnMeta> newOutputSchema = child.outputSchema();
                inputTuple = child.Current();
                currentTuple = new ProjectTuple(inputTuple, outputSchema);
            }
        }
        else {
            if (hasNext()) {
                child.Next();
                inputTuple = child.Current();
                currentTuple = new ProjectTuple(inputTuple, outputSchema); // Create ProjectTuple
            }
        }
    }

    @Override
    public Tuple Current() {
        return currentTuple;
    }

    @Override
    public void Close() {
        child.Close();
        currentTuple = null;
    }

    @Override
    public ArrayList<ColumnMeta> outputSchema() {
        //TODO: return the fields only appear in select items.
        ArrayList<ColumnMeta> result = new ArrayList<>();
        for (TabCol tabCol : outputSchema) {
            for (ColumnMeta colMeta : child.outputSchema()) {
                if (colMeta.tableName.equals(tabCol.getTableName()) &&
                        colMeta.name.equals(tabCol.getColumnName())) {
                    result.add(colMeta);
                    break;
                }
            }
        }
        return result;
    }
}
