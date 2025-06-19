package edu.sustech.cs307.logicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.meta.TabCol;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LogicalProjectOperator extends LogicalOperator {

    private final List<SelectItem<?>> selectItems;
    private final LogicalOperator child;

    public LogicalProjectOperator(LogicalOperator child, List<SelectItem<?>> selectItems) {
        super(Collections.singletonList(child));
        this.child = child;
        this.selectItems = selectItems;
    }

    public LogicalOperator getChild() {
        return child;
    }

    public List<TabCol> getOutputSchema() throws DBException {
        List<TabCol> outputSchema = new ArrayList<>();
        String Aggregate = null;
        boolean have_aggregate = false;
        for (SelectItem<?> selectItem : selectItems) {
            String temp = selectItem.toString().toUpperCase();
            if (temp.contains("COUNT")){
                Aggregate = "count";
                have_aggregate = true;
            }else if (temp.contains("SUM")){
                Aggregate = "sum";
                have_aggregate = true;
            }else if (temp.contains("MAX")){
                Aggregate = "max";
                have_aggregate = true;
            }else if (temp.contains("MIN")){
                Aggregate = "min";
                have_aggregate = true;
            }
            //TODO : add selectItem.getExpression() instance of Column
            if (selectItem.getExpression() instanceof AllColumns column) {
                outputSchema.add(new TabCol("*", "*"));
            } else if (selectItem.getExpression() instanceof Column column) {
                String outputColumnName;
                String sourceTableName = "?";
                outputColumnName = column.getColumnName();
                if (column.getTable() != null && column.getTable().getName() != null) {
                    sourceTableName = column.getTable().getName();
                }
                outputSchema.add(new TabCol(sourceTableName,outputColumnName));
            }
            else if(have_aggregate){
                Function function = (Function) selectItem.getExpression();
                ExpressionList params = function.getParameters();
                if (params != null) {
                    for (Object param : params.getExpressions()) {
                        if (param instanceof Column) {
                            Column col = (Column) param;
                            String outputColumnName = col.getColumnName();
                            String sourceTableName = "?";
                            if (col.getTable() != null && col.getTable().getName() != null) {
                                if (Aggregate.equals("count")){
                                    sourceTableName = "count: "+col.getTable().getName();
                                }
                                else {
                                    sourceTableName = col.getTable().getName();
                                }
                            }
                            outputSchema.add(new TabCol(sourceTableName, outputColumnName));
                        } else if (param instanceof AllColumns&&Aggregate.equals("count")) {
                            outputSchema.add (new TabCol("count", "*"));
                        } else {
                            throw new DBException(ExceptionTypes.NotSupportedOperation(selectItem.getExpression()));
                        }
                    }
                }  else {
                    throw new DBException(ExceptionTypes.NotSupportedOperation(selectItem.getExpression()));
                }
            }
            else {
                throw new DBException(ExceptionTypes.NotSupportedOperation(selectItem.getExpression()));
            }
        }
        return outputSchema;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        String nodeHeader = "ProjectOperator(selectItems=" + selectItems + ")";
        String[] childLines = child.toString().split("\\R");

        // 当前节点
        sb.append(nodeHeader);

        // 子节点处理
        if (childLines.length > 0) {
            sb.append("\n└── ").append(childLines[0]);
            for (int i = 1; i < childLines.length; i++) {
                sb.append("\n    ").append(childLines[i]);
            }
        }

        return sb.toString();
    }

}
