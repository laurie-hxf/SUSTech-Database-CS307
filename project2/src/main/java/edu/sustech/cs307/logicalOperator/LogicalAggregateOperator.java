package edu.sustech.cs307.logicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.meta.TabCol;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

public class LogicalAggregateOperator extends LogicalOperator {
    private final List<SelectItem<?>> selectItems;
    private final LogicalOperator child;

    public LogicalAggregateOperator(LogicalOperator child, List<SelectItem<?>> selectItems) {
        super(Collections.singletonList(child));
        this.child = child;
        this.selectItems = selectItems;
    }

    public List<Pair<String,TabCol>> getOutputSchema() throws DBException {
        TabCol outputSchema = null;
        List<Pair<String,TabCol>> outputPairs = new ArrayList<>();
        boolean have_aggregate = false;
        Expression not_support = null;
        for (SelectItem<?> selectItem : selectItems) {
            String Aggregate = null;
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
            if (Aggregate!=null){
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
                            outputSchema = (new TabCol(sourceTableName, outputColumnName));
                        } else if (param instanceof AllColumns&&Aggregate.equals("count")) {
                            outputSchema = (new TabCol("count", "*"));
                        } else {
                            throw new DBException(ExceptionTypes.NotSupportedOperation(selectItem.getExpression()));
                        }
                    }
                    Pair<String, TabCol> pair = Pair.of(Aggregate,outputSchema);
                    outputPairs.add(pair);
                }  else {
                    throw new DBException(ExceptionTypes.NotSupportedOperation(selectItem.getExpression()));
                }
            }
            not_support = selectItem.getExpression();
        }
        if (!have_aggregate){
            throw new DBException(ExceptionTypes.UnsupportedExpression(not_support));
        }
        return outputPairs;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        String nodeHeader = "AggregateOperator(selectItems=" + selectItems + ")";
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
