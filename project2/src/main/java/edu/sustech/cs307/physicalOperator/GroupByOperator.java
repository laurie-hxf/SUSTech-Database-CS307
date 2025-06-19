package edu.sustech.cs307.physicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.meta.TabCol;

import edu.sustech.cs307.tuple.ProjectTuple;
import edu.sustech.cs307.tuple.Tuple;
import edu.sustech.cs307.value.Value;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import org.pmw.tinylog.Logger;


import java.util.*;

public class GroupByOperator implements PhysicalOperator {
    private final PhysicalOperator child;
    private final Expression groupByExpr;
    private boolean isOpen = false;

    private Iterator<Map.Entry<Object, List<Tuple>>> groupIterator;
    private Map.Entry<Object, List<Tuple>> currentGroup;

    // 缓存所有分组结果
    private Map<Object, List<Tuple>> groups;

    public GroupByOperator(PhysicalOperator child, Expression groupByExpr) {
        this.child = child;
        this.groupByExpr = groupByExpr;
    }

    @Override
    public void Begin() throws DBException {
        Logger.debug("GroupByOperator.Begin() 被调用");
        child.Begin();
        isOpen = true;

        // 分组过程：只执行一次
        groups = new HashMap<>();

        while (child.hasNext()) {
            child.Next();
            Tuple tuple = child.Current();
            if (tuple != null) {
                List<String> key = tuple.evaluateGroupExpression(groupByExpr);
                if (!groups.containsKey(key)) {
                    groups.put(key, new ArrayList<>());
                }
                groups.get(key).add(tuple);

            }
        }

        groupIterator = groups.entrySet().iterator();
    }

    @Override
    public boolean hasNext() {
        return isOpen && groupIterator != null && groupIterator.hasNext();
    }

    @Override
    public void Next() {
        if (hasNext()) {
            currentGroup = groupIterator.next();
        } else {
            currentGroup = null;
        }
    }

    // 当前组的所有 tuple（将来 AggregateOperator 用它做聚合）
    public List<Tuple> CurrentGroupTuples() {
        return currentGroup == null ? null : currentGroup.getValue();
    }

    // 当前组的 key
    public Object CurrentGroupKey() {
        return currentGroup == null ? null : currentGroup.getKey();
    }

    @Override
    public Tuple Current() {
        // GroupBy 本身并不直接产生 tuple，而是交由 AggregateOperator 使用 CurrentGroupTuples()
        return null;
    }

    @Override
    public void Close() {
        isOpen = false;
        groups = null;
        groupIterator = null;
        currentGroup = null;
        child.Close();
    }

    @Override

    public ArrayList<ColumnMeta> outputSchema() {
        ArrayList<ColumnMeta> result = new ArrayList<>();
        List<TabCol> outputSchema = new ArrayList<>();
        if (groupByExpr instanceof ExpressionList list) {
            for (Object subExpr : list.getExpressions()) {
                if (subExpr instanceof Column col) {
                    outputSchema.add(new TabCol(col.getTableName(), col.getColumnName()));
                }
            }
        }

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
//        return child.outputSchema();
    }
}