package edu.sustech.cs307.physicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.logicalOperator.LogicalOperator;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.meta.TabCol;
import edu.sustech.cs307.optimizer.LogicalPlanner;
//import edu.sustech.cs307.optimizer.PhysicalPlanner;
import edu.sustech.cs307.optimizer.PhysicalPlanner;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.tuple.Tuple;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueComparer;
import edu.sustech.cs307.value.ValueType;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.pmw.tinylog.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AntiJoinOperator implements PhysicalOperator {
    private PhysicalOperator child;
    private Expression leftExpression;
    private Expression rightExpression;
    private List<Expression> values;
    private Tuple currentTuple;
    private boolean isOpen = false;
    private boolean expressionlist = false;
    private PhysicalOperator sub_physicalOperator;
    // 标记是否已经准备好下一个元组
    private boolean readyForNext = false;
    private Set<Value> subQueryResultSet;
    private boolean isin;
    public AntiJoinOperator(PhysicalOperator child, Expression left_expr, Expression right_expr,boolean isin,DBManager dbManager) throws DBException {
        this.child = child;
        this.leftExpression = left_expr;
        this.isin = isin;
        this.rightExpression = right_expr;
        values = new ArrayList<>();
        subQueryResultSet = new HashSet<>();
        if (right_expr instanceof ExpressionList) {
            ExpressionList exprList = (ExpressionList) right_expr;
            this.values = exprList.getExpressions();
            expressionlist = true;
        } else if (right_expr instanceof Select) {
            LogicalOperator operator = LogicalPlanner.resolveAndPlan(dbManager, right_expr.toString());
            sub_physicalOperator = PhysicalPlanner.generateOperator(dbManager, operator);
        }
    }
    @Override
    public boolean hasNext() throws DBException {
            Logger.debug("FilterOperator.hasNext() 被调用");
            if (!isOpen) {
                return false;
            }
            // 如果我们还没有准备好下一个元组，就尝试找一个
            if (!readyForNext) {
                return findNext();
            }
            // 如果已经准备好，且currentTuple不为null，则说明有下一个
            return currentTuple != null;
    }

    @Override
    public void Begin() throws DBException {
        if(expressionlist) {
            child.Begin();

        }else{
            child.Begin();
            sub_physicalOperator.Begin();
            while (sub_physicalOperator.hasNext()) {
                sub_physicalOperator.Next();
                Tuple tuple = sub_physicalOperator.Current();
                if (rightExpression instanceof Select select) {
                    PlainSelect plainSelect = select.getPlainSelect();
                    for (SelectItem<?> selectItem : plainSelect.getSelectItems()) {
                        if (selectItem.getExpression() instanceof Column column) {
                            subQueryResultSet.add(tuple.getValue(new TabCol(column.getTableName(), column.getColumnName())));
                        }
                    }
                }
            }
        }
        isOpen = true;
        currentTuple = null;
        readyForNext = false;
    }

    @Override
    public void Next() throws DBException {
            if (!isOpen) {
                return;
            }

            // 如果没有准备好，先准备
            if (!readyForNext) {
                hasNext(); // 这会调用findNext()来准备下一个元组
            }

            // 清除已准备状态，表示需要准备下一个元组
            readyForNext = false;

    }

    @Override
    public Tuple Current() {
        return currentTuple;
    }


    private boolean findNext() throws DBException {
        currentTuple = null;

        // 循环直到找到匹配的元组或没有更多元组
        while (child.hasNext()) {
            child.Next();
            Tuple tuple = child.Current();
            // 如果元组不为空且满足条件，则设置为当前元组并标记为已准备好
            if (tuple != null && judge_in(tuple)) {
                Logger.debug("FilterOperator找到匹配的元组: " + tuple);
                currentTuple = tuple;
                readyForNext = true;
                return true;
            }
        }

        Logger.debug("FilterOperator没有找到更多匹配的元组");
        return false;
    }

    private boolean judge_in(Tuple tuple) throws DBException {
        if(leftExpression instanceof Column column){
            Value temp = tuple.getValue(new TabCol(column.getTableName(), column.getColumnName()));
            boolean t =false;
            if(expressionlist) {
                for (Expression expr : values) {
                    int cmp = ValueComparer.compare(temp, getConstantValue(expr));
                    if (cmp == 0) {
                        t = true;
//                        return true;
                    }
                }
                return (t && isin)|(!t && !isin);
            }
            else{
                if(isin) {
                    return subQueryResultSet.contains(temp);
                }
                else{
                    return !subQueryResultSet.contains(temp);
                }
            }
        }
        return false;
    }

    private Value getConstantValue(Expression expr) {
        if (expr instanceof StringValue) {
            return new Value(((StringValue) expr).getValue(), ValueType.CHAR);
        } else if (expr instanceof DoubleValue) {
            return new Value(((DoubleValue) expr).getValue(), ValueType.FLOAT);
        } else if (expr instanceof LongValue) {
            return new Value(((LongValue) expr).getValue(), ValueType.INTEGER);
        }
        return null; // Unsupported constant type
    }

    @Override
    public void Close() {
        if (child != null) {
            child.Close();
        }
        isOpen = false;
        currentTuple = null;
        readyForNext = false;
        Logger.debug("AntiJoinOperator.Close() 被调用");
    }

    @Override
    public ArrayList<ColumnMeta> outputSchema() {
        return child.outputSchema();
    }
}
