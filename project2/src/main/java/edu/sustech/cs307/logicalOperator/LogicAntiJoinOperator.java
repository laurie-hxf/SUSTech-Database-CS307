package edu.sustech.cs307.logicalOperator;

import net.sf.jsqlparser.expression.Expression;

import java.util.Collections;
import java.util.List;

public class LogicAntiJoinOperator extends LogicalOperator {
    private final Expression leftExpression;
    private final Expression rightExpression;
    private final LogicalOperator child;
    private boolean isin;
    public LogicAntiJoinOperator(LogicalOperator child, Expression left_expression, Expression right_expression,boolean isin) {
        super(Collections.singletonList(child));
        this.child = child;
        this.leftExpression = left_expression;
        this.rightExpression = right_expression;
        this.isin = isin;
    }

    public Expression getLeftExpression() {
        return leftExpression;
    }
    public Expression getRightExpression() {
        return rightExpression;
    }
    public boolean isin() {
        return isin;
    }

    public LogicalOperator getChild() {
        return child;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        String nodeHeader = "LogicalAntiJoinOperator(condition=" +rightExpression.toString()+")";
        LogicalOperator child = getChildren().get(0); // 获取过滤的子节点

        // 拆分子节点的多行字符串
        String[] childLines = child.toString().split("\\R");

        // 当前节点
        sb.append(nodeHeader);

        // 子节点处理
        if (childLines.length > 0) {
            sb.append("\n    └── ").append(childLines[0]);
            for (int i = 1; i < childLines.length; i++) {
                sb.append("\n    ").append(childLines[i]);
            }
        }

        return sb.toString();
    }
}
