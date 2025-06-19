package edu.sustech.cs307.logicalOperator;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.GroupByElement;

import java.util.Collections;

public class LogicalGroupByOperator extends LogicalOperator {
    private final Expression elements;
    private final LogicalOperator child;

    public LogicalGroupByOperator(LogicalOperator child, GroupByElement elements) {
        super(Collections.singletonList(child));
        this.child = child;
        this.elements = elements.getGroupByExpressionList();
    }

    public LogicalOperator getChild() {
        return child;
    }

    public Expression getGroupByExpr() {
        return elements;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        String nodeHeader = "GroupOperator(GroupBy elements=" + elements + ")";
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
