package edu.sustech.cs307.logicalOperator;

import net.sf.jsqlparser.expression.Expression;

import java.util.Collections;

/**
 * 逻辑层 DELETE 算子
 * <p>
 *     对应语句形态：<br>
 *     <code>DELETE FROM tableName [WHERE expr]</code>
 * </p>
 *
 * <ul>
 *   <li>这里只保存 <b>表名</b> 与 <b>where 条件</b>（可为 null）。</li>
 *   <li>真正遍历数据的 TableScan + Filter 会在 {@code PhysicalPlanner} 中组装。</li>
 * </ul>
 */
public class LogicalDeleteOperator extends LogicalOperator {

    /** 要删除的目标表 */
    private final String tableName;
    /** WHERE 条件，可能为 null（即无条件删除） */
    private final Expression whereExpr;

    public LogicalDeleteOperator(String tableName, Expression whereExpr) {
        // Delete 自身没有子节点；TableScan 会在物理阶段再接
        super(Collections.emptyList());
        this.tableName = tableName;
        this.whereExpr = whereExpr;
    }

    public String getTableName() {
        return tableName;
    }

    public Expression getWhereExpr() {
        return whereExpr;
    }

    @Override
    public String toString() {
        return "LogicalDeleteOperator(table=" + tableName +
                (whereExpr == null ? ", where=<none>)"
                        : ", where=" + whereExpr + ')');
    }
}
