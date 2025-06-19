package edu.sustech.cs307.physicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.meta.TabCol;
import edu.sustech.cs307.tuple.JoinTuple;
import edu.sustech.cs307.tuple.Tuple;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueComparer;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;

import java.util.*;

/**
 * 嵌套循环等值连接算子，仅支持单一等值条件 A.x = B.y
 */
public class NestedLoopJoinOperator implements PhysicalOperator {
    private final PhysicalOperator leftOp;
    private final PhysicalOperator rightOp;
    private final TabCol leftCol, rightCol;

    // 外循环当前元组
    private Tuple currentLeft;
    private boolean leftExhausted;

    // 双缓冲：currentTuple 是上一条 Next() 后返回的行，nextTuple 是下一条待返回的行
    private Tuple currentTuple;
    private Tuple nextTuple;

    public NestedLoopJoinOperator(PhysicalOperator leftOp,
                                  PhysicalOperator rightOp,
                                  Collection<Expression> onExprs) {
        this.leftOp  = leftOp;
        this.rightOp = rightOp;

        // 从 onExprs 中取第一个 EqualsTo
        EqualsTo eq = onExprs.stream()
                .filter(e -> e instanceof EqualsTo)
                .map(e -> (EqualsTo)e)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("只支持等值连接"));

        var lc = (net.sf.jsqlparser.schema.Column) eq.getLeftExpression();
        var rc = (net.sf.jsqlparser.schema.Column) eq.getRightExpression();

        this.leftCol  = new TabCol(lc.getTable().getName(), lc.getColumnName());
        this.rightCol = new TabCol(rc.getTable().getName(), rc.getColumnName());
    }

    @Override
    public void Begin() throws DBException {
        // 打开左右算子
        leftOp.Begin();
        rightOp.Begin();

        // 预载第一个 left
        if (leftOp.hasNext()) {
            leftOp.Next();
            currentLeft = leftOp.Current();
            leftExhausted = false;
        } else {
            leftExhausted = true;
        }
        // 预计算第一条 join 结果
        advance();
    }

    /** 把下一条匹配放入 nextTuple（没了就置 null） */
    private void advance() throws DBException {
        nextTuple = null;
        while (!leftExhausted) {
            // 扫描 inner（rightOp）
            while (rightOp.hasNext()) {
                rightOp.Next();
                Tuple rightT = rightOp.Current();

                // 拿出两个 Value
                Value lv = currentLeft.getValue(leftCol);
                Value rv = rightT.getValue(rightCol);
                if (lv != null && rv != null && ValueComparer.compare(lv, rv) == 0) {
                    // 匹配上了，构造 JoinTuple
                    TabCol[] schema = mergeSchema(
                            currentLeft.getTupleSchema(),
                            rightT.getTupleSchema());
                    nextTuple = new JoinTuple(currentLeft, rightT, schema);
                    return;
                }
            }
            // inner 扫描完，重置 inner 算子，外层 next left
            rightOp.Close();
            rightOp.Begin();
            if (leftOp.hasNext()) {
                leftOp.Next();
                currentLeft = leftOp.Current();
            } else {
                leftExhausted = true;
            }
        }
    }

    private TabCol[] mergeSchema(TabCol[] a, TabCol[] b) {
        List<TabCol> all = new ArrayList<>();
        Collections.addAll(all, a);
        Collections.addAll(all, b);
        return all.toArray(new TabCol[0]);
    }

    @Override
    public boolean hasNext() throws DBException {
        return nextTuple != null;
    }

    @Override
    public void Next() throws DBException {
        // currentTuple ← nextTuple，然后再预 load 下一个 nextTuple
        currentTuple = nextTuple;
        advance();
    }

    @Override
    public Tuple Current() {
        return currentTuple;
    }

    @Override
    public void Close() {
        leftOp.Close();
        rightOp.Close();
        currentLeft   = null;
        currentTuple  = null;
        nextTuple     = null;
    }

    @Override
    public ArrayList<ColumnMeta> outputSchema() {
        ArrayList<ColumnMeta> out = new ArrayList<>();
        out.addAll(leftOp.outputSchema());
        out.addAll(rightOp.outputSchema());
        return out;
    }
}
