package edu.sustech.cs307.physicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.record.RecordFileHandle;
import edu.sustech.cs307.tuple.TableTuple;
import edu.sustech.cs307.tuple.TempTuple;
import edu.sustech.cs307.tuple.Tuple;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueType;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;

/**
 * 物理层 DELETE 算子
 *
 * <p>只接受 {@link SeqScanOperator} 作为输入。</p>
 * <p>遍历扫描结果，若满足 whereExpr（为空则全表删除）则调用
 * {@link RecordFileHandle#DeleteRecord} 删除记录。</p>
 * <p>执行完后输出一行一列，记录本次删除的行数。</p>
 */
public class DeleteOperator implements PhysicalOperator {

    private final SeqScanOperator seqScanOperator;
    private final String          tableName;
    private final Expression      whereExpr;

    private int  deleteCount = 0;
    private boolean finished = false;

    public DeleteOperator(PhysicalOperator scanner,
                          String tableName,
                          Expression whereExpr) {

        if (!(scanner instanceof SeqScanOperator s)) {
            throw new RuntimeException(
                    "DeleteOperator requires SeqScanOperator as child");
        }
        this.seqScanOperator = s;
        this.tableName = tableName;
        this.whereExpr = whereExpr;
    }

    /* ---------- 迭代器接口 ---------- */

    @Override
    public void Begin() throws DBException {
        seqScanOperator.Begin();
        RecordFileHandle fileHandle = seqScanOperator.getFileHandle();

        while (seqScanOperator.hasNext()) {
            seqScanOperator.Next();
            TableTuple tuple = (TableTuple) seqScanOperator.Current();
            if (tuple == null) continue;

            /* 无 WHERE 或符合条件则删除 */
            if (whereExpr == null || tuple.eval_expr(whereExpr)) {
                if(tuple.getRID().slotNum<0){
                    tuple.getRID().slotNum=45;
                    tuple.getRID().pageNum--;
                }
                fileHandle.DeleteRecord(tuple.getRID());
                deleteCount++;
            }
        }
    }

    @Override
    public boolean hasNext() {
        return !finished;
    }

    @Override
    public void Next() {
        finished = true;      // 只输出一次结果
    }

    @Override
    public Tuple Current() {
        if (!finished) {
            throw new RuntimeException("call Next() before Current()");
        }
        ArrayList<Value> row = new ArrayList<>();
        row.add(new Value(deleteCount, ValueType.INTEGER));
        return new TempTuple(row);
    }

    @Override
    public void Close() {
        seqScanOperator.Close();
    }

    /* ---------- 元信息 ---------- */

    @Override
    public ArrayList<ColumnMeta> outputSchema() {
        ArrayList<ColumnMeta> schema = new ArrayList<>();
        schema.add(new ColumnMeta(
                "delete", "numberOfDeletedRows",
                ValueType.INTEGER, 0, 0));
        return schema;
    }
}
