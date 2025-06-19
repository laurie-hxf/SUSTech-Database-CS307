package edu.sustech.cs307.physicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.index.InMemoryOrderedIndex;
import edu.sustech.cs307.index.SecondaryIndex;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.meta.TabCol;
import edu.sustech.cs307.meta.TableMeta;
import edu.sustech.cs307.record.RID;
import edu.sustech.cs307.record.Record;
import edu.sustech.cs307.record.RecordFileHandle;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.tuple.TableTuple;
import edu.sustech.cs307.tuple.Tuple;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueComparer;
import edu.sustech.cs307.value.ValueType;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.schema.Column;
import org.pmw.tinylog.Logger;

import javax.swing.plaf.IconUIResource;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

public class FilterIndexOperator implements PhysicalOperator {
    private final PhysicalOperator child;
    private String tableName;
    private final DBManager dbManager;
    private Tuple currentTuple;
    private Iterator<Entry<Value, RID>> indexIterator;
    private Iterator<Entry<Value, List<Value>>> secondIndexIterator;
    private Entry<Value, RID> currentEntry;
    private InMemoryOrderedIndex index;
    private BinaryExpression expression;
    private RID rid;
    private RID rid_temp;
    private RecordFileHandle fileHandle;
    private TableMeta tableMeta;
    private Record record;

    public FilterIndexOperator(PhysicalOperator child,Expression whereExpr ,DBManager dbManager) {
        this.child = child;
        this.dbManager = dbManager;
        this.index = null;
        if(whereExpr instanceof BinaryExpression){
            expression = (BinaryExpression) whereExpr;
        }

    }

    @Override
    public boolean hasNext() {
        if(rid!=null){
            return true;
        }
        return (indexIterator != null && indexIterator.hasNext());
    }

    @Override
    public void Begin() throws DBException {
        child.Begin();
        evaluateBinaryExpression(expression);
        fileHandle = dbManager.getRecordManager().OpenFile(tableName);
        tableMeta = dbManager.getMetaManager().getTable(tableName);
    }
//    List<Entry<Value, RID>> temp = new ArrayList<>();
    @Override
    public void Next() throws DBException {
        if (indexIterator != null) {
            if (indexIterator.hasNext()) {
                currentEntry = indexIterator.next();
                if (currentEntry != null) {
                    rid_temp = currentEntry.getValue();
                    if(rid_temp.slotNum<0){
                        rid_temp.slotNum=45;
                        rid_temp.pageNum-=1;
                    }
                    record = fileHandle.GetRecord(rid_temp);
                }
            }
        }
        else{
            if (rid != null) {
                if(rid.slotNum<0){
                    rid.slotNum=45;
                    rid.pageNum-=1;
                }
                record = fileHandle.GetRecord(rid);
                rid = null;
            }
        }
    }

    @Override
    public Tuple Current() {
//        RID rid_temp;
//        rid_temp = currentEntry.getValue();
        return new TableTuple(tableName, tableMeta, record, rid_temp);

    }

    @Override
    public void Close() {
        child.Close();
    }

    @Override
    public ArrayList<ColumnMeta> outputSchema() {
        return child.outputSchema();
    }



    private void evaluateBinaryExpression(BinaryExpression bexpr) throws DBException {
        Expression leftExpr  = bexpr.getLeftExpression();
        Expression rightExpr = bexpr.getRightExpression();
        Value value = null;
        SecondaryIndex secondaryIndex = null;
        boolean judge = false;
        if(leftExpr instanceof Column col){
            value = getConstantValue(rightExpr);
            if(dbManager.getIndex(col.getTableName(), col.getColumnName()) != null){
                tableName = col.getTableName();
                index = dbManager.getIndex(col.getTableName(), col.getColumnName());
                judge = true;
            }
            else if(dbManager.getSecondaryIndex(col.getTableName(), col.getColumnName()) != null){
                index = dbManager.getIndex(col.getTableName(), "id");
                tableName = col.getTableName();
                List<Value> key = new ArrayList<>();
                Entry<Value, List<Value>> currentEntry;
                secondaryIndex = dbManager.getSecondaryIndex(col.getTableName(), col.getColumnName());
                switch (bexpr.getStringExpression()) {
                    case "="  :
                        key = secondaryIndex.SecondIndexEqualTo(value);
                        break;
                    case "<"  :
                        secondIndexIterator = secondaryIndex.SecondIndexLessThan(value,false);
                        break;
                    case "<=" :
                        secondIndexIterator = secondaryIndex.SecondIndexLessThan(value,true);
                        break;
                    case ">"  :
                        secondIndexIterator = secondaryIndex.SecondIndexMoreThan(value,false);
                        break;
                    case ">=" :
                        secondIndexIterator = secondaryIndex.SecondIndexMoreThan(value,true);
                        break;
                    default   : throw new DBException(
                            ExceptionTypes.UnsupportedOperator(
                                    bexpr.getStringExpression()));
                }
                while (secondIndexIterator!=null&&secondIndexIterator.hasNext()) {
                    currentEntry = secondIndexIterator.next();
                    if (currentEntry != null) {
                        key.addAll(currentEntry.getValue());
                    }
                }
                indexIterator = index.SecondIndexEqualTo(key);
                return;
            }
        }
//        if(rightExpr instanceof Column col){
//            if(dbManager.getIndex(col.getTableName(), col.getColumnName()) != null){
//                tableName = col.getTableName();
//                index = dbManager.getIndex(col.getTableName(), col.getColumnName());
//                judge = true;
//            }
//        }
//        value  = (leftExpr  instanceof Column col)
//                ? null
//                : getConstantValue(leftExpr);
//
//        value = (rightExpr instanceof Column col)
//                ? value
//                : getConstantValue(rightExpr);
        value = getConstantValue(rightExpr);

        if(!judge|value==null){
            throw new DBException(
                    ExceptionTypes.UnsupportedOperator(
                            bexpr.getStringExpression()));
        }

        switch (bexpr.getStringExpression()) {
            case "="  :
                rid = index.EqualTo(value);
                break;
            case "<"  :
                indexIterator = index.LessThan(value,false);
                break;
            case "<=" :
                indexIterator = index.LessThan(value,true);
                break;
            case ">"  :
                indexIterator = index.MoreThan(value,false);
                break;
            case ">=" :
                indexIterator = index.MoreThan(value,true);
                break;
            default   : throw new DBException(
                    ExceptionTypes.UnsupportedOperator(
                            bexpr.getStringExpression()));
        }

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


}
