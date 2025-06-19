package edu.sustech.cs307.physicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.index.InMemoryOrderedIndex;
import edu.sustech.cs307.meta.TableMeta;
import edu.sustech.cs307.record.RID;
import edu.sustech.cs307.record.Record;
import edu.sustech.cs307.record.RecordFileHandle;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.tuple.TableTuple;
import edu.sustech.cs307.tuple.Tuple;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.value.Value;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

public class InMemoryIndexScanOperator implements PhysicalOperator {
    private final InMemoryOrderedIndex index;
    private Iterator<Entry<Value, RID>> currentIterator;
    private Entry<Value, RID> currentEntry;
    private final ArrayList<ColumnMeta> schema;
    private String TableName;
    private String ColumnName;
    private TableMeta tableMeta;
    private Record currentRecord;
    private RecordFileHandle fileHandle;
    private DBManager dbManager;

    public InMemoryIndexScanOperator(String TableName, String ColumnName, DBManager dbManager) throws DBException {
        this.index = dbManager.getIndex(TableName, ColumnName);
        this.schema = new ArrayList<>();
        this.TableName = TableName;
        this.ColumnName = ColumnName;
        this.tableMeta = dbManager.getMetaManager().getTable(TableName);
        this.dbManager = dbManager;
        // TODO: 设置正确的schema
    }

    @Override
    public boolean hasNext() {
        return currentIterator != null && currentIterator.hasNext();
    }

    @Override
    public void Begin() throws DBException {
        // 默认扫描所有记录
        fileHandle = dbManager.getRecordManager().OpenFile(TableName);
        currentIterator = index.Range(null, null, true, true);
        if (hasNext()) {
            currentEntry = currentIterator.next();
        }
    }

    @Override
    public void Next() throws DBException {
        if (hasNext()) {
            currentEntry = currentIterator.next();
//            RID rid = currentEntry.getValue();
            currentRecord = fileHandle.GetRecord(currentEntry.getValue());
        }
    }

    @Override
    public Tuple Current() {
        if (currentEntry == null) {
            return null;
        }
        RID rid = currentEntry.getValue();
        return new TableTuple(TableName, tableMeta, currentRecord, rid);
        // TODO: 根据RID获取实际的记录数据
//        return null;
    }

    @Override
    public void Close() {
        currentIterator = null;
        currentEntry = null;
    }

    @Override
    public ArrayList<ColumnMeta> outputSchema() {
        return tableMeta.columns_list;
    }
//    public Tuple GetValue(RID rid) {
//        index.
//    }
}