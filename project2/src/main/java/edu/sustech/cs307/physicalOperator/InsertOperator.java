package edu.sustech.cs307.physicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.record.RID;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.tuple.TableTuple;
import edu.sustech.cs307.tuple.TempTuple;
import edu.sustech.cs307.tuple.Tuple;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.pmw.tinylog.Logger;

import java.util.List;
import java.util.ArrayList;

public class InsertOperator implements PhysicalOperator {
    private final String data_file;
    private final List<Value> values;
    private final DBManager dbManager;
    private final int columnSize;
    private final List<String> columns;
    private int rowCount;
    private boolean outputed;

    public InsertOperator(String data_file, List<String> columnNames, List<Value> values, DBManager dbManager) {
        this.data_file = data_file;
        this.values = values;
        this.dbManager = dbManager;
        this.columnSize = columnNames.size();
        this.rowCount = 0;
        this.outputed = false;
        this.columns = columnNames;
    }

    @Override
    public boolean hasNext() {
        return !this.outputed;
    }

    @Override
    public void Begin() throws DBException {
        try {
            var fileHandle = dbManager.getRecordManager().OpenFile(data_file);
            // Serialize values to ByteBuf

//            var recordFileHandle = dbManager.getRecordManager().OpenFile(tableName);

            // 将values转换为ByteBuf
            ByteBuf buffer = Unpooled.buffer();
//            for (Value value : values) {
//                buffer.writeBytes(value.ToByte());
//            }
//
//            RID rid = fileHandle.InsertRecord(buffer);

            for (int i = 0; i < values.size(); i++) {
                buffer.writeBytes(values.get(i).ToByte());
                if (i != 0 && (i + 1) % columnSize == 0) {
//                    fileHandle.InsertRecord(buffer);
                    RID rid = fileHandle.InsertRecord(buffer);
                    if(rid.slotNum<0){
                        Logger.info("error");
                        Logger.info(rid.pageNum);
                        Logger.info(rid.slotNum);
                    }
                    buffer.clear();
                    // 更新索引
                    var tableMeta = dbManager.getMetaManager().getTable(data_file);
                    if (tableMeta.getIndexes() != null && !tableMeta.getIndexes().isEmpty()) {
                        for (String columnName : tableMeta.getIndexes().keySet()) {
                            // 找到索引列在values中的位置
                            int columnIndex = columns.indexOf(columnName);
                            if (columnIndex != -1) {
                                // 获取索引对象
                                var index = dbManager.getIndex(data_file, columnName);
                                if (index != null) {
                                    // 更新索引
                                    index.insert(values.get(columnIndex), rid);
                                }
                                var secondIndex = dbManager.getSecondaryIndex(data_file, columnName);
                                if (secondIndex != null) {
                                    secondIndex.insert(values.get(columnIndex),values.get(0));
                                }
                            }
                        }
                    }

                }
            }

            this.rowCount = values.size() / columnSize;
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to insert record: " + e.getMessage() + "\n");
        }
    }

    @Override
    public void Next() {
    }

    @Override
    public Tuple Current() {
        ArrayList<Value> values = new ArrayList<>();
        values.add(new Value(rowCount, ValueType.INTEGER));
        this.outputed = true;
        return new TempTuple(values);
    }

    @Override
    public void Close() {
    }

    @Override
    public ArrayList<ColumnMeta> outputSchema() {
        ArrayList<ColumnMeta> outputSchema = new ArrayList<>();
        outputSchema.add(new ColumnMeta("insert", "numberOfInsertRows", ValueType.INTEGER, 0, 0));
        return outputSchema;
    }

    public void reset() {
        // nothing to do
    }

    public Tuple getNextTuple() {
        return null;
    }
}
