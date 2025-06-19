package edu.sustech.cs307.logicalOperator.dml;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.index.InMemoryOrderedIndex;
import edu.sustech.cs307.meta.TabCol;
import edu.sustech.cs307.meta.TableMeta;
import edu.sustech.cs307.physicalOperator.SeqScanOperator;
import edu.sustech.cs307.record.RID;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.tuple.TableTuple;
import edu.sustech.cs307.tuple.Tuple;
import edu.sustech.cs307.value.Value;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import org.pmw.tinylog.Logger;

import java.io.File;

public class CreateIndexExecutor implements DMLExecutor {
    private final CreateIndex createIndexStmt;
    private final DBManager dbManager;
    private final String sql;

    public CreateIndexExecutor(CreateIndex createIndex, DBManager dbManager, String sql) {
        this.createIndexStmt = createIndex;
        this.dbManager = dbManager;
        this.sql = sql;
    }

    @Override
    public void execute() throws DBException {
        String tableName = createIndexStmt.getTable().getName();
        String indexName = createIndexStmt.getIndex().getName();
        String columnName = createIndexStmt.getIndex().getColumnsNames().get(0); // 目前只支持单列索引

        // 获取表元数据
        TableMeta tableMeta = dbManager.getMetaManager().getTable(tableName);
        if (tableMeta == null) {
            throw new DBException(ExceptionTypes.TableDoseNotExist(tableName));
        }

        // 检查列是否存在
        if (!tableMeta.hasColumn(columnName)) {
            throw new DBException(ExceptionTypes.ColumnDoseNotExist(columnName));
        }
        // 创建索引
        tableMeta.getIndexes().put(columnName, TableMeta.IndexType.BTREE);

        // 保存元数据
        dbManager.getMetaManager().saveToJson();
        if(columnName.equals("id")) {
            dbManager.initializeIndexes();
        }else{
            dbManager.createIndex(tableName,columnName);
        }

        Logger.info("Successfully created index {} on table {} (column {})", indexName, tableName, columnName);
    }
}