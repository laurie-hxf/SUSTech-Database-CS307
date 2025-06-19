package edu.sustech.cs307.system;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.index.Index;
import edu.sustech.cs307.index.SecondaryIndex;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.meta.MetaManager;
import edu.sustech.cs307.meta.TableMeta;
import edu.sustech.cs307.storage.BufferPool;
import edu.sustech.cs307.storage.DiskManager;
import edu.sustech.cs307.value.ValueType;
import edu.sustech.cs307.index.InMemoryOrderedIndex;
import org.apache.commons.lang3.StringUtils;
import org.pmw.tinylog.Logger;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DBManager {
    private final MetaManager metaManager;
    /* --- --- --- */
    private final DiskManager diskManager;
    private final BufferPool bufferPool;
    private final RecordManager recordManager;
    
    // 添加索引缓存
    private final Map<String, InMemoryOrderedIndex> indexCache;
    private final Map<String, SecondaryIndex> SecondIndexCache;
    private boolean en_index;

    public DBManager(DiskManager diskManager, BufferPool bufferPool, RecordManager recordManager,
            MetaManager metaManager) {
        this.diskManager = diskManager;
        this.bufferPool = bufferPool;
        this.recordManager = recordManager;
        this.metaManager = metaManager;
        this.indexCache = new ConcurrentHashMap<>();
        this.SecondIndexCache = new ConcurrentHashMap<>();
        en_index = true;
        // 初始化所有索引
        initializeIndexes();
    }
    public void en_index(boolean en) {
        en_index = en;
    }
    public boolean isEn_index() {
        return en_index&&(!indexCache.isEmpty() ||!SecondIndexCache.isEmpty());
    }
    // 初始化所有索引
    public void initializeIndexes() {
        try {
            Set<String> tableNames = metaManager.getTableNames();
            for (String tableName : tableNames) {
                TableMeta tableMeta = metaManager.getTable(tableName);
                if (tableMeta.getIndexes() != null) {
                    for (String columnName : tableMeta.getIndexes().keySet()) {
                        String indexKey = tableName + "." + columnName;
                        if (!indexCache.containsKey(indexKey)&&columnName.equals("id")) {
                            InMemoryOrderedIndex index = new InMemoryOrderedIndex(this, tableName, columnName);
                            indexCache.put(indexKey, index);
                        }else if (!indexCache.containsKey(indexKey)) {
                            SecondaryIndex secondaryIndex = new SecondaryIndex(this, tableName, columnName);
                            SecondIndexCache.put(indexKey, secondaryIndex);
                        }
                    }
                }
            }
        } catch (DBException e) {
            Logger.error("Error initializing indexes: " + e.getMessage());
        }
    }
    
    // 获取索引
    public InMemoryOrderedIndex getIndex(String tableName, String columnName) {
        String indexKey = tableName + "." + columnName;
        return indexCache.get(indexKey);
    }
    public SecondaryIndex getSecondaryIndex(String tableName, String columnName) {
        String indexKey = tableName + "." + columnName;
        return SecondIndexCache.get(indexKey);
    }
    
    // 创建新索引
    public void createIndex(String tableName, String columnName) throws DBException {
        TableMeta tableMeta = metaManager.getTable(tableName);
        if (tableMeta == null) {
            throw new DBException(ExceptionTypes.TableDoseNotExist(tableName));
        }
        if (!tableMeta.hasColumn(columnName)) {
            throw new DBException(ExceptionTypes.ColumnDoseNotExist(columnName));
        }
        // 创建并缓存索引
        String indexKey = tableName + "." + columnName;
        SecondaryIndex index = new SecondaryIndex(this, tableName, columnName);
        SecondIndexCache.put(indexKey, index);
    }

    public BufferPool getBufferPool() {
        return bufferPool;
    }

    public RecordManager getRecordManager() {
        return recordManager;
    }

    public DiskManager getDiskManager() {
        return diskManager;
    }

    public MetaManager getMetaManager() {
        return metaManager;
    }

    public boolean isDirExists(String dir) {
        File file = new File(dir);
        return file.exists() && file.isDirectory();
    }

    /**
     * Displays a formatted table listing all available tables in the database.
     * The output is presented in a bordered ASCII table format with centered table
     * names.
     * Each table name is displayed in a separate row within the ASCII borders.
     */
    private String center(String text, int width) {
        int padding = width - text.length();
        int padLeft = padding / 2;
        int padRight = padding - padLeft;
        return " ".repeat(padLeft) + text + " ".repeat(padRight);
    }
    public void showTables() {
//        throw new RuntimeException("Not implement");
        Set<String> tableNames = metaManager.getTableNames();
        if (tableNames.isEmpty()) {
            Logger.info("|-----------|");
            Logger.info("|   TABLE   |");
            Logger.info("|-----------|");
            Logger.info("|   (none)  |");
            Logger.info("|-----------|");
            return;
        }
        int maxLength = "TABLE".length();
        for (String name : tableNames) {
            if (name.length() > maxLength) {
                maxLength = name.length();
            }
        }
        String border = "|" + "-".repeat(maxLength + 2) + "|";
        String header = "| " + center("TABLE", maxLength) + " |";

        Logger.info(border);
        Logger.info(header);
        Logger.info(border);

        for (String name : tableNames) {
            String row = "| " + center(name, maxLength) + " |";
            Logger.info(row);
        }
        Logger.info(border);
        //TODO: complete show table
        // | -- TABLE -- |
        // | -- ${table} -- |
        // | ----------- |
    }

    public void descTable(String tableName) throws DBException {
//        throw new RuntimeException("Not implemented yet");
        TableMeta tableMeta = metaManager.getTable(tableName);

        if (tableMeta == null) {
            throw new DBException(ExceptionTypes.TableDoseNotExist(tableName));
        }

        ArrayList<ColumnMeta> columns = tableMeta.columns_list; // 优先使用有序列表

        if (columns == null || columns.isEmpty()) {
            // 如果有序列表为空，尝试从 map 获取 (但顺序不保证)
            // 或者直接显示没有列，这里简化为直接显示没有列
            Logger.info("|----------------------|");
            Logger.info("|    TABLE Field       |");
            Logger.info("|----------------------|");
            Logger.info("| (No columns defined) |");
            Logger.info("|----------------------|");
            return;
        }

        // 1. 计算每列所需的最大宽度
        int maxFieldLength = "TABLE Field".length(); // 表头"TABLE Field"的长度
        int maxTypeLength = "Column Type".length(); // 表头"Column Type"的长度

        for (ColumnMeta column : columns) {
            if (column.name != null && column.name.length() > maxFieldLength) {
                maxFieldLength = column.name.length();
            }
            // 假设 ColumnMeta 有 getSQLTypeString() 方法
            String typeStr = column.type.toString();
            if (typeStr != null && typeStr.length() > maxTypeLength) {
                maxTypeLength = typeStr.length();
            }
        }

        // 2. 构建边框和表头
        // 每个单元格左右各加一个空格作为内边距，所以总宽度是 maxLength + 2
        String fieldBorderPart = "-".repeat(maxFieldLength + 2);
        String typeBorderPart = "-".repeat(maxTypeLength + 2);
        String border = "|" + fieldBorderPart + "|" + typeBorderPart + "|";

        String headerField = center("Field", maxFieldLength);
        String headerType = center("Type", maxTypeLength);
        String header = "| " + headerField + " | " + headerType + " |";

        // 3. 打印表头
        Logger.info(border);
        Logger.info(header);
        Logger.info(border);

        // 4. 打印每一行数据
        for (ColumnMeta column : columns) {
            String fieldName = center(column.name, maxFieldLength);
            String fieldType = center(column.type.toString(), maxTypeLength); // 使用 getSQLTypeString()
            String row = "| " + fieldName + " | " + fieldType + " |";
            Logger.info(row);
        }

        // 5. 打印表尾
        Logger.info(border);
        //TODO: complete describe table
        // | -- TABLE Field -- | -- Column Type --|
        // | --  ${table field} --| -- ${table type} --|
    }

    /**
     * Creates a new table in the database with specified name and column metadata.
     * This method sets up both the table metadata and the physical storage
     * structure.
     *
     * @param table_name The name of the table to be created
     * @param columns    List of column metadata defining the table structure
     * @throws DBException If there is an error during table creation
     */
    public void createTable(String table_name, ArrayList<ColumnMeta> columns) throws DBException {
        TableMeta tableMeta = new TableMeta(
                table_name, columns);
        metaManager.createTable(tableMeta);
        String table_folder = String.format("%s/%s", diskManager.getCurrentDir(), table_name);
        File file_folder = new File(table_folder);
        if (!file_folder.exists()) {
            file_folder.mkdirs();
        }
        int record_size = 0;
        for (var col : columns) {
            record_size += col.len;
        }
        String data_file = String.format("%s/%s", table_name, "data");
        recordManager.CreateFile(data_file, record_size);
    }

    /**
     * Drops a table from the database by removing its metadata and associated
     * files.
     * 
     * @param table_name The name of the table to be dropped
     * @throws DBException If the table directory does not exist or encounters IO
     *                     errors during deletion
     */
    public void dropTable(String table_name) throws DBException {
        // TODO: finish drop table method
        metaManager.dropTable(table_name);
        String tableFolderPath = String.format("%s/%s", diskManager.getCurrentDir(), table_name);
        File tableFolder = new File(tableFolderPath);
        if (tableFolder.exists()) {
            try {
                deleteDirectory(tableFolder); // 调用你之前有的 deleteDirectory 辅助方法
            } catch (DBException e) {
                throw new DBException(ExceptionTypes.BadIOError("Failed to delete table data files for: " + table_name + ". Error: " + e.getMessage()));
            }
        }
        Logger.info("Successfully dropped table: {}", table_name);

    }

    /**
     * Recursively deletes a directory and all its contents.
     * If the given file is a directory, it first deletes all its entries
     * recursively.
     * Finally deletes the file/directory itself.
     *
     * @param file The file or directory to be deleted
     * @throws IOException If deletion of any file or directory fails
     */
    private void deleteDirectory(File file) throws DBException {
        if (file.isDirectory()) {
            File[] entries = file.listFiles();
            if (entries != null) {
                for (File entry : entries) {
                    deleteDirectory(entry);
                }
            }
        }
        if (!file.delete()) {
            throw new DBException(ExceptionTypes.BadIOError("File deletion failed: " + file.getAbsolutePath()));
        }
    }

    /**
     * Checks if a table exists in the database.
     *
     * @param table the name of the table to check
     * @return true if the table exists, false otherwise
     */
    public boolean isTableExists(String table) {
        return metaManager.getTableNames().contains(table);
    }

    /**
     * Closes the database manager and performs cleanup operations.
     * This method flushes all pages in the buffer pool, dumps disk manager
     * metadata,
     * and saves meta manager state to JSON format.
     *
     * @throws DBException if an error occurs during the closing process
     */
    public void closeDBManager() throws DBException {
        this.bufferPool.FlushAllPages(null);
        DiskManager.dump_disk_manager_meta(this.diskManager);
        this.metaManager.saveToJson();
    }
}
