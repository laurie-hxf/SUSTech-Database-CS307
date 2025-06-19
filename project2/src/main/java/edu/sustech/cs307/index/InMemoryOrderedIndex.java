package edu.sustech.cs307.index;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.meta.TabCol;
import edu.sustech.cs307.physicalOperator.SeqScanOperator;
import edu.sustech.cs307.record.RID;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.tuple.TableTuple;
import edu.sustech.cs307.value.Value;

import java.util.*;

import org.pmw.tinylog.Logger;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;

public class InMemoryOrderedIndex implements Index {
    private final TreeMap<Value, RID> indexMap;
    private final String TableName;
    private final String columnName;


    public InMemoryOrderedIndex(DBManager dbManager,String tableName,String columnName) throws DBException {
            this.TableName = tableName;
            this.columnName = columnName;
            this.indexMap = new TreeMap<>();
            SeqScanOperator scanOperator = new SeqScanOperator(tableName, dbManager);
            scanOperator.Begin();
            TabCol tabCol = new TabCol(tableName, columnName);
            while (scanOperator.hasNext()) {
                scanOperator.Next();
                TableTuple tuple = (TableTuple) scanOperator.Current();
                Value value = tuple.getValue(tabCol);
                RID rid = tuple.getRID();
                this.indexMap.put(value, rid);
            }
            Logger.info("index created");
    }



    @Override
    public RID EqualTo(Value value) {
        // or throw an exception if preferred
        return indexMap.getOrDefault(value, null);
    }

    public Iterator<Entry<Value, RID>> SecondIndexEqualTo(List<Value> values) {
        List<Entry<Value, RID>> result = new ArrayList<>();

        for (Value value : values) {
            RID rid = indexMap.get(value);
            if (rid != null) {
                result.add(new AbstractMap.SimpleEntry<>(value, rid));
            }
        }

        return result.iterator();
    }

    /**
     * 返回一个迭代器，该迭代器用于遍历所有严格小于给定值的条目。
     * 
     * @param value 要比较的值
     * @return 一个迭代器，按从大到小的顺序遍历所有严格小于给定值的条目
     */
    @Override
    public Iterator<Entry<Value, RID>> LessThan(Value value, boolean isEqual) {
        // 获取严格小于value的所有条目视图
        NavigableMap<Value, RID> subMap = indexMap.headMap(value, isEqual);

        // 使用descendingMap获取从大到小的迭代器
        return subMap.descendingMap().entrySet().iterator();
    }

    /**
     * 返回一个迭代器，遍历所有严格大于给定值的条目。
     *
     * @param value 要比较的值
     * @return 一个迭代器，包含所有严格大于指定值的条目
     */
    @Override
    public Iterator<Entry<Value, RID>> MoreThan(Value value, boolean isEqual) {
        // 获取严格大于value的所有条目视图
        NavigableMap<Value, RID> subMap = indexMap.tailMap(value, isEqual);
        return subMap.entrySet().iterator();
    }

    /**
     * 返回指定范围内的条目迭代器。
     * 
     * @param low        范围的下界
     * @param high       范围的上界
     * @param leftEqual  是否包含下界
     * @param rightEqual 是否包含上界
     * @return 指定范围内条目的迭代器
     */
    @Override
    public Iterator<Entry<Value, RID>> Range(Value low, Value high, boolean leftEqual, boolean rightEqual) {
        // 获取范围视图（左闭右开）
        if (low == null && high == null) {
            return indexMap.entrySet().iterator();
        }
        NavigableMap<Value, RID> subMap = indexMap.subMap(
                low, leftEqual,
                high, rightEqual);

        return subMap.entrySet().iterator();
    }
    
    // 添加新记录到索引
    public void insert(Value value, RID rid) {
        indexMap.put(value, rid);
    }
    
    // 从索引中删除记录
    public void delete(Value value) {
        indexMap.remove(value);
    }
}
