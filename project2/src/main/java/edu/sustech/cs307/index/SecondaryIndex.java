package edu.sustech.cs307.index;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.meta.TabCol;
import edu.sustech.cs307.physicalOperator.SeqScanOperator;
import edu.sustech.cs307.record.RID;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.tuple.TableTuple;
import edu.sustech.cs307.value.Value;

import java.util.*;

public class SecondaryIndex implements Index{
    private final TreeMap<Value, List<Value>> indexMap;//辅助以及主键
    private final String TableName;
    private final String columnName;
    public SecondaryIndex(DBManager dbManager, String tableName, String columnName) throws DBException {
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
            TabCol tabCol1 = new TabCol(tableName, "id");
            Value value1 = tuple.getValue(tabCol1);
            List<Value> values;
            if(!indexMap.containsKey(value)){
                values = new ArrayList<>();
                values.add(value1);
            }
            else{
                values = indexMap.get(value);
                values.add(value1);
            }
            this.indexMap.put(value, values);
        }

    }
    @Override
    public RID EqualTo(Value value) {
        return null;
    }

    @Override
    public Iterator<Map.Entry<Value, RID>> LessThan(Value value, boolean isEqual) {
        return null;
    }

    @Override
    public Iterator<Map.Entry<Value, RID>> MoreThan(Value value, boolean isEqual) {
        return null;
    }

    @Override
    public Iterator<Map.Entry<Value, RID>> Range(Value low, Value high, boolean leftEqual, boolean rightEqual) {
        return null;
    }


    public List<Value> SecondIndexEqualTo(Value value) {
        return indexMap.getOrDefault(value, Collections.emptyList());
    }

    public Iterator<Map.Entry<Value, List<Value>>> SecondIndexLessThan(Value value, boolean isEqual) {
        // 获取严格小于value的所有条目视图
        NavigableMap<Value, List<Value>> subMap = indexMap.headMap(value, isEqual);

        // 使用descendingMap获取从大到小的迭代器
        return subMap.descendingMap().entrySet().iterator();
    }


    public Iterator<Map.Entry<Value, List<Value>>> SecondIndexMoreThan(Value value, boolean isEqual) {
        // 获取严格大于value的所有条目视图
        NavigableMap<Value, List<Value>> subMap = indexMap.tailMap(value, isEqual);
        return subMap.entrySet().iterator();
    }

    public Iterator<Map.Entry<Value, List<Value>>> SecondIndexRange(Value low, Value high, boolean leftEqual, boolean rightEqual) {
        // 获取范围视图（左闭右开）
        if (low == null && high == null) {
            return indexMap.entrySet().iterator();
        }
        NavigableMap<Value, List<Value>> subMap = indexMap.subMap(
                low, leftEqual,
                high, rightEqual);

        return subMap.entrySet().iterator();
    }

    public void insert(Value value, Value value1) {
        if(!indexMap.containsKey(value)){
            indexMap.get(value).add(value1);
        }
        else{
            List<Value> list = new ArrayList<>();
            list.add(value1);
            indexMap.put(value, list);
        }
    }
}
