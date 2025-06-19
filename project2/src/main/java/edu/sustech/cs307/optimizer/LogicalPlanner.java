package edu.sustech.cs307.optimizer;

import java.io.StringReader;
import java.util.List;
import java.util.Set;

import edu.sustech.cs307.logicalOperator.dml.*;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.parser.JSqlParser;
import net.sf.jsqlparser.statement.DescribeStatement;
import net.sf.jsqlparser.statement.ExplainStatement;
import net.sf.jsqlparser.statement.ShowStatement;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.show.ShowTablesStatement;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.logicalOperator.*;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.exception.DBException;

import net.sf.jsqlparser.statement.select.SelectItem; // 导入 SelectExpressionItem 类

public class LogicalPlanner {
    public static LogicalOperator resolveAndPlan(DBManager dbManager, String sql) throws DBException {
        JSqlParser parser = new CCJSqlParserManager();
        Statement stmt = null;
        try {
            stmt = parser.parse(new StringReader(sql));
        } catch (JSQLParserException e) {
            throw new DBException(ExceptionTypes.InvalidSQL(sql, e.getMessage()));
        }
        LogicalOperator operator = null;
        // Query

        if (stmt instanceof Select selectStmt) {
            operator = handleSelect(dbManager, selectStmt);
        } else if (stmt instanceof Insert insertStmt) {
            operator = handleInsert(dbManager, insertStmt);
        } else if (stmt instanceof Update updateStmt) {
            operator = handleUpdate(dbManager, updateStmt);
        }
        //TODO: add condition of handleDelete
        // functional
        else if (stmt instanceof Delete del) {
            operator = new LogicalDeleteOperator(
                    del.getTable().getName(), del.getWhere());
        }
        else if (stmt instanceof CreateTable createTableStmt) {
            CreateTableExecutor createTable = new CreateTableExecutor(createTableStmt, dbManager, sql);
            createTable.execute();
            return null;
        }
        else if (stmt instanceof CreateIndex createIndexStmt) {
            CreateIndexExecutor createIndex = new CreateIndexExecutor(createIndexStmt, dbManager, sql);
            createIndex.execute();
            return null;
        }
        else if (stmt instanceof ExplainStatement explainStatement) {
            ExplainExecutor explainExecutor = new ExplainExecutor(explainStatement, dbManager);
            explainExecutor.execute();
            return null;
        } else if (stmt instanceof ShowStatement showStatement) {
            ShowDatabaseExecutor showDatabaseExecutor = new ShowDatabaseExecutor(showStatement);
            showDatabaseExecutor.execute();
            return null;
        }else if (stmt instanceof ShowTablesStatement showTablesStmt) { // Assuming JSqlParser has this
            // You might need a new Executor, e.g., ShowTablesExecutor
            ShowTablesExecutor showTablesExecutor = new ShowTablesExecutor(showTablesStmt, dbManager);
            showTablesExecutor.execute();
            return null;
        } else if (stmt instanceof DescribeStatement describeStatement) {
            DescribeExecutor describeExecutor = new DescribeExecutor(describeStatement,dbManager);
            describeExecutor.execute();
            return null;
        } else if (stmt instanceof Drop dropStatement) {
            DropExecutor dropExecutor = new DropExecutor(dropStatement,dbManager);
            dropExecutor.execute();
            return null;
        }
        else {
            throw new DBException(ExceptionTypes.UnsupportedCommand((stmt.toString())));
        }
        return operator;
    }

    public static boolean containsAggregate(List<SelectItem<?>> selectItems) {
        if (selectItems == null || selectItems.isEmpty()) {
            return false;
        }
        for (SelectItem<?> selectItem : selectItems) {
            String temp = selectItem.toString();
            if (temp.contains("count")|temp.contains("max")|temp.contains("min")|temp.contains("sum")){
                return true;
            }
        }
        return false;
    }


    public static LogicalOperator handleSelect(DBManager dbManager, Select selectStmt) throws DBException {
        PlainSelect plainSelect = selectStmt.getPlainSelect();
        if (plainSelect.getFromItem() == null) {
            throw new DBException(ExceptionTypes.UnsupportedCommand((plainSelect.toString())));
        }
        LogicalOperator root = new LogicalTableScanOperator(plainSelect.getFromItem().toString(), dbManager);

        int depth = 0;
        if (plainSelect.getJoins() != null) {
            for (Join join : plainSelect.getJoins()) {
                root = new LogicalJoinOperator(
                        root,
                        new LogicalTableScanOperator(join.getRightItem().toString(), dbManager),
                        join.getOnExpressions(),
                        depth);
                depth += 1;
            }
        }
        Expression expression = plainSelect.getWhere();
        // 在 Join 之后应用 Filter，Filter 的输入是 Join 的结果 (root)
        if (plainSelect.getWhere() != null) {
             if(expression instanceof InExpression inExpression){
                root = new LogicAntiJoinOperator(root,inExpression.getLeftExpression(),inExpression.getRightExpression(),!inExpression.isNot());
            }else {
                root = new LogicalFilterOperator(root, plainSelect.getWhere());
            }
        }
        if (plainSelect.getGroupBy() != null) {
            root = new LogicalGroupByOperator(root, plainSelect.getGroupBy());
        }
        if (containsAggregate(plainSelect.getSelectItems())) {
            root = new LogicalAggregateOperator(root, plainSelect.getSelectItems());
        }
        root = new LogicalProjectOperator(root, plainSelect.getSelectItems());

        return root;
    }

    private static LogicalOperator handleInsert(DBManager dbManager, Insert insertStmt) {
        return new LogicalInsertOperator(insertStmt.getTable().getName(), insertStmt.getColumns(),
                insertStmt.getValues());
    }

    private static LogicalOperator handleUpdate(DBManager dbManager, Update updateStmt) throws DBException {
        LogicalOperator root = new LogicalTableScanOperator(updateStmt.getTable().getName(), dbManager);
        return new LogicalUpdateOperator(root, updateStmt.getTable().getName(), updateStmt.getUpdateSets(),
                updateStmt.getWhere());
    }


}
