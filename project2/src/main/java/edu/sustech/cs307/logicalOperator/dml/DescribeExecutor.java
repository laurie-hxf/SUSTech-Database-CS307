package edu.sustech.cs307.logicalOperator.dml;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.system.DBManager;
import net.sf.jsqlparser.statement.DescribeStatement;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class DescribeExecutor implements DMLExecutor{
    private final DBManager dbManager;
    private final DescribeStatement describeTableStmt;
    public DescribeExecutor(DescribeStatement describeTableStmt,DBManager dbManager){
        this.dbManager = dbManager;
        this.describeTableStmt = describeTableStmt;

    }
    public void execute() throws DBException {
        String table = describeTableStmt.getTable().getName();
        dbManager.descTable(table);
    }
}
