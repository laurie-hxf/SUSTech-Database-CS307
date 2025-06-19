package edu.sustech.cs307.logicalOperator.dml;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.system.DBManager;
import net.sf.jsqlparser.statement.DescribeStatement;
import net.sf.jsqlparser.statement.drop.Drop;

public class DropExecutor implements DMLExecutor{
    private final Drop dropStmt;
    private final DBManager dbManager;
    public DropExecutor(Drop dropStmt, DBManager dbManager){
        this.dropStmt = dropStmt;
        this.dbManager = dbManager;
    }
    public void execute() throws DBException {
        String table = dropStmt.getName().getName();
        dbManager.dropTable(table);
    }

}
