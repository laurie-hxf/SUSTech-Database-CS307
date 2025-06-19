package edu.sustech.cs307.logicalOperator.dml;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.system.DBManager;
import net.sf.jsqlparser.statement.show.ShowTablesStatement;

public class ShowTablesExecutor implements DMLExecutor {
    ShowTablesStatement showTablesStatement;
    private final DBManager dbManager;
    public ShowTablesExecutor(ShowTablesStatement showTablesStatement, DBManager dbManager) {
        this.showTablesStatement = showTablesStatement;
        this.dbManager = dbManager;
    }
    public void execute() throws DBException {
        dbManager.showTables();
    }

}
