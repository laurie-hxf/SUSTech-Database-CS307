package edu.sustech.cs307.logicalOperator.dml;

import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.optimizer.LogicalPlanner;
import edu.sustech.cs307.logicalOperator.LogicalOperator;

import net.sf.jsqlparser.statement.ExplainStatement;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;
import org.pmw.tinylog.Logger;

public class ExplainExecutor implements DMLExecutor {

    private final ExplainStatement explainStatement;
    private final DBManager dbManager;

    public ExplainExecutor(ExplainStatement explainStatement, DBManager dbManager) {
        this.explainStatement = explainStatement;
        this.dbManager = dbManager;
    }

    @Override
    public void execute() throws DBException {
       //TODO: finish this function here, and add log info
        Statement stmt = explainStatement.getStatement();
        LogicalOperator operator = null;
        if (stmt instanceof Select selectStmt) {
            operator = LogicalPlanner.handleSelect(dbManager, selectStmt);
        }
        if (operator != null) {
            String planString = operator.toString();
            for (String line : planString.split("\n")) {
                Logger.info(line);
            }
        } else {
            Logger.info("Could not generate a plan for the given statement.");
        }

    }
}
