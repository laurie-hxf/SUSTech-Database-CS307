package edu.sustech.cs307.tuple;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.meta.TabCol;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueComparer;
import edu.sustech.cs307.value.ValueType;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.List;

public abstract class Tuple {
    public abstract Value getValue(TabCol tabCol) throws DBException;

    public abstract TabCol[] getTupleSchema();

    public abstract Value[] getValues() throws DBException;

    public boolean eval_expr(Expression expr) throws DBException {
        return evaluateCondition(this, expr);
    }

//    private boolean evaluateCondition(Tuple tuple, Expression whereExpr) {
//        //todo: add Or condition
//        if (whereExpr instanceof AndExpression andExpr) {
//            // Recursively evaluate left and right expressions
//            return evaluateCondition(tuple, andExpr.getLeftExpression())
//                    && evaluateCondition(tuple, andExpr.getRightExpression());
//        } else if (whereExpr instanceof BinaryExpression binaryExpression) {
//            return evaluateBinaryExpression(tuple, binaryExpression);
//        } else if (whereExpr instanceof OrExpression orExpr) {
//            return evaluateCondition(tuple, orExpr.getLeftExpression());
//        } else {
//            return true; // For non-binary and non-AND expressions, just return true for now
//        }
//    }

    private boolean evaluateCondition(Tuple tuple, Expression expr) throws DBException {
        // 1) 递归解析 AND / OR
        if (expr instanceof AndExpression andExpr) {
            return evaluateCondition(tuple, andExpr.getLeftExpression())
                    && evaluateCondition(tuple, andExpr.getRightExpression());
        }
        if (expr instanceof OrExpression  orExpr) {
            return evaluateCondition(tuple, orExpr.getLeftExpression())
                    || evaluateCondition(tuple, orExpr.getRightExpression());
        }

        // 2) 括号 -> 去掉括号递归
//        if (expr instanceof Parenthesis parenthesis) {
//            return evaluateCondition(tuple, parenthesis.getExpression());
//        }

        // 3) NOT 表达式
        if (expr instanceof net.sf.jsqlparser.expression.NotExpression not) {
            return !evaluateCondition(tuple, not.getExpression());
        }

        // 4) 普通二元比较
        if (expr instanceof BinaryExpression binary) {
            return evaluateBinaryExpression(tuple, binary);
        }

        /* 其他情况（如常量 TRUE/FALSE）默认 true */
        return true;
    }

    //    private boolean evaluateBinaryExpression(Tuple tuple, BinaryExpression binaryExpr) {
//        Expression leftExpr = binaryExpr.getLeftExpression();
//        Expression rightExpr = binaryExpr.getRightExpression();
//        String operator = binaryExpr.getStringExpression();
//        Value leftValue = null;
//        Value rightValue = null;
//
//        try {
//            if (leftExpr instanceof Column leftColumn) {
//                leftValue = tuple.getValue(new TabCol(leftColumn.getTableName(), leftColumn.getColumnName()));
//                if (leftValue.type == ValueType.CHAR) {
//                    leftValue = new Value(leftValue.toString());
//                }
//            } else {
//                leftValue = getConstantValue(leftExpr); // Handle constant left value
//            }
//
//            if (rightExpr instanceof Column rightColumn) {
//                rightValue = tuple.getValue(new TabCol(rightColumn.getTableName(), rightColumn.getColumnName()));
//            } else {
//                rightValue = getConstantValue(rightExpr); // Handle constant right value
//
//            }
//
//            if (leftValue == null || rightValue == null)
//                return false;
//
//            int comparisonResult = ValueComparer.compare(leftValue, rightValue);
//            if (operator.equals("=")) {
//                return comparisonResult == 0;
//            } else if (operator.equals(">")) {
//                return comparisonResult > 0;
//            } else if (operator.equals(">=")) {
//                return comparisonResult >= 0;
//            } else if (operator.equals("<")) {
//                return comparisonResult < 0;
//            } else if (operator.equals("<=")) {
//                return comparisonResult <= 0;
//            } else {
//                throw new RuntimeException("op");
//            }
//
//
//        } catch (DBException e) {
//            e.printStackTrace(); // Handle exception properly
//        }
//        return false;
//    }
    private boolean evaluateBinaryExpression(Tuple tuple,
                                             BinaryExpression bexpr) throws DBException {
        Expression leftExpr  = bexpr.getLeftExpression();
        Expression rightExpr = bexpr.getRightExpression();

        Value leftValue  = (leftExpr  instanceof Column col)
                ? tuple.getValue(new TabCol(col.getTableName(), col.getColumnName()))
                : getConstantValue(leftExpr);

        Value rightValue = (rightExpr instanceof Column col)
                ? tuple.getValue(new TabCol(col.getTableName(), col.getColumnName()))
                : getConstantValue(rightExpr);

        if (leftValue == null || rightValue == null) return false;

        int cmp = ValueComparer.compare(leftValue, rightValue);

        /* 支持  =  !=  <>  >  >=  <  <=  */
        switch (bexpr.getStringExpression()) {
            case "="  : return cmp == 0;
            case "!=" :
            case "<>" : return cmp != 0;
            case ">"  : return cmp > 0;
            case ">=" : return cmp >= 0;
            case "<"  : return cmp < 0;
            case "<=" : return cmp <= 0;
            default   : throw new DBException(
                    ExceptionTypes.UnsupportedOperator(
                            bexpr.getStringExpression()));
        }
    }

    private Value getConstantValue(Expression expr) {
        if (expr instanceof StringValue) {
            return new Value(((StringValue) expr).getValue(), ValueType.CHAR);
        } else if (expr instanceof DoubleValue) {
            return new Value(((DoubleValue) expr).getValue(), ValueType.FLOAT);
        } else if (expr instanceof LongValue) {
            return new Value(((LongValue) expr).getValue(), ValueType.INTEGER);
        }
        return null; // Unsupported constant type
    }

    public Value evaluateExpression(Expression expr) throws DBException {
        if (expr instanceof StringValue) {
            return new Value(((StringValue) expr).getValue(), ValueType.CHAR);
        } else if (expr instanceof DoubleValue) {
            return new Value(((DoubleValue) expr).getValue(), ValueType.FLOAT);
        } else if (expr instanceof LongValue) {
            return new Value(((LongValue) expr).getValue(), ValueType.INTEGER);
        } else if (expr instanceof Column) {
            Column col = (Column) expr;
            return getValue(new TabCol(col.getTableName(), col.getColumnName()));
        } else {
            throw new DBException(ExceptionTypes.UnsupportedExpression(expr));
        }
    }

    public List<String> evaluateGroupExpression(Expression expr) throws DBException {
        try {
            if (expr instanceof ExpressionList list) {
                List<String> values = new ArrayList<>();
                for (Object subExpr : list.getExpressions()) {
                    if (subExpr instanceof Column col) {
                        values.add(getValue(new TabCol(col.getTableName(), col.getColumnName())).toString());
                    }
                }
                return values;
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
