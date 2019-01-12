package org.apache.iotdb.db.qp.logical.crud;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class presents series condition which is general(e.g. numerical comparison) or defined by
 * user. Function is used for bottom operator.<br>
 * FunctionOperator has a {@code seriesPath}, and other filter condition.
 */

public class FunctionOperator extends FilterOperator {
    private Logger LOG = LoggerFactory.getLogger(FunctionOperator.class);

    public FunctionOperator(int tokenIntType) {
        super(tokenIntType);
        operatorType = OperatorType.FUNC;
    }

    @Override
    public boolean addChildOperator(FilterOperator op) {
        LOG.error("cannot add child to leaf FilterOperator, now it's FunctionOperator");
        return false;
    }
    
}
