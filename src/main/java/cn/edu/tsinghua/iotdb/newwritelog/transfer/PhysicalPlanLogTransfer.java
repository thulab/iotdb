package cn.edu.tsinghua.iotdb.newwritelog.transfer;

import cn.edu.tsinghua.iotdb.exception.WALOverSizedException;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;

import java.io.IOException;
import java.nio.BufferOverflowException;

public class PhysicalPlanLogTransfer {

    public static byte[] operatorToLog(PhysicalPlan plan) throws WALOverSizedException {
        Codec<PhysicalPlan> codec = null;
        switch (plan.getOperatorType()) {
            case INSERT:
                codec = (Codec<PhysicalPlan>) PhysicalPlanCodec.fromOpcode(SystemLogOperator.INSERT).codec;
                break;
            case UPDATE:
                codec = (Codec<PhysicalPlan>) PhysicalPlanCodec.fromOpcode(SystemLogOperator.UPDATE).codec;
                break;
            case DELETE:
                codec = (Codec<PhysicalPlan>) PhysicalPlanCodec.fromOpcode(SystemLogOperator.DELETE).codec;
                break;
            case OVERFLOWFLUSHSTART:
                codec = (Codec<PhysicalPlan>) PhysicalPlanCodec.fromOpcode(SystemLogOperator.OVERFLOWFLUSHSTART).codec;
                break;
            case OVERFLOWFLUSHEND:
                codec = (Codec<PhysicalPlan>) PhysicalPlanCodec.fromOpcode(SystemLogOperator.OVERFLOWFLUSHEND).codec;
                break;
            case BUFFERFLUSHSTART:
                codec = (Codec<PhysicalPlan>) PhysicalPlanCodec.fromOpcode(SystemLogOperator.BUFFERFLUSHSTART).codec;
                break;
            case BUFFERFLUSHEND:
                codec = (Codec<PhysicalPlan>) PhysicalPlanCodec.fromOpcode(SystemLogOperator.BUFFERFLUSHEND).codec;
                break;
            default:
                throw new UnsupportedOperationException("SystemLogOperator given is not supported. " + plan.getOperatorType());
        }
        try {
            return codec.encode(plan);
        } catch (BufferOverflowException e) {
            throw new WALOverSizedException("Plan " + plan.toString() + " is too big to write to WAL");
        }
    }

    public static PhysicalPlan logToOperator(byte[] opInBytes) throws IOException {
        // the first byte determines the opCode
        int opCode = opInBytes[0];
        Codec<PhysicalPlan> codec = (Codec<PhysicalPlan>) PhysicalPlanCodec.fromOpcode(opCode).codec;
        return codec.decode(opInBytes);
    }
}
