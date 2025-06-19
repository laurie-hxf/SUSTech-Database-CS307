package edu.sustech.cs307.value;

import java.nio.ByteBuffer;

public class Value implements Comparable<Value>{
    public Object value;
    public ValueType type;
    public static final int INT_SIZE = 8;
    public static final int FLOAT_SIZE = 8;
    public static final int CHAR_SIZE = 64;

    public Value(Object value, ValueType type) {
        this.value = value;
        this.type = type;
    }

    public Value(Long value) {
        this.value = value;
        type = ValueType.INTEGER;
    }

    public Value(Double value) {
        this.value = value;
        type = ValueType.FLOAT;
    }

    public Value(String value) {
        this.value = value;
        type = ValueType.CHAR;
    }

    /**
     * 将当前值转换为字节数组。
     * 
     * @return 字节数组表示的值，根据值的类型（INTEGER、FLOAT、CHAR）进行转换。
     * @throws RuntimeException 如果值的类型不受支持。
     */
    public byte[] ToByte() {
        return switch (type) {
            case INTEGER -> {
                ByteBuffer buffer1 = ByteBuffer.allocate(8);
                buffer1.putLong((long) value);
                yield buffer1.array();
            }
            case FLOAT -> {
                ByteBuffer buffer2 = ByteBuffer.allocate(8);
                buffer2.putDouble((double) value);
                yield buffer2.array();
            }
            case CHAR -> {
                String str = (String) value;
                ByteBuffer buffer3 = ByteBuffer.allocate(64);
                buffer3.putInt(str.length());
                buffer3.put(str.getBytes());
                yield buffer3.array();
            }
            default -> throw new RuntimeException("Unsupported value type: " + type);
        };
    }

    public int compareTo(Value other) {
        return switch (this.type) {
            case INTEGER -> Long.compare((long) this.value, (long) other.value);
            case FLOAT -> Double.compare((double) this.value, (double) other.value);
            case CHAR -> ((String) this.value).compareTo((String) other.value);
            default -> throw new RuntimeException("Unsupported type: " + type);
        };
    }


    @Override
    public boolean equals(Object obj) {
        Value other = (Value) obj;
        return compareTo(other) == 0;
    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }
    /**
     * 根据给定的字节数组和值类型创建一个 Value 对象。
     *
     * @param bytes 字节数组，表示要转换的值。
     * @param type  值的类型，支持 INTEGER、FLOAT 和 CHAR。
     * @return 转换后的 Value 对象。
     * @throws RuntimeException 如果提供的值类型不受支持。
     */
    public static Value FromByte(byte[] bytes, ValueType type) {
        return switch (type) {
            case INTEGER -> {
                ByteBuffer buffer1 = ByteBuffer.wrap(bytes);
                yield new Value(buffer1.getLong());
            }
            case FLOAT -> {
                ByteBuffer buffer2 = ByteBuffer.wrap(bytes);
                yield new Value(buffer2.getDouble());
            }
            case CHAR -> {
                ByteBuffer buffer3 = ByteBuffer.wrap(bytes);
                var length = buffer3.getInt();
                // int is 4 byte
                String s = new String(bytes, 4, length);
                yield new Value(s);
            }
            default -> throw new RuntimeException("Unsupported value type: " + type);
        };

    }
    public Value add(Value other) {
        if (this.type != other.type) {
            throw new RuntimeException("Cannot add values of different types: " + this.type + " and " + other.type);
        }

        return switch (this.type) {
            case INTEGER -> new Value((Long) this.value + (Long) other.value);
            case FLOAT -> new Value((Double) this.value + (Double) other.value);
            case CHAR -> new Value((String) this.value + (String) other.value);
            default -> throw new RuntimeException("Unsupported type for addition: " + this.type);
        };
    }

    @Override
    public String toString() {
        switch (type) {
            case INTEGER, FLOAT ->{
                return this.value.toString();
            }
            case CHAR -> {
                byte[] bytes = ((String) this.value).getBytes();
                ByteBuffer buffer3 = ByteBuffer.wrap(bytes);
                var length = buffer3.getInt();
                // int is 4 byte
                return new String(bytes, 4, length);
            }
            default -> throw new RuntimeException("Unsupported value type: " + type);
        }
    }
}
