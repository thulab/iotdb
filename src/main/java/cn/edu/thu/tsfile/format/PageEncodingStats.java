/**
 * Autogenerated by Thrift Compiler (0.9.2)
 * <p>
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *
 * @generated
 */
package cn.edu.thu.tsfile.format;

import org.apache.thrift.EncodingUtils;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;
import org.apache.thrift.scheme.TupleScheme;

import javax.annotation.Generated;
import java.util.*;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
/**
 * statistics of a given page type and encoding
 */
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2016-9-9")
public class PageEncodingStats implements org.apache.thrift.TBase<PageEncodingStats, PageEncodingStats._Fields>, java.io.Serializable, Cloneable, Comparable<PageEncodingStats> {
    public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
    private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("PageEncodingStats");
    private static final org.apache.thrift.protocol.TField PAGE_TYPE_FIELD_DESC = new org.apache.thrift.protocol.TField("page_type", org.apache.thrift.protocol.TType.I32, (short) 1);
    private static final org.apache.thrift.protocol.TField ENCODING_FIELD_DESC = new org.apache.thrift.protocol.TField("encoding", org.apache.thrift.protocol.TType.I32, (short) 2);
    private static final org.apache.thrift.protocol.TField NUM_PAGES_FIELD_DESC = new org.apache.thrift.protocol.TField("num_pages", org.apache.thrift.protocol.TType.I32, (short) 3);
    private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
    // isset id assignments
    private static final int __NUM_PAGES_ISSET_ID = 0;

    static {
        schemes.put(StandardScheme.class, new PageEncodingStatsStandardSchemeFactory());
        schemes.put(TupleScheme.class, new PageEncodingStatsTupleSchemeFactory());
    }

    static {
        Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
        tmpMap.put(_Fields.PAGE_TYPE, new org.apache.thrift.meta_data.FieldMetaData("page_type", org.apache.thrift.TFieldRequirementType.REQUIRED,
                new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, PageType.class)));
        tmpMap.put(_Fields.ENCODING, new org.apache.thrift.meta_data.FieldMetaData("encoding", org.apache.thrift.TFieldRequirementType.REQUIRED,
                new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, Encoding.class)));
        tmpMap.put(_Fields.NUM_PAGES, new org.apache.thrift.meta_data.FieldMetaData("num_pages", org.apache.thrift.TFieldRequirementType.REQUIRED,
                new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
        metaDataMap = Collections.unmodifiableMap(tmpMap);
        org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(PageEncodingStats.class, metaDataMap);
    }

    /**
     * the page type (data/dic/...) *
     *
     * @see PageType
     */
    public PageType page_type; // required
    /**
     * encoding of the page *
     *
     * @see Encoding
     */
    public Encoding encoding; // required
    /**
     * number of pages of this type with this encoding *
     */
    public int num_pages; // required
    private byte __isset_bitfield = 0;

    public PageEncodingStats() {
    }

    public PageEncodingStats(
            PageType page_type,
            Encoding encoding,
            int num_pages) {
        this();
        this.page_type = page_type;
        this.encoding = encoding;
        this.num_pages = num_pages;
        setNum_pagesIsSet(true);
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public PageEncodingStats(PageEncodingStats other) {
        __isset_bitfield = other.__isset_bitfield;
        if (other.isSetPage_type()) {
            this.page_type = other.page_type;
        }
        if (other.isSetEncoding()) {
            this.encoding = other.encoding;
        }
        this.num_pages = other.num_pages;
    }

    public PageEncodingStats deepCopy() {
        return new PageEncodingStats(this);
    }

    @Override
    public void clear() {
        this.page_type = null;
        this.encoding = null;
        setNum_pagesIsSet(false);
        this.num_pages = 0;
    }

    /**
     * the page type (data/dic/...) *
     *
     * @see PageType
     */
    public PageType getPage_type() {
        return this.page_type;
    }

    /**
     * the page type (data/dic/...) *
     *
     * @see PageType
     */
    public PageEncodingStats setPage_type(PageType page_type) {
        this.page_type = page_type;
        return this;
    }

    public void unsetPage_type() {
        this.page_type = null;
    }

    /** Returns true if field page_type is set (has been assigned a value) and false otherwise */
    public boolean isSetPage_type() {
        return this.page_type != null;
    }

    public void setPage_typeIsSet(boolean value) {
        if (!value) {
            this.page_type = null;
        }
    }

    /**
     * encoding of the page *
     *
     * @see Encoding
     */
    public Encoding getEncoding() {
        return this.encoding;
    }

    /**
     * encoding of the page *
     *
     * @see Encoding
     */
    public PageEncodingStats setEncoding(Encoding encoding) {
        this.encoding = encoding;
        return this;
    }

    public void unsetEncoding() {
        this.encoding = null;
    }

    /** Returns true if field encoding is set (has been assigned a value) and false otherwise */
    public boolean isSetEncoding() {
        return this.encoding != null;
    }

    public void setEncodingIsSet(boolean value) {
        if (!value) {
            this.encoding = null;
        }
    }

    /**
     * number of pages of this type with this encoding *
     */
    public int getNum_pages() {
        return this.num_pages;
    }

    /**
     * number of pages of this type with this encoding *
     */
    public PageEncodingStats setNum_pages(int num_pages) {
        this.num_pages = num_pages;
        setNum_pagesIsSet(true);
        return this;
    }

    public void unsetNum_pages() {
        __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __NUM_PAGES_ISSET_ID);
    }

    /** Returns true if field num_pages is set (has been assigned a value) and false otherwise */
    public boolean isSetNum_pages() {
        return EncodingUtils.testBit(__isset_bitfield, __NUM_PAGES_ISSET_ID);
    }

    public void setNum_pagesIsSet(boolean value) {
        __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __NUM_PAGES_ISSET_ID, value);
    }

    public void setFieldValue(_Fields field, Object value) {
        switch (field) {
            case PAGE_TYPE:
                if (value == null) {
                    unsetPage_type();
                } else {
                    setPage_type((PageType) value);
                }
                break;

            case ENCODING:
                if (value == null) {
                    unsetEncoding();
                } else {
                    setEncoding((Encoding) value);
                }
                break;

            case NUM_PAGES:
                if (value == null) {
                    unsetNum_pages();
                } else {
                    setNum_pages((Integer) value);
                }
                break;

        }
    }

    public Object getFieldValue(_Fields field) {
        switch (field) {
            case PAGE_TYPE:
                return getPage_type();

            case ENCODING:
                return getEncoding();

            case NUM_PAGES:
                return Integer.valueOf(getNum_pages());

        }
        throw new IllegalStateException();
    }

    /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
    public boolean isSet(_Fields field) {
        if (field == null) {
            throw new IllegalArgumentException();
        }

        switch (field) {
            case PAGE_TYPE:
                return isSetPage_type();
            case ENCODING:
                return isSetEncoding();
            case NUM_PAGES:
                return isSetNum_pages();
        }
        throw new IllegalStateException();
    }

    @Override
    public boolean equals(Object that) {
        if (that == null)
            return false;
        if (that instanceof PageEncodingStats)
            return this.equals((PageEncodingStats) that);
        return false;
    }

    public boolean equals(PageEncodingStats that) {
        if (that == null)
            return false;

        boolean this_present_page_type = true && this.isSetPage_type();
        boolean that_present_page_type = true && that.isSetPage_type();
        if (this_present_page_type || that_present_page_type) {
            if (!(this_present_page_type && that_present_page_type))
                return false;
            if (!this.page_type.equals(that.page_type))
                return false;
        }

        boolean this_present_encoding = true && this.isSetEncoding();
        boolean that_present_encoding = true && that.isSetEncoding();
        if (this_present_encoding || that_present_encoding) {
            if (!(this_present_encoding && that_present_encoding))
                return false;
            if (!this.encoding.equals(that.encoding))
                return false;
        }

        boolean this_present_num_pages = true;
        boolean that_present_num_pages = true;
        if (this_present_num_pages || that_present_num_pages) {
            if (!(this_present_num_pages && that_present_num_pages))
                return false;
            if (this.num_pages != that.num_pages)
                return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        List<Object> list = new ArrayList<Object>();

        boolean present_page_type = true && (isSetPage_type());
        list.add(present_page_type);
        if (present_page_type)
            list.add(page_type.getValue());

        boolean present_encoding = true && (isSetEncoding());
        list.add(present_encoding);
        if (present_encoding)
            list.add(encoding.getValue());

        boolean present_num_pages = true;
        list.add(present_num_pages);
        if (present_num_pages)
            list.add(num_pages);

        return list.hashCode();
    }

    @Override
    public int compareTo(PageEncodingStats other) {
        if (!getClass().equals(other.getClass())) {
            return getClass().getName().compareTo(other.getClass().getName());
        }

        int lastComparison = 0;

        lastComparison = Boolean.valueOf(isSetPage_type()).compareTo(other.isSetPage_type());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetPage_type()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.page_type, other.page_type);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        lastComparison = Boolean.valueOf(isSetEncoding()).compareTo(other.isSetEncoding());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetEncoding()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.encoding, other.encoding);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        lastComparison = Boolean.valueOf(isSetNum_pages()).compareTo(other.isSetNum_pages());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetNum_pages()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.num_pages, other.num_pages);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        return 0;
    }

    public _Fields fieldForId(int fieldId) {
        return _Fields.findByThriftId(fieldId);
    }

    public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
        schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
        schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("PageEncodingStats(");
        boolean first = true;

        sb.append("page_type:");
        if (this.page_type == null) {
            sb.append("null");
        } else {
            sb.append(this.page_type);
        }
        first = false;
        if (!first) sb.append(", ");
        sb.append("encoding:");
        if (this.encoding == null) {
            sb.append("null");
        } else {
            sb.append(this.encoding);
        }
        first = false;
        if (!first) sb.append(", ");
        sb.append("num_pages:");
        sb.append(this.num_pages);
        first = false;
        sb.append(")");
        return sb.toString();
    }

    public void validate() throws org.apache.thrift.TException {
        // check for required fields
        if (page_type == null) {
            throw new org.apache.thrift.protocol.TProtocolException("Required field 'page_type' was not present! Struct: " + toString());
        }
        if (encoding == null) {
            throw new org.apache.thrift.protocol.TProtocolException("Required field 'encoding' was not present! Struct: " + toString());
        }
        // alas, we cannot check 'num_pages' because it's a primitive and you chose the non-beans generator.
        // check for sub-struct validity
    }

    private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
        try {
            write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
        } catch (org.apache.thrift.TException te) {
            throw new java.io.IOException(te);
        }
    }

    private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
        try {
            // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
            __isset_bitfield = 0;
            read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
        } catch (org.apache.thrift.TException te) {
            throw new java.io.IOException(te);
        }
    }

    /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
    public enum _Fields implements org.apache.thrift.TFieldIdEnum {
        /**
         * the page type (data/dic/...) *
         *
         * @see PageType
         */
        PAGE_TYPE((short) 1, "page_type"),
        /**
         * encoding of the page *
         *
         * @see Encoding
         */
        ENCODING((short) 2, "encoding"),
        /**
         * number of pages of this type with this encoding *
         */
        NUM_PAGES((short) 3, "num_pages");

        private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

        static {
            for (_Fields field : EnumSet.allOf(_Fields.class)) {
                byName.put(field.getFieldName(), field);
            }
        }

        private final short _thriftId;
        private final String _fieldName;

        _Fields(short thriftId, String fieldName) {
            _thriftId = thriftId;
            _fieldName = fieldName;
        }

        /**
         * Find the _Fields constant that matches fieldId, or null if its not found.
         */
        public static _Fields findByThriftId(int fieldId) {
            switch (fieldId) {
                case 1: // PAGE_TYPE
                    return PAGE_TYPE;
                case 2: // ENCODING
                    return ENCODING;
                case 3: // NUM_PAGES
                    return NUM_PAGES;
                default:
                    return null;
            }
        }

        /**
         * Find the _Fields constant that matches fieldId, throwing an exception
         * if it is not found.
         */
        public static _Fields findByThriftIdOrThrow(int fieldId) {
            _Fields fields = findByThriftId(fieldId);
            if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
            return fields;
        }

        /**
         * Find the _Fields constant that matches name, or null if its not found.
         */
        public static _Fields findByName(String name) {
            return byName.get(name);
        }

        public short getThriftFieldId() {
            return _thriftId;
        }

        public String getFieldName() {
            return _fieldName;
        }
    }

    private static class PageEncodingStatsStandardSchemeFactory implements SchemeFactory {
        public PageEncodingStatsStandardScheme getScheme() {
            return new PageEncodingStatsStandardScheme();
        }
    }

    private static class PageEncodingStatsStandardScheme extends StandardScheme<PageEncodingStats> {

        public void read(org.apache.thrift.protocol.TProtocol iprot, PageEncodingStats struct) throws org.apache.thrift.TException {
            org.apache.thrift.protocol.TField schemeField;
            iprot.readStructBegin();
            while (true) {
                schemeField = iprot.readFieldBegin();
                if (schemeField.type == org.apache.thrift.protocol.TType.STOP) {
                    break;
                }
                switch (schemeField.id) {
                    case 1: // PAGE_TYPE
                        if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
                            struct.page_type = PageType.findByValue(iprot.readI32());
                            struct.setPage_typeIsSet(true);
                        } else {
                            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
                        }
                        break;
                    case 2: // ENCODING
                        if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
                            struct.encoding = Encoding.findByValue(iprot.readI32());
                            struct.setEncodingIsSet(true);
                        } else {
                            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
                        }
                        break;
                    case 3: // NUM_PAGES
                        if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
                            struct.num_pages = iprot.readI32();
                            struct.setNum_pagesIsSet(true);
                        } else {
                            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
                        }
                        break;
                    default:
                        org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
                }
                iprot.readFieldEnd();
            }
            iprot.readStructEnd();

            // check for required fields of primitive type, which can't be checked in the validate method
            if (!struct.isSetNum_pages()) {
                throw new org.apache.thrift.protocol.TProtocolException("Required field 'num_pages' was not found in serialized data! Struct: " + toString());
            }
            struct.validate();
        }

        public void write(org.apache.thrift.protocol.TProtocol oprot, PageEncodingStats struct) throws org.apache.thrift.TException {
            struct.validate();

            oprot.writeStructBegin(STRUCT_DESC);
            if (struct.page_type != null) {
                oprot.writeFieldBegin(PAGE_TYPE_FIELD_DESC);
                oprot.writeI32(struct.page_type.getValue());
                oprot.writeFieldEnd();
            }
            if (struct.encoding != null) {
                oprot.writeFieldBegin(ENCODING_FIELD_DESC);
                oprot.writeI32(struct.encoding.getValue());
                oprot.writeFieldEnd();
            }
            oprot.writeFieldBegin(NUM_PAGES_FIELD_DESC);
            oprot.writeI32(struct.num_pages);
            oprot.writeFieldEnd();
            oprot.writeFieldStop();
            oprot.writeStructEnd();
        }

    }

    private static class PageEncodingStatsTupleSchemeFactory implements SchemeFactory {
        public PageEncodingStatsTupleScheme getScheme() {
            return new PageEncodingStatsTupleScheme();
        }
    }

    private static class PageEncodingStatsTupleScheme extends TupleScheme<PageEncodingStats> {

        @Override
        public void write(org.apache.thrift.protocol.TProtocol prot, PageEncodingStats struct) throws org.apache.thrift.TException {
            TTupleProtocol oprot = (TTupleProtocol) prot;
            oprot.writeI32(struct.page_type.getValue());
            oprot.writeI32(struct.encoding.getValue());
            oprot.writeI32(struct.num_pages);
        }

        @Override
        public void read(org.apache.thrift.protocol.TProtocol prot, PageEncodingStats struct) throws org.apache.thrift.TException {
            TTupleProtocol iprot = (TTupleProtocol) prot;
            struct.page_type = PageType.findByValue(iprot.readI32());
            struct.setPage_typeIsSet(true);
            struct.encoding = Encoding.findByValue(iprot.readI32());
            struct.setEncodingIsSet(true);
            struct.num_pages = iprot.readI32();
            struct.setNum_pagesIsSet(true);
        }
    }

}

