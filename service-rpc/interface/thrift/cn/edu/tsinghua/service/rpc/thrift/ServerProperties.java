/**
 * Autogenerated by Thrift Compiler (0.9.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package cn.edu.tsinghua.service.rpc.thrift;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerProperties implements org.apache.thrift.TBase<ServerProperties, ServerProperties._Fields>, java.io.Serializable, Cloneable, Comparable<ServerProperties> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("ServerProperties");

  private static final org.apache.thrift.protocol.TField VERSION_FIELD_DESC = new org.apache.thrift.protocol.TField("version", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField AGGREGATION_TIME_CONSTANT_FIELD_DESC = new org.apache.thrift.protocol.TField("aggregationTimeConstant", org.apache.thrift.protocol.TType.LIST, (short)2);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new ServerPropertiesStandardSchemeFactory());
    schemes.put(TupleScheme.class, new ServerPropertiesTupleSchemeFactory());
  }

  public String version; // required
  public List<String> aggregationTimeConstant; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    VERSION((short)1, "version"),
    AGGREGATION_TIME_CONSTANT((short)2, "aggregationTimeConstant");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // VERSION
          return VERSION;
        case 2: // AGGREGATION_TIME_CONSTANT
          return AGGREGATION_TIME_CONSTANT;
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

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.VERSION, new org.apache.thrift.meta_data.FieldMetaData("version", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.AGGREGATION_TIME_CONSTANT, new org.apache.thrift.meta_data.FieldMetaData("aggregationTimeConstant", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(ServerProperties.class, metaDataMap);
  }

  public ServerProperties() {
  }

  public ServerProperties(
    String version,
    List<String> aggregationTimeConstant)
  {
    this();
    this.version = version;
    this.aggregationTimeConstant = aggregationTimeConstant;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ServerProperties(ServerProperties other) {
    if (other.isSetVersion()) {
      this.version = other.version;
    }
    if (other.isSetAggregationTimeConstant()) {
      List<String> __this__aggregationTimeConstant = new ArrayList<String>(other.aggregationTimeConstant);
      this.aggregationTimeConstant = __this__aggregationTimeConstant;
    }
  }

  public ServerProperties deepCopy() {
    return new ServerProperties(this);
  }

  @Override
  public void clear() {
    this.version = null;
    this.aggregationTimeConstant = null;
  }

  public String getVersion() {
    return this.version;
  }

  public ServerProperties setVersion(String version) {
    this.version = version;
    return this;
  }

  public void unsetVersion() {
    this.version = null;
  }

  /** Returns true if field version is set (has been assigned a value) and false otherwise */
  public boolean isSetVersion() {
    return this.version != null;
  }

  public void setVersionIsSet(boolean value) {
    if (!value) {
      this.version = null;
    }
  }

  public int getAggregationTimeConstantSize() {
    return (this.aggregationTimeConstant == null) ? 0 : this.aggregationTimeConstant.size();
  }

  public java.util.Iterator<String> getAggregationTimeConstantIterator() {
    return (this.aggregationTimeConstant == null) ? null : this.aggregationTimeConstant.iterator();
  }

  public void addToAggregationTimeConstant(String elem) {
    if (this.aggregationTimeConstant == null) {
      this.aggregationTimeConstant = new ArrayList<String>();
    }
    this.aggregationTimeConstant.add(elem);
  }

  public List<String> getAggregationTimeConstant() {
    return this.aggregationTimeConstant;
  }

  public ServerProperties setAggregationTimeConstant(List<String> aggregationTimeConstant) {
    this.aggregationTimeConstant = aggregationTimeConstant;
    return this;
  }

  public void unsetAggregationTimeConstant() {
    this.aggregationTimeConstant = null;
  }

  /** Returns true if field aggregationTimeConstant is set (has been assigned a value) and false otherwise */
  public boolean isSetAggregationTimeConstant() {
    return this.aggregationTimeConstant != null;
  }

  public void setAggregationTimeConstantIsSet(boolean value) {
    if (!value) {
      this.aggregationTimeConstant = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case VERSION:
      if (value == null) {
        unsetVersion();
      } else {
        setVersion((String)value);
      }
      break;

    case AGGREGATION_TIME_CONSTANT:
      if (value == null) {
        unsetAggregationTimeConstant();
      } else {
        setAggregationTimeConstant((List<String>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case VERSION:
      return getVersion();

    case AGGREGATION_TIME_CONSTANT:
      return getAggregationTimeConstant();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case VERSION:
      return isSetVersion();
    case AGGREGATION_TIME_CONSTANT:
      return isSetAggregationTimeConstant();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof ServerProperties)
      return this.equals((ServerProperties)that);
    return false;
  }

  public boolean equals(ServerProperties that) {
    if (that == null)
      return false;

    boolean this_present_version = true && this.isSetVersion();
    boolean that_present_version = true && that.isSetVersion();
    if (this_present_version || that_present_version) {
      if (!(this_present_version && that_present_version))
        return false;
      if (!this.version.equals(that.version))
        return false;
    }

    boolean this_present_aggregationTimeConstant = true && this.isSetAggregationTimeConstant();
    boolean that_present_aggregationTimeConstant = true && that.isSetAggregationTimeConstant();
    if (this_present_aggregationTimeConstant || that_present_aggregationTimeConstant) {
      if (!(this_present_aggregationTimeConstant && that_present_aggregationTimeConstant))
        return false;
      if (!this.aggregationTimeConstant.equals(that.aggregationTimeConstant))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(ServerProperties other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetVersion()).compareTo(other.isSetVersion());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetVersion()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.version, other.version);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetAggregationTimeConstant()).compareTo(other.isSetAggregationTimeConstant());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetAggregationTimeConstant()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.aggregationTimeConstant, other.aggregationTimeConstant);
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
    StringBuilder sb = new StringBuilder("ServerProperties(");
    boolean first = true;

    sb.append("version:");
    if (this.version == null) {
      sb.append("null");
    } else {
      sb.append(this.version);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("aggregationTimeConstant:");
    if (this.aggregationTimeConstant == null) {
      sb.append("null");
    } else {
      sb.append(this.aggregationTimeConstant);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (version == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'version' was not present! Struct: " + toString());
    }
    if (aggregationTimeConstant == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'aggregationTimeConstant' was not present! Struct: " + toString());
    }
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
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class ServerPropertiesStandardSchemeFactory implements SchemeFactory {
    public ServerPropertiesStandardScheme getScheme() {
      return new ServerPropertiesStandardScheme();
    }
  }

  private static class ServerPropertiesStandardScheme extends StandardScheme<ServerProperties> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, ServerProperties struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // VERSION
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.version = iprot.readString();
              struct.setVersionIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // AGGREGATION_TIME_CONSTANT
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list118 = iprot.readListBegin();
                struct.aggregationTimeConstant = new ArrayList<String>(_list118.size);
                for (int _i119 = 0; _i119 < _list118.size; ++_i119)
                {
                  String _elem120;
                  _elem120 = iprot.readString();
                  struct.aggregationTimeConstant.add(_elem120);
                }
                iprot.readListEnd();
              }
              struct.setAggregationTimeConstantIsSet(true);
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
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, ServerProperties struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.version != null) {
        oprot.writeFieldBegin(VERSION_FIELD_DESC);
        oprot.writeString(struct.version);
        oprot.writeFieldEnd();
      }
      if (struct.aggregationTimeConstant != null) {
        oprot.writeFieldBegin(AGGREGATION_TIME_CONSTANT_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, struct.aggregationTimeConstant.size()));
          for (String _iter121 : struct.aggregationTimeConstant)
          {
            oprot.writeString(_iter121);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ServerPropertiesTupleSchemeFactory implements SchemeFactory {
    public ServerPropertiesTupleScheme getScheme() {
      return new ServerPropertiesTupleScheme();
    }
  }

  private static class ServerPropertiesTupleScheme extends TupleScheme<ServerProperties> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, ServerProperties struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeString(struct.version);
      {
        oprot.writeI32(struct.aggregationTimeConstant.size());
        for (String _iter122 : struct.aggregationTimeConstant)
        {
          oprot.writeString(_iter122);
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, ServerProperties struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.version = iprot.readString();
      struct.setVersionIsSet(true);
      {
        org.apache.thrift.protocol.TList _list123 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, iprot.readI32());
        struct.aggregationTimeConstant = new ArrayList<String>(_list123.size);
        for (int _i124 = 0; _i124 < _list123.size; ++_i124)
        {
          String _elem125;
          _elem125 = iprot.readString();
          struct.aggregationTimeConstant.add(_elem125);
        }
      }
      struct.setAggregationTimeConstantIsSet(true);
    }
  }

}

