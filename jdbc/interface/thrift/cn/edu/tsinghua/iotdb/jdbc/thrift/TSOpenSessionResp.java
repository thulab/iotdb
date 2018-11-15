/**
 * Autogenerated by Thrift Compiler (0.9.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package cn.edu.tsinghua.iotdb.jdbc.thrift;

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

public class TSOpenSessionResp implements org.apache.thrift.TBase<TSOpenSessionResp, TSOpenSessionResp._Fields>, java.io.Serializable, Cloneable, Comparable<TSOpenSessionResp> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TSOpenSessionResp");

  private static final org.apache.thrift.protocol.TField STATUS_FIELD_DESC = new org.apache.thrift.protocol.TField("status", org.apache.thrift.protocol.TType.STRUCT, (short)1);
  private static final org.apache.thrift.protocol.TField SERVER_PROTOCOL_VERSION_FIELD_DESC = new org.apache.thrift.protocol.TField("serverProtocolVersion", org.apache.thrift.protocol.TType.I32, (short)2);
  private static final org.apache.thrift.protocol.TField SESSION_HANDLE_FIELD_DESC = new org.apache.thrift.protocol.TField("sessionHandle", org.apache.thrift.protocol.TType.STRUCT, (short)3);
  private static final org.apache.thrift.protocol.TField CONFIGURATION_FIELD_DESC = new org.apache.thrift.protocol.TField("configuration", org.apache.thrift.protocol.TType.MAP, (short)4);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TSOpenSessionRespStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TSOpenSessionRespTupleSchemeFactory());
  }

  public TS_Status status; // required
  /**
   * 
   * @see TSProtocolVersion
   */
  public TSProtocolVersion serverProtocolVersion; // required
  public TS_SessionHandle sessionHandle; // optional
  public Map<String,String> configuration; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    STATUS((short)1, "status"),
    /**
     * 
     * @see TSProtocolVersion
     */
    SERVER_PROTOCOL_VERSION((short)2, "serverProtocolVersion"),
    SESSION_HANDLE((short)3, "sessionHandle"),
    CONFIGURATION((short)4, "configuration");

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
        case 1: // STATUS
          return STATUS;
        case 2: // SERVER_PROTOCOL_VERSION
          return SERVER_PROTOCOL_VERSION;
        case 3: // SESSION_HANDLE
          return SESSION_HANDLE;
        case 4: // CONFIGURATION
          return CONFIGURATION;
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
  private _Fields optionals[] = {_Fields.SESSION_HANDLE,_Fields.CONFIGURATION};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.STATUS, new org.apache.thrift.meta_data.FieldMetaData("status", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TS_Status.class)));
    tmpMap.put(_Fields.SERVER_PROTOCOL_VERSION, new org.apache.thrift.meta_data.FieldMetaData("serverProtocolVersion", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, TSProtocolVersion.class)));
    tmpMap.put(_Fields.SESSION_HANDLE, new org.apache.thrift.meta_data.FieldMetaData("sessionHandle", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TS_SessionHandle.class)));
    tmpMap.put(_Fields.CONFIGURATION, new org.apache.thrift.meta_data.FieldMetaData("configuration", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TSOpenSessionResp.class, metaDataMap);
  }

  public TSOpenSessionResp() {
    this.serverProtocolVersion = cn.edu.tsinghua.iotdb.jdbc.thrift.TSProtocolVersion.TSFILE_SERVICE_PROTOCOL_V1;

  }

  public TSOpenSessionResp(
    TS_Status status,
    TSProtocolVersion serverProtocolVersion)
  {
    this();
    this.status = status;
    this.serverProtocolVersion = serverProtocolVersion;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TSOpenSessionResp(TSOpenSessionResp other) {
    if (other.isSetStatus()) {
      this.status = new TS_Status(other.status);
    }
    if (other.isSetServerProtocolVersion()) {
      this.serverProtocolVersion = other.serverProtocolVersion;
    }
    if (other.isSetSessionHandle()) {
      this.sessionHandle = new TS_SessionHandle(other.sessionHandle);
    }
    if (other.isSetConfiguration()) {
      Map<String,String> __this__configuration = new HashMap<String,String>(other.configuration);
      this.configuration = __this__configuration;
    }
  }

  public TSOpenSessionResp deepCopy() {
    return new TSOpenSessionResp(this);
  }

  @Override
  public void clear() {
    this.status = null;
    this.serverProtocolVersion = cn.edu.tsinghua.iotdb.jdbc.thrift.TSProtocolVersion.TSFILE_SERVICE_PROTOCOL_V1;

    this.sessionHandle = null;
    this.configuration = null;
  }

  public TS_Status getStatus() {
    return this.status;
  }

  public TSOpenSessionResp setStatus(TS_Status status) {
    this.status = status;
    return this;
  }

  public void unsetStatus() {
    this.status = null;
  }

  /** Returns true if field status is set (has been assigned a value) and false otherwise */
  public boolean isSetStatus() {
    return this.status != null;
  }

  public void setStatusIsSet(boolean value) {
    if (!value) {
      this.status = null;
    }
  }

  /**
   * 
   * @see TSProtocolVersion
   */
  public TSProtocolVersion getServerProtocolVersion() {
    return this.serverProtocolVersion;
  }

  /**
   * 
   * @see TSProtocolVersion
   */
  public TSOpenSessionResp setServerProtocolVersion(TSProtocolVersion serverProtocolVersion) {
    this.serverProtocolVersion = serverProtocolVersion;
    return this;
  }

  public void unsetServerProtocolVersion() {
    this.serverProtocolVersion = null;
  }

  /** Returns true if field serverProtocolVersion is set (has been assigned a value) and false otherwise */
  public boolean isSetServerProtocolVersion() {
    return this.serverProtocolVersion != null;
  }

  public void setServerProtocolVersionIsSet(boolean value) {
    if (!value) {
      this.serverProtocolVersion = null;
    }
  }

  public TS_SessionHandle getSessionHandle() {
    return this.sessionHandle;
  }

  public TSOpenSessionResp setSessionHandle(TS_SessionHandle sessionHandle) {
    this.sessionHandle = sessionHandle;
    return this;
  }

  public void unsetSessionHandle() {
    this.sessionHandle = null;
  }

  /** Returns true if field sessionHandle is set (has been assigned a value) and false otherwise */
  public boolean isSetSessionHandle() {
    return this.sessionHandle != null;
  }

  public void setSessionHandleIsSet(boolean value) {
    if (!value) {
      this.sessionHandle = null;
    }
  }

  public int getConfigurationSize() {
    return (this.configuration == null) ? 0 : this.configuration.size();
  }

  public void putToConfiguration(String key, String val) {
    if (this.configuration == null) {
      this.configuration = new HashMap<String,String>();
    }
    this.configuration.put(key, val);
  }

  public Map<String,String> getConfiguration() {
    return this.configuration;
  }

  public TSOpenSessionResp setConfiguration(Map<String,String> configuration) {
    this.configuration = configuration;
    return this;
  }

  public void unsetConfiguration() {
    this.configuration = null;
  }

  /** Returns true if field configuration is set (has been assigned a value) and false otherwise */
  public boolean isSetConfiguration() {
    return this.configuration != null;
  }

  public void setConfigurationIsSet(boolean value) {
    if (!value) {
      this.configuration = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case STATUS:
      if (value == null) {
        unsetStatus();
      } else {
        setStatus((TS_Status)value);
      }
      break;

    case SERVER_PROTOCOL_VERSION:
      if (value == null) {
        unsetServerProtocolVersion();
      } else {
        setServerProtocolVersion((TSProtocolVersion)value);
      }
      break;

    case SESSION_HANDLE:
      if (value == null) {
        unsetSessionHandle();
      } else {
        setSessionHandle((TS_SessionHandle)value);
      }
      break;

    case CONFIGURATION:
      if (value == null) {
        unsetConfiguration();
      } else {
        setConfiguration((Map<String,String>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case STATUS:
      return getStatus();

    case SERVER_PROTOCOL_VERSION:
      return getServerProtocolVersion();

    case SESSION_HANDLE:
      return getSessionHandle();

    case CONFIGURATION:
      return getConfiguration();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case STATUS:
      return isSetStatus();
    case SERVER_PROTOCOL_VERSION:
      return isSetServerProtocolVersion();
    case SESSION_HANDLE:
      return isSetSessionHandle();
    case CONFIGURATION:
      return isSetConfiguration();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TSOpenSessionResp)
      return this.equals((TSOpenSessionResp)that);
    return false;
  }

  public boolean equals(TSOpenSessionResp that) {
    if (that == null)
      return false;

    boolean this_present_status = true && this.isSetStatus();
    boolean that_present_status = true && that.isSetStatus();
    if (this_present_status || that_present_status) {
      if (!(this_present_status && that_present_status))
        return false;
      if (!this.status.equals(that.status))
        return false;
    }

    boolean this_present_serverProtocolVersion = true && this.isSetServerProtocolVersion();
    boolean that_present_serverProtocolVersion = true && that.isSetServerProtocolVersion();
    if (this_present_serverProtocolVersion || that_present_serverProtocolVersion) {
      if (!(this_present_serverProtocolVersion && that_present_serverProtocolVersion))
        return false;
      if (!this.serverProtocolVersion.equals(that.serverProtocolVersion))
        return false;
    }

    boolean this_present_sessionHandle = true && this.isSetSessionHandle();
    boolean that_present_sessionHandle = true && that.isSetSessionHandle();
    if (this_present_sessionHandle || that_present_sessionHandle) {
      if (!(this_present_sessionHandle && that_present_sessionHandle))
        return false;
      if (!this.sessionHandle.equals(that.sessionHandle))
        return false;
    }

    boolean this_present_configuration = true && this.isSetConfiguration();
    boolean that_present_configuration = true && that.isSetConfiguration();
    if (this_present_configuration || that_present_configuration) {
      if (!(this_present_configuration && that_present_configuration))
        return false;
      if (!this.configuration.equals(that.configuration))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(TSOpenSessionResp other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetStatus()).compareTo(other.isSetStatus());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStatus()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.status, other.status);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetServerProtocolVersion()).compareTo(other.isSetServerProtocolVersion());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetServerProtocolVersion()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.serverProtocolVersion, other.serverProtocolVersion);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetSessionHandle()).compareTo(other.isSetSessionHandle());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSessionHandle()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.sessionHandle, other.sessionHandle);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetConfiguration()).compareTo(other.isSetConfiguration());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetConfiguration()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.configuration, other.configuration);
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
    StringBuilder sb = new StringBuilder("TSOpenSessionResp(");
    boolean first = true;

    sb.append("status:");
    if (this.status == null) {
      sb.append("null");
    } else {
      sb.append(this.status);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("serverProtocolVersion:");
    if (this.serverProtocolVersion == null) {
      sb.append("null");
    } else {
      sb.append(this.serverProtocolVersion);
    }
    first = false;
    if (isSetSessionHandle()) {
      if (!first) sb.append(", ");
      sb.append("sessionHandle:");
      if (this.sessionHandle == null) {
        sb.append("null");
      } else {
        sb.append(this.sessionHandle);
      }
      first = false;
    }
    if (isSetConfiguration()) {
      if (!first) sb.append(", ");
      sb.append("configuration:");
      if (this.configuration == null) {
        sb.append("null");
      } else {
        sb.append(this.configuration);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (status == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'status' was not present! Struct: " + toString());
    }
    if (serverProtocolVersion == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'serverProtocolVersion' was not present! Struct: " + toString());
    }
    // check for sub-struct validity
    if (status != null) {
      status.validate();
    }
    if (sessionHandle != null) {
      sessionHandle.validate();
    }
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

  private static class TSOpenSessionRespStandardSchemeFactory implements SchemeFactory {
    public TSOpenSessionRespStandardScheme getScheme() {
      return new TSOpenSessionRespStandardScheme();
    }
  }

  private static class TSOpenSessionRespStandardScheme extends StandardScheme<TSOpenSessionResp> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TSOpenSessionResp struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // STATUS
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.status = new TS_Status();
              struct.status.read(iprot);
              struct.setStatusIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // SERVER_PROTOCOL_VERSION
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.serverProtocolVersion = TSProtocolVersion.findByValue(iprot.readI32());
              struct.setServerProtocolVersionIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // SESSION_HANDLE
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.sessionHandle = new TS_SessionHandle();
              struct.sessionHandle.read(iprot);
              struct.setSessionHandleIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // CONFIGURATION
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map16 = iprot.readMapBegin();
                struct.configuration = new HashMap<String,String>(2*_map16.size);
                for (int _i17 = 0; _i17 < _map16.size; ++_i17)
                {
                  String _key18;
                  String _val19;
                  _key18 = iprot.readString();
                  _val19 = iprot.readString();
                  struct.configuration.put(_key18, _val19);
                }
                iprot.readMapEnd();
              }
              struct.setConfigurationIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, TSOpenSessionResp struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.status != null) {
        oprot.writeFieldBegin(STATUS_FIELD_DESC);
        struct.status.write(oprot);
        oprot.writeFieldEnd();
      }
      if (struct.serverProtocolVersion != null) {
        oprot.writeFieldBegin(SERVER_PROTOCOL_VERSION_FIELD_DESC);
        oprot.writeI32(struct.serverProtocolVersion.getValue());
        oprot.writeFieldEnd();
      }
      if (struct.sessionHandle != null) {
        if (struct.isSetSessionHandle()) {
          oprot.writeFieldBegin(SESSION_HANDLE_FIELD_DESC);
          struct.sessionHandle.write(oprot);
          oprot.writeFieldEnd();
        }
      }
      if (struct.configuration != null) {
        if (struct.isSetConfiguration()) {
          oprot.writeFieldBegin(CONFIGURATION_FIELD_DESC);
          {
            oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, struct.configuration.size()));
            for (Map.Entry<String, String> _iter20 : struct.configuration.entrySet())
            {
              oprot.writeString(_iter20.getKey());
              oprot.writeString(_iter20.getValue());
            }
            oprot.writeMapEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TSOpenSessionRespTupleSchemeFactory implements SchemeFactory {
    public TSOpenSessionRespTupleScheme getScheme() {
      return new TSOpenSessionRespTupleScheme();
    }
  }

  private static class TSOpenSessionRespTupleScheme extends TupleScheme<TSOpenSessionResp> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TSOpenSessionResp struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      struct.status.write(oprot);
      oprot.writeI32(struct.serverProtocolVersion.getValue());
      BitSet optionals = new BitSet();
      if (struct.isSetSessionHandle()) {
        optionals.set(0);
      }
      if (struct.isSetConfiguration()) {
        optionals.set(1);
      }
      oprot.writeBitSet(optionals, 2);
      if (struct.isSetSessionHandle()) {
        struct.sessionHandle.write(oprot);
      }
      if (struct.isSetConfiguration()) {
        {
          oprot.writeI32(struct.configuration.size());
          for (Map.Entry<String, String> _iter21 : struct.configuration.entrySet())
          {
            oprot.writeString(_iter21.getKey());
            oprot.writeString(_iter21.getValue());
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TSOpenSessionResp struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.status = new TS_Status();
      struct.status.read(iprot);
      struct.setStatusIsSet(true);
      struct.serverProtocolVersion = TSProtocolVersion.findByValue(iprot.readI32());
      struct.setServerProtocolVersionIsSet(true);
      BitSet incoming = iprot.readBitSet(2);
      if (incoming.get(0)) {
        struct.sessionHandle = new TS_SessionHandle();
        struct.sessionHandle.read(iprot);
        struct.setSessionHandleIsSet(true);
      }
      if (incoming.get(1)) {
        {
          org.apache.thrift.protocol.TMap _map22 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.configuration = new HashMap<String,String>(2*_map22.size);
          for (int _i23 = 0; _i23 < _map22.size; ++_i23)
          {
            String _key24;
            String _val25;
            _key24 = iprot.readString();
            _val25 = iprot.readString();
            struct.configuration.put(_key24, _val25);
          }
        }
        struct.setConfigurationIsSet(true);
      }
    }
  }

}

