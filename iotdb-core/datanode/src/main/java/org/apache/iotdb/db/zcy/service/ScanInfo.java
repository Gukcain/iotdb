/**
 * Autogenerated by Thrift Compiler (0.13.0)
 *
 * <p>DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *
 * @generated
 */
package org.apache.iotdb.db.zcy.service;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(
    value = "Autogenerated by Thrift Compiler (0.13.0)",
    date = "2024-04-23")
public class ScanInfo
    implements org.apache.thrift.TBase<ScanInfo, ScanInfo._Fields>,
        java.io.Serializable,
        Cloneable,
        Comparable<ScanInfo> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC =
      new org.apache.thrift.protocol.TStruct("ScanInfo");

  private static final org.apache.thrift.protocol.TField SCAN_OPERATOR_COUNT_FIELD_DESC =
      new org.apache.thrift.protocol.TField(
          "scanOperatorCount", org.apache.thrift.protocol.TType.I32, (short) 1);
  private static final org.apache.thrift.protocol.TField SOURCE_IDS_FIELD_DESC =
      new org.apache.thrift.protocol.TField(
          "sourceIds", org.apache.thrift.protocol.TType.LIST, (short) 2);
  private static final org.apache.thrift.protocol.TField SCAN_OFFSETS_FIELD_DESC =
      new org.apache.thrift.protocol.TField(
          "scanOffsets", org.apache.thrift.protocol.TType.LIST, (short) 3);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY =
      new ScanInfoStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY =
      new ScanInfoTupleSchemeFactory();

  public int scanOperatorCount; // required
  public @org.apache.thrift.annotation.Nullable java.util.List<java.lang.Integer>
      sourceIds; // required
  public @org.apache.thrift.annotation.Nullable java.util.List<java.lang.Long>
      scanOffsets; // required

  /**
   * The set of fields this struct contains, along with convenience methods for finding and
   * manipulating them.
   */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    SCAN_OPERATOR_COUNT((short) 1, "scanOperatorCount"),
    SOURCE_IDS((short) 2, "sourceIds"),
    SCAN_OFFSETS((short) 3, "scanOffsets");

    private static final java.util.Map<java.lang.String, _Fields> byName =
        new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /** Find the _Fields constant that matches fieldId, or null if its not found. */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByThriftId(int fieldId) {
      switch (fieldId) {
        case 1: // SCAN_OPERATOR_COUNT
          return SCAN_OPERATOR_COUNT;
        case 2: // SOURCE_IDS
          return SOURCE_IDS;
        case 3: // SCAN_OFFSETS
          return SCAN_OFFSETS;
        default:
          return null;
      }
    }

    /** Find the _Fields constant that matches fieldId, throwing an exception if it is not found. */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null)
        throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /** Find the _Fields constant that matches name, or null if its not found. */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __SCANOPERATORCOUNT_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;

  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap =
        new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(
        _Fields.SCAN_OPERATOR_COUNT,
        new org.apache.thrift.meta_data.FieldMetaData(
            "scanOperatorCount",
            org.apache.thrift.TFieldRequirementType.REQUIRED,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(
        _Fields.SOURCE_IDS,
        new org.apache.thrift.meta_data.FieldMetaData(
            "sourceIds",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.ListMetaData(
                org.apache.thrift.protocol.TType.LIST,
                new org.apache.thrift.meta_data.FieldValueMetaData(
                    org.apache.thrift.protocol.TType.I32))));
    tmpMap.put(
        _Fields.SCAN_OFFSETS,
        new org.apache.thrift.meta_data.FieldMetaData(
            "scanOffsets",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.ListMetaData(
                org.apache.thrift.protocol.TType.LIST,
                new org.apache.thrift.meta_data.FieldValueMetaData(
                    org.apache.thrift.protocol.TType.I64))));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(ScanInfo.class, metaDataMap);
  }

  public ScanInfo() {}

  public ScanInfo(
      int scanOperatorCount,
      java.util.List<java.lang.Integer> sourceIds,
      java.util.List<java.lang.Long> scanOffsets) {
    this();
    this.scanOperatorCount = scanOperatorCount;
    setScanOperatorCountIsSet(true);
    this.sourceIds = sourceIds;
    this.scanOffsets = scanOffsets;
  }

  /** Performs a deep copy on <i>other</i>. */
  public ScanInfo(ScanInfo other) {
    __isset_bitfield = other.__isset_bitfield;
    this.scanOperatorCount = other.scanOperatorCount;
    if (other.isSetSourceIds()) {
      java.util.List<java.lang.Integer> __this__sourceIds =
          new java.util.ArrayList<java.lang.Integer>(other.sourceIds);
      this.sourceIds = __this__sourceIds;
    }
    if (other.isSetScanOffsets()) {
      java.util.List<java.lang.Long> __this__scanOffsets =
          new java.util.ArrayList<java.lang.Long>(other.scanOffsets);
      this.scanOffsets = __this__scanOffsets;
    }
  }

  public ScanInfo deepCopy() {
    return new ScanInfo(this);
  }

  @Override
  public void clear() {
    setScanOperatorCountIsSet(false);
    this.scanOperatorCount = 0;
    this.sourceIds = null;
    this.scanOffsets = null;
  }

  public int getScanOperatorCount() {
    return this.scanOperatorCount;
  }

  public ScanInfo setScanOperatorCount(int scanOperatorCount) {
    this.scanOperatorCount = scanOperatorCount;
    setScanOperatorCountIsSet(true);
    return this;
  }

  public void unsetScanOperatorCount() {
    __isset_bitfield =
        org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __SCANOPERATORCOUNT_ISSET_ID);
  }

  /**
   * Returns true if field scanOperatorCount is set (has been assigned a value) and false otherwise
   */
  public boolean isSetScanOperatorCount() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __SCANOPERATORCOUNT_ISSET_ID);
  }

  public void setScanOperatorCountIsSet(boolean value) {
    __isset_bitfield =
        org.apache.thrift.EncodingUtils.setBit(
            __isset_bitfield, __SCANOPERATORCOUNT_ISSET_ID, value);
  }

  public int getSourceIdsSize() {
    return (this.sourceIds == null) ? 0 : this.sourceIds.size();
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Iterator<java.lang.Integer> getSourceIdsIterator() {
    return (this.sourceIds == null) ? null : this.sourceIds.iterator();
  }

  public void addToSourceIds(int elem) {
    if (this.sourceIds == null) {
      this.sourceIds = new java.util.ArrayList<java.lang.Integer>();
    }
    this.sourceIds.add(elem);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.List<java.lang.Integer> getSourceIds() {
    return this.sourceIds;
  }

  public ScanInfo setSourceIds(
      @org.apache.thrift.annotation.Nullable java.util.List<java.lang.Integer> sourceIds) {
    this.sourceIds = sourceIds;
    return this;
  }

  public void unsetSourceIds() {
    this.sourceIds = null;
  }

  /** Returns true if field sourceIds is set (has been assigned a value) and false otherwise */
  public boolean isSetSourceIds() {
    return this.sourceIds != null;
  }

  public void setSourceIdsIsSet(boolean value) {
    if (!value) {
      this.sourceIds = null;
    }
  }

  public int getScanOffsetsSize() {
    return (this.scanOffsets == null) ? 0 : this.scanOffsets.size();
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Iterator<java.lang.Long> getScanOffsetsIterator() {
    return (this.scanOffsets == null) ? null : this.scanOffsets.iterator();
  }

  public void addToScanOffsets(long elem) {
    if (this.scanOffsets == null) {
      this.scanOffsets = new java.util.ArrayList<java.lang.Long>();
    }
    this.scanOffsets.add(elem);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.List<java.lang.Long> getScanOffsets() {
    return this.scanOffsets;
  }

  public ScanInfo setScanOffsets(
      @org.apache.thrift.annotation.Nullable java.util.List<java.lang.Long> scanOffsets) {
    this.scanOffsets = scanOffsets;
    return this;
  }

  public void unsetScanOffsets() {
    this.scanOffsets = null;
  }

  /** Returns true if field scanOffsets is set (has been assigned a value) and false otherwise */
  public boolean isSetScanOffsets() {
    return this.scanOffsets != null;
  }

  public void setScanOffsetsIsSet(boolean value) {
    if (!value) {
      this.scanOffsets = null;
    }
  }

  public void setFieldValue(
      _Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
      case SCAN_OPERATOR_COUNT:
        if (value == null) {
          unsetScanOperatorCount();
        } else {
          setScanOperatorCount((java.lang.Integer) value);
        }
        break;

      case SOURCE_IDS:
        if (value == null) {
          unsetSourceIds();
        } else {
          setSourceIds((java.util.List<java.lang.Integer>) value);
        }
        break;

      case SCAN_OFFSETS:
        if (value == null) {
          unsetScanOffsets();
        } else {
          setScanOffsets((java.util.List<java.lang.Long>) value);
        }
        break;
    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
      case SCAN_OPERATOR_COUNT:
        return getScanOperatorCount();

      case SOURCE_IDS:
        return getSourceIds();

      case SCAN_OFFSETS:
        return getScanOffsets();
    }
    throw new java.lang.IllegalStateException();
  }

  /**
   * Returns true if field corresponding to fieldID is set (has been assigned a value) and false
   * otherwise
   */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
      case SCAN_OPERATOR_COUNT:
        return isSetScanOperatorCount();
      case SOURCE_IDS:
        return isSetSourceIds();
      case SCAN_OFFSETS:
        return isSetScanOffsets();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null) return false;
    if (that instanceof ScanInfo) return this.equals((ScanInfo) that);
    return false;
  }

  public boolean equals(ScanInfo that) {
    if (that == null) return false;
    if (this == that) return true;

    boolean this_present_scanOperatorCount = true;
    boolean that_present_scanOperatorCount = true;
    if (this_present_scanOperatorCount || that_present_scanOperatorCount) {
      if (!(this_present_scanOperatorCount && that_present_scanOperatorCount)) return false;
      if (this.scanOperatorCount != that.scanOperatorCount) return false;
    }

    boolean this_present_sourceIds = true && this.isSetSourceIds();
    boolean that_present_sourceIds = true && that.isSetSourceIds();
    if (this_present_sourceIds || that_present_sourceIds) {
      if (!(this_present_sourceIds && that_present_sourceIds)) return false;
      if (!this.sourceIds.equals(that.sourceIds)) return false;
    }

    boolean this_present_scanOffsets = true && this.isSetScanOffsets();
    boolean that_present_scanOffsets = true && that.isSetScanOffsets();
    if (this_present_scanOffsets || that_present_scanOffsets) {
      if (!(this_present_scanOffsets && that_present_scanOffsets)) return false;
      if (!this.scanOffsets.equals(that.scanOffsets)) return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + scanOperatorCount;

    hashCode = hashCode * 8191 + ((isSetSourceIds()) ? 131071 : 524287);
    if (isSetSourceIds()) hashCode = hashCode * 8191 + sourceIds.hashCode();

    hashCode = hashCode * 8191 + ((isSetScanOffsets()) ? 131071 : 524287);
    if (isSetScanOffsets()) hashCode = hashCode * 8191 + scanOffsets.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(ScanInfo other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison =
        java.lang.Boolean.valueOf(isSetScanOperatorCount())
            .compareTo(other.isSetScanOperatorCount());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetScanOperatorCount()) {
      lastComparison =
          org.apache.thrift.TBaseHelper.compareTo(this.scanOperatorCount, other.scanOperatorCount);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetSourceIds()).compareTo(other.isSetSourceIds());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSourceIds()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.sourceIds, other.sourceIds);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison =
        java.lang.Boolean.valueOf(isSetScanOffsets()).compareTo(other.isSetScanOffsets());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetScanOffsets()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.scanOffsets, other.scanOffsets);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @org.apache.thrift.annotation.Nullable
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot)
      throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("ScanInfo(");
    boolean first = true;

    sb.append("scanOperatorCount:");
    sb.append(this.scanOperatorCount);
    first = false;
    if (!first) sb.append(", ");
    sb.append("sourceIds:");
    if (this.sourceIds == null) {
      sb.append("null");
    } else {
      sb.append(this.sourceIds);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("scanOffsets:");
    if (this.scanOffsets == null) {
      sb.append("null");
    } else {
      sb.append(this.scanOffsets);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // alas, we cannot check 'scanOperatorCount' because it's a primitive and you chose the
    // non-beans generator.
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(
          new org.apache.thrift.protocol.TCompactProtocol(
              new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in)
      throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and
      // doesn't call the default constructor.
      __isset_bitfield = 0;
      read(
          new org.apache.thrift.protocol.TCompactProtocol(
              new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class ScanInfoStandardSchemeFactory
      implements org.apache.thrift.scheme.SchemeFactory {
    public ScanInfoStandardScheme getScheme() {
      return new ScanInfoStandardScheme();
    }
  }

  private static class ScanInfoStandardScheme
      extends org.apache.thrift.scheme.StandardScheme<ScanInfo> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, ScanInfo struct)
        throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true) {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) {
          break;
        }
        switch (schemeField.id) {
          case 1: // SCAN_OPERATOR_COUNT
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.scanOperatorCount = iprot.readI32();
              struct.setScanOperatorCountIsSet(true);
            } else {
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // SOURCE_IDS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list0 = iprot.readListBegin();
                struct.sourceIds = new java.util.ArrayList<java.lang.Integer>(_list0.size);
                int _elem1;
                for (int _i2 = 0; _i2 < _list0.size; ++_i2) {
                  _elem1 = iprot.readI32();
                  struct.sourceIds.add(_elem1);
                }
                iprot.readListEnd();
              }
              struct.setSourceIdsIsSet(true);
            } else {
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // SCAN_OFFSETS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list3 = iprot.readListBegin();
                struct.scanOffsets = new java.util.ArrayList<java.lang.Long>(_list3.size);
                long _elem4;
                for (int _i5 = 0; _i5 < _list3.size; ++_i5) {
                  _elem4 = iprot.readI64();
                  struct.scanOffsets.add(_elem4);
                }
                iprot.readListEnd();
              }
              struct.setScanOffsetsIsSet(true);
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
      if (!struct.isSetScanOperatorCount()) {
        throw new org.apache.thrift.protocol.TProtocolException(
            "Required field 'scanOperatorCount' was not found in serialized data! Struct: "
                + toString());
      }
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, ScanInfo struct)
        throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(SCAN_OPERATOR_COUNT_FIELD_DESC);
      oprot.writeI32(struct.scanOperatorCount);
      oprot.writeFieldEnd();
      if (struct.sourceIds != null) {
        oprot.writeFieldBegin(SOURCE_IDS_FIELD_DESC);
        {
          oprot.writeListBegin(
              new org.apache.thrift.protocol.TList(
                  org.apache.thrift.protocol.TType.I32, struct.sourceIds.size()));
          for (int _iter6 : struct.sourceIds) {
            oprot.writeI32(_iter6);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.scanOffsets != null) {
        oprot.writeFieldBegin(SCAN_OFFSETS_FIELD_DESC);
        {
          oprot.writeListBegin(
              new org.apache.thrift.protocol.TList(
                  org.apache.thrift.protocol.TType.I64, struct.scanOffsets.size()));
          for (long _iter7 : struct.scanOffsets) {
            oprot.writeI64(_iter7);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }
  }

  private static class ScanInfoTupleSchemeFactory
      implements org.apache.thrift.scheme.SchemeFactory {
    public ScanInfoTupleScheme getScheme() {
      return new ScanInfoTupleScheme();
    }
  }

  private static class ScanInfoTupleScheme extends org.apache.thrift.scheme.TupleScheme<ScanInfo> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, ScanInfo struct)
        throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot =
          (org.apache.thrift.protocol.TTupleProtocol) prot;
      oprot.writeI32(struct.scanOperatorCount);
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetSourceIds()) {
        optionals.set(0);
      }
      if (struct.isSetScanOffsets()) {
        optionals.set(1);
      }
      oprot.writeBitSet(optionals, 2);
      if (struct.isSetSourceIds()) {
        {
          oprot.writeI32(struct.sourceIds.size());
          for (int _iter8 : struct.sourceIds) {
            oprot.writeI32(_iter8);
          }
        }
      }
      if (struct.isSetScanOffsets()) {
        {
          oprot.writeI32(struct.scanOffsets.size());
          for (long _iter9 : struct.scanOffsets) {
            oprot.writeI64(_iter9);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, ScanInfo struct)
        throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot =
          (org.apache.thrift.protocol.TTupleProtocol) prot;
      struct.scanOperatorCount = iprot.readI32();
      struct.setScanOperatorCountIsSet(true);
      java.util.BitSet incoming = iprot.readBitSet(2);
      if (incoming.get(0)) {
        {
          org.apache.thrift.protocol.TList _list10 =
              new org.apache.thrift.protocol.TList(
                  org.apache.thrift.protocol.TType.I32, iprot.readI32());
          struct.sourceIds = new java.util.ArrayList<java.lang.Integer>(_list10.size);
          int _elem11;
          for (int _i12 = 0; _i12 < _list10.size; ++_i12) {
            _elem11 = iprot.readI32();
            struct.sourceIds.add(_elem11);
          }
        }
        struct.setSourceIdsIsSet(true);
      }
      if (incoming.get(1)) {
        {
          org.apache.thrift.protocol.TList _list13 =
              new org.apache.thrift.protocol.TList(
                  org.apache.thrift.protocol.TType.I64, iprot.readI32());
          struct.scanOffsets = new java.util.ArrayList<java.lang.Long>(_list13.size);
          long _elem14;
          for (int _i15 = 0; _i15 < _list13.size; ++_i15) {
            _elem14 = iprot.readI64();
            struct.scanOffsets.add(_elem14);
          }
        }
        struct.setScanOffsetsIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(
      org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme())
            ? STANDARD_SCHEME_FACTORY
            : TUPLE_SCHEME_FACTORY)
        .getScheme();
  }
}