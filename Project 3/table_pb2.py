# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# NO CHECKED-IN PROTOBUF GENCODE
# source: table.proto
# Protobuf Python Version: 5.27.2
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(
    _runtime_version.Domain.PUBLIC,
    5,
    27,
    2,
    '',
    'table.proto'
)
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0btable.proto\x12\x05table\"\x1d\n\tUploadReq\x12\x10\n\x08\x63sv_data\x18\x01 \x01(\x0c\"0\n\x0eUploadResponse\x12\r\n\x05\x65rror\x18\x01 \x01(\t\x12\x0f\n\x07success\x18\x02 \x01(\x08\"+\n\tColSumReq\x12\x0e\n\x06\x63olumn\x18\x01 \x01(\t\x12\x0e\n\x06\x66ormat\x18\x02 \x01(\t\".\n\x0e\x43olSumResponse\x12\r\n\x05total\x18\x01 \x01(\x03\x12\r\n\x05\x65rror\x18\x02 \x01(\t2q\n\x05Table\x12\x33\n\x06Upload\x12\x10.table.UploadReq\x1a\x15.table.UploadResponse\"\x00\x12\x33\n\x06\x43olSum\x12\x10.table.ColSumReq\x1a\x15.table.ColSumResponse\"\x00\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'table_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  DESCRIPTOR._loaded_options = None
  _globals['_UPLOADREQ']._serialized_start=22
  _globals['_UPLOADREQ']._serialized_end=51
  _globals['_UPLOADRESPONSE']._serialized_start=53
  _globals['_UPLOADRESPONSE']._serialized_end=101
  _globals['_COLSUMREQ']._serialized_start=103
  _globals['_COLSUMREQ']._serialized_end=146
  _globals['_COLSUMRESPONSE']._serialized_start=148
  _globals['_COLSUMRESPONSE']._serialized_end=194
  _globals['_TABLE']._serialized_start=196
  _globals['_TABLE']._serialized_end=309
# @@protoc_insertion_point(module_scope)
