unit DBReaderMidas;

(*
Midas (TClientDataSet, CDS) database file reader

Author: Sergey Bodrov, 2024
License: MIT

https://alexeevd.narod.ru/papers/cds_format.htm

*)

interface

uses
  Windows, SysUtils, Classes, Variants, DB, DBReaderBase;

type
  TDSProp = record
    PropName: string;
    PropSize: Word;
    PropType: Word;
    PropData: array of Byte;
  end;

  TDSFieldDef = record
    FieldName: string;
    FieldSize: Word;
    FieldType: Word;
    FieldAttrs: Word;
    FieldPropCount: Word;
    FieldProps: array of TDSProp;
  end;

  TDBReaderMidas = class(TDBReader)
  private
    FDbVersion: Integer;
    FColCount: Integer;
    FFieldsDef: array of TDSFieldDef;
    FFirstRecPosition: Int64;

    // ASize - bytes of data size number (1-byte, 2-word, 4-DWord)
    function ReadVarSizeData(ASize: Byte): AnsiString;
    function ReadFixSizeData(ASize: Integer): AnsiString;

    function ReadInt(): Integer;
    function ReadUInt(): Cardinal;
    function ReadBool(): Boolean;
    function ReadFloat(): Double;


  public
    procedure AfterConstruction(); override;
    procedure BeforeDestruction(); override;

    function OpenFile(AFileName: string): Boolean; override;
    // Read table data from DB to AList
    // AName - table name
    // ACount - how many items read
    procedure ReadTable(AName: string; ACount: Int64 = MaxInt; AList: TDbRowsList = nil); override;
  end;

implementation

type
  TDSPacketHeader = packed record
    Signature: Cardinal;       // Signature = [$96, $19, $E0, $BD]
    Version: Integer;          // 1
    Int32_18: Integer;         // 18
    FieldCount: Word;          // Field count
    RecordCount: Cardinal;     // Record count
    Int32_3: Integer;          // 3
    MetaSize: Word;            // probably, metadata size
  end;

  {TDSRecord = record
    Status: Byte;
    StatusBits: array of Byte;
  end; }

const
  FIELD_TYPE_UNKNOWN = 0;  // Unknown
  FIELD_TYPE_INT     = 1;  // Signed integer
  FIELD_TYPE_UINT    = 2;  // Unsigned integer
  FIELD_TYPE_BOOL    = 3;  // Boolean
  FIELD_TYPE_FLOAT   = 4;  // IEEE float
  FIELD_TYPE_BCD     = 5;  // BCD
  FIELD_TYPE_DATE    = 6;  // Date (32 bit)
  FIELD_TYPE_TIME    = 7;  // Time (32 bit)
  FIELD_TYPE_TIMESTAMP = 8; // Time-stamp (64 bit)
  FIELD_TYPE_ZSTRING = 9;  // Multi-byte string
  FIELD_TYPE_UNICODE = 10; // Unicode string
  FIELD_TYPE_BYTES   = 11; // Bytes
  FIELD_TYPE_ADT     = 12; // Abstract Data Type
  FIELD_TYPE_ARRAY   = 13; // Array type (not attribute)
  FIELD_TYPE_NESTED  = 14; // Embedded (nested table type)
  FIELD_TYPE_REF     = 15; // Referenc

  MASK_FIELD_TYPE    = $3F;  // mask to retrieve Field Type
  MASK_VARYNG_FLD    = $40;  // Varying attribute type
  MASK_ARRAY_FLD     = $80;  // Array attribute type

  FIELD_ATTR_HIDDEN    = $01; // Field is hidden
  FIELD_ATTR_READONLY  = $02; // Field is readonly
  FIELD_ATTR_REQUIRED  = $04; // Field value required
  FIELD_ATTR_LINK      = $08; // Linking field

  REC_STAT_UNMODIFIED  = $00; // Unmodified record
  REC_STAT_ORG         = $01; // Original record (was changed)
  REC_STAT_DELETED     = $02; // Record was deleted
  REC_STAT_NEW         = $04; // Record was inserted
  REC_STAT_MODIFIED    = $08; // Record was changed
  REC_STAT_UNUSED      = $20; // Record not used anymore (hole)
  REC_STAT_UPD         = $40; // Detail modification Ins/Del/Mod. Can be combined with other status.

function FieldTypeToDbFieldType(AFieldType: Word): TFieldType;
begin
  case AFieldType of
    FIELD_TYPE_INT:  Result := ftInteger;
    FIELD_TYPE_UINT: Result := ftInteger;
    FIELD_TYPE_BOOL: Result := ftBoolean;
    FIELD_TYPE_FLOAT: Result := ftFloat;
    FIELD_TYPE_BCD: Result := ftBCD;
    FIELD_TYPE_DATE: Result := ftDate;
    FIELD_TYPE_TIME: Result := ftTime;
    FIELD_TYPE_TIMESTAMP: Result := ftDateTime;
    FIELD_TYPE_ZSTRING: Result := ftString;
    FIELD_TYPE_UNICODE: Result := ftWideString;
    FIELD_TYPE_BYTES: Result := ftBytes;
    FIELD_TYPE_ADT: Result := ftADT;
    FIELD_TYPE_ARRAY: Result := ftArray;
    FIELD_TYPE_NESTED: Result := ftDataSet;
    FIELD_TYPE_REF: Result := ftReference;
  else
    Result := ftUnknown;
  end;
end;

{ TDBReaderMidas }

procedure TDBReaderMidas.AfterConstruction;
begin
  inherited;

end;

procedure TDBReaderMidas.BeforeDestruction;
begin
  FreeAndNil(FFile);
  inherited;
end;

function TDBReaderMidas.ReadFixSizeData(ASize: Integer): AnsiString;
begin
  Result := '';
  if ASize > 0 then
  begin
    SetLength(Result, ASize);
    FFile.ReadBuffer(Result[1], ASize);
  end;
end;

function TDBReaderMidas.ReadFloat: Double;
begin
  FFile.ReadBuffer(Result, SizeOf(Result));
end;

function TDBReaderMidas.ReadInt: Integer;
begin
  FFile.ReadBuffer(Result, SizeOf(Result));
end;

function TDBReaderMidas.ReadUInt: Cardinal;
begin
  FFile.ReadBuffer(Result, SizeOf(Result));
end;

function TDBReaderMidas.ReadBool: Boolean;
var
  bt: Byte;
begin
  FFile.ReadBuffer(bt, SizeOf(bt));
  Result := (bt <> 0);
end;

procedure TDBReaderMidas.ReadTable(AName: string; ACount: Int64; AList: TDbRowsList);
var
  i, iByteOffs, iBitOffs: Integer;
  btRecStatus, iByteMask: Byte;
  iStatusBytesCount: Integer;
  FieldsStatus: array of Byte;
  sData: AnsiString;
  TmpRow: TDbRowItem;
begin
  // columns status bitmap
  iStatusBytesCount := (FColCount div 4);
  if (FColCount mod 4) > 0 then
    Inc(iStatusBytesCount);
  SetLength(FieldsStatus, iStatusBytesCount);

  if Assigned(AList) then
  begin
    AList.TableName := ExtractFileName(FFile.FileName);
    SetLength(AList.FieldsDef, FColCount);
    for i := 0 to FColCount - 1 do
    begin
      AList.FieldsDef[i].Name := FFieldsDef[i].FieldName;
      AList.FieldsDef[i].FieldType := FieldTypeToDbFieldType(FFieldsDef[i].FieldType);
      AList.FieldsDef[i].Size := FFieldsDef[i].FieldSize;
    end;
  end;

  FFile.Position := FFirstRecPosition;
  // records
  while (FFile.Position < FFile.Size) and (ACount > 0) do
  begin
    // record status
    FFile.ReadBuffer(btRecStatus, SizeOf(btRecStatus));
    // fields status bitmap
    FFile.ReadBuffer(FieldsStatus[0], iStatusBytesCount);

    TmpRow := TDbRowItem.Create(AList);
    SetLength(TmpRow.Values, FColCount);

    // fields data
    for i := 0 to FColCount - 1 do
    begin
      // Field status (is Null?)
      iByteOffs := i div 4;
      iBitOffs := (i mod 4) * 2;
      iByteMask := $3 shl iBitOffs;
      if ((FieldsStatus[iByteOffs] and iByteMask) shr iBitOffs) = 1 then
      begin
        TmpRow.Values[i] := Null;
        Continue;
      end;

      //FieldsDef[i].FieldSize
      case (FFieldsDef[i].FieldType and MASK_FIELD_TYPE) of
        FIELD_TYPE_INT:
          TmpRow.Values[i] := ReadInt();
        FIELD_TYPE_UINT:
          TmpRow.Values[i] := ReadUInt();
        FIELD_TYPE_BOOL:
          TmpRow.Values[i] := ReadBool();
        FIELD_TYPE_FLOAT:
          TmpRow.Values[i] := ReadFloat();

        FIELD_TYPE_ZSTRING:
        begin
          if (FFieldsDef[i].FieldType and MASK_VARYNG_FLD) > 0 then
            sData := ReadVarSizeData(FFieldsDef[i].FieldSize)
          else
          begin
            sData := ReadFixSizeData(FFieldsDef[i].FieldSize);
          end;
          TmpRow.Values[i] := sData;
          if not Assigned(AList) then
            LogInfo(Format('Field %s=%s', [FFieldsDef[i].FieldName, sData]));
        end;
      else
        FFile.Seek(FFieldsDef[i].FieldSize, soFromCurrent);
      end;
    end;

    if Assigned(AList) then
      AList.Add(TmpRow)
    else
      TmpRow.Free();

    Dec(ACount);
  end;

end;

function TDBReaderMidas.ReadVarSizeData(ASize: Byte): AnsiString;
var
  Size1: Byte;
  Size2: Word;
  Size4: Cardinal;
begin
  Result := '';
  Size4 := 0;
  case ASize of
    1:
    begin
      FFile.ReadBuffer(Size1, 1);
      Size4 := Size1;
    end;
    2:
    begin
      FFile.ReadBuffer(Size2, 2);
      Size4 := Size2;
    end;
    4:
    begin
      FFile.ReadBuffer(Size4, 4);
    end;
  end;
  if Size4 > 0 then
  begin
    SetLength(Result, Size4);
    FFile.ReadBuffer(Result[1], Size4);
  end;
end;

function ReadProp(AStream: TStream; var AProp: TDSProp): Boolean;
var
  btLen: Byte;
  iSize: Cardinal;
begin
  Result := False;
  // prop name
  AStream.ReadBuffer(btLen, 1);
  if btLen = 0 then Exit;
  SetLength(AProp.PropName, btLen);
  AStream.ReadBuffer(AProp.PropName[1], btLen);
  // prop size, type
  AStream.ReadBuffer(AProp.PropSize, SizeOf(Word));
  AStream.ReadBuffer(AProp.PropType, SizeOf(Word));
  // prop data
  if AProp.PropType = 130 then
  begin
    // array of UInt32
    iSize := 0;
    AStream.ReadBuffer(iSize, AProp.PropSize);
    AStream.Seek(iSize * SizeOf(Cardinal), soFromCurrent);
  end
  else
  if AProp.PropSize > 0 then
  begin
    SetLength(AProp.PropData, AProp.PropSize);
    AStream.ReadBuffer(AProp.PropData[1], AProp.PropSize);
  end;
  Result := True;
end;

function ReadFieldDef(AStream: TStream; var AFieldDef: TDSFieldDef): Boolean;
var
  btLen: Byte;
  i: Integer;
begin
  Result := False;
  // field name
  AStream.ReadBuffer(btLen, 1);
  if btLen = 0 then Exit;
  SetLength(AFieldDef.FieldName, btLen);
  AStream.ReadBuffer(AFieldDef.FieldName[1], btLen);
  // size, type, attrs
  AStream.ReadBuffer(AFieldDef.FieldSize, SizeOf(Word));
  AStream.ReadBuffer(AFieldDef.FieldType, SizeOf(Word));
  AStream.ReadBuffer(AFieldDef.FieldAttrs, SizeOf(Word));
  // props
  AStream.ReadBuffer(AFieldDef.FieldPropCount, SizeOf(Word));
  if AFieldDef.FieldPropCount > 0 then
  begin
    SetLength(AFieldDef.FieldProps, AFieldDef.FieldPropCount);
    for i := 0 to AFieldDef.FieldPropCount - 1 do
    begin
      if not ReadProp(AStream, AFieldDef.FieldProps[i]) then
        Exit;
    end;
  end;
  Result := True;
end;

function TDBReaderMidas.OpenFile(AFileName: string): Boolean;
var
  //RawData: array [0..2047] of Byte;
  PktHead: TDSPacketHeader;
  TmpProp: TDSProp;
  iPropsCount: Word;
  i: Integer;
begin
  Result := inherited OpenFile(AFileName);
  if not Result then Exit;
  Result := False;

  // read header
  FFile.ReadBuffer(PktHead, SizeOf(PktHead));

  FDbVersion := PktHead.Version;
  FColCount := PktHead.FieldCount;

  // read field defs
  SetLength(FFieldsDef, FColCount);
  for i := 0 to FColCount - 1 do
  begin
    if not ReadFieldDef(FFile, FFieldsDef[i]) then Exit;
  end;

  // read packet props
  FFile.ReadBuffer(iPropsCount, SizeOf(iPropsCount));
  for i := 0 to iPropsCount - 1 do
  begin
    if not ReadProp(FFile, TmpProp) then Exit;
  end;
  FFirstRecPosition := FFile.Position;

  //ReadTable('', 100);
  Result := True;
end;

end.
