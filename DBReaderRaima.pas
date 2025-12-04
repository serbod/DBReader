unit DBReaderRaima;

(*
Raima database file reader (.dbd)

Author: Sergey Bodrov, 2025 Minsk
License: MIT

https://wiki.openstreetmap.org/wiki/OSM_Map_On_Magellan/Format/raimadb

Tested on Velocis (L2.00)

*)

{$ifdef FPC}
  {$MODE Delphi}
{$endif}

interface

uses
  {$ifdef FPC}
  LazUTF8, {LConvEncoding,}
  {$else}
  Windows, {for OemToChar}
  {$endif}
  SysUtils, Classes, Variants, DBReaderBase, DB, Types;

type
  TRdmFileRec = record
    Name: string;   // 50 zero-terminated
    Flags: Word;    // 2  $63=Closed
    Compress: Word; // 2  $63=Compression Data
    RecLen: Word;   // 2  Aligned record length
    PageSize: Word; // 2  Page size = $200
    Flags2: Word;   // 2  $40=Compressed
  end;

  TRdmTableRec = record
    FileIndex: Word;  // 2 Index in files list
    RecSize: Word;    // 2 Record length
    Unknown_4: Word;  // 2 $0
    FieldIndex: Word; // 2 Index of first field in fields list
    FieldCount: Word; // 2 Fields count
    Unknown_0A: Word; // 2 $0
    Name: string;     // $0A-terminated
  end;

  TRdmFieldRec = record
    FieldFlag: Byte;   // u8  field flag ($75=PrimaryKey  $6E, $64)
    FieldType: Byte;   // u8  field type
    Size: Word;        // u16 field length
    Dimension: Word;   // u16 items count
    Sub1: Word;        // u16
    Sub2: Word;        // u16
    ExtFileId: Word;   // u16 related file ID (for keys and blobs)
    Sub4: Word;        // u16 unique (incremental) for each ExtFileId
    Offset: Word;      // u16 offset
    TableId: Word;     // u16 table index
    Sub5: Word;        // u16
    //Sub6: Word;        // u8
    Name: string;      // $0A-terminated
  end;

type
  { TRdmTableInfo }

  TRdmTableInfo = class(TDbRowsList)
  public
    Fields: array of TRdmFieldRec;
    FileName: string;
    FileId: Integer;
    TableId: Integer;
    PageSize: Integer;
    RecSize: Integer;
    RecSizeAligned: Integer;

    procedure AfterConstruction; override;
    // predefined table
    function IsSystem(): Boolean; override;

    procedure AddFieldDef(AName: string; AType, ASize: Integer);
  end;

  { TRdmTableInfoList }

  TRdmTableInfoList = class(TList)
  protected
    procedure Notify(Ptr: Pointer; Action: TListNotification); override;
  public
    function GetItem(AIndex: Integer): TRdmTableInfo;
    function GetByName(AName: string): TRdmTableInfo;
    //function GetByTableID(ATabID: Cardinal): TRdmTableInfo;
    procedure SortByName();
  end;

  { TDBReaderRaima }

  TDBReaderRaima = class(TDBReader)
  private
    FPagesList: TRdmTableInfo;
    FTablesList: TRdmTableInfoList;
    FBlobStream: TStream;
    FBlobFileId: Integer;  // FileID for FBlobStream
    FBlobUnitSize: Integer;

    FIsMetadataLoaded: Boolean;
    FDBPath: string;

    // APagePos contain offset to next page after reading
    function ReadDataPage(var APageBuf: TByteDynArray; var APagePos: Int64;
      ATableInfo: TRdmTableInfo; AList: TDbRowsList): Boolean;

    function ReadRowData(const APageBuf: TByteDynArray; ARowOffs, ARowSize: Integer;
      ATableInfo: TRdmTableInfo; AList: TDbRowsList): Boolean;

    // read optional BLOB data from raw position data
    function ReadBlobData(const ARaw: AnsiString; ABlobFileID: Integer): AnsiString;

    procedure FillTablesList();

  public
    FilesArr: array of TRdmFileRec;
    TablesArr: array of TRdmTableRec;
    FieldsArr: array of TRdmFieldRec;

    procedure AfterConstruction(); override;
    procedure BeforeDestruction(); override;

    function OpenFile(AFileName: string; AStream: TStream = nil): Boolean; override;
    // Read table data from DB to AList
    // AName - table name
    // ACount - how many items read
    procedure ReadTable(AName: string; ACount: Int64 = MaxInt; AList: TDbRowsList = nil); override;
    // get detailed multi-line description of table
    function FillTableInfoText(ATableName: string; ALines: TStrings): Boolean; override;

    // get tables count
    function GetTablesCount(): Integer; override;
    // get table by index 0..GetTablesCount()-1
    function GetTableByIndex(AIndex: Integer): TDbRowsList; override;

  end;

implementation

const
  RDM_FIELD_TYPE_FLOAT        = $46; // Float (8 byte) Float64
  RDM_FIELD_TYPE_MEMO         = $62; // Memo (4 byte) blob addr
  RDM_FIELD_TYPE_CHAR         = $63; // Char (1 byte) UInt8
  RDM_FIELD_TYPE_INTEGER      = $6C; // Integer (4 byte) Int32
  RDM_FIELD_TYPE_SHORT        = $73; // Short (2 byte) Int16

  RDM_REC_HEAD_SIZE           = 4;   // SomeID (2 byte) + TabIdx (2 byte)
  RDM_BLOB_ADDR_NULL          = #0#0#0#0;  // null blob

type
  // file header
  TRdmFileHeaderRec = record
    Signature: array [0..5] of Byte; // 6 signature, version
    PageSize: Word;          // 2 Page size
    FilesCount: Word;        // 2 Files count
    TablesCount: Word;       // 2 Tables count
    FieldsCount: Word;       // 2 fields count
    Unknown_0E: Word;        // 2
    Unknown_10: Word;        // 2
    Unknown_12: Word;        // 2
    Unknown_14: Word;        // 2
  end;

  // blob unit header (size = 18)
  TRdmBlobUnitRec = record
    BlobType: Cardinal;  // [00] 4 BlobType? =2
    NextUnit: Cardinal;  // [04] 4 next unit index
    PrevUnit: Cardinal;  // [08] 4 prev unit index
    PartNum: Cardinal;   // [0C] 4 blob part index
    DataSize: Cardinal;  // [10] 4 data size (first part = total size)
    Sub3: Word;          // [14] 2 CRC? depends on size
    Data: AnsiString;    // [16]
  end;

function RdmFieldTypeToDbFieldType(AValue: Integer): TFieldType;
begin
  case AValue of
    RDM_FIELD_TYPE_FLOAT:    Result := ftFloat;
    RDM_FIELD_TYPE_MEMO:     Result := ftMemo;
    RDM_FIELD_TYPE_CHAR:     Result := ftString;
    RDM_FIELD_TYPE_INTEGER:  Result := ftInteger;
    RDM_FIELD_TYPE_SHORT:    Result := ftInteger;
  else
    Result := ftUnknown;
  end;
end;

function RdmFieldTypeName(AValue, ASize: Integer): string;
begin
  case AValue of
    RDM_FIELD_TYPE_FLOAT:    Result := 'FLOAT';
    RDM_FIELD_TYPE_MEMO:     Result := 'MEMO';
    RDM_FIELD_TYPE_CHAR:     Result := 'CHAR';
    RDM_FIELD_TYPE_INTEGER:  Result := 'INT';
    RDM_FIELD_TYPE_SHORT:    Result := 'SHORT';
  else
    Result := 'Unknown_' + IntToHex(AValue, 4);
  end;
  if ASize <> 0 then
  begin
    Result := Format('%s(%d)', [Result, ASize]);
  end;
end;

{ TRdmTableInfo }

procedure TRdmTableInfo.AfterConstruction;
begin
  inherited AfterConstruction;
  Fields := [];
  //SetLength(FieldsDef, 4);
end;

function TRdmTableInfo.IsSystem(): Boolean;
begin
  Result := (TableName = 'sys_pages');
end;

procedure TRdmTableInfo.AddFieldDef(AName: string; AType, ASize: Integer);
var
  n: Integer;
begin
  n := Length(FieldsDef);
  SetLength(FieldsDef, n+1);
  FieldsDef[n].Name := AName;
  FieldsDef[n].FieldType := RdmFieldTypeToDbFieldType(AType);
  FieldsDef[n].Size := ASize;
end;

{ TRdmTableInfoList }

procedure TRdmTableInfoList.Notify(Ptr: Pointer; Action: TListNotification);
begin
  inherited Notify(Ptr, Action);
  if Action = lnDeleted then
    TRdmTableInfo(Ptr).Free;
end;

function TRdmTableInfoList.GetItem(AIndex: Integer): TRdmTableInfo;
begin
  Result := nil;
  if AIndex < Count then
    Result := TRdmTableInfo(Get(AIndex));
end;

function TRdmTableInfoList.GetByName(AName: string): TRdmTableInfo;
var
  i: Integer;
begin
  for i := 0 to Count - 1 do
  begin
    Result := GetItem(i);
    if Result.TableName = AName then
      Exit;
  end;
  Result := nil;
end;

function _DoSortByName(Item1, Item2: Pointer): Integer;
var
  TmpItem1, TmpItem2: TRdmTableInfo;
begin
  TmpItem1 := TRdmTableInfo(Item1);
  TmpItem2 := TRdmTableInfo(Item2);
  Result := AnsiCompareStr(TmpItem1.TableName, TmpItem2.TableName);
end;

procedure TRdmTableInfoList.SortByName();
begin
  Sort(@_DoSortByName);
end;

{ TDBReaderRaima }

procedure TDBReaderRaima.AfterConstruction;
begin
  inherited;
  //FIsSingleTable := True;
  FPagesList := TRdmTableInfo.Create();
  FPagesList.TableName := 'sys_pages';
  FPagesList.AddFieldDef('Offs', RDM_FIELD_TYPE_INTEGER, 4);
  FPagesList.AddFieldDef('Type', RDM_FIELD_TYPE_CHAR, 40);
  FPagesList.AddFieldDef('Size', RDM_FIELD_TYPE_INTEGER, 4);
  FPagesList.AddFieldDef('RecCount', RDM_FIELD_TYPE_INTEGER, 4);

  FTablesList := TRdmTableInfoList.Create;
  //FTablesList.Add(FPagesList);
end;

procedure TDBReaderRaima.BeforeDestruction;
begin
  FreeAndNil(FTablesList);
  FreeAndNil(FPagesList);
  inherited;
end;

function TDBReaderRaima.FillTableInfoText(ATableName: string; ALines: TStrings): Boolean;
var
  i: Integer;
  s: string;
  TmpTable: TRdmTableInfo;
begin
  Result := False;
  TmpTable := FTablesList.GetByName(ATableName);
  if not Assigned(TmpTable) then
    Exit;

  with TmpTable do
  begin
    ALines.Add(Format('== Table Id=%d  Name=%s', [TmpTable.TableId, TableName]));
    ALines.Add(Format('RecSize=%d', [TmpTable.RecSize]));
    ALines.Add(Format('RecSizeAligned=%d', [TmpTable.RecSizeAligned]));
    ALines.Add(Format('FileName=%s', [TmpTable.FileName]));
    ALines.Add(Format('FileId=%d', [TmpTable.FileId]));

    ALines.Add(Format('== Fields  Count=%d', [Length(Fields)]));
    for i := Low(Fields) to High(Fields) do
    begin
      s := Format('%.2d Name=%-20s  Type=$%x[%x] %-8s  Dim=%d  Size=%d  Offs=%d  Sub=%d %d  %d %d  %d',
        [
          i,
          Fields[i].Name,
          Fields[i].FieldType,
          Fields[i].FieldFlag,
          RdmFieldTypeName(Fields[i].FieldType, Fields[i].Size),
          Fields[i].Dimension,
          Fields[i].Size,
          Fields[i].Offset,
          Fields[i].Sub1,
          Fields[i].Sub2,
          Fields[i].ExtFileID,
          Fields[i].Sub4,
          Fields[i].Sub5
        ]);
      ALines.Add(s);
    end;

  end;
end;

function TDBReaderRaima.GetTablesCount(): Integer;
begin
  Result := FTablesList.Count;
end;

function TDBReaderRaima.GetTableByIndex(AIndex: Integer): TDbRowsList;
begin
  Result := FTablesList.GetItem(AIndex);
end;

function TDBReaderRaima.OpenFile(AFileName: string; AStream: TStream): Boolean;
var
  PageBuf: TByteDynArray;
  FileHeader: TRdmFileHeaderRec;
  rdr: TRawDataReader;
  iPagePos: Int64;
  i, n, iPageSize: Integer;
  FilesArr: array of TRdmFileRec;
  TablesArr: array of TRdmTableRec;
  FieldsArr: array of TRdmFieldRec;
  s: string;
  bt: Byte;
begin
  Result := inherited OpenFile(AFileName, AStream);
  if not Result then Exit;

  FDBPath := IncludeTrailingPathDelimiter(ExtractFileDir(AFileName));

  // todo check signature
  if UpperCase(ExtractFileExt(AFileName)) <> '.DBD' then
  begin
    LogInfo('Not database definition file!');
    Result := False;
    Exit;
  end;

  if FFile.Size > $FFFFFF then // 16 Mb
  begin
    LogInfo('Database definition file size is too large!');
    Result := False;
    Exit;
  end;

  FillTablesList();
end;

function TDBReaderRaima.ReadDataPage(var APageBuf: TByteDynArray;
  var APagePos: Int64; ATableInfo: TRdmTableInfo; AList: TDbRowsList): Boolean;
var
  rdr: TRawDataReader;
  RawData: TByteDynArray;
  iPageID, iRowID, Unkn2, Unkn3: Cardinal;
  i, iTabId, iRowPos, iTmpPos, iPageRecSize: Integer;
  TabInfo: TRdmTableInfo;
  FieldType: Byte;
  FieldSize: Integer;
  TmpRow: TDbRowItem;
  v: Variant;
  s: string;
begin
  Result := False;
  rdr.Init(APageBuf[0], False);
  RawData := [];
  TabInfo := ATableInfo;

  if APagePos = 0 then
  begin
    // first page
    // 4  file ID ?
    // 4
    // 4
    // 4  records count
    // records (4 byte)
    Exit;
  end;

  //Exit;

  // 0C 00         = 12
  // 03 00 00 00   = 3
  // 01 00
  // 03 00 00 00
  // 'хозяин'
  // 00 00 ....
  // len=132

  // 0C 00         = 12
  // 03 00 00 00   = 3
  // 02 00
  // 02 00 00 00
  // 'хозорган'
  // 00 00 ....
  // len=132

  // 4  records count ?
  iPageID := rdr.ReadUInt32;

  iPageRecSize := ATableInfo.RecSizeAligned;

  while (rdr.GetPosition() + 6) < Length(APageBuf) do
  begin
    iRowPos := rdr.GetPosition();

    // records
    // 2 ??
    // 2 table idx
    // 2 ??
    // 2 ??
    iTabId := rdr.ReadUInt16;
    iRowID := rdr.ReadUInt16;
    Unkn2 := rdr.ReadUInt16;
    Unkn3 := rdr.ReadUInt16;

    // skip records from other tables
    if iTabId <> ATableInfo.TableId then
    begin
      rdr.SetPosition(iRowPos + iPageRecSize);
      Continue;
    end;

    TmpRow := TDbRowItem.Create(AList);
    AList.Add(TmpRow);

    if IsDebugRows then
    begin
      iTmpPos := rdr.GetPosition();
      rdr.SetPosition(iRowPos);
      TmpRow.RawData := rdr.ReadBytes(iPageRecSize);
      rdr.SetPosition(iTmpPos);
      // !!!
      //BufToFile(TmpRow.RawData[1], Length(TmpRow.RawData), 'rec.raw');
    end;

    // read fields
    SetLength(TmpRow.Values, Length(TabInfo.Fields));
    for i := Low(TabInfo.Fields) to High(TabInfo.Fields) do
    begin
      FieldType := TabInfo.Fields[i].FieldType;
      FieldSize := TabInfo.Fields[i].Size;
      rdr.SetPosition(iRowPos + TabInfo.Fields[i].Offset);

      //iTmpPos := rdr.GetPosition();
      v := Null;
      case FieldType of
        RDM_FIELD_TYPE_FLOAT:    v := rdr.ReadDouble;
        RDM_FIELD_TYPE_MEMO:
        begin
          s := rdr.ReadBytes(FieldSize);
          if s <> RDM_BLOB_ADDR_NULL then
          begin
            s := ReadBlobData(s, TabInfo.Fields[i].ExtFileId);
            s := WinCPToUTF8(s);
            v := s;
          end;
        end;
        RDM_FIELD_TYPE_CHAR:
        begin
          //s := rdr.ReadBytes(FieldSize);
          //s := Trim(s);
          s := rdr.ReadCString(FieldSize);
          s := WinCPToUTF8(s);
          v := s;
        end;
        RDM_FIELD_TYPE_INTEGER:  v := rdr.ReadInt32;
        RDM_FIELD_TYPE_SHORT:    v := rdr.ReadInt16;
      else
        s := rdr.ReadBytes(FieldSize);
        v := BufferToHex(s[1], FieldSize);
      end;
      TmpRow.Values[i] := v;

    end;

    rdr.SetPosition(iRowPos + iPageRecSize);
  end;
end;

function TDBReaderRaima.ReadRowData(const APageBuf: TByteDynArray; ARowOffs,
  ARowSize: Integer; ATableInfo: TRdmTableInfo; AList: TDbRowsList): Boolean;
var
  rdr: TRawDataReader;
  TmpRow: TDbRowItem;
  i, iFieldSize: Integer;
  nTabNum, nRecNum: Cardinal;
  btFieldType: Byte;
  v: Variant;
  s: string;
begin
  Result := False;
  if (not FIsMetadataLoaded)
  or (not Assigned(ATableInfo))
  or (not Assigned(AList)) then
    Exit;

  rdr.Init(APageBuf[0], False);

  nTabNum := rdr.ReadUInt16;
  nRecNum := rdr.ReadUInt32;

  Exit;

  TmpRow := TDbRowItem.Create(AList);
  AList.Add(TmpRow);
  if IsDebugRows then
  begin
    rdr.SetPosition(0);
    TmpRow.RawData := rdr.ReadBytes(ARowSize);
  end;

  SetLength(TmpRow.Values, Length(ATableInfo.Fields));

  for i := 0 to Length(ATableInfo.Fields)-1 do
  begin
    btFieldType := ATableInfo.Fields[i].FieldType;
    iFieldSize := ATableInfo.Fields[i].Size;

    rdr.SetPosition(ATableInfo.Fields[i].Offset + 4+1+4); // skip TabNum + RecType + RecNum
    v := Null;
    case btFieldType of
      RDM_FIELD_TYPE_CHAR:     v := rdr.ReadUInt8;
      RDM_FIELD_TYPE_SHORT:    v := rdr.ReadInt16;
      RDM_FIELD_TYPE_INTEGER:  v := rdr.ReadInt32;

      {TPS_FIELD_TYPE_STRING:
      begin
        s := rdr.ReadBytes(iFieldSize);
        v := s;
      end;
      TPS_FIELD_TYPE_CSTRING:
      begin
        s := rdr.ReadCString(iFieldSize);
        v := s;
      end;
      TPS_FIELD_TYPE_PSTRING:
      begin
        s := rdr.ReadCString(iFieldSize);
        v := s;
      end;
      TPS_FIELD_TYPE_GROUP:
      begin
        s := rdr.ReadBytes(iFieldSize);
        if s <> '' then
          v := BufferToHex(s[1], Length(s));
      end;  }

    end;

    TmpRow.Values[i] := v;
  end;

end;

function TDBReaderRaima.ReadBlobData(const ARaw: AnsiString; ABlobFileID: Integer): AnsiString;
var
  s: string;
  UnitIndex, n: Cardinal;
  rdr: TRawDataReader;
  nPos: Int64;
  sData: AnsiString;
  BlobUnit: TRdmBlobUnitRec;
begin
  Result := '';
  if (FBlobFileId <> ABlobFileID) or (not Assigned(FBlobStream)) then
  begin
    FreeAndNil(FBlobStream);
    if ABlobFileID > High(FilesArr) then
      Exit;
    s := FDBPath + FilesArr[ABlobFileID].Name;
    FBlobStream := TFileStream.Create(s, fmOpenRead + fmShareDenyNone);
    FBlobFileId := ABlobFileID;
    FBlobUnitSize := FilesArr[ABlobFileID].PageSize;
  end;
  if not Assigned(FBlobStream) then
    Exit;

  UnitIndex := 0;
  if (Length(ARaw) > 0) and (Length(ARaw) <= 4) then
  begin
    rdr.Init(ARaw[1], False);
    // 4 unit index
    UnitIndex := rdr.ReadUInt32;
  end;

  if UnitIndex = 0 then
    Exit;


  nPos := UnitIndex * FBlobUnitSize; // unit position
  if nPos >= FBlobStream.Size then
  begin
    Result := BufferToHex(ARaw[1], 4);
    Exit;
  end;

  while nPos + FBlobUnitSize < FBlobStream.Size do
  begin
    FBlobStream.Position := nPos;
    SetLength(sData, FBlobUnitSize);
    FBlobStream.Read(sData[1], FBlobUnitSize);
    rdr.Init(sData[1], False);

    // read blob unit
    BlobUnit.BlobType := rdr.ReadUInt32;      // [00] 4 BlobType? UnitsCount? (=2 for single unit)
    BlobUnit.NextUnit := rdr.ReadUInt32;      // [04] 4 next unit index
    BlobUnit.PrevUnit := rdr.ReadUInt32;      // [08] 4 prev unit index
    BlobUnit.PartNum := rdr.ReadUInt32;       // [0C] 4 blob part index
    BlobUnit.DataSize := rdr.ReadUInt32;      // [10] 4 data size
    BlobUnit.Sub3 := rdr.ReadUInt16;          // [14] 2 CRC? depends on size
    //Result := Result + Format('t=%d n=%d p=%d i=%d s=%d [%4x] ',
    //  [BlobUnit.BlobType, BlobUnit.NextUnit, BlobUnit.PrevUnit, BlobUnit.PartNum, BlobUnit.DataSize, BlobUnit.Sub3]);
    n := FBlobUnitSize - rdr.GetPosition();
    if n > BlobUnit.DataSize then
      n := BlobUnit.DataSize;

    BlobUnit.Data := rdr.ReadBytes(n);
    Result := Result + BlobUnit.Data;

    if BlobUnit.NextUnit = 0 then
      Exit;
    nPos := BlobUnit.NextUnit * FBlobUnitSize; // unit position
  end;

end;

procedure TDBReaderRaima.FillTablesList();
var
  PageBuf: TByteDynArray;
  FileHeader: TRdmFileHeaderRec;
  rdr: TRawDataReader;
  i, ii, iFile, iField, iPos: Integer;
  s: string;
  bt: Byte;
  TmpTable: TRdmTableInfo;
begin
  // read database definition file

  // init reader
  PageBuf := [];
  SetLength(PageBuf, FFile.Size);
  FFile.Position := 0;
  FFile.Read(PageBuf[0], FFile.Size);
  rdr.Init(PageBuf[0], False);

  // read file header
  rdr.ReadToBuffer(FileHeader.Signature, 6); // 6 signature, version
  FileHeader.PageSize := rdr.ReadUInt16;     // 2 Page size
  FileHeader.FilesCount := rdr.ReadUInt16;   // 2 Files count
  FileHeader.TablesCount := rdr.ReadUInt16;  // 2 Tables count
  FileHeader.FieldsCount := rdr.ReadUInt16;  // 2 fields count
  FileHeader.Unknown_0E := rdr.ReadUInt16;   // 2
  FileHeader.Unknown_10 := rdr.ReadUInt16;   // 2
  FileHeader.Unknown_12 := rdr.ReadUInt16;   // 2
  FileHeader.Unknown_14 := rdr.ReadUInt16;   // 2

  // read files records
  SetLength(FilesArr, FileHeader.FilesCount);
  for i := 0 to FileHeader.FilesCount-1 do
  begin
    FilesArr[i].Name := Trim(rdr.ReadBytes(50));   // 50 zero-terminated
    FilesArr[i].Flags := rdr.ReadUInt16;    // 2  $63=Closed
    FilesArr[i].Compress := rdr.ReadUInt16; // 2  $63=Compression Data
    FilesArr[i].RecLen := rdr.ReadUInt16;   // 2  Aligned record length
    FilesArr[i].PageSize := rdr.ReadUInt16; // 2  Page size = $200
    FilesArr[i].Flags2 := rdr.ReadUInt16;   // 2  $40=Compressed

    LogInfo(Format('File %2d: %-20s PageSize=%4d  RecLen=%d',
      [i, FilesArr[i].Name, FilesArr[i].PageSize, FilesArr[i].RecLen]));
  end;

  // read tables records
  SetLength(TablesArr, FileHeader.TablesCount);
  for i := 0 to FileHeader.TablesCount-1 do
  begin
    TablesArr[i].FileIndex := rdr.ReadUInt16;  // 2 Index in files list
    TablesArr[i].RecSize := rdr.ReadUInt16;    // 2 Record length
    TablesArr[i].Unknown_4 := rdr.ReadUInt16;  // 2 $0
    TablesArr[i].FieldIndex := rdr.ReadUInt16; // 2 Index of first field in fields list
    TablesArr[i].FieldCount := rdr.ReadUInt16; // 2 Fields count
    TablesArr[i].Unknown_0A := rdr.ReadUInt16; // 2 $0
  end;

  // read fields records
  SetLength(FieldsArr, FileHeader.FieldsCount);
  for i := 0 to FileHeader.FieldsCount-1 do
  begin
    FieldsArr[i].FieldFlag := rdr.ReadUInt8;   // u8 field flag
    FieldsArr[i].FieldType := rdr.ReadUInt8;   // u8 field type
    FieldsArr[i].Size := rdr.ReadUInt16;       // u16 field length
    FieldsArr[i].Dimension := rdr.ReadUInt16;  // u16 items count
    FieldsArr[i].Sub1 := rdr.ReadUInt16;       // u16
    FieldsArr[i].Sub2 := rdr.ReadUInt16;       // u16
    FieldsArr[i].ExtFileId := rdr.ReadUInt16;  // u16
    FieldsArr[i].Sub4 := rdr.ReadUInt16;       // u16
    FieldsArr[i].Offset := rdr.ReadUInt16;     // u16 offset
    FieldsArr[i].TableId := rdr.ReadUInt16;    // u16 table index
    FieldsArr[i].Sub5 := rdr.ReadUInt16;       // u16
    //FieldsArr[i].Sub6 := rdr.ReadUInt8;        // u8
  end;

  // skip Unknown_0E (12 bytes) records
  rdr.ReadBytes(FileHeader.Unknown_0E * 12);

  // skip Unknown_10 (8 bytes) records
  rdr.ReadBytes(FileHeader.Unknown_10 * 8);

  // skip Unknown_12 (4 bytes) records
  rdr.ReadBytes(FileHeader.Unknown_12 * 4);

  // skip Unknown_14 (8 bytes) records
  rdr.ReadBytes(FileHeader.Unknown_14 * 8);

  // read tables names ($0A-terminated)
  for i := 0 to FileHeader.TablesCount-1 do
  begin
    s := '';
    repeat
      bt := rdr.ReadUInt8;
      if bt = $0A then
        TablesArr[i].Name := s
      else
        s := s + Chr(bt);
    until bt = $0A;
  end;

  // read fields names ($0A-terminated)
  for i := 0 to FileHeader.FieldsCount-1 do
  begin
    s := '';
    repeat
      bt := rdr.ReadUInt8;
      if bt = $0A then
        FieldsArr[i].Name := s
      else
        s := s + Chr(bt);
    until bt = $0A;
  end;

  // Unknown_0E or Unknown_10 names
  // todo

  // fill tables list
  for i := 0 to FileHeader.TablesCount-1 do
  begin
    TmpTable := TRdmTableInfo.Create();
    FTablesList.Add(TmpTable);

    TmpTable.TableName := TablesArr[i].Name;
    TmpTable.TableId := i;
    TmpTable.RecSize := TablesArr[i].RecSize;
    iFile := TablesArr[i].FileIndex;
    TmpTable.FileId := iFile;
    TmpTable.FileName := FilesArr[iFile].Name;
    TmpTable.PageSize := FilesArr[iFile].PageSize;
    TmpTable.RecSizeAligned := FilesArr[iFile].RecLen;

    if TablesArr[i].FieldCount = $FFFF then
    begin
      // SYSTEM table
      Continue;
    end;

    // table fields
    SetLength(TmpTable.Fields, TablesArr[i].FieldCount);
    SetLength(TmpTable.FieldsDef, TablesArr[i].FieldCount);
    for ii := 0 to TablesArr[i].FieldCount-1 do
    begin
      iField := TablesArr[i].FieldIndex + ii;
      if iField > High(FieldsArr) then
      begin
        TmpTable.Fields[ii].Name := 'unknown';
        TmpTable.FieldsDef[ii].Name := 'unknown';
        Continue;
      end;
      TmpTable.Fields[ii] := FieldsArr[iField];

      // field definition
      TmpTable.FieldsDef[ii].FieldType := RdmFieldTypeToDbFieldType(FieldsArr[iField].FieldType);
      TmpTable.FieldsDef[ii].Name := FieldsArr[iField].Name;
      TmpTable.FieldsDef[ii].TypeName := RdmFieldTypeName(FieldsArr[iField].FieldType, FieldsArr[iField].Size);
      TmpTable.FieldsDef[ii].Size := FieldsArr[iField].Size;
      TmpTable.FieldsDef[ii].RawOffset := FieldsArr[iField].Offset;
    end;
  end;
  FreeAndNil(FFile);

  FIsMetadataLoaded := True;
end;

procedure TDBReaderRaima.ReadTable(AName: string; ACount: Int64; AList: TDbRowsList);
var
  i: Integer;
  TmpRow: TDbRowItem;
  TmpTable: TRdmTableInfo;
  iPageSize: Integer;
  iPagePos: Int64;
  PageBuf: TByteDynArray;
begin
  AList.Clear;
  AList.TableName := AName;
  TmpTable := FTablesList.GetByName(AName);
  if not Assigned(TmpTable) then
    Exit;
  AList.FieldsDef := TmpTable.FieldsDef;

  if TmpTable.Count <> 0 then
  begin
    // copy data
    for i := 0 to TmpTable.Count - 1 do
    begin
      TmpRow := TDbRowItem.Create(AList);
      TmpRow.Assign(TmpTable.GetItem(i));
      AList.Add(TmpRow);
    end;
  end
  else
  begin
    // scan pages
    iPageSize := TmpTable.PageSize;
    PageBuf := [];
    SetLength(PageBuf, iPageSize);
    iPagePos := 0;
    if not inherited OpenFile(FDBPath + TmpTable.FileName) then
    begin
      LogInfo('File not found: ' + TmpTable.FileName);
      Exit;
    end;
    while iPagePos + iPageSize <= FFile.Size do
    begin
      FFile.Position := iPagePos;
      FFile.Read(PageBuf[0], iPageSize);

      ReadDataPage(PageBuf, iPagePos, TmpTable, AList);
      Inc(iPagePos, iPageSize);

      if Assigned(OnPageReaded) then
        OnPageReaded(Self);
    end;
  end;

end;

end.
