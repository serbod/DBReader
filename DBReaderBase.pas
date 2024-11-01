unit DBReaderBase;

(*
Database file reader base classes and defines

Author: Sergey Bodrov, 2024 Minsk
License: MIT

*)

interface

uses
  SysUtils, Classes, Variants, DB;

type
  //TDbReader = class;
  TDbRowsList = class;

  TDbFieldDefRec = record
    Name: string;
    TypeName: string;        // field type name
    FieldType: TFieldType;
    Size: Integer;
    RawOffset: Cardinal;     // raw data offset
  end;

  TDbRowItem = class(TObject)
  protected
    FOwner: TDbRowsList;
  public
    RawData: AnsiString;
    Values: array of Variant;
    constructor Create(AOwner: TDbRowsList);
    function GetFieldAsStr(AFieldIndex: Integer): string; virtual;
    property Owner: TDbRowsList read FOwner;
  end;

  TDbRowsList = class(TList)
  protected
    procedure Notify(Ptr: Pointer; Action: TListNotification); override;
    // set FieldsInfo[AIndex] properties for system tables
    //procedure SetFieldInfo(var AIndex: Integer; AName: string; AType, ASize: Integer);
  public
    FieldsDef: array of TDbFieldDefRec;
    TableName: string;
    function GetItem(AIndex: Integer): TDbRowItem;
  end;

  { DB reader base class }

  TDBReader = class(TComponent)
  protected
    FFile: TStream;
    FOnLog: TGetStrProc;
    FIsSingleTable: Boolean;

  public
    IsLogPages: Boolean;
    FileName: string;

    procedure AfterConstruction(); override;
    procedure BeforeDestruction(); override;

    procedure LogInfo(AStr: string); virtual;
    function OpenFile(AFileName: string; AStream: TStream = nil): Boolean; virtual;
    // Read table data from DB to AList
    // AName - table name
    // ACount - how many items read
    procedure ReadTable(AName: string; ACount: Int64 = MaxInt; AList: TDbRowsList = nil); virtual; abstract;
    // get detailed multi-line description of table
    function FillTableInfoText(ATableName: string; ALines: TStrings): Boolean; virtual;

    property OnLog: TGetStrProc read FOnLog write FOnLog;
    // detabase file contain single table
    property IsSingleTable: Boolean read FIsSingleTable;
  end;

  TDbReaderDataSet = class(TDataSet)
  private
    FIsOpen: Boolean;
    FCursor: Integer;
    FRowsList: TDbRowsList;
  protected
    procedure InternalHandleException; override;
    // Cursor init/close
    procedure InternalInitFieldDefs; override;
    procedure InternalOpen; override;
    function IsCursorOpen: Boolean; override;
    procedure InternalClose; override;
    // Record init/fill/close
    function GetRecord(Buffer: PChar; GetMode: TGetMode; DoCheck: Boolean):
      TGetResult; override;
    function AllocRecordBuffer: PChar; override;
    procedure FreeRecordBuffer(var Buffer: PChar); override;
    procedure InternalInitRecord(Buffer: PChar); override;
    // Internal Navigation
    procedure InternalFirst; override;
    procedure InternalLast; override;
    procedure InternalSetToRecord(Buffer: PChar); override;
    // Records count
    function GetRecordCount: Integer; override;
    // External navigation (1-based)
    procedure SetRecNo(Value: Integer); override;
    function GetRecNo: Integer; override;
    // Access
    function GetCanModify: Boolean; override;
  public
    // read field data
    function GetFieldData(Field: TField; Buffer: Pointer): Boolean; override;
    constructor Create(AOwner: TComponent); override;

    procedure AssignRowsList(ARowsList: TDbRowsList);
  end;

  TRawDataReader = record
    Data: AnsiString;
    nPos: Integer;

    procedure Init(AData: AnsiString; AStartPos: Integer = 0);
    function ReadUInt8: Byte;
    function ReadUInt16: Word;
    function ReadUInt32: Cardinal;
    function ReadInt8: ShortInt;
    function ReadInt16: SmallInt;
    function ReadInt32: Integer;
  end;

  TInnerFileStream = class(TFileStream)
  public
    InnerFilePos: Int64;
    InnerFileSize: Int64;
    InnerFileName: string;
    function Seek(const Offset: Int64; Origin: TSeekOrigin): Int64; override;
    constructor Create(AFileName, AInnerFileName: string; APos, ASize: Int64);
  end;

procedure BufToFile(const ABuf; ABufSize: Integer; AFileName: string);
procedure StrToFile(AStr: AnsiString; AFileName: string);
// Returns Buffer content in HEX, capital letters, without spaces
// Example: 010ABC
function BufToHex(const Buffer; BufferSize: Integer): string;
// Example: [01 0A BC]
function BufferToHex(const Buffer; BufferSize: Integer): string;

implementation

type
  TRecordBuffer = record
    RecordNum: Integer; // 1-based
  end;
  PRecordBuffer = ^TRecordBuffer;

procedure BufToFile(const ABuf; ABufSize: Integer; AFileName: string);
var
  fs: TFileStream;
begin
  if ABufSize = 0 then Exit;
  if FileExists(AFileName) then
    DeleteFile(AFileName);

  fs := TFileStream.Create(AFileName, fmCreate);
  try
    fs.Write(ABuf, ABufSize);
  finally
    fs.Free();
  end;
end;

procedure StrToFile(AStr: AnsiString; AFileName: string);
begin
  if AStr = '' then Exit;
  BufToFile(AStr[1], Length(AStr), AFileName);
end;

// Returns Buffer content in HEX, capital letters, without spaces
// Example: 010ABC
function BufToHex(const Buffer; BufferSize: Integer): string;
var
  i: Integer;
  pb: PByte;
begin
  Result := '';
  pb := @Buffer;
  for i := 0 to BufferSize - 1 do
  begin
    Result := Result + IntToHex(pb^, 2);
    Inc(pb);
  end;
end;

// Example: [01 0A BC]
function BufferToHex(const Buffer; BufferSize: Integer): string;
var
  i: Integer;
  pb: PByte;
begin
  Result := '[';
  pb := @Buffer;
  for i := 0 to BufferSize - 1 do
  begin
    if i > 0 then Result := Result + ' ';
    Result := Result + IntToHex(pb^, 2);
    Inc(pb);
  end;
  Result := Result + ']';
end;

{ TDbRowsList }

function TDbRowsList.GetItem(AIndex: Integer): TDbRowItem;
begin
  Result := TDbRowItem(Get(AIndex));
end;

procedure TDbRowsList.Notify(Ptr: Pointer; Action: TListNotification);
begin
  inherited;
  if Action = lnDeleted then
    TDbRowItem(Ptr).Free;
end;

{procedure TDbRowsList.SetFieldInfo(var AIndex: Integer; AName: string; AType, ASize: Integer);
begin
  if AIndex >= Length(FieldsInfo) then
    SetLength(FieldsInfo, AIndex+1);
  FieldsInfo[AIndex].Name := AName;
  FieldsInfo[AIndex].VType := Byte(AType);
  FieldsInfo[AIndex].Size := ASize;
end; }

{ TDbRowItem }

constructor TDbRowItem.Create(AOwner: TDbRowsList);
begin
  inherited Create;
  FOwner := AOwner;
end;

function TDbRowItem.GetFieldAsStr(AFieldIndex: Integer): string;
var
  dt: TDateTime;
begin

  if VarIsNull(Values[AFieldIndex]) then
    Result := '<null>'
  else
  if VarIsType(Values[AFieldIndex], varDate) then
  begin
    dt := Values[AFieldIndex];
    if Trunc(dt) = 0 then
      Result := FormatDateTime('HH:NN:SS', dt)
    else if Frac(dt) = 0 then
      Result := FormatDateTime('YYYY-MM-DD', dt)
    else
      Result := FormatDateTime('YYYY-MM-DD HH:NN:SS', dt);
  end
  else
    Result := VarToStrDef(Values[AFieldIndex], '');
end;

{ TDBReader }

procedure TDBReader.AfterConstruction;
begin
  inherited;
end;

procedure TDBReader.BeforeDestruction;
begin
  FreeAndNil(FFile);
  inherited;
end;

function TDBReader.FillTableInfoText(ATableName: string; ALines: TStrings): Boolean;
begin
  Result := False;
end;

procedure TDBReader.LogInfo(AStr: string);
begin
  if Assigned(OnLog) then OnLog(AStr);
end;

function TDBReader.OpenFile(AFileName: string; AStream: TStream): Boolean;
begin
  Result := False;
  FreeAndNil(FFile);
  if not FileExists(AFileName) then Exit;
  if Assigned(AStream) then
    FFile := AStream
  else
    FFile := TFileStream.Create(AFileName, fmOpenRead + fmShareDenyNone);
  FileName := AFileName;
  Result := True;
end;

{ TDbReaderDataSet }

constructor TDbReaderDataSet.Create(AOwner: TComponent);
begin
  inherited;
  FIsOpen := False;
end;

procedure TDbReaderDataSet.InternalHandleException;
begin
  //raise Exception.Create('TDbReaderDataSet');
end;

procedure TDbReaderDataSet.InternalInitFieldDefs;
var
  i: Integer;
begin
  FieldDefs.Clear;
  for i := 0 to Length(FRowsList.FieldsDef) - 1 do
  begin
    with FieldDefs.AddFieldDef do
    begin
      DataType := FRowsList.FieldsDef[i].FieldType;
      FieldNo := i+1;
      Name := FRowsList.FieldsDef[i].Name;
      Size := FRowsList.FieldsDef[i].Size;
    end;
  end;
end;

procedure TDbReaderDataSet.InternalOpen;
begin
  InternalInitFieldDefs;
  if DefaultFields then
    CreateFields;
  BindFields(True);
  FIsOpen := True;
  FCursor := 0;
end;

function TDbReaderDataSet.IsCursorOpen: Boolean;
begin
  Result := FIsOpen;
end;

procedure TDbReaderDataSet.InternalClose;
begin
  BindFields(False); // unbind fields
  if DefaultFields then
    DestroyFields;
  FIsOpen := False;
end;

function TDbReaderDataSet.GetRecord(Buffer: PChar; GetMode: TGetMode; DoCheck: Boolean): TGetResult;
begin
  Result := grOK;
  case GetMode of
    gmPrior:
      if FCursor <= 1 then
        Result := grBOF
      else
        Dec(FCursor);
    gmNext:
      if FCursor >= RecordCount then
        Result := grEOF
      else
        Inc(FCursor);
    gmCurrent: if (FCursor < 1) or (FCursor > RecordCount) then
        Result := grError;
  end;
  if Result = grOK then
    PRecordBuffer(Buffer).RecordNum := FCursor;
  if (Result = grError) and DoCheck then
    DatabaseError('Error in GetRecord()');
end;

function TDbReaderDataSet.AllocRecordBuffer: PChar;
begin
  GetMem(Result, SizeOf(TRecordBuffer));
end;

procedure TDbReaderDataSet.FreeRecordBuffer(var Buffer: PChar);
begin
  FreeMem(Buffer, SizeOf(TRecordBuffer));
end;

procedure TDbReaderDataSet.InternalInitRecord(Buffer: PChar);
begin
  inherited;
end;

procedure TDbReaderDataSet.InternalFirst;
begin
  FCursor := 0;
end;

procedure TDbReaderDataSet.InternalLast;
begin
  FCursor := RecordCount + 1;
end;

procedure TDbReaderDataSet.InternalSetToRecord(Buffer: PChar);
begin
  FCursor := PRecordBuffer(Buffer)^.RecordNum;
end;

procedure TDbReaderDataSet.AssignRowsList(ARowsList: TDbRowsList);
begin
  FRowsList := ARowsList;
end;

function TDbReaderDataSet.GetCanModify: Boolean;
begin
  Result := False;
end;

function TDbReaderDataSet.GetFieldData(Field: TField; Buffer: Pointer): Boolean;
var
  RecItem: TDbRowItem;
  n: Integer;
begin
  Result := False;
  if not Assigned(FRowsList) or (Length(FRowsList.FieldsDef) < Field.FieldNo) then
    Exit;

  RecItem := FRowsList.GetItem(PRecordBuffer(ActiveBuffer)^.RecordNum-1);

  Result := True;
  n := Field.FieldNo-1;
  case FRowsList.FieldsDef[n].FieldType of
    ftString: PAnsiString(Buffer)^ := RecItem.Values[n];
    ftSmallint: PSmallInt(Buffer)^ := RecItem.Values[n];
    ftInteger: PInteger(Buffer)^ := RecItem.Values[n];
    ftWord: PWord(Buffer)^ := RecItem.Values[n];
    ftBoolean: PBoolean(Buffer)^ := RecItem.Values[n];
    ftFloat: PExtended(Buffer)^ := RecItem.Values[n];
    ftCurrency: PCurrency(Buffer)^ := RecItem.Values[n];
    ftDate: PDate(Buffer)^ := RecItem.Values[n];
    ftTime: PDateTime(Buffer)^ := RecItem.Values[n];
    ftDateTime: PDateTime(Buffer)^ := RecItem.Values[n];
    {todo: blobs}
  else
    Result := False;
  end;
end;

function TDbReaderDataSet.GetRecNo: Integer;
begin
  Result := PRecordBuffer(ActiveBuffer)^.RecordNum;
end;

function TDbReaderDataSet.GetRecordCount: Integer;
begin
  Result := FRowsList.Count;
end;

procedure TDbReaderDataSet.SetRecNo(Value: Integer);
begin
  if (Value < 1) or (Value >= RecordCount + 1) then
    exit;
  FCursor := Value;
  Resync([]);
end;

{ TRawDataReader }

procedure TRawDataReader.Init(AData: AnsiString; AStartPos: Integer);
begin
  Data := AData;
  if AStartPos = 0 then
    nPos := 1
  else
    nPos := AStartPos;
end;

function TRawDataReader.ReadInt16: SmallInt;
begin
  Result := 0;
  Move(Data[nPos], Result, SizeOf(Result));
  Inc(nPos, SizeOf(Result));
end;

function TRawDataReader.ReadInt32: Integer;
begin
  Result := 0;
  Move(Data[nPos], Result, SizeOf(Result));
  Inc(nPos, SizeOf(Result));
end;

function TRawDataReader.ReadInt8: ShortInt;
begin
  Result := 0;
  Move(Data[nPos], Result, SizeOf(Result));
  Inc(nPos, SizeOf(Result));
end;

function TRawDataReader.ReadUInt16: Word;
begin
  Result := 0;
  Move(Data[nPos], Result, SizeOf(Result));
  Inc(nPos, SizeOf(Result));
end;

function TRawDataReader.ReadUInt32: Cardinal;
begin
  Result := 0;
  Move(Data[nPos], Result, SizeOf(Result));
  Inc(nPos, SizeOf(Result));
end;

function TRawDataReader.ReadUInt8: Byte;
begin
  Result := 0;
  Move(Data[nPos], Result, SizeOf(Result));
  Inc(nPos, SizeOf(Result));
end;

{ TInnerFileStream }

constructor TInnerFileStream.Create(AFileName, AInnerFileName: string; APos, ASize: Int64);
begin
  inherited Create(AFileName, fmOpenRead or fmShareDenyNone);
  InnerFilePos := APos;
  InnerFileSize := ASize;
  InnerFileName := AInnerFileName;
  inherited Seek(InnerFilePos, soBeginning);
end;

function TInnerFileStream.Seek(const Offset: Int64; Origin: TSeekOrigin): Int64;
begin
  case Origin of
    soBeginning: Result := inherited Seek(InnerFilePos + Offset, Origin);
    soCurrent: Result := inherited Seek(Offset, Origin);
    soEnd: Result := inherited Seek(InnerFilePos + InnerFileSize - Offset, soBeginning);
  end;
  Dec(Result, InnerFilePos);
end;

end.
