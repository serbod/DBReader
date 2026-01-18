unit DBReader1S;
(*
1S data file (.1CD) reader
IS - where "I" is Roman number "1"

Author: Sergey Bodrov, 2026 Minsk
License: MIT

https://infostart.ru/1c/articles/19734/
https://infostart.ru/1c/articles/536343/


*)

interface

uses
  Types, Classes, SysUtils, DBReaderBase, DB;

type
  // Stream Page Header
  TIsPageHead = record
    PageID: Cardinal; // 0-based index of block in file
    ObjType: Integer;  // $FD1C or $01FD1C
    Ver1: Integer;
    Ver2: Integer;
    Ver3: Integer;
    Size: Int64;
    Pages: array of Cardinal; // up to (PageSize-24)/4 pages id
  end;

  TIsFieldDef = record
    Num: Integer;
    Name: string;
    DataType: Byte;
    IsNullable: Boolean;
    Length: Integer;
    Precision: Integer;
    IsCaseSense: Boolean;
    Size: Integer;       // real size
    Position: Integer;
  end;

  { TIsTableInfo }

  TIsTableInfo = class(TDbRowsList)
  public
    DataPageHead: TIsPageHead;
    BlobPageHead: TIsPageHead;
    IndexPageHead: TIsPageHead;
    RowCount: Integer;
    ColCount: Integer;
    RowSize: Integer;
    RecordLock: Boolean;
    FieldInfoArr: array of TIsFieldDef;
    TableDefStr: string;
    // contain no rows
    function IsEmpty(): Boolean; override;
    // predefined table
    function IsSystem(): Boolean; override;
    // not defined in metadata
    function IsGhost(): Boolean; override;

    // set table metadata from DefStr string
    procedure SetDefStr(AStr: string);
    // Example: {"IBVERSION","N",0,10,0,"CS"}
    procedure AddFieldDef(AStr: string);
  end;

  { TIsTableInfoList }

  TIsTableInfoList = class(TList)
  protected
    procedure Notify(Ptr: Pointer; Action: TListNotification); override;
  public
    function GetItem(AIndex: Integer): TIsTableInfo;
    function GetByName(AName: string): TIsTableInfo;
    //procedure SortByName();
  end;

  { TDBReaderIS }

  TDBReaderIS = class(TDBReader)
  private
    FTableList: TIsTableInfoList;
    FFileVerArr: array [0..3] of Integer;
    FPageSize: Integer;
    FBlobChunksOnPage: Integer;
    FMaxPagesInHead: Integer;
    FRootPageHead: TIsPageHead;

    // leaf page (page of PageId list)
    FLeafPageBuf: TByteDynArray;
    FLeafPageId: Cardinal;

    // APageHead.PageID must be set
    function ReadPageHead(var APageHead: TIsPageHead): Boolean;

    // read raw row data
    function ReadRowData(var ABuf: TByteDynArray; ARowPos: Int64;
      ATableInfo: TIsTableInfo; AList: TDbRowsList): Boolean;

    function ReadBlob(const APageHead: TIsPageHead; AChunkId, ABlobSize: Cardinal; var AData: AnsiString): Boolean;

    function ReadRoot(): Boolean;
    // read root data 8.3.8
    function ReadRoot838(): Boolean;

  public
    FileVerStr: string;

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

    property TableList: TIsTableInfoList read FTableList;
  end;

implementation

const
  IS_DEF_PAGE_SIZE   = $1000;
  IS_BLOB_CHUNK_SIZE = $100;
  IS_OBJ_TYPE_FREE   = $FF1C;  // free pages list
  IS_OBJ_TYPE_SMALL  = $FD1C;  // small object (pages list)
  IS_OBJ_TYPE_LARGE  = $01FD1C;  // large object (pages list of pages lists)

  IS_FILE_SIGNAT = '1CDBMSV8';
  IS_OBJ_SIGNAT  = '1CDBOBV8';

  // Data types
  IS_DATA_TYPE_NONE     = $00; // not defined
  IS_DATA_TYPE_BINARY   = $01; // 'B' Size=Len
  IS_DATA_TYPE_BOOL     = $02; // 'L' Size=1
  IS_DATA_TYPE_NUMBER   = $03; // 'N' Size=(Len+2)/2  (byte = 2 digits)
  IS_DATA_TYPE_CHAR     = $04; // 'NC' Size=Len*2  unicode
  IS_DATA_TYPE_VARCHAR  = $05; // 'NVC' Size=Len*2 + 2 unicode
  IS_DATA_TYPE_VERSION  = $06; // 'RV' Size=4*4  (first field)
  IS_DATA_TYPE_TEXT     = $07; // 'NT' Size=8  BlobPageId, BlobSize
  IS_DATA_TYPE_BLOB     = $08; // 'I' Size=8  BlobPageId, BlobSize
  IS_DATA_TYPE_DATETIME = $09; // 'DT' Size=7  YYMDHNS  (byte = 2 digits)
  IS_DATA_TYPE_LOCK     = $0A; // 'RL' Size=4*2  (first field, if no 'RV' field)

type
  TIsBlobChunk = record
    NextPageId: Cardinal;
    Size: Integer;  // Int16
    Data: AnsiString;
  end;

  TIsRootHead = record
    Lang: AnsiString;
    TabCount: Integer;
    TabChunks: array of Cardinal;
  end;

function DataTypeToStr(AType, ASize: Integer): string;
begin
  case AType of
    IS_DATA_TYPE_BINARY:   Result := Format('BINARY(%d)', [ASize]);
    IS_DATA_TYPE_BOOL:     Result := 'BOOL';
    IS_DATA_TYPE_NUMBER:   Result := Format('NUMBER(%d)', [ASize]);
    IS_DATA_TYPE_CHAR:     Result := Format('CHAR(%d)', [ASize]);
    IS_DATA_TYPE_VARCHAR:  Result := Format('VARCHAR(%d)', [ASize]);
    IS_DATA_TYPE_VERSION:  Result := 'VERSION';
    IS_DATA_TYPE_TEXT:     Result := 'TEXT';
    IS_DATA_TYPE_BLOB:     Result := 'BLOB';
    IS_DATA_TYPE_DATETIME: Result := 'DATETIME';
    IS_DATA_TYPE_LOCK:     Result := 'LOCK';
  else
    Result := 'UNKNOWN_'+IntToStr(AType);
  end;
end;

function DataTypeToDbFieldType(AType: Integer): TFieldType;
begin
  case AType of
    IS_DATA_TYPE_BINARY:   Result := ftBytes;
    IS_DATA_TYPE_BOOL:     Result := ftBoolean;
    IS_DATA_TYPE_NUMBER:   Result := ftFloat;  // ftBCD ???
    IS_DATA_TYPE_CHAR:     Result := ftString;
    IS_DATA_TYPE_VARCHAR:  Result := ftString;
    IS_DATA_TYPE_VERSION:  Result := ftBytes;
    IS_DATA_TYPE_TEXT:     Result := ftMemo;
    IS_DATA_TYPE_BLOB:     Result := ftBlob;
    IS_DATA_TYPE_DATETIME: Result := ftDateTime;
    IS_DATA_TYPE_LOCK:     Result := ftString;
  else
    Result := ftUnknown;
  end;
end;

function GetBcdFromByte(bt: Byte): Integer;
begin
  Result := 0;
  if (bt and $0F) <= $09 then
    Result := (bt and $0F);
  if (bt and $F0) <= $90 then
    Result := Result + (((bt and $F0) shr 4) * 10);
end;

// Returns list text from "{" to "}" including sub-lists
function GetListStr(const AStr: string; AStartPos: Integer): string;
var
  iMax, iPos, iDepth, iCount: Integer;
begin
  Result := '';
  iDepth := 0;
  iCount := 0;
  iMax := Length(AStr);
  iPos := AStartPos;
  while iPos <= iMax do
  begin
    case AStr[iPos] of
      '{':
      begin
        Inc(iDepth);
        if iDepth = 1 then
          AStartPos := iPos;
      end;
      '}':
      begin
        Dec(iDepth);
        if iDepth = 0 then
        begin
          iCount := iPos - AStartPos + 1;  // include "}"
          Break;
        end;
      end;
    end;
    Inc(iPos);
  end;

  if iCount > 0 then
    Result := Copy(AStr, AStartPos, iCount);
end;

// Return list value from "{" or "," position to "," or "}"
// On success, APos changed to end of value
// Remove quotes " from string value
function GetNextListStrValue(const AStr: string; var APos: Integer; out AValue: string): Boolean;
var
  iMax, iPos, iFrom: Integer;
begin
  Result := False;
  AValue := '';

  iFrom := 0;
  iMax := Length(AStr);
  iPos := APos;
  while iPos <= iMax do
  begin
    if (iFrom = 0) and (AStr[iPos] in ['{', ',']) then
      iFrom := iPos + 1
    else
    if (iFrom > 0) and (AStr[iPos] = '{') then
    begin
      // value is list
      AValue := GetListStr(AStr, iPos);
      APos := iFrom + Length(AValue);
      Result := True;
      Exit;
    end
    else
    if (iFrom > 0) and (AStr[iPos] in ['}', ',']) then
    begin
      if AStr[iPos] = '}' then
        Inc(iPos);
      if (iPos - iFrom) > 0 then
      begin
        AValue := Copy(AStr, iFrom, (iPos - iFrom));
        if AValue[1] = '"' then
          AValue := AnsiDequotedStr(AValue, '"');
      end;
      APos := iPos;
      Result := True;
      Exit;
    end;
    Inc(iPos);
  end;

end;

{ TIsTableInfo }

function TIsTableInfo.IsEmpty(): Boolean;
begin
  Result := (RowCount = 0);
end;

function TIsTableInfo.IsSystem(): Boolean;
begin
  Result := inherited IsSystem();
end;

function TIsTableInfo.IsGhost(): Boolean;
begin
  Result := (TableDefStr = '');
end;

procedure TIsTableInfo.SetDefStr(AStr: string);
var
  i, iPosTab, iPos: Integer;
  s, sTab, sName, sField: AnsiString;
begin
  DataPageHead.PageID := 0;
  BlobPageHead.PageID := 0;
  IndexPageHead.PageID := 0;
  RecordLock := False;
  FieldInfoArr := [];
  FieldsDef := [];
  RowCount := -1;

  TableDefStr := Trim(AStr);
  (* table definition example
  {"IBVERSION",0,
  {"Fields",
  {"IBVERSION","N",0,10,0,"CS"},
  {"PLATFORMVERSIONREQ","N",0,10,0,"CS"}
  },
  {"Indexes"},
  {"Recordlock","0"},
  {"Files",4,0,0}
  }
  *)

  // parse table definition
  iPosTab := 1;
  if GetNextListStrValue(TableDefStr, iPosTab, sTab) then
  begin
    TableName := sTab;
    while GetNextListStrValue(TableDefStr, iPosTab, sTab) do
    begin
      if (sTab <> '') and (sTab[1] = '{') then
      begin
        iPos := 1;
        GetNextListStrValue(sTab, iPos, sName);
        if sName = 'Fields' then
        begin
          while GetNextListStrValue(sTab, iPos, sField) do
          begin
            sField := Trim(sField);
            AddFieldDef(sField);
          end;
        end
        else
        if sName = 'Files' then
        begin
          if GetNextListStrValue(sTab, iPos, s) then // data
            DataPageHead.PageID := StrToIntDef(s, 0);
          if GetNextListStrValue(sTab, iPos, s) then // blob
            BlobPageHead.PageID := StrToIntDef(s, 0);
          if GetNextListStrValue(sTab, iPos, s) then // index
            IndexPageHead.PageID := StrToIntDef(s, 0);
        end
        else
        if sName = 'Recordlock' then
        begin
          if GetNextListStrValue(sTab, iPos, s) then // data
            RecordLock := (s = '1');
        end;
      end;
    end;
  end;

  // todo: insert RecordLock field, if RecordLock=1 and no 'RV' field present

  // sync fields defs
  SetLength(FieldsDef, Length(FieldInfoArr));
  RowSize := 1;
  iPos := 1; // skip first byte
  for i := Low(FieldsDef) to High(FieldsDef) do
  begin
    FieldsDef[i].Name := FieldInfoArr[i].Name;
    FieldsDef[i].FieldType := DataTypeToDbFieldType(FieldInfoArr[i].DataType);
    FieldsDef[i].TypeName := DataTypeToStr(FieldInfoArr[i].DataType, FieldInfoArr[i].Length);
    FieldsDef[i].Size := FieldInfoArr[i].Size;
    FieldInfoArr[i].Position := iPos;
    FieldsDef[i].RawOffset := FieldInfoArr[i].Position;
    Inc(iPos, FieldInfoArr[i].Size);
    Inc(RowSize, FieldInfoArr[i].Size);
  end;
  // rows can't be less 5 bytes
  if RowSize < 5 then
    RowSize := 5;
end;

procedure TIsTableInfo.AddFieldDef(AStr: string);
var
  sName, sType, sNullable, sLength, sPrecision, sCaseSense: string;
  iPos, n, iType, iSize: Integer;
begin
  // Example: {"IBVERSION","N",0,10,0,"CS"}
  iPos := 1;
  // Name
  if not GetNextListStrValue(AStr, iPos, sName) then Exit;
  // DataType
  if not GetNextListStrValue(AStr, iPos, sType) then Exit;
  // Nullable
  if not GetNextListStrValue(AStr, iPos, sNullable) then Exit;
  // Length
  if not GetNextListStrValue(AStr, iPos, sLength) then Exit;
  // Precision
  if not GetNextListStrValue(AStr, iPos, sPrecision) then Exit;
  // CaseSensitive
  if not GetNextListStrValue(AStr, iPos, sCaseSense) then Exit;

  iType := 0;
  if sType = 'B' then iType := IS_DATA_TYPE_BINARY
  else if sType = 'L'   then iType := IS_DATA_TYPE_BOOL
  else if sType = 'N'   then iType := IS_DATA_TYPE_NUMBER
  else if sType = 'NC'  then iType := IS_DATA_TYPE_CHAR
  else if sType = 'NVC' then iType := IS_DATA_TYPE_VARCHAR
  else if sType = 'RV'  then iType := IS_DATA_TYPE_VERSION
  else if sType = 'NT'  then iType := IS_DATA_TYPE_TEXT
  else if sType = 'I'   then iType := IS_DATA_TYPE_BLOB
  else if sType = 'DT'  then iType := IS_DATA_TYPE_DATETIME;

  // real field size
  iSize := StrToIntDef(sLength, 0);
  case iType of
    IS_DATA_TYPE_BOOL:     iSize := 1;
    IS_DATA_TYPE_NUMBER:   iSize := (iSize + 2) div 2;
    IS_DATA_TYPE_CHAR:     iSize := iSize * 2;
    IS_DATA_TYPE_VARCHAR:  iSize := (iSize * 2) + 2;
    IS_DATA_TYPE_VERSION:  iSize := 4*4;
    IS_DATA_TYPE_TEXT,
    IS_DATA_TYPE_BLOB:     iSize := 8;
    IS_DATA_TYPE_DATETIME: iSize := 7;
  end;
  if (sNullable = '1') then
    Inc(iSize);

  n := Length(FieldInfoArr);
  SetLength(FieldInfoArr, n+1);
  FieldInfoArr[n].Num := n+1;
  FieldInfoArr[n].Name := sName;
  FieldInfoArr[n].DataType := iType;
  FieldInfoArr[n].IsNullable := (sNullable = '1');
  FieldInfoArr[n].Length := StrToIntDef(sLength, 0);
  FieldInfoArr[n].Precision := StrToIntDef(sPrecision, 0);
  FieldInfoArr[n].IsCaseSense := (sCaseSense = 'CS');
  FieldInfoArr[n].Size := iSize;
  FieldInfoArr[n].Position := 0;  // will be set later
end;

{ TIsTableInfoList }

procedure TIsTableInfoList.Notify(Ptr: Pointer; Action: TListNotification);
begin
  inherited;
  if Action = lnDeleted then
    TIsTableInfo(Ptr).Free;
end;

function TIsTableInfoList.GetItem(AIndex: Integer): TIsTableInfo;
begin
  Result := TIsTableInfo(Get(AIndex));
end;

function TIsTableInfoList.GetByName(AName: string): TIsTableInfo;
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

{ TDBReaderIS }

function TDBReaderIS.ReadPageHead(var APageHead: TIsPageHead): Boolean;
var
  PageBuf: TByteDynArray;
  rdr: TRawDataReader;
  i, nPages, iLast: Integer;
begin
  Result := False;
  if APageHead.PageID = 0 then
    Exit;

  // init reader
  PageBuf := [];
  SetLength(PageBuf, FPageSize);
  FFile.Position := APageHead.PageID * FPageSize;
  FFile.Read(PageBuf[0], FPageSize);
  rdr.Init(PageBuf[0], False);

  // read page header
  APageHead.ObjType := rdr.ReadInt32();
  APageHead.Ver1 := rdr.ReadInt32();
  APageHead.Ver2 := rdr.ReadInt32();
  APageHead.Ver3 := rdr.ReadInt32();
  APageHead.Size := rdr.ReadInt64();

  iLast := -1;
  if APageHead.ObjType = IS_OBJ_TYPE_SMALL then
  begin
    nPages := (FPageSize - 24) div 4;
    SetLength(APageHead.Pages, nPages);
    for i := 0 to nPages-1 do
    begin
      APageHead.Pages[i] := rdr.ReadUInt32();
      if APageHead.Pages[i] <> 0 then
        iLast := i;
    end;
  end;

  // trim empty pages
  SetLength(APageHead.Pages, iLast + 1);

  Result := True;
end;

function TDBReaderIS.ReadRowData(var ABuf: TByteDynArray; ARowPos: Int64;
  ATableInfo: TIsTableInfo; AList: TDbRowsList): Boolean;
var
  rdr: TRawDataReader;
  i, iPrevPos, iCol, iSize, iDataLen: Integer;
  nItemId, nSize: Cardinal;
  bt, btType: Byte;
  TmpRow: TDbRowItem;
  v: Variant;
  s, sData: AnsiString;
  w, w2, w3: Word;
  dt: TDateTime;
begin
  Result := True;
  rdr.Init(ABuf[0]);

  bt := rdr.ReadUInt8;
  if bt = 1 then
  begin
    // empty record
    nItemId := rdr.ReadUInt32;  // next empty record index
    Exit;
  end;

  TmpRow := TDbRowItem.Create(AList);
  AList.Add(TmpRow);

  if IsDebugRows then
  begin
    iPrevPos := rdr.GetPosition();
    rdr.SetPosition(0);
    TmpRow.RawData := rdr.ReadBytes(ATableInfo.RowSize);
    rdr.SetPosition(iPrevPos);
  end;

  SetLength(TmpRow.Values, Length(ATableInfo.FieldInfoArr));
  for iCol := Low(ATableInfo.FieldInfoArr) to High(ATableInfo.FieldInfoArr) do
  begin
    btType := ATableInfo.FieldInfoArr[iCol].DataType;
    iSize := ATableInfo.FieldInfoArr[iCol].Size;
    rdr.SetPosition(ATableInfo.FieldInfoArr[iCol].Position);

    v := Null;
    if ATableInfo.FieldInfoArr[iCol].IsNullable then
    begin
      bt := rdr.ReadUInt8;
      if bt = 0 then
      begin
        TmpRow.Values[iCol] := v;
        Continue;
      end;
    end;

    case btType of
      IS_DATA_TYPE_BINARY:
      begin
        if iSize = 16 then // GUID
        begin
          // AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE
          // DDDD EEEE EEEE EEEE CCCC BBBB AAAA AAAA
          sData := rdr.ReadBytes(iSize);
          s := Format('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s',
            [
              sData[13], sData[14], // AA
              sData[15], sData[16],
              sData[11], sData[12], // BB
              sData[09], sData[10], // CC
              sData[01], sData[02], // DD
              sData[03], sData[04], // EE
              sData[05], sData[06],
              sData[07], sData[08]
            ]);
        end
        else
          s := rdr.ReadBytes(iSize);
        v := s;
      end;

      IS_DATA_TYPE_BOOL:
        v := (rdr.ReadUInt8() <> 0);

      IS_DATA_TYPE_NUMBER:
      begin
        sData := rdr.ReadBytes(iSize);
        s := '';
        w2 := 0; // sign -
        for i := 1 to Length(sData) do
        begin
          w := GetBcdFromByte(Ord(sData[i]));
          if (i = 1) then // sign
          begin
            if (w >= 10) then
            begin
              w2 := 1;
              w := w - 10;
            end;
          end;
          s := s + IntToStr(w);
        end;
        // remove trailing digit
        if Length(s) > ATableInfo.FieldInfoArr[iCol].Length then
          SetLength(s, ATableInfo.FieldInfoArr[iCol].Length);
        // remove leading zeroes (slow!!!)
        // todo: optimize
        while Copy(s, 1, 1) = '0' do
          s := Copy(s, 2, MaxInt);
        if s = '' then
          s := '0';
        // precision dot
        w := ATableInfo.FieldInfoArr[iCol].Precision;
        if (w > 0) then
        begin
          // w=2
          // 123
          if w >= Length(s) then
            s := '0.'+ s
          else
            System.Insert('.', s, Length(s)-w+1);
        end;
        if w2 = 0 then
          s := '-' + s;
        v := s;
      end;

      IS_DATA_TYPE_CHAR:
      begin
        s := rdr.ReadBytes(iSize);
        s := WideDataToStr(s);
        v := s;
      end;

      IS_DATA_TYPE_VARCHAR:
      begin
        iDataLen := rdr.ReadUInt16;
        s := rdr.ReadBytes(iDataLen * 2);
        s := WideDataToStr(s);
        v := s;
      end;

      IS_DATA_TYPE_VERSION:
        v := rdr.ReadBytes(iSize);

      IS_DATA_TYPE_TEXT,
      IS_DATA_TYPE_BLOB:
      begin
        nItemId := rdr.ReadUInt32;
        nSize := rdr.ReadUInt32;
        s := '';
        if ReadBlob(ATableInfo.BlobPageHead, nItemId, nSize, s) then
        begin
          if btType = IS_DATA_TYPE_TEXT then
            s := WideDataToStr(s);
          v := s;
        end;
      end;

      IS_DATA_TYPE_DATETIME:
      begin
        s := rdr.ReadBytes(iSize);
        // YYMD
        w := GetBcdFromByte(Ord(s[1])) * 100;
        w := w + GetBcdFromByte(Ord(s[2]));
        w2 := GetBcdFromByte(Ord(s[3]));
        w3 := GetBcdFromByte(Ord(s[4]));
        if TryEncodeDate(w, w2, w3, dt) then
        begin
          v := dt;
          // HMS
          w := GetBcdFromByte(Ord(s[5]));
          w2 := GetBcdFromByte(Ord(s[6]));
          w3 := GetBcdFromByte(Ord(s[7]));
          if TryEncodeTime(w, w2, w3, 0, dt) then
            v := v + dt;
        end;
      end;

      IS_DATA_TYPE_LOCK:
        v := rdr.ReadBytes(iSize);

    end;

    TmpRow.Values[iCol] := v;
  end;
end;

function TDBReaderIS.ReadBlob(const APageHead: TIsPageHead; AChunkId, ABlobSize: Cardinal;
  var AData: AnsiString): Boolean;
var
  PageBuf: TByteDynArray;
  rdr, rdr2: TRawDataReader;
  NextChunkId: Cardinal;
  iPage, iPrevPage, iChunkPos, iChunkSize: Integer;
  iPageA, iPageB, iPageAId, iPageBId: Integer;
  iPagePos, iPagePosPrev, iPageAPos: Int64;
  //BlobChunk: TIsBlobChunk;
begin
  Result := False;
  AData := '';
  if Length(APageHead.Pages) = 0 then
    Exit;

  PageBuf := [];

  iPagePosPrev := 0;
  NextChunkId := AChunkId;
  while NextChunkId <> 0 do
  begin
    // in which page is next chunk
    iPage := NextChunkId div FBlobChunksOnPage;

    if APageHead.ObjType = IS_OBJ_TYPE_SMALL then
    begin
      if iPage >= Length(APageHead.Pages) then
      begin
        LogInfo('Error: blob page out of list');
        Exit;
      end;
      iPagePos := APageHead.Pages[iPage] * FPageSize;
    end
    else
    if APageHead.ObjType = IS_OBJ_TYPE_LARGE then
    begin
      iPageA := iPage div FMaxPagesInHead; // tree
      iPageB := iPage mod FMaxPagesInHead; // leaf

      if iPageA >= Length(APageHead.Pages) then
      begin
        LogInfo('Error: blob head page out of list');
        Exit;
      end;
      iPageAId := APageHead.Pages[iPageA];
      iPageAPos := iPageAId * FPageSize;

      // read leaf page
      if FLeafPageId <> iPageAId then
      begin
        FLeafPageId := iPageAId;
        // init leaf page reader
        FFile.Position := iPageAPos;
        FFile.Read(FLeafPageBuf[0], FPageSize);
      end;
      rdr2.Init(FLeafPageBuf[0]);

      // get actual page position
      rdr2.SetPosition(iPageB * 4);
      iPageBId := rdr2.ReadUInt32;
      iPagePos := iPageBId * FPageSize;
    end;

    if iPagePos <> iPagePosPrev then
    begin
      // init reader
      SetLength(PageBuf, FPageSize);
      FFile.Position := iPagePos;
      FFile.Read(PageBuf[0], FPageSize);
      rdr.Init(PageBuf[0], False);
    end;
    iPagePosPrev := iPagePos;

    // read blob chunk
    iChunkPos := (NextChunkId mod FBlobChunksOnPage) * IS_BLOB_CHUNK_SIZE;
    rdr.SetPosition(iChunkPos);
    NextChunkId := rdr.ReadUInt32;
    iChunkSize := rdr.ReadUInt16;
    AData := AData + rdr.ReadBytes(iChunkSize);
    if (ABlobSize > 0) and (Length(AData) > ABlobSize) then
    begin
      SetLength(AData, ABlobSize);
      Break;
    end;
  end;

  Result := True;
end;

function TDBReaderIS.ReadRoot(): Boolean;
begin
  Result := False;
  if (FFileVerArr[0] <> 8) then
    Exit;
  if ( (FFileVerArr[1] > 3) or ((FFileVerArr[1] = 3) and (FFileVerArr[2] >= 8)) )
  then
  begin
    // new root format
    Result := ReadRoot838();
    Exit;
  end;

  // old root format
  LogInfo('Only 8.3.8+ version supported!');
end;

function TDBReaderIS.ReadRoot838(): Boolean;
var
  rdr: TRawDataReader;
  i, iRootSize: Integer;
  RootHead: TIsRootHead;
  RootData: AnsiString;
  s: string;
  TmpTab: TIsTableInfo;
begin
  Result := False;
  FRootPageHead.PageID := 2;
  if not ReadPageHead(FRootPageHead) then
    Exit;

  RootData := '';
  if not ReadBlob(FRootPageHead, 1, 0, RootData) then
    Exit;

  if RootData = '' then
    Exit;

  // root head
  iRootSize := Length(RootData);
  rdr.Init(RootData[1]);
  RootHead.Lang := rdr.ReadCString(32, True);
  RootHead.TabCount := rdr.ReadInt32;
  if RootHead.TabCount > 100500 then
  begin
    LogInfo('Error: Over 100500 root tables!');
    Exit;
  end;
  if RootHead.TabCount > ((iRootSize-36) div 4) then
  begin
    LogInfo('Error: Root tables count more, than data size!');
    Exit;
  end;
  SetLength(RootHead.TabChunks, RootHead.TabCount);
  for i := 0 to RootHead.TabCount-1 do
  begin
    RootHead.TabChunks[i] := rdr.ReadUInt32;
  end;

  // read tables
  for i := 0 to RootHead.TabCount-1 do
  begin
    RootData := '';
    if not ReadBlob(FRootPageHead, RootHead.TabChunks[i], 0, RootData) then
    begin
      LogInfo('Error: Root table not readed: ' + IntToStr(i));
      Continue;
    end;

    if RootData <> '' then
    begin
      rdr.Init(RootData[1]);
      s := rdr.ReadCString(Length(RootData));
      //LogInfo(Format('=== Table[%d]: %s', [i, s]));
      TmpTab := TIsTableInfo.Create();
      TmpTab.SetDefStr(s);
      TableList.Add(TmpTab);
    end;
  end;

  Result := True;
end;

procedure TDBReaderIS.AfterConstruction();
begin
  inherited AfterConstruction();
  FTableList := TIsTableInfoList.Create();
end;

procedure TDBReaderIS.BeforeDestruction();
begin
  FreeAndNil(FTableList);
  inherited BeforeDestruction();
end;

function TDBReaderIS.OpenFile(AFileName: string; AStream: TStream): Boolean;
var
  PageBuf: TByteDynArray;
  rdr: TRawDataReader;
  s: AnsiString;
begin
  Result := inherited OpenFile(AFileName, AStream);
  if not Result then Exit;
  Result := False;
  PageBuf := [];

  // init reader
  SetLength(PageBuf, IS_DEF_PAGE_SIZE);
  FFile.Position := 0;
  FFile.Read(PageBuf[0], IS_DEF_PAGE_SIZE);
  rdr.Init(PageBuf[0], False);

  // read header
  s := rdr.ReadBytes(8);  // '1CDBMSV8'
  if s <> '1CDBMSV8' then
  begin
    LogInfo('Error: Wrong file signature!');
    Exit;
  end;
  FFileVerArr[0] := rdr.ReadUInt8;
  FFileVerArr[1] := rdr.ReadUInt8;
  FFileVerArr[2] := rdr.ReadUInt8;
  FFileVerArr[3] := rdr.ReadUInt8;

  FileVerStr := Format('%d.%d.%d.%d', [FFileVerArr[0], FFileVerArr[1], FFileVerArr[2], FFileVerArr[3]]); // version
  rdr.ReadUInt32;  // page count
  rdr.ReadInt32;  // ? = 1
  FPageSize := rdr.ReadInt32;  // block size
  if FPageSize <= 0 then
    FPageSize := IS_DEF_PAGE_SIZE
  else if (FPageSize mod $1000) <> 0 then
  begin
    LogInfo(Format('Error: Wrong page size=$%x', [FPageSize]));
    Exit;
  end;
  FBlobChunksOnPage := FPageSize div IS_BLOB_CHUNK_SIZE;
  FMaxPagesInHead := (FPageSize - 24) div 4;
  SetLength(FLeafPageBuf, FPageSize);

  LogInfo(Format('File version=%s  page_size=%d', [FileVerStr, FPageSize]));

  // read root
  Result := ReadRoot();

end;

procedure TDBReaderIS.ReadTable(AName: string; ACount: Int64;
  AList: TDbRowsList);
var
  TmpTable: TIsTableInfo;
  PageBuf, RowBuf: TByteDynArray;
  i, iRowSize, iRowOffs, iRowRest: Integer;
  iPagePos, iRowPos, iRowPosMax: Int64;
begin
  TmpTable := FTableList.GetByName(AName);
  if not Assigned(TmpTable) then
  begin
    LogInfo('!Table not found: ' + AName);
    Exit;
  end;

  // read page map
  if (TmpTable.DataPageHead.PageID <> 0) and (Length(TmpTable.DataPageHead.Pages) = 0) then
    ReadPageHead(TmpTable.DataPageHead);
  if (TmpTable.BlobPageHead.PageID <> 0) and (Length(TmpTable.BlobPageHead.Pages) = 0) then
    ReadPageHead(TmpTable.BlobPageHead);

  if not Assigned(AList) then
    AList := TmpTable
  else
  begin
    AList.Clear();
    AList.FieldsDef := TmpTable.FieldsDef;

    iRowSize := TmpTable.RowSize;
    iRowPosMax := TmpTable.DataPageHead.Size;
    SetLength(RowBuf, iRowSize);
    iRowPos := 0;
    iRowRest := 0; // unreaded part of row

    // read pages
    SetLength(PageBuf, FPageSize);
    for i := Low(TmpTable.DataPageHead.Pages) to High(TmpTable.DataPageHead.Pages) do
    begin
      iPagePos := (TmpTable.DataPageHead.Pages[i]) * FPageSize;
      if iPagePos + FPageSize <= FFile.Size then
      begin
        FFile.Position := iPagePos;
        FFile.Read(PageBuf[0], FPageSize);

        iRowOffs := iRowRest;
        // read remaining part of row
        if iRowRest > 0 then
        begin
          Move(PageBuf[0], RowBuf[iRowSize-iRowRest], iRowRest);
          ReadRowData(RowBuf, iRowPos, TmpTable, AList);
          iRowRest := 0;
          Inc(iRowPos, iRowSize);
          if iRowPos >= iRowPosMax then
            Break;
        end;

        // read rows
        while iRowOffs < FPageSize do
        begin
          if (iRowOffs + iRowSize) <= FPageSize then
          begin
            Move(PageBuf[iRowOffs], RowBuf[0], iRowSize);
            ReadRowData(RowBuf, iRowPos, TmpTable, AList);
            Inc(iRowPos, iRowSize);
            if iRowPos >= iRowPosMax then
              Break;
          end
          else
          begin
            // row not fit in page
            iRowRest := FPageSize - iRowOffs; // size of row part on this page
            Move(PageBuf[iRowOffs], RowBuf[0], iRowRest);
            iRowRest := iRowSize - iRowRest; // size of row part on next page
            //Break;
          end;
          Inc(iRowOffs, iRowSize);
        end;

        if iRowPos >= iRowPosMax then
          Break;
      end
      else
        Assert(False, 'Page out of file: ' + IntToStr(TmpTable.DataPageHead.Pages[i]));

      if Assigned(OnPageReaded) then
        OnPageReaded(Self);
    end;
  end;

end;

function TDBReaderIS.FillTableInfoText(ATableName: string; ALines: TStrings): Boolean;
var
  i: Integer;
  TmpTable: TIsTableInfo;
  s: string;
begin
  Result := False;
  TmpTable := TableList.GetByName(ATableName);
  if not Assigned(TmpTable) then
    Exit;

  ALines.Add(Format('== Table Name=%s  RowCount=%d', [TmpTable.TableName, TmpTable.RowCount]));
  ALines.Add(Format('RowSize=%d', [TmpTable.RowSize]));
  ALines.Add(Format('DataSize=%d', [TmpTable.DataPageHead.Size]));

  ALines.Add(Format('== FieldInfo  Count=%d', [Length(TmpTable.FieldInfoArr)]));
  for i := Low(TmpTable.FieldInfoArr) to High(TmpTable.FieldInfoArr) do
  begin
    s := Format('%.2d Name=%-20s  %s  Size=%d',
      [i,
        TmpTable.FieldInfoArr[i].Name,
        DataTypeToStr(TmpTable.FieldInfoArr[i].DataType, TmpTable.FieldInfoArr[i].Length),
        TmpTable.FieldInfoArr[i].Size
      ]);
    ALines.Add(s);
  end;

  // DefStr can be multiline
  ALines.Add('== TableDef ==');
  ALines.Add(TmpTable.TableDefStr);
end;

function TDBReaderIS.GetTablesCount(): Integer;
begin
  Result := FTableList.Count;
end;

function TDBReaderIS.GetTableByIndex(AIndex: Integer): TDbRowsList;
begin
  Result := FTableList.GetItem(AIndex);
end;

end.

