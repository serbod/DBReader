unit DBReaderFirebird;

(*
Interbase/FireBird (GDB, FDB) database file reader

Author: Sergey Bodrov, 2024
License: MIT

Thanks to Norman Dunbar for database structure explanation!
https://www.ibexpert.net/ibe/pmwiki.php?n=Doc.StructureOfADataPage

-- tested versions:
InterBase 6  (ODS 10)
InterBase 7  (ODS 11)
FireBird 2   (ODS 11)
FireBird 2.5 (ODS 11.2)
*)

{$A8}  // align fields in records by 8 bytes

interface

uses
  Windows, SysUtils, Classes, Variants, DBReaderBase, DB;

type
  TDBReaderFB = class;
  TRDB_RelationsItem = class;
  TRDB_RowsList = class;

  TRDB_FieldInfoRec = record
    Size: Integer;
    FieldType: Word;     // from FIELDS
    FieldLength: Word;   // data length, not size
    FieldSubType: Integer;
    Name: string;
    DType: Byte;         // from FORMATS
    Scale: Integer;
    Length: Word;        // from FORMATS
    SubType: Word;
    Flags: Cardinal;
    Offset: Cardinal;

    function AsString(): string;
    function GetSize(): Integer;
  end;

  TRDB_Blob = record
    RelID: Integer;
    RowID: Cardinal;
    BlobType: Integer;
    Data: AnsiString;
  end;

  TRDB_RowItem = class(TDbRowItem)
  protected
    FDataPos: Integer;

    FReader: TDBReaderFB;
    FRelID: Integer;
    FRowID: Cardinal;    // (PageSeq * FMaxRecPerPage) + RecIndex
    function ReadInt(ABytes: Integer = 4): Integer;
    function ReadInt64(ABytes: Integer = 8): Int64;
    function ReadCurrency(ABytes: Integer = 8): Currency;
    function ReadFloat(ABytes: Integer = 8): Double;
    // AType 0-DateTime 1-Date 2-Time
    function ReadDateTime(AType: Integer = 0; ABytes: Integer = 8): TDateTime;
    function ReadBlob(AType: Integer = 0; ASize: Integer = 0): TRDB_Blob;
    function ReadVarChar(ACount: Integer; ASize: Integer = 0): string;
    function ReadChar(ACount: Integer): string;
    function IsNullValue(AIndex: Integer): Boolean;
    function IsBlobValue(AIndex: Integer): Boolean;
  public
    TableInfo: TRDB_RelationsItem;
    constructor Create(AOwner: TDbRowsList; AReader: TDBReaderFB; ARelID: Integer; ARowID: Cardinal); reintroduce;
    procedure ReadRawData(const AData; ALen: Integer);
    procedure ReadData(const AData: AnsiString); virtual;
    function GetAsText(): string; virtual;
    function GetBlobValue(var AValue: TRDB_Blob): AnsiString;
    function GetFieldAsStr(AFieldIndex: Integer): string; override;
    function GetValueByName(AName: string): Variant; virtual;

    property Reader: TDBReaderFB read FReader;
    property RelID: Integer read FRelID;
    property RowID: Cardinal read FRowID;
  end;
  TRDB_RowClass = class of TRDB_RowItem;

  TRDB_RowsList = class(TDbRowsList)
  protected
    // set FieldsInfo[AIndex] properties for system tables
    procedure SetFieldInfo(var AIndex: Integer; var AOffs: Cardinal; AName: string; AType, ALen, ASub: Word);
    // fill FieldsInfo for system table by ARelID
    procedure FillSysTableInfo(ARelID: Integer);
    // ODS 11 (FB 2.1)
    procedure FillSysTableInfo_fb11(ARelID: Integer);
    // ODS 10 (IB )
    procedure FillSysTableInfo_ib10(ARelID: Integer);
    // ODS 11 (IB )
    procedure FillSysTableInfo_ib11(ARelID: Integer);
  public
    FieldsInfo: array of TRDB_FieldInfoRec;
    RelationID: Integer;
    ItemClass: TRDB_RowClass;
    function GetItem(AIndex: Integer): TRDB_RowItem;
    constructor Create(AItemClass: TRDB_RowClass); reintroduce;
  end;

  // RDB$PAGES Rel=0
  TRDB_PagesItem = class(TRDB_RowItem)
  public
    PageNum: Integer;             // INTEGER
    RelationID: Word;             // SMALLINT
    PageSeq: Integer;             // INTEGER
    PageType: Word;               // SMALLINT
    procedure ReadData(const AData: AnsiString); override;
    function GetAsText(): string; override;
  end;

  // RDB$FIELDS Rel=2  FB 2.1 (ODS 11)
  TRDB_FieldsItem = class(TRDB_RowItem)
  public
    FieldName: AnsiString;        // CHAR(31)
    QueryName: AnsiString;        // CHAR(31)
    ValidationBlr: TRDB_Blob;    // BLOB SUB_TYPE 2
    ValidationSource: TRDB_Blob; // BLOB SUB_TYPE TEXT
    ComputedBlr: TRDB_Blob;      // BLOB SUB_TYPE 2
    ComputedSource: TRDB_Blob;   // BLOB SUB_TYPE TEXT
    DefaultValue: TRDB_Blob;     // BLOB SUB_TYPE 2
    DefaultSource: TRDB_Blob;    // BLOB SUB_TYPE TEXT
    FieldLength: Word;            // SMALLINT
    FieldScale: Word;             // SMALLINT
    FieldType: Word;              // SMALLINT
    FieldSubType: Word;           // SMALLINT
    MissingValue: TRDB_Blob;     // BLOB SUB_TYPE 2
    MissingSource: TRDB_Blob;    // BLOB SUB_TYPE TEXT
    Description: TRDB_Blob;      // BLOB SUB_TYPE TEXT
    SystemFlag: Word;             // SMALLINT
    QueryHeader: TRDB_Blob;      // BLOB SUB_TYPE TEXT
    SegmentLength: Word;          // SMALLINT
    EditString: AnsiString;       // VARCHAR(125)
    ExternalLength: Word;         // SMALLINT
    ExternalScale: Word;          // SMALLINT
    ExternalType: Word;           // SMALLINT
    Dimensions: Word;             // SMALLINT
    NullFlag: Word;               // SMALLINT
    CharacterLength: Word;        // SMALLINT
    CollationID: Word;            // SMALLINT
    CharacterSetID: Word;         // SMALLINT
    FieldPrecision: Word;         // SMALLINT
    procedure ReadData(const AData: AnsiString); override;
    function GetAsText(): string; override;
  end;

  // RDB$RELATION_FIELDS Rel=5  FB 2.X
  TRDB_RelationFieldsItem = class(TRDB_RowItem)
  public
    FieldName: AnsiString;       // CHAR(31)
    RelationName: AnsiString;    // CHAR(31)
    FieldSource: AnsiString;     // CHAR(31)
    QueryName: AnsiString;       // CHAR(31)
    BaseField: AnsiString;       // CHAR(31)
    EditString: AnsiString;      // VARCHAR(125)
    FieldPosition: Word;         // SMALLINT
    QueryHeader: TRDB_Blob;     // BLOB SUB_TYPE TEXT
    UpdateFlag: Word;            // SMALLINT
    FieldID: Word;               // SMALLINT
    ViewContext: Word;           // SMALLINT
    Description: TRDB_Blob;      // BLOB SUB_TYPE TEXT
    DefaultValue: TRDB_Blob;     // BLOB SUB_TYPE 2
    SystemFlag: Word;            // SMALLINT
    SecurityClass: AnsiString;   // CHAR(31)
    ComplexName: AnsiString;     // CHAR(31)
    NullFlag: Word;              // SMALLINT
    DefaultSource: TRDB_Blob;    // BLOB SUB_TYPE TEXT
    CollationID: Word;           // SMALLINT
    procedure ReadData(const AData: AnsiString); override;
    function GetAsText(): string; override;
  end;

  // RDB$FORMATS Rel=8   FB 2.X
  TRDB_FormatsItem = class(TRDB_RowItem)
  public
    RelationID: Word;            // SMALLINT
    Format: Word;                // SMALLINT
    Descriptor: TRDB_Blob;      // BLOB SUB_TYPE 6
    procedure ReadData(const AData: AnsiString); override;
    function GetAsText(): string; override;
    function FillFieldInfo(var AFieldInfo: TRDB_FieldInfoRec; AFieldIndex: Integer): Boolean;
  end;

  // RDB$RELATIONS Rel=6   FB 2.X
  TRDB_RelationsItem = class(TRDB_RowItem)
  public
    _FieldsInfo: array of TRDB_FieldInfoRec;
    _Format: TRDB_FormatsItem;
    ViewBlr: TRDB_Blob;          // BLOB SUB_TYPE 2
    ViewSource: TRDB_Blob;       // BLOB SUB_TYPE TEXT
    Description: TRDB_Blob;      // BLOB SUB_TYPE TEXT
    RelationID: Word;            // SMALLINT
    SystemFlag: Word;            // SMALLINT
    DBKeyLen: Word;              // SMALLINT
    Format: Word;                // SMALLINT
    FieldID: Word;               // SMALLINT
    RelationName: AnsiString;    // CHAR(31)
    SecurityClass: AnsiString;   // CHAR(31)
    ExternalFile: AnsiString;    // CHAR(253)
    FRuntime: TRDB_Blob;         // BLOB SUB_TYPE 5
    ExternalDescription: TRDB_Blob; // BLOB SUB_TYPE 8
    OwnerName: AnsiString;       // CHAR(31)
    DefaultClass: AnsiString;    // CHAR(31)
    Flags: Word;                 // SMALLINT
    RelationType: Word;          // SMALLINT
    procedure ReadData(const AData: AnsiString); override;
    function GetAsText(): string; override;

    function GetRuntime(): AnsiString;
    property Runtime: AnsiString read GetRuntime;
  end;

  // User Table row
  TRDB_TableRowItem = class(TRDB_RowItem)
  public
    procedure ReadData(const AData: AnsiString); override;
    function GetAsText(): string; override;
  end;

  { Database table - metadata, data, blobs, indexes, etc.. }
  {TFBTable = class(TObject)
  private
    FDataList: TRDB_TableList;
    FPageList: TRDB_TableList;
    FRelationID: Integer;
    FName: string;
  public
    TableInfo: TRDB_RelationsItem;
    constructor Create(ARelationID: Integer; AName: string;
      AItemClass: TRDB_TableClass = TRDB_TableRowItem);
    destructor Destroy(); override;
  end; }



  TDBReaderFB = class(TDBReader)
  private
    FPageSize: Integer;
    FOdsVersion: Integer;
    FMaxRecPerPage: Word;              // for calculation of Seq/Rec from RecID
    FIsMetadataReaded: Boolean;        // true after FillTableInfo()
    FIsInterBase: Boolean;

    //FOdsHeader: Ods_header_page;

    // raw page for blob reader
    FBlobRawPage: TByteArray;
    FBlobRawPageNum: Cardinal;

    FPagesList: TRDB_RowsList;           // (rel=0)
    FFieldsList: TRDB_RowsList;          // (rel=2)
    FRelationFieldsList: TRDB_RowsList;  // (rel=5)
    FRelationsList: TRDB_RowsList;       // (rel=6)
    FFormatsList: TRDB_RowsList;         // (rel=8)

    function GetRelationName(ARelID: Integer): string;
    function GetFieldSizeByTypeLen(AType, ALen, ASubType: Word): Integer;

    procedure SortPagesList();
    // find page by Type/Rel/Seq. If Result <0 it position for insert
    function FindPageIndex(APageType: Word; ARelationID, ASequence: Cardinal): Integer;
    // find page by Type/Rel/Seq
    function FindPageNum(APageType: Word; ARelationID, ASequence: Cardinal; out APageNum: Cardinal): Boolean;
    function GetBlobFromPage(const ARawPage: TByteArray; AIndex: Integer): AnsiString;
    // Return blob content for Relation/RowID
    function GetBlobData(ARelationID, ARowID: Cardinal): AnsiString;
    // Get partial data, recursive
    function GetFragmentData(APageNum, ALineID: Cardinal; ARecursionID: Integer = 0): AnsiString;

    procedure ReadSystemTable(ARelationID, ARowID: Cardinal; AData: AnsiString);
    // Fill table info from fields items
    procedure FillTableInfo();
  public
    IsLogPages: Boolean;
    IsLogBlobs: Boolean;
    IsClearRawData: Boolean;
    DebugRelID: Integer;
    procedure AfterConstruction(); override;
    procedure BeforeDestruction(); override;

    function OpenFile(AFileName: string): Boolean; override;

    // Read table data from DB to AList
    // AName - table name
    // ACount - how many items read
    procedure ReadTable(AName: string; ACount: Int64 = MaxInt; AList: TDbRowsList = nil); override;

    procedure DumpSystemTables(AFileName: string);
    procedure DumpTable(ATableName, AFileName: string);
    // get detailed multi-line description of table
    function FillTableInfoText(ATableName: string; ALines: TStrings): Boolean; override;

    property RelationsList: TRDB_RowsList read FRelationsList;       // (rel=6)
    property OdsVersion: Integer read FOdsVersion;
    property IsInterBase: Boolean read FIsInterBase;  // False if FireBird

    property OnLog: TGetStrProc read FOnLog write FOnLog;
  end;


function RleDecompress(AData: AnsiString; APadding: Boolean = True): AnsiString;

implementation

type
  // Basic page header
  TFBPageHead = packed record
    PageType: Byte;        // PAGE_TYPE_
    Flags: Byte;           // PAGE_FLAG_
    Reserved: Word;        // for alignment
    Generation: Cardinal;
    Scn: Cardinal;
    PageNo: Cardinal;      // for validation
  end;

  // Uncomplete (!) header
  TFBOdsPageHead = packed record
    PageHead: TFBPageHead;
    PageSize: Word;              // Page size of database
    OdsVersion: Word;            // Version of on-disk structure
    PagesPage: Cardinal;         // Page number of PAGES relation
    NextPage: Cardinal;          // Page number of next hdr page
  end;

  // Basic Data page header
  TFBDataPageHead = packed record
    PageHead: TFBPageHead;
    Sequence: Cardinal;
    RelationID: Word;
    Count: Word;
  end;

  // Data page record position
  TFBDataPageRec = packed record
    Offset: Word;
    Length: Word;
  end;


  TFBDataRecHead = packed record
    transaction: Cardinal;      // transaction id (lowest 32 bits)
    b_page: Cardinal;           // back pointer
    b_line: Word;               // back line
    flags: Word;                // flags, etc
    format: Byte;               // format version
  end;

  TFBDataFragRecHead = packed record
    transaction: Cardinal;      // transaction id (lowest 32 bits)
    b_page: Cardinal;           // back pointer
    b_line: Word;               // back line
    flags: Word;                // flags, etc
    format: Byte;               // format version
    Unused: Byte;
    tra_high: Word;             // higher bits of transaction id
    f_page: Cardinal;           // next fragment page
    f_line: Word;               // next fragment line
    //Unused2: Byte;
  end;

  // Record header for blob
  TFBBlobRecHead = record
    LeadPage: Cardinal;            // First data page number
    MaxSequence: Cardinal;	       // Number of data pages
    MaxSegment: Word;              // Longest segment
    Flags: Word;                   // flags, etc
    Level: Byte;                   // Number of address levels, see blb_level in blb.h
    Count: Cardinal;               // Total number of segments
    Length: Cardinal;              // Total length of data
    SubType: Word;                 // Blob sub-type
    Charset: Byte;                 // Blob charset (since ODS 11.1)
    Unused: Byte;
    //Page: array[0..0] of Cardinal;  // Page vector for blob pages
  end;

  // Format descriptor item (ODS 11+, FB 2.X+)
  TFBFormatRec = packed record
    DType: Byte;        // 1    DTYPE_
    Scale: Shortint;    // 1
    Length: Word;       // 2    Field length in bytes
    SubType: Word;      // 2    For BLOB/TEXT and integers
    Flags: Word;        // 2
    Offset: Cardinal;   // 4    Position from begining of row
  end;

  TFBBlobPageHead = packed record
    PageHead: TFBPageHead;
    LeadPage: Cardinal; // This field holds the page number for the first page for this blob
    Sequence: Cardinal; // The sequence number of this page within the page range for this blob.
    Length: Word;       // The length of the blob data on this page, in bytes.
    Padding: Word;      // Not used for any data, used as padding.
  end;

  // Blob data field record
  TBlobFieldRec = packed record
    RelationID: Integer;
    RowID: Cardinal;
  end;

const
  // Page types
  PAGE_TYPE_UNDEFINED    = 0;
  PAGE_TYPE_HEADER       = 1;   // Database header page
  PAGE_TYPE_PAGES        = 2;   // Page inventory page
  PAGE_TYPE_TRANSACTIONS = 3;   // Transaction inventory page
  PAGE_TYPE_POINTER      = 4;   // Pointer page
  PAGE_TYPE_DATA         = 5;   // Data page
  PAGE_TYPE_ROOT         = 6;   // Index root page
  PAGE_TYPE_INDEX        = 7;   // Index (B-tree) page
  PAGE_TYPE_BLOB         = 8;   // Blob data page
  PAGE_TYPE_IDS          = 9;   // Gen-ids
  PAGE_TYPE_SCNS         = 10;  // SCN's inventory page

  PAGE_FLAG_BLOB_POINTERS = 1; // Blob pointer page, not data page

  // Record header flags
  REC_DELETED     = 1;    // record is logically deleted
  REC_CHAIN       = 2;    // record is an old version
  REC_FRAGMENT    = 4;    // record is a fragment
  REC_INCOMPLETE  = 8;    // record is incomplete
  REC_BLOB        = 16;   // isn't a record but a blob
  REC_STREAM_BLOB = 32;   // blob is a stream mode blob
  REC_DELTA       = 32;   // prior version is differences only
  REC_LARGE       = 64;   // object is large
  REC_DAMAGED     = 128;  // object is known to be damaged
  REC_GC_ACTIVE   = 256;  // garbage collecting dead record version
  REC_UK_MODIFIED = 512;  // record key field values are changed
  REC_LONG_TRANUM = 1024; // transaction number is 64-bit

  DTYPE_TEXT      = 1;
  DTYPE_CSTRING   = 2;  // Len
  DTYPE_VARYNG    = 3;  // 2 + Len
  DTYPE_PACKED    = 6;
  DTYPE_BYTE      = 7;  // 1  Byte
  DTYPE_SHORT     = 8;  // 2  Word
  DTYPE_LONG      = 9;  // 4  Cardinal
  DTYPE_QUAD      = 10; // 8  DWord
  DTYPE_REAL      = 11; // 4  Single
  DTYPE_DOUBLE    = 12; // 8  Double
  DTYPE_DFLOAT    = 13; //
  DTYPE_SQL_DATE  = 14; // 4  Integer
  DTYPE_SQL_TIME  = 15; // 4  Cardinal
  DTYPE_TIMESTAMP = 16; // 8  DateTime
  DTYPE_BLOB      = 17; // 8  RelID/RowID
  DTYPE_ARRAY     = 18;
  DTYPE_INT64     = 19; // 8 Int64

  FB_EPOCH_DIFF          = 15018;  // Days between Delphi and Firebird epoch

function OdsPageTypeToStr(AValue: Byte): string;
begin
  case AValue of
    PAGE_TYPE_HEADER:       Result := 'Database header page';
    PAGE_TYPE_PAGES:        Result := 'Page inventory page';
    PAGE_TYPE_TRANSACTIONS: Result := 'Transaction inventory page';
    PAGE_TYPE_POINTER:      Result := 'Pointer page';
    PAGE_TYPE_DATA:         Result := 'Data page';
    PAGE_TYPE_ROOT:         Result := 'Index root page';
    PAGE_TYPE_INDEX:        Result := 'Index (B-tree) page';
    PAGE_TYPE_BLOB:         Result := 'Blob data page';
    PAGE_TYPE_IDS:          Result := 'Gen-ids';
    PAGE_TYPE_SCNS:         Result := 'SCN inventory page';
  else
    Result := 'Undefined (' + IntToStr(AValue) + ')';
  end;
end;

// see also GetFieldSizeByTypeLen()
function FieldTypeToStr(AType, ALen, ASubType: Word): string;
begin
  case AType of
    7: Result := 'SMALLINT';
    8: Result := 'INTEGER';
    //8: Values[i] := ReadInt64(); // INTEGER
    10: Result := 'FLOAT';
    14: // CHAR
    begin
      Result := Format('CHAR(%d)', [ALen]);
      case ASubType of
        3: Result := Result + 'UTF8';
      end;
    end;
    16: // CURRENCY
    begin
      Result := 'INT64';
      if ASubType = 1 then
        Result := 'CURRENCY';  // ???
    end;
    27: Result := 'DOUBLE';  //'INT64';
    35: Result := 'DATETIME';
    37: Result := Format('VARCHAR(%d)', [ALen]);
    261: // BLOB
    begin
      case ASubType of
        0: Result := 'BINARY';
        1: Result := 'TEXT';
        2: Result := 'BLR';
      else
        Result := 'BLOB_' + IntToStr(ASubType);
      end;
      Result := Format('%s(%d)', [Result, ALen]);
    end
  else
    Result := '#'+IntToStr(AType);
  end;
end;

// see also GetFieldSizeByTypeLen()
function FieldTypeToDbFieldType(AType, ALen, ASubType: Word): TFieldType;
begin
  case AType of
    7: Result := ftSmallint;
    8: Result := ftInteger;
    //8: Values[i] := ReadInt64(); // INTEGER
    10: Result := ftFloat;
    14: Result := ftString;
    16:
    begin
      Result := ftLargeint;
      if ASubType = 1 then
        Result := ftCurrency;
    end;
    27: Result := ftFloat; // ftLargeint;
    35: Result := ftDateTime;
    37: Result := ftString;
    261: // BLOB
    begin
      case ASubType of
        0: Result := ftBytes;
        1: Result := ftString;
        2: Result := ftString;
      else
        Result := ftBytes;
      end;
    end
  else
    Result := ftUnknown;
  end;
end;

function DataTypeToStr(AType, ALen, ASubType: Word): string;
begin
  case AType of
    0: Result := 'Unknown';
    1:
    begin
      Result := 'Text';
      case ASubType of
        0: Result := Result + Format(' CHAR(%d)', [ALen]);
        1: Result := Result + '(binary)'; // strings can contain null bytes
        2: Result := Result + '(ASCII)';  // string contains only ASCII characters
        3: Result := Result + '(unicode)'; // string represents system metadata
      else
        // sub = codepage
        Result := Result + Format(' CHAR(%d) CP_%d', [ALen, ASubType]);
      end;
    end;
    2: Result := Format('CString(%d)', [ALen]);
    3: Result := Format('Varyng(%d) CP_%d', [ALen, ASubType]);
    // 4, 5 - ?
    6: Result := 'Packed';
    7: Result := 'Byte';
    8: Result := 'Short'; // 'SMALLINT';
    9:
    begin
      Result := 'Long';
      case ASubType of
        0: Result := 'INTEGER';
        1: Result := 'NUMERIC';
        2: Result := 'DECIMAL';
      end;
    end;
    10: Result := 'Quad';
    11: Result := 'Real';
    12: Result := 'Double';
    13: Result := 'DFloat';
    14: Result := 'SqlDate';
    15: Result := 'SqlTime';
    16: Result := 'Timestamp';  // DATETIME
    17: // BLOB
    begin
      case ASubType of
        0: Result := 'BINARY';
        1: Result := 'TEXT';
        2: Result := 'BLR';
      else
        Result := 'BLOB_' + IntToStr(ASubType);
      end;
      Result := 'Blob ' + Result;
    end;
    18: Result := 'Array';
    19: Result := 'Int64';
    //20: Result := '';
  else
    Result := '#'+IntToStr(AType);
  end;

end;


function DataFlagsToStr(AValue: Word): string;
begin
  Result := '[';
  if (AValue and REC_DELETED) > 0 then
    Result := Result + 'del ';
  if (AValue and REC_CHAIN) > 0 then
    Result := Result + 'old ';
  if (AValue and REC_FRAGMENT) > 0 then
    Result := Result + 'frag ';
  if (AValue and REC_INCOMPLETE) > 0 then
    Result := Result + 'incompl ';
  if (AValue and REC_BLOB) > 0 then
    Result := Result + 'blob ';
  if (AValue and REC_STREAM_BLOB) > 0 then
    Result := Result + 'sblob ';
  if (AValue and REC_DELTA) > 0 then
    Result := Result + 'delta ';
  if (AValue and REC_LARGE) > 0 then
    Result := Result + 'large ';
  if (AValue and REC_DAMAGED) > 0 then
    Result := Result + 'damg ';
  if (AValue and REC_GC_ACTIVE) > 0 then
    Result := Result + 'gc ';
  if (AValue and REC_UK_MODIFIED) > 0 then
    Result := Result + 'mod ';
  if (AValue and REC_LONG_TRANUM) > 0 then
    Result := Result + 'tran64 ';
  Result := Trim(Result) + ']';
end;

function RleDecompress(AData: AnsiString; APadding: Boolean = True): AnsiString;
var
  nLen: ShortInt;
  nPos, nDataLen, nResLen: Integer;
begin
  Result := '';
  nDataLen := Length(AData);
  if nDataLen = 0 then Exit;

  nPos := 1;
  while nPos <= nDataLen do
  begin
    Move(AData[nPos], nLen, 1);
    Inc(nPos);
    
    if nLen > 0 then
    begin
      Result := Result + Copy(AData, nPos, nLen);
      Inc(nPos, nLen);
    end
    else if nLen < 0 then
    begin
      nResLen := Length(Result);
      SetLength(Result, nResLen + Abs(nLen));
      FillChar(Result[nResLen + 1], Abs(nLen), AData[nPos]);
      Inc(nPos);
    end
    else
      Break;
  end;
end;

procedure StrToFile(AStr: AnsiString; AFileName: string);
var
  fs: TFileStream;
begin
  if AStr = '' then Exit;
  if FileExists(AFileName) then
    DeleteFile(AFileName);

  fs := TFileStream.Create(AFileName, fmCreate);
  try
    fs.Write(AStr[1], Length(AStr));
  finally
    fs.Free();
  end;
end;

function DataAsStr(const AData; ALen: Integer): string;
var
  i: Integer;
begin
  Result := '';
  if ALen = 0 then Exit;

  SetLength(Result, ALen);
  Move(AData, Result[1], ALen);
  for i := 1 to ALen do
  begin
    if Ord(Result[i]) < 32 then
      Result[i] := '.';
  end;
end;

// Выдает HEX-строку содержимого буфера
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

// Выдает HEX-строку содержимого буфера, заглавными буквами без пробелов
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

{ TFBReader }

procedure TDBReaderFB.AfterConstruction;
begin
  inherited;
  
  FPagesList := TRDB_RowsList.Create(TRDB_PagesItem);
  FFieldsList := TRDB_RowsList.Create(TRDB_FieldsItem);
  FRelationFieldsList := TRDB_RowsList.Create(TRDB_RelationFieldsItem);
  FRelationsList := TRDB_RowsList.Create(TRDB_RelationsItem);
  FFormatsList := TRDB_RowsList.Create(TRDB_FormatsItem);

  DebugRelID := -1;
  FMaxRecPerPage := 239; // ??

  FBlobRawPageNum := MAXDWORD;
end;

procedure TDBReaderFB.BeforeDestruction;
begin
  FreeAndNil(FFormatsList);
  FreeAndNil(FRelationsList);
  FreeAndNil(FRelationFieldsList);
  FreeAndNil(FFieldsList);
  FreeAndNil(FPagesList);
  inherited;
end;

procedure TDBReaderFB.DumpSystemTables(AFileName: string);
var
  i: Integer;
  TmpRelItem: TRDB_RelationsItem;
  TmpFormatsItem: TRDB_FormatsItem;
  sl, slText: TStringList;
  s: string;
begin
  // (6) relations
  slText := TStringList.Create();
  for i := 0 to FRelationsList.Count - 1 do
  begin
    TmpRelItem := FRelationsList.GetItem(i) as TRDB_RelationsItem;
    s := Format(
         'ViewBlr=%s' + sLineBreak
       + 'ViewSource=%s' + sLineBreak
       + 'Description=%s' + sLineBreak
       + 'RelationID=%d' + sLineBreak
       + 'SystemFlag=%d' + sLineBreak
       + 'DBKeyLen=%d' + sLineBreak
       + 'Format=%d' + sLineBreak
       + 'FieldID=%d' + sLineBreak    // SMALLINT
       + 'RelationName=%s' + sLineBreak    // CHAR(31)
       + 'SecurityClass=%s' + sLineBreak   // CHAR(31)
       + 'ExternalFile=%s' + sLineBreak    // CHAR(253)
       + 'FRuntime=%s' + sLineBreak         // BLOB SUB_TYPE 5
       + 'ExternalDescription=%s' + sLineBreak // BLOB SUB_TYPE 8
       + 'OwnerName=%s' + sLineBreak       // CHAR(31)
       + 'DefaultClass=%s' + sLineBreak    // CHAR(31)
       + 'Flags=%d' + sLineBreak                 // SMALLINT
       + 'RelationType=%d' + sLineBreak          // SMALLINT
       , [
         TmpRelItem.GetBlobValue(TmpRelItem.ViewBlr)
       , TmpRelItem.GetBlobValue(TmpRelItem.ViewSource)
       , TmpRelItem.GetBlobValue(TmpRelItem.Description)
       , TmpRelItem.RelationID
       , TmpRelItem.SystemFlag
       , TmpRelItem.DBKeyLen
       , TmpRelItem.Format
       , TmpRelItem.FieldID
       , TmpRelItem.RelationName
       , TmpRelItem.SecurityClass
       , TmpRelItem.ExternalFile
       , TmpRelItem.Runtime
       , TmpRelItem.GetBlobValue(TmpRelItem.ExternalDescription)
       , TmpRelItem.OwnerName
       , TmpRelItem.DefaultClass
       , TmpRelItem.Flags
       , TmpRelItem.RelationType
         ]
    );
    slText.Add(Format('== RowID=%d ==', [TmpRelItem.RowID]));
    slText.Add(s);
  end;

  // (8) formats
  slText.Add('===========================');
  slText.Add('== RDB$FORMATS           ==');
  slText.Add('===========================');
  for i := 0 to FFormatsList.Count - 1 do
  begin
    TmpFormatsItem := FFormatsList.GetItem(i) as TRDB_FormatsItem;
    slText.Add(Format('== RowID=%d ==', [TmpFormatsItem.RowID]));
    s := Format(
         'RelationID=%d' + sLineBreak    // SMALLINT
       + 'Format=%d' + sLineBreak        // SMALLINT
       + 'Descriptor[%d:%d]=%s' + sLineBreak    // BLOB SUB_TYPE 6
       , [
         TmpFormatsItem.RelationID
       , TmpFormatsItem.Format
       , TmpFormatsItem.Descriptor.RelID, TmpFormatsItem.Descriptor.RowID, TmpFormatsItem.GetBlobValue(TmpFormatsItem.Descriptor)
         ]
    );
    slText.Add(s);
  end;
  
  try
    slText.SaveToFile(AFileName);
  finally
    slText.Free();
  end;
end;

procedure TDBReaderFB.DumpTable(ATableName, AFileName: string);
var
  i, ii: Integer;
  TmpList: TRDB_RowsList;
  TmpItem: TRDB_TableRowItem;
  RelItem: TRDB_RelationsItem;
  sl, slText: TStringList;
  s: string;
  nType, nLen: Word;
  nSize, nSub: Integer;
  TmpBlob: TRDB_Blob;
  dt: TDateTime;
begin
  // find relation item, get RelID
  ATableName := UpperCase(ATableName);
  RelItem := nil;
  for i := 0 to FRelationsList.Count - 1 do
  begin
    RelItem := FRelationsList.GetItem(i) as TRDB_RelationsItem;
    if UpperCase(RelItem.RelationName) = ATableName then
      Break;
    RelItem := nil;
  end;
  if not Assigned(RelItem) then Exit;
  //RelID := RelItem.RelationID;

  TmpList := TRDB_RowsList.Create(TRDB_TableRowItem);

  ReadTable(ATableName, MaxInt, TmpList);

  slText := TStringList.Create();

  // dump table info
  slText.Add(Format('== TableName=%s  RelationID=%d', [RelItem.RelationName, RelItem.RelationID]));
  for i := Low(RelItem._FieldsInfo) to High(RelItem._FieldsInfo) do
  begin
    if RelItem._FieldsInfo[i].DType = 0 then
    begin
      slText.Add(Format('%.2d FieldName=%s  Size=%d  Type=%s  Len=%d  SubType=%d',
        [i,
         RelItem._FieldsInfo[i].Name,
         RelItem._FieldsInfo[i].Size,
         FieldTypeToStr(RelItem._FieldsInfo[i].FieldType, RelItem._FieldsInfo[i].FieldLength, RelItem._FieldsInfo[i].FieldSubType),
         RelItem._FieldsInfo[i].FieldLength,
         RelItem._FieldsInfo[i].FieldSubType]));
    end
    else
    begin
      slText.Add(Format('%.2d FieldName=%s  DType=%s  Len=%d  SubType=%d  Flags=%d  Offs=%d',
        [i,
         RelItem._FieldsInfo[i].Name,
         DataTypeToStr(RelItem._FieldsInfo[i].DType, RelItem._FieldsInfo[i].Length, RelItem._FieldsInfo[i].SubType),
         RelItem._FieldsInfo[i].Length,
         RelItem._FieldsInfo[i].SubType,
         RelItem._FieldsInfo[i].Flags,
         RelItem._FieldsInfo[i].Offset
         ]));
    end;
  end;
  slText.Add('==');

  // data rows
  for i := 0 to TmpList.Count - 1 do
  begin
    TmpItem := TmpList.GetItem(i) as TRDB_TableRowItem;
    slText.Add(Format('== RowID=%d ==', [TmpItem.RowID]));
    for ii := Low(RelItem._FieldsInfo) to High(RelItem._FieldsInfo) do
    begin
      nType := RelItem._FieldsInfo[ii].FieldType;
      nSize := RelItem._FieldsInfo[ii].Size;
      nLen := RelItem._FieldsInfo[ii].FieldLength;
      nSub := RelItem._FieldsInfo[ii].FieldSubType;

      Assert(High(RelItem._FieldsInfo) = High(TmpItem.Values), 
          ' FieldsCount=' + IntToStr(Length(RelItem._FieldsInfo))
        + ' ValuesCount=' + IntToStr(Length(TmpItem.Values))
        + ' RowID=' + IntToStr(TmpItem.RowID)
        );
      
      s := '';
      if VarIsNull(TmpItem.Values[ii]) then
        s := 'NULL'
      else
      begin
        case nType of
          7: s := VarToStrDef(TmpItem.Values[ii], ''); // SMALLINT
          8: s := VarToStrDef(TmpItem.Values[ii], ''); // INTEGER
          //8: Values[i] := ReadInt64(); // INTEGER
          10: s := VarToStrDef(TmpItem.Values[ii], ''); // FLOAT
          14: s := VarToStrDef(TmpItem.Values[ii], ''); // CHAR
          27: s := VarToStrDef(TmpItem.Values[ii], ''); // INT64
          35:
          begin
            s := VarToStrDef(TmpItem.Values[ii], ''); // DATETIME
            //dt := TmpItem.Values[i];
            //s := FormatDateTime('YYYY-MM-DD HH:NN:SS', dt);
          end;
          37: s := VarToStrDef(TmpItem.Values[ii], ''); // VARCHAR
          261:
          begin
            if VarIsOrdinal(TmpItem.Values[ii]) then
            begin
              TmpBlob.RelID := TmpItem.RelID;
              TmpBlob.RowID := TmpItem.Values[ii];
              s := TmpItem.GetBlobValue(TmpBlob);
            end
            else
              s := VarToStrDef(TmpItem.Values[ii], '');
          end;
        else
          s := '?';
        end;
      end;

      slText.Add(RelItem._FieldsInfo[ii].Name + '=' + s);
      
    end;
  end;

  try
    slText.SaveToFile(AFileName);
  finally
    slText.Free();
    TmpList.Free();
  end;
end;

procedure TDBReaderFB.FillTableInfo;
var
  i, ii, n: Integer;
  RelField: TRDB_RelationFieldsItem;
  Rel: TRDB_RelationsItem;
  Field: TRDB_FieldsItem;
begin
  SortPagesList();
  //LogInfo('=== Sorted pages ===');
  //for i := 0 to FPagesList.Count - 1 do
  //  LogInfo(Format('%s', [FPagesList.GetItem(i).GetAsText]));
  
  for i := 0 to FRelationFieldsList.Count - 1 do
  begin
    RelField := FRelationFieldsList.GetItem(i) as TRDB_RelationFieldsItem;
    // find relation
    Rel := nil;
    for ii := 0 to FRelationsList.Count - 1 do
    begin
      Rel := FRelationsList.GetItem(ii) as TRDB_RelationsItem;
      if ((RelField.RelationName <> '') and (Rel.RelationName = RelField.RelationName)) then
        Break
      else // RelationName is empty for InterBase
      if ((RelField.RelationName = '') and (Rel.RelationName = RelField.FieldSource)) then
        Break;
      Rel := nil;
    end;
    //Assert(Assigned(Field), 'Not found Relation for RelField RelationName=' + RelField.RelationName + '  FieldSource=' + RelField.FieldSource);
    if not Assigned(Rel) then
    begin
      LogInfo(Format('Not found Relation for RelField RelationName=%s  FieldSource=%s', [RelField.RelationName, RelField.FieldSource]));
      Continue;
    end;

    if not Assigned(Rel._Format) then
    begin
      // find format for relation
      for ii := 0 to FFormatsList.Count - 1 do
      begin
        if (FFormatsList.GetItem(ii) as TRDB_FormatsItem).RelationID = Rel.RelationID then
        begin
          Rel._Format := (FFormatsList.GetItem(ii) as TRDB_FormatsItem);
          Break;
        end;
      end;
    end;

    // find field
    Field := nil;
    for ii := 0 to FFieldsList.Count - 1 do
    begin
      Field := FFieldsList.GetItem(ii) as TRDB_FieldsItem;
      if (Field.FieldName = RelField.FieldSource)
      or (Field.FieldName = RelField.BaseField) then
        Break;
      Field := nil;
    end;
    //Assert(Assigned(Field), 'Not found Field for RelField FieldSource=' + RelField.FieldSource + '  BaseField=' + RelField.BaseField);
    if not Assigned(Field) then
    begin
      LogInfo(Format('Not found Field for RelField FieldSource=%s  BaseField=%s', [RelField.FieldSource, RelField.BaseField]));
      Continue;
    end;

    // update relation info
    n := RelField.FieldID;    // always 0 for InterBase ???
    if IsInterBase then
      n := Length(Rel._FieldsInfo);
    //n := RelField.FieldPosition;
    if (n + 1) > Length(Rel._FieldsInfo) then
      SetLength(Rel._FieldsInfo, n + 1);
    Rel._FieldsInfo[n].Name := RelField.FieldName;
    Rel._FieldsInfo[n].Size := GetFieldSizeByTypeLen(Field.FieldType, Field.FieldLength, Field.FieldSubType);
    Rel._FieldsInfo[n].DType := 0;  // default
    Rel._FieldsInfo[n].FieldType := Field.FieldType;
    Rel._FieldsInfo[n].FieldLength := Field.FieldLength;
    Rel._FieldsInfo[n].FieldSubType := Field.FieldSubType;

    if Assigned(Rel._Format) then
    begin
      Rel._Format.FillFieldInfo(Rel._FieldsInfo[n], n);
    end;
  end;
  FIsMetadataReaded := True;
end;

function ComparePageItems(Item1, Item2: Pointer): Integer;
var
  A, B: TRDB_PagesItem;
begin
  Result := 0;
  A := TRDB_PagesItem(Item1);
  B := TRDB_PagesItem(Item2);
  if A.PageType > B.PageType then
    Result := 1
  else
  if A.PageType < B.PageType then
    Result := -1
  else
  begin
    if A.RelationID > B.RelationID then
      Result := 1
    else
    if A.RelationID < B.RelationID then
      Result := -1
    else
    begin
      if A.PageSeq > B.PageSeq then
        Result := 1
      else
      if A.PageSeq < B.PageSeq then
        Result := -1;
    end;
  end;
end;

procedure TDBReaderFB.SortPagesList;
begin
  FPagesList.Sort(ComparePageItems);
end;

function TDBReaderFB.FindPageIndex(APageType: Word; ARelationID, ASequence: Cardinal): Integer;
var
  iL, iR, iM, iCmpRes: Integer;
  M: TRDB_PagesItem;
begin
  Result := -1;
  // binary search
  iL := 0;
  iR := FPagesList.Count-1;
  while iL <= iR do
  begin
    iM := (iL + iR) div 2;
    M := TRDB_PagesItem(FPagesList.Get(iM));
    // compare
    iCmpRes := 0;
    if APageType > M.PageType then
      iCmpRes := 1
    else
    if APageType < M.PageType then
      iCmpRes := -1
    else
    begin
      if ARelationID > M.RelationID then
        iCmpRes := 1
      else
      if ARelationID < M.RelationID then
        iCmpRes := -1
      else
      begin
        if ASequence > M.PageSeq then
          iCmpRes := 1
        else
        if ASequence < M.PageSeq then
          iCmpRes := -1
      end;
    end;
    // get next mid
    if iCmpRes > 0 then
    begin
      iL := iM + 1;
      Result := -(iM + 2);
    end
    else
    if iCmpRes < 0 then
    begin
      iR := iM - 1;
      Result := -(iM + 1);
    end
    else
    begin
      Result := iM;
      Break;
    end;
  end;
end;

function TDBReaderFB.FindPageNum(APageType: Word; ARelationID, ASequence: Cardinal; out APageNum: Cardinal): Boolean;
var
  i: Integer;
  M: TRDB_PagesItem;
begin
  Result := False;
  // binary search
  i := FindPageIndex(APageType, ARelationID, ASequence);
  if i >= 0 then
  begin
    M := TRDB_PagesItem(FPagesList.Get(i));
    APageNum := M.PageNum;
    Result := True;
  end;
end;

function TDBReaderFB.GetBlobData(ARelationID, ARowID: Cardinal): AnsiString;
var
  PageSeq, RowIndex: Integer;
  PageNum: Cardinal;
  nPos: Int64;
begin
  Result := '';
  PageSeq := ARowID div FMaxRecPerPage;
  RowIndex := ARowID mod FMaxRecPerPage;
  if FindPageNum(PAGE_TYPE_DATA, ARelationID, PageSeq, PageNum) then
  begin
    if FBlobRawPageNum <> PageNum then
    begin
      // read raw page
      nPos := Int64(PageNum) * FPageSize;
      if nPos < (FFile.Size - FPageSize) then
      begin
        FFile.Position := nPos;
        FFile.Read(FBlobRawPage, FPageSize);
        FBlobRawPageNum := PageNum;
      end;
    end;

    if FBlobRawPageNum = PageNum then
      Result := GetBlobFromPage(FBlobRawPage, RowIndex);
  end;
end;

function TDBReaderFB.GetBlobFromPage(const ARawPage: TByteArray; AIndex: Integer): AnsiString;
var
  DataPageHead: TFBDataPageHead;
  DataPageRecs: array of TFBDataPageRec;
  i, iOffs, iDataHeadLen, iDataOffs, iDataLen: Integer;
  BlobRecHead: TFBBlobRecHead;
begin
  Result := '';
  // Get header
  Move(ARawPage, DataPageHead, SizeOf(DataPageHead));
  Assert(DataPageHead.PageHead.PageType = PAGE_TYPE_DATA, 'Not data page');
  if DataPageHead.PageHead.PageType <> PAGE_TYPE_DATA then
    Exit;
  Assert(DataPageHead.Count > 0, 'No data on page');
  if DataPageHead.Count = 0 then
    Exit;
  Assert(AIndex < DataPageHead.Count, 'Rec Index more than Count');
  if AIndex >= DataPageHead.Count then
    Exit;
  // read rows list
  SetLength(DataPageRecs, DataPageHead.Count);
  iOffs := SizeOf(DataPageHead);
  Move(ARawPage[iOffs], DataPageRecs[0], DataPageHead.Count * SizeOf(TFBDataPageRec));
  // get row
  i := AIndex;
  Move(ARawPage[DataPageRecs[i].Offset], BlobRecHead, SizeOf(TFBBlobRecHead));
  //Assert((BlobRecHead.Flags and REC_BLOB) <> 0, 'Rec is not blob');
  if (BlobRecHead.Flags and REC_BLOB) = 0 then
    Exit;

  iDataHeadLen := SizeOf(BlobRecHead);
  iDataOffs := DataPageRecs[i].Offset + iDataHeadLen;
  iDataLen := DataPageRecs[i].Length - iDataHeadLen;

  // align
  //iDataOffs := iDataOffs + (iDataOffs mod 4);

  // get data
  if iDataLen > 0 then
  begin
    //Inc(iDataOffs, SizeOf(BlobRecHead) - 4);
    if BlobRecHead.SubType = 1 then  // TEXT
    begin
      Inc(iDataOffs, 2);
      Dec(iDataLen, 2);
    end;
    if BlobRecHead.SubType = 2 then  // BLR
    begin
      Inc(iDataOffs, 9);
      Dec(iDataLen, 9);
    end;
    if BlobRecHead.SubType = 3 then
    begin
      Inc(iDataOffs, 6);
      Dec(iDataLen, 6);
    end;
    if BlobRecHead.SubType = 5 then
    begin
      Inc(iDataOffs, 8);
      Dec(iDataLen, 8);
    end;

    if iDataLen > 0 then
    begin
      SetLength(Result, iDataLen);
      Move(ARawPage[iDataOffs], Result[1], iDataLen);
    end;
  end;
end;

function TDBReaderFB.GetFieldSizeByTypeLen(AType, ALen, ASubType: Word): Integer;
begin
  case AType of
    7: Result := 2; // SMALLINT
    8: Result := 4; // INTEGER
    10: Result := 8; // FLOAT
    14: // CHAR
    begin
      Result := ALen;
      if ASubType = 3 then
        Inc(Result);
    end;
    16: Result := 8; // INT64
    27: Result := 8; // DOUBLE
    35: Result := 8; // TIMESTAMP (DateTime)
    37: Result := ALen + 2; // VARCHAR
    261: // BLOB
    begin
      Result := ALen; // BINARY
      case ASubType of
        1: Result := 14; // TEXT
        2: Result := 4;  // BLR
      end;
    end;
  else
    Result := 0;
  end;
end;

function TDBReaderFB.GetFragmentData(APageNum, ALineID: Cardinal;
  ARecursionID: Integer): AnsiString;
var
  nPos: Int64;
  DataPageHead: TFBDataPageHead;
  DataPageRecs: array of TFBDataPageRec;
  DataRecHead: TFBDataRecHead;
  DataFragRecHead: TFBDataFragRecHead;
  i, iOffs, iDataHeadLen, iDataOffs, iDataLen: Integer;
  s: string;
begin
  Result := '';
  if FBlobRawPageNum <> APageNum then
  begin
    // read raw page
    nPos := Int64(APageNum) * FPageSize;
    if nPos < (FFile.Size - FPageSize) then
    begin
      FFile.Position := nPos;
      FFile.Read(FBlobRawPage, FPageSize);
      FBlobRawPageNum := APageNum;
    end;
  end;
  if FBlobRawPageNum <> APageNum then Exit;

  // Get header
  Move(FBlobRawPage, DataPageHead, SizeOf(DataPageHead));
  Assert(DataPageHead.PageHead.PageType = PAGE_TYPE_DATA, 'Not data page');
  if DataPageHead.PageHead.PageType <> PAGE_TYPE_DATA then
    Exit;
  Assert(DataPageHead.Count > 0, 'No data on page');
  if DataPageHead.Count = 0 then
    Exit;
  Assert(ALineID < DataPageHead.Count, 'Rec Index more than Count');
  if ALineID >= DataPageHead.Count then
    Exit;
  // read rows list
  SetLength(DataPageRecs, DataPageHead.Count);
  iOffs := SizeOf(DataPageHead);
  Move(FBlobRawPage[iOffs], DataPageRecs[0], DataPageHead.Count * SizeOf(TFBDataPageRec));
  // get row
  i := ALineID;
  Move(FBlobRawPage[DataPageRecs[i].Offset], DataRecHead, SizeOf(DataRecHead));
  Assert((DataRecHead.Flags and REC_FRAGMENT) <> 0, 'Rec is not fragment');
  if (DataRecHead.Flags and REC_FRAGMENT) = 0 then
    Exit;

  iDataHeadLen := SizeOf(DataRecHead);
  iDataOffs := DataPageRecs[i].Offset + iDataHeadLen;
  iDataLen := DataPageRecs[i].Length - iDataHeadLen;

  if iDataLen <> 0 then
  begin
    SetLength(Result, iDataLen);
    Move(FBlobRawPage[iDataOffs], Result[1], iDataLen);
    Result := RleDecompress(Result);
  end;

  {s := Result;
  if Length(s) > 0 then
    s := DataAsStr(s[1], Length(s));
  LogInfo(Format('FRAG Page=%d  RowID=%d BackPag:Lin=%d:%d Flags=%s Fmt=%d  Raw(%d)=%s Data(%d)=%s%s',
    [APageNum,
     (DataPageHead.Sequence * FMaxRecPerPage) + Cardinal(i),
     DataRecHead.b_page, DataRecHead.b_line,
     DataFlagsToStr(DataRecHead.flags),
     DataRecHead.format,
     iDataLen, BufferToHex(FBlobRawPage[iDataOffs], 4),
     Length(Result), BufferToHex(Result[1], 4), s
    ]));    }
  
end;

function TDBReaderFB.GetRelationName(ARelID: Integer): string;
var
  i: Integer;
  TmpItem: TRDB_RowItem;
begin
  for i := 0 to FRelationsList.Count - 1 do
  begin
    TmpItem := FRelationsList.GetItem(i);
    if (TmpItem as TRDB_RelationsItem).RelationID = ARelID then
    begin
      Result := (TmpItem as TRDB_RelationsItem).RelationName;
      Exit;
    end;
  end;
  Result := '';
end;

function TDBReaderFB.FillTableInfoText(ATableName: string; ALines: TStrings): Boolean;
var
  i: Integer;
  RelItem: TRDB_RelationsItem;
  TmpField: TRDB_FieldInfoRec;
  s: string;
begin
  Result := False;
  if not Assigned(ALines) then Exit;
  
  // table info
  RelItem := nil;
  for i := 0 to RelationsList.Count - 1 do
  begin
    RelItem := RelationsList.GetItem(i) as TRDB_RelationsItem;
    if RelItem.RelationName = ATableName then
      Break;
    RelItem := nil;
  end;
  if Assigned(RelItem) then
  begin
    with ALines do
    begin
      Add(Format('== Table Rel=%d  Name=%s  Desc=%s', [RelItem.RelationID, RelItem.RelationName, RelItem.GetBlobValue(RelItem.Description)]));
      Add(Format('RelationType=%d', [RelItem.RelationType]));
      Add(Format('SystemFlag=%d', [RelItem.SystemFlag]));
      Add(Format('Flags=%d', [RelItem.Flags]));
      Add(Format('OwnerName=%s', [RelItem.OwnerName]));
      Add(Format('Runtime=%s', [RelItem.Runtime]));
      Add(Format('DefaultClass=%s', [RelItem.DefaultClass]));
      Add(Format('ExternalFile=%s', [RelItem.ExternalFile]));
      Add(Format('SecurityClass=%s', [RelItem.SecurityClass]));
      Add(Format('FieldID=%d', [RelItem.FieldID]));
      Add(Format('Format=%d', [RelItem.Format]));
      Add(Format('DBKeyLen=%d', [RelItem.DBKeyLen]));
      Add(Format('ViewBlr=%s', [RelItem.GetBlobValue(RelItem.ViewBlr)]));
      Add(Format('ViewSource=%s', [RelItem.GetBlobValue(RelItem.ViewSource)]));
      Add(Format('== Fields  Count=%d', [Length(RelItem._FieldsInfo)]));
    end;
    for i := 0 to Length(RelItem._FieldsInfo) - 1 do
    begin
      TmpField := RelItem._FieldsInfo[i];
      s := Format('%.2d Name=%s  Type=%s', [i, RelItem._FieldsInfo[i].Name, RelItem._FieldsInfo[i].AsString]);
      if RelItem._FieldsInfo[i].FieldType <> 0 then
      begin
        s := s + Format('  Field=(%s Type=%d.%d Len=%d)',
          [FieldTypeToStr(RelItem._FieldsInfo[i].FieldType, RelItem._FieldsInfo[i].FieldLength, RelItem._FieldsInfo[i].FieldSubType),
           RelItem._FieldsInfo[i].FieldType,
           RelItem._FieldsInfo[i].FieldSubType,
           RelItem._FieldsInfo[i].FieldLength
          ]);
      end;
      if RelItem._FieldsInfo[i].DType <> 0 then
      begin
        s := s + Format('  Format=(%s  DType=%d.%d  Len=%d  Flags=%d  Offs=%d)',
          [DataTypeToStr(RelItem._FieldsInfo[i].DType, RelItem._FieldsInfo[i].Length, RelItem._FieldsInfo[i].SubType),
           RelItem._FieldsInfo[i].DType,
           RelItem._FieldsInfo[i].SubType,
           RelItem._FieldsInfo[i].Length,
           RelItem._FieldsInfo[i].Flags,
           RelItem._FieldsInfo[i].Offset
          ]);
      end;
      ALines.Add(s);
    end;
    if Assigned(RelItem._Format) then
      ALines.Add(Format('== Format  Length=%d', [Length(RelItem._Format.Descriptor.Data)]));
    Result := True;
  end;
end;

function TDBReaderFB.OpenFile(AFileName: string): Boolean;
var
  i, nPage, iOffs, iDataHeadLen, iDataOffs, iDataLen: Integer;
  iLen, iPageIndex: Integer;
  CurRowID: Cardinal;
  nPos: Int64;
  PageHead: TFBPageHead;
  OdsHeader: TFBOdsPageHead;
  //PtrPageHead: pointer_page;
  PtrPageNumbers: array of Cardinal;
  DataPageHead: TFBDataPageHead;
  DataPageRecs: array of TFBDataPageRec;
  BlobPageHead: TFBBlobPageHead;
  RawPage: TByteArray;
  DataRecHead: TFBDataRecHead;
  DataFragRecHead: TFBDataFragRecHead;
  s: string;
  TmpPageItem: TRDB_PagesItem;
  BlobRecHead: TFBBlobRecHead;
begin
  Result := inherited OpenFile(AFileName);
  if not Result then Exit;

  // read header page
  FFile.Read(OdsHeader, SizeOf(OdsHeader));
  FPageSize := OdsHeader.PageSize;
  FOdsVersion := (OdsHeader.OdsVersion and $0F);

  LogInfo(Format('=== %d %s ===', [0, OdsPageTypeToStr(OdsHeader.PageHead.PageType)]));
  LogInfo(Format('PageSize=%d', [OdsHeader.PageSize]));
  s := Format('ODS Version=%d.%d', [(OdsHeader.OdsVersion and $0F), ((OdsHeader.OdsVersion and $7FF0) shr 4)]);
  FIsInterBase := (OdsHeader.OdsVersion and $8000) = 0;
  if FIsInterBase then
    s := s + ' (InterBase)'
  else
    s := s + ' (FireBird)';
  LogInfo(s);
  if IsLogPages then
  begin
    LogInfo(Format('PAGES=%d', [OdsHeader.PagesPage]));
    LogInfo(Format('Next Header=%d', [OdsHeader.NextPage]));
  end;

  // pages list
  nPage := 1;
  nPos := nPage * FPageSize;
  while nPos < (FFile.Size - 1024) do
  begin
    FFile.Position := nPos;
    FFile.Read(RawPage, FPageSize);
    Move(RawPage, PageHead, SizeOf(PageHead));
    if IsLogPages then
      LogInfo(Format('=== %d %s ===', [nPage, OdsPageTypeToStr(PageHead.PageType)]));

    case PageHead.PageType of
      {PAGE_TYPE_POINTER:
      begin
        // read database pointer page
        Move(RawPage, PtrPageHead, SizeOf(PtrPageHead));
        if IsLogPages then
        begin
          LogInfo(Format('Relation ID=%d', [PtrPageHead.ppg_relation]));
          LogInfo(Format('Next PTR Page=%d', [PtrPageHead.ppg_next]));
          LogInfo(Format('Count=%d', [PtrPageHead.ppg_count]));
        end;

        if PtrPageHead.ppg_count > 0 then
        begin
          SetLength(PtrPageNumbers, PtrPageHead.ppg_count);
          iOffs := SizeOf(PtrPageHead) - SizeOf(Cardinal);
          Move(RawPage[iOffs], PtrPageNumbers[0], PtrPageHead.ppg_count * SizeOf(Cardinal));

          for i := 0 to PtrPageHead.ppg_count - 1 do
          begin
            if IsLogPages then
              LogInfo(Format('Page Index=%d', [PtrPageNumbers[i]]));
          end;
        end;
      end; }

      PAGE_TYPE_DATA:
      begin
        // read data page
        Move(RawPage, DataPageHead, SizeOf(DataPageHead));

        // add to pages list
        iPageIndex := FindPageIndex(PageHead.PageType, DataPageHead.RelationID, DataPageHead.Sequence);
        if iPageIndex < 0 then
        begin
          TmpPageItem := TRDB_PagesItem.Create(FPagesList, Self, DataPageHead.RelationID, 0);
          TmpPageItem.PageType := PageHead.PageType;
          TmpPageItem.PageNum := nPage;
          TmpPageItem.RelationID := DataPageHead.RelationID;
          TmpPageItem.PageSeq := DataPageHead.Sequence;
          FPagesList.Insert(-iPageIndex-1, TmpPageItem);
        end;

        s := GetRelationName(DataPageHead.RelationID);
        if IsLogPages and (DataPageHead.RelationID = DebugRelID) then
          LogInfo(Format('=== #%d Seq=%d  Rel=%d (%s) Count=%d ===', [nPage, DataPageHead.Sequence, DataPageHead.RelationID, s, DataPageHead.Count]));

        if DataPageHead.Count > 0 then
        begin
          SetLength(DataPageRecs, DataPageHead.Count);
          iOffs := SizeOf(DataPageHead);
          Move(RawPage[iOffs], DataPageRecs[0], DataPageHead.Count * SizeOf(TFBDataPageRec));

          for i := 0 to DataPageHead.Count - 1 do
          begin
            if DataPageRecs[i].Length = 0 then
              Continue;
            CurRowID := (DataPageHead.Sequence * FMaxRecPerPage) + Cardinal(i);

            Move(RawPage[DataPageRecs[i].Offset], DataRecHead, SizeOf(DataRecHead));
            iDataHeadLen := SizeOf(DataRecHead);
            iDataOffs := DataPageRecs[i].Offset + iDataHeadLen;
            iDataLen := DataPageRecs[i].Length - iDataHeadLen;
            s := '';
            if (iDataLen > 0) and (DataRecHead.flags = 0) then
            begin
              SetLength(s, iDataLen);
              Move(RawPage[iDataOffs], s[1], iDataLen);
              s := RleDecompress(s);

              // == Sysyem tables
              if DataPageHead.RelationID < 40 then
                ReadSystemTable(DataPageHead.RelationID, CurRowID, s);

              if Length(s) > 4 then
                s := DataAsStr(s[5], Length(s)-4);

            end
            else
            if (DataRecHead.flags and REC_INCOMPLETE) > 0 then
            begin
              // incomplete
              Move(RawPage[DataPageRecs[i].Offset], DataFragRecHead, SizeOf(DataFragRecHead));
              iDataHeadLen := SizeOf(DataFragRecHead);
              iDataOffs := DataPageRecs[i].Offset + iDataHeadLen;
              iDataLen := DataPageRecs[i].Length - iDataHeadLen;
              if iDataLen > 0 then
              begin
                SetLength(s, iDataLen);
                Move(RawPage[iDataOffs], s[1], iDataLen);
                s := RleDecompress(s);
              end;
              {LogInfo(Format('INCOMPLETE Page=%d  RowID=%d  Flags=%s Fmt=%d  FragPage:Line=%d:%d  Unused=%d  Raw(%d)=%s Data(%d)=%s%s',
                [nPage,
                 (DataPageHead.Sequence * FMaxRecPerPage) + Cardinal(i),
                 DataFlagsToStr(DataFragRecHead.flags),
                 DataFragRecHead.format,
                 DataFragRecHead.f_page, DataFragRecHead.f_line,
                 DataFragRecHead.Unused,
                 iDataLen, BufferToHex(RawPage[iDataOffs], 4),
                 Length(s), BufferToHex(s[1], 4), DataAsStr(s[1], Length(s))
                ]));
              LogInfo(Format('INCOMPLETE Raw(%d)=%s', [iDataLen, BufferToHex(RawPage[iDataOffs], iDataLen)]));  }
              s := s + GetFragmentData(DataFragRecHead.f_page, DataFragRecHead.f_line);
              // == Sysyem tables
              if DataPageHead.RelationID < 40 then
                ReadSystemTable(DataPageHead.RelationID, CurRowID, s);
            end;
            
            // == BLOB record
            if (iDataLen > 0) and ((DataRecHead.flags and REC_BLOB) > 0) then
            begin
              // blob
              Move(RawPage[DataPageRecs[i].Offset], BlobRecHead, SizeOf(BlobRecHead));
              iDataHeadLen := SizeOf(BlobRecHead);
              iDataOffs := DataPageRecs[i].Offset + iDataHeadLen;
              iDataLen := DataPageRecs[i].Length - iDataHeadLen;

              //Move(RawPage[iDataOffs], BlobRecHead, SizeOf(BlobRecHead));

              if IsLogBlobs and (DataPageHead.RelationID = DebugRelID) then
              begin
                s := BufferToHex(RawPage[iDataOffs], 10);
                LogInfo(Format('DATA_REC_BLOB page=%d lead=%d  max_seq=%d  max_seg=%d  flags=%d  level=%d  count=%d  len=%d  sub_type=%d  blh_charset=%d %s',
                  [nPage,
                   BlobRecHead.LeadPage,
                   BlobRecHead.MaxSequence,
                   BlobRecHead.MaxSegment,
                   BlobRecHead.Flags,
                   BlobRecHead.Level,
                   BlobRecHead.Count,
                   BlobRecHead.Length,
                   BlobRecHead.SubType,
                   BlobRecHead.Charset,
                   s]));
              end;

              //iLen := iDataLen - SizeOf(BlobRecHead) - 4;
              iLen := iDataLen;
              //iLen := 0;
              if iLen > 0 then
              begin
                //Inc(iDataOffs, SizeOf(BlobRecHead) - 4);
                if BlobRecHead.SubType = 1 then  // TEXT
                begin
                  Inc(iDataOffs, 2);
                  Dec(iLen, 2);
                end;
                if BlobRecHead.SubType = 2 then  // BLR
                begin
                  Inc(iDataOffs, 9);
                  Dec(iLen, 9);
                end;
                if BlobRecHead.SubType = 3 then
                begin
                  Inc(iDataOffs, 6);
                  Dec(iLen, 6);
                end;
                if BlobRecHead.SubType = 5 then
                begin
                  Inc(iDataOffs, 8);
                  Dec(iLen, 8);
                end;

                if iLen > 0 then
                begin
                  SetLength(s, iLen);
                  Move(RawPage[iDataOffs], s[1], iLen);
                end;
                //s := RleDecompress(s);

                // == System tables (blobs)
                {if DataPageHead.RelationID < 40 then
                  ReadSystemTable(DataPageHead.RelationID, CurRowID, s, True); }

                if Length(s) > 0 then
                  s := DataAsStr(s[1], Length(s));

              end
              else
              begin
                SetLength(s, iDataLen);
                Move(RawPage[iDataOffs], s[1], iDataLen);
                //s := RleDecompress(s);

                if Length(s) > 0 then
                  s := DataAsStr(s[1], Length(s));
              end;

              if IsLogBlobs and (DataPageHead.RelationID = DebugRelID) then
                LogInfo(Format('DATA_REC_blob Rel=%d seq=%d flags=%s n=%d size=%d data=%s',
                  [DataPageHead.RelationID, DataPageHead.Sequence, DataFlagsToStr(DataRecHead.flags), i, iDataLen, s]));
            end;

            if IsLogPages and (DataPageHead.RelationID = DebugRelID) then
            begin
              LogInfo(Format('i=%d off=%d len=%d TID=%d flags=%s data=%s',
                [i, DataPageRecs[i].Offset, DataPageRecs[i].Length, DataRecHead.transaction,
                 DataFlagsToStr(DataRecHead.flags), s]));
            end;
          end;
        end;
      end;

      PAGE_TYPE_BLOB:
      begin
        // read blob page
        Move(RawPage, BlobPageHead, SizeOf(BlobPageHead));
        if BlobPageHead.Length > 0 then
        begin
          if (BlobPageHead.PageHead.Flags and PAGE_FLAG_BLOB_POINTERS) > 0 then
          begin
            // page with pointers to blobs
            SetLength(PtrPageNumbers, BlobPageHead.Length div 4);
            Move(RawPage[SizeOf(BlobPageHead)], PtrPageNumbers[0], BlobPageHead.Length);
            if IsLogBlobs then
              LogInfo(Format('blob lead=%d seq=%d len=%d count=%d',
                [BlobPageHead.LeadPage, BlobPageHead.Sequence, BlobPageHead.Length, Length(PtrPageNumbers)]));
          end
          else
          begin
            // page with blob
            SetLength(s, BlobPageHead.Length);
            Move(RawPage[SizeOf(BlobPageHead)], s[1], BlobPageHead.Length);
            if IsLogBlobs then
              LogInfo(Format('blob lead=%d seq=%d data=%s',
                [BlobPageHead.LeadPage, BlobPageHead.Sequence, DataAsStr(s[1], Length(s))]));
          end;
        end;
        //s := GetRelationName(DataPageHead.dpg_relation);
        //if IsLogPages then
        //  LogInfo(Format('Seq=%d  Rel=%d (%s) Count=%d', [DataPageHead.dpg_sequence, DataPageHead.dpg_relation, DataPageHead.dpg_count]));

      end;
    end;


    Inc(nPage);
    nPos := nPage * FPageSize;
    if nPage > 1000 then Break;

  end;
  FillTableInfo();

  // pages inventory

  //OdsPageTypeToStr
{  LogInfo(Format('', []));
  LogInfo(Format('', []));
  LogInfo(Format('', []));
  LogInfo(Format('', []));
  LogInfo(Format('', []));
  LogInfo(Format('', []));   }
end;

procedure TDBReaderFB.ReadSystemTable(ARelationID, ARowID: Cardinal; AData: AnsiString);
var
  TabItem: TRDB_RowItem;
  TableList: TRDB_RowsList;
  TmpPageItem: TRDB_PagesItem;
  iPageIndex: Integer;
begin
  TableList := nil;
  case ARelationID of
    0: TableList := FPagesList;
    2: TableList := FFieldsList;
    5: TableList := FRelationFieldsList;
    6: TableList := FRelationsList;
    8: TableList := FFormatsList;
  end;
  if not Assigned(TableList) then Exit;

  TabItem := TableList.ItemClass.Create(TableList, Self, ARelationID, ARowID);
  TabItem.ReadData(AData);
  if ARelationID = 0 then
  begin
    TmpPageItem := TabItem as TRDB_PagesItem;
    // add to pages list
    iPageIndex := FindPageIndex(TmpPageItem.PageType, TmpPageItem.RelationID, TmpPageItem.PageSeq);
    if iPageIndex < 0 then
    begin
      FPagesList.Insert(-iPageIndex-1, TmpPageItem);
    end;
  end
  else
    TableList.Add(TabItem);
  //LogInfo(TabItem.GetAsText());
end;

procedure TDBReaderFB.ReadTable(AName: string; ACount: Int64; AList: TDbRowsList);
var
  RelItem: TRDB_RelationsItem;
  RowList: TRDB_RowsList;
  TmpPageItem: TRDB_PagesItem;
  i, nPage, iOffs, iDataHeadLen, iDataOffs, iDataLen: Integer;
  nPos: Int64;
  RelID, iPageIndex: Integer;
  RawPage: TByteArray;
  PageHead: TFBPageHead;
  DataPageHead: TFBDataPageHead;
  DataPageRecs: array of TFBDataPageRec;
  DataRecHead: TFBDataRecHead;
  DataFragRecHead: TFBDataFragRecHead;
  s, sRaw: AnsiString;
  TableRowItem: TRDB_TableRowItem;
begin
  if not Assigned(AList) then Exit;
  AList.Clear();

  // find relation item, get RelID
  AName := UpperCase(AName);
  RelItem := nil;
  for i := 0 to FRelationsList.Count - 1 do
  begin
    RelItem := FRelationsList.GetItem(i) as TRDB_RelationsItem;
    if UpperCase(RelItem.RelationName) = AName then
      Break;
    RelItem := nil;
  end;
  if not Assigned(RelItem) then Exit;
  RelID := RelItem.RelationID;

  // Show table info
  LogInfo(Format('== TableName=%s  RelationID=%d', [RelItem.RelationName, RelItem.RelationID]));
  AList.TableName := RelItem.RelationName;
  //AList.RelationID := RelItem.RelationID;
  SetLength(AList.FieldsDef, Length(RelItem._FieldsInfo));
  if (AList is TRDB_RowsList) then
    SetLength((AList as TRDB_RowsList).FieldsInfo, Length(RelItem._FieldsInfo));
  for i := Low(RelItem._FieldsInfo) to High(RelItem._FieldsInfo) do
  begin
    if (AList is TRDB_RowsList) then
      (AList as TRDB_RowsList).FieldsInfo[i] := RelItem._FieldsInfo[i];
    AList.FieldsDef[i].Name := RelItem._FieldsInfo[i].Name;
    AList.FieldsDef[i].TypeName :=  RelItem._FieldsInfo[i].AsString;
    AList.FieldsDef[i].FieldType := FieldTypeToDbFieldType(RelItem._FieldsInfo[i].FieldType, 0, RelItem._FieldsInfo[i].FieldSubType);
    AList.FieldsDef[i].Size := RelItem._FieldsInfo[i].Size;
    AList.FieldsDef[i].RawOffset := RelItem._FieldsInfo[i].Offset;
    {LogInfo(Format('%.2d FieldName=%s  Size=%d  Type=%s  Len=%d  SubType=%d',
      [i,
       RelItem._FieldsInfo[i].Name,
       RelItem._FieldsInfo[i].Size,
       FieldTypeToStr(RelItem._FieldsInfo[i].FieldType, RelItem._FieldsInfo[i].FieldLength, RelItem._FieldsInfo[i].FieldSubType),
       RelItem._FieldsInfo[i].FieldLength,
       RelItem._FieldsInfo[i].FieldSubType]));  }
  end;
  //LogInfo('==');

  if (AList is TRDB_RowsList) then
  begin
    RowList := (AList as TRDB_RowsList);
    // fill rel info form list info, if not filled
    if (FOdsVersion = 11) and IsInterBase then
      RowList.FillSysTableInfo_ib11(RelID)
    else if (FOdsVersion = 11) then
      RowList.FillSysTableInfo_fb11(RelID)
    else if (FOdsVersion = 10) and IsInterBase then
      RowList.FillSysTableInfo_ib10(RelID)
    else
      RowList.FillSysTableInfo(RelID);
    Assert(High(RelItem._FieldsInfo) = High(RowList.FieldsInfo),
      Format(' List (%d) and item (%d) FieldsInfo not same length!', [Length(RowList.FieldsInfo), Length(RelItem._FieldsInfo)]));
    // copy fields format info from table to relation item
    for i := Low(RowList.FieldsInfo) to High(RowList.FieldsInfo) do
    begin
      if RelItem._FieldsInfo[i].DType = 0 then
      begin
        RelItem._FieldsInfo[i].DType := RowList.FieldsInfo[i].DType;
        RelItem._FieldsInfo[i].Length := RowList.FieldsInfo[i].Length;
        RelItem._FieldsInfo[i].Scale := RowList.FieldsInfo[i].Scale;
        RelItem._FieldsInfo[i].SubType := RowList.FieldsInfo[i].SubType;
        RelItem._FieldsInfo[i].Offset := RowList.FieldsInfo[i].Offset;
      end;
    end;
  end;

  if ACount = 0 then
    Exit;
  // find pages for RelID
  {for i := 0 to FPagesList.Count - 1 do
  begin
    PageItem := FPagesList.GetItem(i) as TRDB_PagesItem;
    if (PageItem.RelationID <> RelID) and (PageItem.PageType <> 4) then
      Continue;

  end;  }

  // read pages
  nPage := 1;
  nPos := nPage * FPageSize;
  while nPos < (FFile.Size - 1024) do
  begin
    FFile.Position := nPos;
    FFile.Read(RawPage, FPageSize);
    Inc(nPage);
    nPos := nPage * FPageSize;
    // read header
    Move(RawPage, PageHead, SizeOf(PageHead));

    case PageHead.PageType of
      PAGE_TYPE_DATA:
      begin
        // read data page
        Move(RawPage, DataPageHead, SizeOf(DataPageHead));
        if DataPageHead.RelationID <> RelID then
          Continue;

        // add to pages list
        iPageIndex := FindPageIndex(PageHead.PageType, DataPageHead.RelationID, DataPageHead.Sequence);
        if iPageIndex < 0 then
        begin
          TmpPageItem := TRDB_PagesItem.Create(FPagesList, Self, DataPageHead.RelationID, 0);
          TmpPageItem.PageType := PageHead.PageType;
          TmpPageItem.PageNum := nPage;
          TmpPageItem.RelationID := DataPageHead.RelationID;
          TmpPageItem.PageSeq := DataPageHead.Sequence;
          FPagesList.Insert(-iPageIndex-1, TmpPageItem);
        end;

        if DataPageHead.Count > 0 then
        begin
          SetLength(DataPageRecs, DataPageHead.Count);
          iOffs := SizeOf(DataPageHead);
          Move(RawPage[iOffs], DataPageRecs[0], DataPageHead.Count * SizeOf(DataPageRecs[0]));

          for i := 0 to DataPageHead.Count - 1 do
          begin
            if DataPageRecs[i].Length = 0 then
              Continue;
              
            Move(RawPage[DataPageRecs[i].Offset], DataRecHead, SizeOf(DataRecHead));
            iDataHeadLen := SizeOf(DataRecHead);
            iDataOffs := DataPageRecs[i].Offset + iDataHeadLen;
            iDataLen := DataPageRecs[i].Length - iDataHeadLen;

            s := '';
            if (iDataLen > 0) and (DataRecHead.flags = 0) then
            begin
              SetLength(s, iDataLen);
              Move(RawPage[iDataOffs], s[1], iDataLen);
              TableRowItem := TRDB_TableRowItem.Create(AList, Self, DataPageHead.RelationID, (DataPageHead.Sequence * FMaxRecPerPage) + Cardinal(i));
              TableRowItem.TableInfo := RelItem;
              //TableRowItem._RowID := (DataPageHead.Sequence * FMaxRecPerPage) + i;
              //TableRowItem.ReadRawData(RawPage[DataPageRecs[i].Offset], DataPageRecs[i].Length);
              if (DataRecHead.flags = 0) then
              begin
                // data record
                s := RleDecompress(s);
                TableRowItem.ReadData(s);
              end;

              if Assigned(AList) then
                AList.Add(TableRowItem)
              else
              begin
                LogInfo(TableRowItem.GetAsText);
                TableRowItem.Free();
              end;
              Dec(ACount);
              if ACount <= 0 then
                Exit;
              {sRaw := s;
              if Length(s) > 0 then
                s := DataAsStr(s[1], Length(s));
              LogInfo(Format('Page=%d  RowID=%d  Flags=%s  Raw(%d)=%s  Data(%d)=%s%s',
                [nPage,
                 (DataPageHead.Sequence * FMaxRecPerPage) + Cardinal(i),
                 DataFlagsToStr(DataRecHead.flags),
                 iDataLen, BufferToHex(RawPage[iDataOffs], 4),
                 Length(s), BufferToHex(sRaw[1], 4), s
                ]));   }
            end
            else
            if (DataRecHead.flags and REC_INCOMPLETE) > 0 then
            begin
              // incomplete
              Move(RawPage[DataPageRecs[i].Offset], DataFragRecHead, SizeOf(DataFragRecHead));
              iDataHeadLen := SizeOf(DataFragRecHead);
              iDataOffs := DataPageRecs[i].Offset + iDataHeadLen;
              iDataLen := DataPageRecs[i].Length - iDataHeadLen;
              if iDataLen > 0 then
              begin
                SetLength(s, iDataLen);
                Move(RawPage[iDataOffs], s[1], iDataLen);
                s := RleDecompress(s);
              end;
              {LogInfo(Format('INCOMPLETE Page=%d  RowID=%d  Flags=%s Fmt=%d  FragPage:Line=%d:%d  Unused=%d  Raw(%d)=%s Data(%d)=%s%s',
                [nPage,
                 (DataPageHead.Sequence * FMaxRecPerPage) + Cardinal(i),
                 DataFlagsToStr(DataFragRecHead.flags),
                 DataFragRecHead.format,
                 DataFragRecHead.f_page, DataFragRecHead.f_line,
                 DataFragRecHead.Unused,
                 iDataLen, BufferToHex(RawPage[iDataOffs], 4),
                 Length(s), BufferToHex(s[1], 4), DataAsStr(s[1], Length(s))
                ]));
              LogInfo(Format('INCOMPLETE Raw(%d)=%s', [iDataLen, BufferToHex(RawPage[iDataOffs], iDataLen)]));  }
              s := s + GetFragmentData(DataFragRecHead.f_page, DataFragRecHead.f_line);

              // read data to item
              TableRowItem := TRDB_TableRowItem.Create(AList, Self, DataPageHead.RelationID, (DataPageHead.Sequence * FMaxRecPerPage) + Cardinal(i));
              TableRowItem.TableInfo := RelItem;
              TableRowItem.ReadData(s);
              if Assigned(AList) then
                AList.Add(TableRowItem)
              else
              begin
                LogInfo(TableRowItem.GetAsText);
                TableRowItem.Free();
              end;

              {// debug all fragments
              sRaw := s;
              if Length(s) > 0 then
                s := DataAsStr(s[1], Length(s));
              LogInfo(Format('Page=%d  RowID=%d  Flags=[]  Raw(%d)=%s Data(%d)=%s%s',
                [nPage,
                 (DataPageHead.Sequence * FMaxRecPerPage) + Cardinal(i),
                 //DataFlagsToStr(DataRecHead.flags),
                 Length(s),
                 BufferToHex(RawPage[iDataOffs], 4),
                 Length(s), BufferToHex(sRaw[1], 4), s
                ]));   }
            end
            {else
            if (DataRecHead.flags and REC_FRAGMENT) > 0 then
            begin
              // fragment
              if iDataLen > 0 then
              begin
                SetLength(s, iDataLen);
                Move(RawPage[iDataOffs], s[1], iDataLen);
                //s := BufferToHex(RawPage[iDataOffs], iDataLen);
                s := RleDecompress(s);
                s := DataAsStr(RawPage[iDataOffs], iDataLen);
              end;
              LogInfo(Format('FRAGMENT Page=%d  Line=%d  Flags=%s  DataLen=%d  Data=%s',
                [nPage,
                 //(DataPageHead.Sequence * FMaxRecPerPage) + Cardinal(i),
                 i,
                 DataFlagsToStr(DataRecHead.flags),
                 iDataLen,
                 s
                ]));
            end   }
            else if (DataRecHead.flags and REC_FRAGMENT) > 0 then
              // ok
            else if (DataRecHead.flags and REC_BLOB) > 0 then
              // ok
            else
            begin
              if iDataLen > 0 then
              begin
                s := DataAsStr(RawPage[iDataOffs], iDataLen);
              end;
              LogInfo(Format('Page:Line=%d:%d  Flags=%s  Data(%d)=%s',
                [nPage, i,
                 DataFlagsToStr(DataRecHead.flags),
                 iDataLen,
                 s
                ]));
            
            end;
          end;
        end;

      end;
    end;
  end;
end;

{ TRDB_TableItem }

constructor TRDB_RowItem.Create(AOwner: TDbRowsList; AReader: TDBReaderFB; ARelID: Integer; ARowID: Cardinal);
begin
  inherited Create(AOwner);
  FReader := AReader;
  FRelID := ARelID;
  FRowID := ARowID;
end;

function TRDB_RowItem.GetAsText: string;
begin
  Result := '';
end;

function TRDB_RowItem.GetBlobValue(var AValue: TRDB_Blob): AnsiString;
begin
  if AValue.Data = '' then
    AValue.Data := Reader.GetBlobData(AValue.RelID, AValue.RowID);
  Result := AValue.Data;
end;

function TRDB_RowItem.GetFieldAsStr(AFieldIndex: Integer): string;
var
  d: Double;
begin
  if (AFieldIndex >= 0) and (AFieldIndex < Length(Values)) then
  begin
    if VarIsNull(Values[AFieldIndex]) then
      Result := '<null>'
    else
    if VarIsFloat(Values[AFieldIndex]) then
    begin
      // float point in short form
      d := Values[AFieldIndex];
      Result := Format('%.6g', [d]);
    end
    else
    if VarIsOrdinal(Values[AFieldIndex]) and IsBlobValue(AFieldIndex) then
    begin
      // read blob
      try
        Result := Reader.GetBlobData(RelID, Values[AFieldIndex]);
      except on E: Exception do
        Result := E.Message;
      end;
      //Values[AFieldIndex] := Result;
    end
    else
    if VarIsStr(Values[AFieldIndex]) then
    begin
      Result := Values[AFieldIndex];
      if Length(Result) > 0 then
        Result := DataAsStr(Result[1], Length(Result));
    end
    else
      Result := VarToStrDef(Values[AFieldIndex], '');
  end
  else
    Result := '<out of index>'
end;

function TRDB_RowItem.GetValueByName(AName: string): Variant;
var
  i: Integer;
begin
  Result := null;
  if Assigned(TableInfo) then
  begin
    AName := UpperCase(AName);
    for i := Low(TableInfo._FieldsInfo) to High(TableInfo._FieldsInfo) do
    begin
      if UpperCase(TableInfo._FieldsInfo[i].Name) = AName then
      begin
        Result := Values[i];
        Exit;
      end;
    end;
  end;
end;

function TRDB_RowItem.IsBlobValue(AIndex: Integer): Boolean;
begin
  Result := False;
  if Assigned(TableInfo) and (AIndex <= High(TableInfo._FieldsInfo)) then
  begin
    if (TableInfo._FieldsInfo[AIndex].DType = DTYPE_BLOB)
    or ((TableInfo._FieldsInfo[AIndex].DType = DTYPE_TEXT) and (TableInfo._FieldsInfo[AIndex].SubType = 1))
    or (TableInfo._FieldsInfo[AIndex].FieldType = 261)
    then
      Result := True;
  end;
end;

function TRDB_RowItem.IsNullValue(AIndex: Integer): Boolean;
var
  FlagIndex, FlagOffs: Integer;
begin
  Result := False;
  FlagIndex := AIndex div 8;
  FlagOffs := AIndex mod 8;

  if FlagIndex < Length(RawData) then
    Result := (Ord(RawData[FlagIndex+1]) and (Byte(1) shl FlagOffs)) <> 0;
end;

// Type 0 = 8 bytes
// Type 1 = 10 bytes
// Type 2 = 8 bytes
function TRDB_RowItem.ReadBlob(AType, ASize: Integer): TRDB_Blob;
var
  nMaxLen, nPos: Integer;
  wRow, wRel: Word;
  rec: TBlobFieldRec;
begin
  Result.RelID := 0;
  Result.RowID := 0;
  Result.BlobType := AType;
  Result.Data := '';
  if ASize = 0 then
  begin
    case AType of
      0: nMaxLen := 8;  // BIN
      1: nMaxLen := 8;  // TEXT
      2: nMaxLen := 4;  // BLR ??
      5: nMaxLen := 10; // ??
      6: nMaxLen := 6;  // ??
      8: nMaxLen := 9;  // ??
    else
      nMaxLen := 8;
    end;
  end
  else
    nMaxLen := ASize;

  // Align
  //nPos := FDataPos + ((FDataPos-1) mod 4);
  nPos := FDataPos;

  FillChar(rec, SizeOf(rec), 0);
  if AType in [0, 1, 5, 6, 8] then
  begin
    if nPos-1 + SizeOf(rec) <= Length(RawData) then
    begin
      Move(RawData[nPos], rec, SizeOf(rec));
      //Result := Format('%d:%d', [rec.RelationID, rec.RecID]);
      Result.RelID := rec.RelationID;
      Result.RowID := rec.RowID;
      if (rec.RelationID <> 0) then
        Result.Data := Reader.GetBlobData(RelID, rec.RowID)
    end;

    if Reader.IsLogBlobs and ((rec.RelationID <> 0) or (rec.RowID <> 0)) then
    begin
      Reader.LogInfo( Format('BLOB_FIELD RelID=%d RowID=%d : type=%d rel=%d rec=%d raw(%d)=%s dump(%d)=%s data=%s',
        [RelID, RowID,
        AType, rec.RelationID, rec.RowID,
        nPos, BufferToHex(RawData[nPos], SizeOf(rec)),
        FDataPos, BufferToHex(RawData[FDataPos], nMaxLen),
        Result.Data]) );
    end;
  end
  else
  if AType = 2 then
  begin
    if FDataPos-1 + SizeOf(rec) <= Length(RawData) then
    begin
      Move(RawData[FDataPos], wRow, SizeOf(wRow));
      Move(RawData[FDataPos+2], wRel, SizeOf(wRel));
      rec.RowID := wRow;
      rec.RelationID := wRel;
      //Result := Format(':%d', [rec.RecID]);
      if rec.RelationID = Self.RelID then
      begin
        Result.RowID := Self.RowID-1;
        Result.RelID := Self.RelID;
        //Result.Data := Reader.GetBlobData(RelID, rec.RowID);
        Result.Data := Format('!BLOB_BLR row=%d rel=%d', [rec.RowID, rec.RelationID]);
      end
      else
      begin
        Result.RowID := rec.RowID;
        //Result.Data := Reader.GetBlobData(RelID, rec.RowID);
        Result.Data := Format('BLOB_BLR row=%d rel=%d %s', [rec.RowID, rec.RelationID, BufferToHex(RawData[FDataPos], 4)]);
      end;
    end;
  end
  else
    Result.Data := BufferToHex(RawData[FDataPos], nMaxLen);

  Inc(FDataPos, nMaxLen);
end;

function TRDB_RowItem.ReadChar(ACount: Integer): string;
var
  nPos: Integer;
begin
  Result := '';

  nPos := FDataPos;
  while ((nPos - FDataPos) < ACount) and (nPos <= Length(RawData)) do
  begin
    if RawData[nPos] = #0 then
      Break;
    Inc(nPos);
  end;
  if nPos > FDataPos then
    Result := Trim(Copy(RawData, FDataPos, nPos-FDataPos));

  Inc(FDataPos, ACount);
end;

function TRDB_RowItem.ReadCurrency(ABytes: Integer): Currency;
begin
  Result := 0;
  if (ABytes > 0) and (Length(RawData) >= (FDataPos + ABytes)) then
    Move(RawData[FDataPos], Result, 8);
  Inc(FDataPos, ABytes);
end;

procedure TRDB_RowItem.ReadData(const AData: AnsiString);
begin
  // decompress ?
  // read header ?
  RawData := AData;
  FDataPos := 5;
end;

function TRDB_RowItem.ReadDateTime(AType, ABytes: Integer): TDateTime;
var
  Days, Days2, Part, nPos: Integer;
  //yy, mm, dd: Word;
  //dt: TDateTime;
  //s: string;
begin
  // alignment
  //nPos := FDataPos + 4 - ((FDataPos-1) mod 4);
  nPos := FDataPos;

  Result := 0;
  Part := 0;
  // AType 0-DateTime 1-Date 2-Time
  if AType < 2 then
  begin
    // days from epoch begining
    Move(RawData[nPos], Days, 4);
    if Days = 0 then
      Result := 0
    else
      Result := Days - FB_EPOCH_DIFF;
    if AType = 0 then
      Move(RawData[nPos+4], Part, 4);
  end
  else
  begin
    // part of day
    Move(RawData[nPos], Part, 4);
  end;
  // part of day
  Result := Result + (1 / (24*60*60*10000) * Part);

{$ifdef DEBUG}
  // not needed
  if Result < -(366 * 3000) then
    Result := EncodeDate(0001, 01, 01)
  else if Result > (366 * 3000) then
    Result := EncodeDate(3000, 01, 01);

  {if RelID = DebugRelID then
    _Log( IntToHex(FDataPos-1, 4)
       + ' ' + BufferToHex(FRawData[FDataPos], 12)
       + ' ' + IntToStr(nPos-FDataPos)
       + ' ' + BufferToHex(FRawData[nPos], 4) + '=' + IntToStr(Days)
       //+ ' ' + BufferToHex(Days2, 4) + '=' + IntToStr(Days2)
       + ' = ' + FormatDateTime('YYYY-MM-DD HH:NN:SS', Result)
    );  }
{$endif}
  Inc(FDataPos, ABytes);
end;

function TRDB_RowItem.ReadFloat(ABytes: Integer): Double;
var
  fSingle: Single;
begin
  if (FDataPos+ABytes) <= Length(RawData) then
  begin
    if ABytes = 4 then
    begin
      Move(RawData[FDataPos], fSingle, SizeOf(fSingle));
      Result := fSingle;
    end
    else
      Move(RawData[FDataPos], Result, SizeOf(Result))
  end
  else
    Result := 0;
  Inc(FDataPos, ABytes);
end;

function TRDB_RowItem.ReadInt(ABytes: Integer): Integer;
begin
  Result := 0;
  if (ABytes > 0) and (Length(RawData) >= (FDataPos + ABytes)) then
    Move(RawData[FDataPos], Result, ABytes);
  Inc(FDataPos, ABytes);
end;

function TRDB_RowItem.ReadInt64(ABytes: Integer): Int64;
begin
  Result := 0;
  if (ABytes > 0) and (Length(RawData) >= (FDataPos + ABytes)) then
    Move(RawData[FDataPos], Result, 8);
  Inc(FDataPos, ABytes);
end;

procedure TRDB_RowItem.ReadRawData(const AData; ALen: Integer);
var
  sData: AnsiString;
  ptr: PByte;
  DataRecHead: TFBDataRecHead;
  iDataLen: Integer;
begin
  if ALen = 0 then Exit;
  ptr := Addr(AData);
  Move(AData, DataRecHead, SizeOf(DataRecHead));
  iDataLen := ALen - SizeOf(DataRecHead);
  if (iDataLen > 0) and (DataRecHead.flags = 0) then
  begin
    SetLength(sData, iDataLen);
    Inc(ptr, SizeOf(DataRecHead));
    Move(ptr^, sData[1], iDataLen);
    sData := RleDecompress(sData);
    ReadData(sData);
  end;
end;

function TRDB_RowItem.ReadVarChar(ACount: Integer; ASize: Integer = 0): string;
var
  nPos: Integer;
  nLen: Word;
begin
  Result := '';
  if ASize = 0 then
  begin
    ASize := ACount + 2 + 4;  // 2 bytes lenght
    // align
    nPos := FDataPos + ASize;
    nPos := nPos + ((nPos-1) mod 2);
    ASize := nPos - FDataPos;
  end;

  nPos := FDataPos;
  // read length SMALLINT
  Move(RawData[nPos], nLen, SizeOf(nLen));
  Inc(nPos, SizeOf(nLen));
  if nLen > ACount then
    nLen := ACount;

  if (nPos + nLen <= Length(RawData)) then
    Result := Copy(RawData, nPos, nLen);

  Inc(FDataPos, ASize);
end;

{ TRDB_PagesItem }

function TRDB_PagesItem.GetAsText: string;
begin
  Result := Format('PageNum=%d Type=%d RelID=%d Seq=%d',
                  [PageNum, PageType, RelationID, PageSeq]);
end;

procedure TRDB_PagesItem.ReadData(const AData: AnsiString);
begin
  inherited;
  PageNum := ReadInt(4);             // INTEGER
  RelationID := ReadInt(2);          // SMALLINT
  PageSeq := ReadInt(4);             // INTEGER
  PageType := ReadInt(2);            // SMALLINT

  RawData := '';
end;

{ TRDB_FieldsItem }

function TRDB_FieldsItem.GetAsText: string;
begin
  Result := Format('Field=%s Len=%d Type=%d Sub=%d',
                  [FieldName, FieldLength, FieldType, FieldSubType]);
end;

procedure TRDB_FieldsItem.ReadData(const AData: AnsiString);
begin
  //StrToFile(AData, 'test_s1.data');
  inherited;

  if Reader.IsInterBase then
  begin
    if Reader.OdsVersion = 10 then
    begin
      FieldName := ReadChar(31);       // CHAR(31)
      Inc(FDataPos, 1);
      QueryName := ReadChar(31);       // CHAR(31)
      Inc(FDataPos, 1);
    end
    else
    begin
      FieldName := ReadChar(67);       // CHAR(67)
      Inc(FDataPos, 1);
      QueryName := ReadChar(67);       // CHAR(67)
      Inc(FDataPos, 1);
    end;
    ValidationBlr := ReadBlob(2, 8);    // BLOB SUB_TYPE 2
    ValidationSource := ReadBlob(1, 8); // BLOB SUB_TYPE TEXT    8
    ComputedBlr := ReadBlob(2, 8);      // BLOB SUB_TYPE 2      8
    ComputedSource := ReadBlob(1, 8); // BLOB SUB_TYPE TEXT    8
    DefaultValue := ReadBlob(2, 8);     // BLOB SUB_TYPE 2    8
    DefaultSource := ReadBlob(1, 8); // BLOB SUB_TYPE TEXT    8
    if Reader.OdsVersion = 10 then
      FDataPos := 116 + 1
    else
      FDataPos := 188 + 1;  // pos 188
  end
  else // FireBird
  begin
    FieldName := ReadChar(31);       // CHAR(31)
    QueryName := ReadChar(31);       // CHAR(31)
    ValidationBlr := ReadBlob(2);    // BLOB SUB_TYPE 2
    ValidationSource := ReadBlob(1, 14); // BLOB SUB_TYPE TEXT    14
    ComputedBlr := ReadBlob(2);      // BLOB SUB_TYPE 2
    ComputedSource := ReadBlob(1, 14); // BLOB SUB_TYPE TEXT    14
    DefaultValue := ReadBlob(2);     // BLOB SUB_TYPE 2
    DefaultSource := ReadBlob(1, 14); // BLOB SUB_TYPE TEXT    14
  end;
  FieldLength := ReadInt(2);       // SMALLINT
  FieldScale := ReadInt(2);        // SMALLINT
  FieldType := ReadInt(2);         // SMALLINT
  FieldSubType := ReadInt(2);      // SMALLINT
  MissingValue := ReadBlob(2);     // BLOB SUB_TYPE 2
  MissingSource := ReadBlob(1, 10); // BLOB SUB_TYPE TEXT   10
  Description := ReadBlob(1, 10);  // BLOB SUB_TYPE TEXT   10
  SystemFlag := ReadInt(2);        // SMALLINT
  QueryHeader := ReadBlob(1, 14);  // BLOB SUB_TYPE TEXT 14
  SegmentLength := ReadInt(2);     // SMALLINT
  EditString := ReadVarChar(125, 128);  // VARCHAR(125) + 3
  ExternalLength := ReadInt(2);    // SMALLINT
  ExternalScale := ReadInt(2);     // SMALLINT
  ExternalType := ReadInt(2);      // SMALLINT
  Dimensions := ReadInt(2);        // SMALLINT
  NullFlag := ReadInt(2);          // SMALLINT
  CharacterLength := ReadInt(2);   // SMALLINT
  CollationID := ReadInt(2);       // SMALLINT
  CharacterSetID := ReadInt(2);    // SMALLINT
  FieldPrecision := ReadInt(2);    // SMALLINT

  RawData := '';
end;

{ TRDB_RelationFieldsItem }

function TRDB_RelationFieldsItem.GetAsText: string;
begin
  Result := Format('FieldID=%d Field=%s Table=%s Src=%s',
                  [FieldID, FieldName, RelationName, FieldSource]);
end;

procedure TRDB_RelationFieldsItem.ReadData(const AData: AnsiString);
begin
  //StrToFile(AData, 'test_s1.data');
  inherited;
  FieldName := ReadChar(31);       // CHAR(31)
  RelationName := ReadChar(31);    // CHAR(31)
  FieldSource := ReadChar(31);     // CHAR(31)
  QueryName := ReadChar(31);       // CHAR(31)
  if Reader.IsInterBase and (Reader.OdsVersion = 11) then
  begin
    BaseField := ReadChar(63);       // CHAR(31)
    EditString := ReadVarChar(125, 129); // VARCHAR(125)        125+4
  end
  else // FireBird
  begin
    BaseField := ReadChar(31);       // CHAR(31)
    EditString := ReadVarChar(125, 129); // VARCHAR(125)        125+4
  end;
  FieldPosition := ReadInt(2);     // SMALLINT
  QueryHeader := ReadBlob(1, 14);  // BLOB SUB_TYPE TEXT      14
  UpdateFlag := ReadInt(2);        // SMALLINT
  FieldID := ReadInt(2);           // SMALLINT                         (unique for RelationName)
  ViewContext := ReadInt(2);       // SMALLINT
  Description := ReadBlob(1, 14);  // BLOB SUB_TYPE TEXT      14
  DefaultValue := ReadBlob(2, 4);  // BLOB SUB_TYPE 2         4    !!!
  SystemFlag := ReadInt(2);        // SMALLINT
  SecurityClass := ReadChar(31);   // CHAR(31)
  ComplexName := ReadChar(31);     // CHAR(31)
  NullFlag := ReadInt(2);          // SMALLINT
  DefaultSource := ReadBlob(1);    // BLOB SUB_TYPE TEXT
  CollationID := ReadInt(2);       // SMALLINT

  RawData := '';
end;

{ TRDB_RelationsItem }

function TRDB_RelationsItem.GetAsText: string;
begin
  Result := SysUtils.Format('FieldID=%d Table=%s Runtime=%s',
                   [FieldID, RelationName, Runtime]);
end;

function TRDB_RelationsItem.GetRuntime: AnsiString;
begin
  Result := GetBlobValue(FRuntime);
end;

procedure TRDB_RelationsItem.ReadData(const AData: AnsiString);
begin
  //StrToFile(AData, 'test_s1.data');
  inherited;
  //if (Owner as TRDB_RowsList). then
  if Reader.IsInterBase then
  begin
    ViewBlr := ReadBlob(1);         // BLOB SUB_TYPE 2 BLR   ? 4
    ViewSource := ReadBlob(1);      // BLOB SUB_TYPE 1 TEXT  ? 8
    Description := ReadBlob(1);     // BLOB SUB_TYPE 1 TEXT  ? 8
    //Inc(FDataPos, 8);               // skip 8
  end
  else // FireBird
  begin
    ViewBlr := ReadBlob(2);         // BLOB SUB_TYPE 2 BLR   ? 4
    ViewSource := ReadBlob(1);      // BLOB SUB_TYPE 1 TEXT  ? 8
    Description := ReadBlob(1);     // BLOB SUB_TYPE 1 TEXT  ? 8
    Inc(FDataPos, 8);               // skip 8
  end;
  RelationID := ReadInt(2);       // SMALLINT
  SystemFlag := ReadInt(2);       // SMALLINT
  DBKeyLen := ReadInt(2);         // SMALLINT
  Format := ReadInt(2);           // SMALLINT
  FieldID := ReadInt(2);          // SMALLINT
  RelationName := ReadChar(31);   // CHAR(31)
  SecurityClass := ReadChar(31);  // CHAR(31)
  ExternalFile := ReadChar(253);  // CHAR(253)
  Inc(FDataPos, 3);               // skip 3
  FRuntime := ReadBlob(5, 8);     // BLOB SUB_TYPE 5    8
  ExternalDescription := ReadBlob(8, 8); // BLOB SUB_TYPE 8   8
  OwnerName := ReadChar(31);      // CHAR(31)
  DefaultClass := ReadChar(31);   // CHAR(31)
  Flags := ReadInt(2);            // SMALLINT
  RelationType := ReadInt(2);     // SMALLINT

  {Reader.LogInfo( SysUtils.Format('RelationID=%d FieldID=%d RelationName=%s OwnerName=%s',
     [RelationID, FieldID, RelationName, OwnerName]) ); }
  RawData := '';
end;

{ TRDB_FormatsItem }

function TRDB_FormatsItem.FillFieldInfo(var AFieldInfo: TRDB_FieldInfoRec; AFieldIndex: Integer): Boolean;
var
  nPre, iPos, iRecSize: Integer;
  FormatRec: TFBFormatRec;
  sData: AnsiString;
  rdr: TRawDataReader;
begin
  Result := False;

  nPre := 2;
  iRecSize := 12;  // SizeOf(TFBFormatRec)
  if (Reader.OdsVersion = 11) and Reader.IsInterBase then
  begin
    nPre := 0;
    iRecSize := 16;
  end
  else
  if (Reader.OdsVersion = 11) then
    nPre := 2
  else
  if Reader.OdsVersion = 12 then
    nPre := 4;

  iPos := nPre + (AFieldIndex * iRecSize);
  sData := GetBlobValue(Descriptor);
  if iPos + iRecSize > Length(sData) then
    Exit;
  //Move(sData[iPos+1], FormatRec, SizeOf(FormatRec));
  //StrToFile(sData, 'test_s1.data');
  rdr.Init(sData, iPos+1);

  // Format rec
  {AFieldInfo.DType := FormatRec.DType;
  AFieldInfo.Scale := FormatRec.Scale;
  AFieldInfo.Length := FormatRec.Length;
  AFieldInfo.SubType := FormatRec.SubType;
  AFieldInfo.Flags := FormatRec.Flags;
  AFieldInfo.Offset := FormatRec.Offset;  }
  //
  if Reader.IsInterBase and (Reader.OdsVersion = 11) then
  begin
    rdr.ReadUInt16;
    rdr.ReadUInt8;
    AFieldInfo.DType := rdr.ReadUInt8;
    AFieldInfo.Scale := rdr.ReadInt16;
    AFieldInfo.Length := rdr.ReadUInt16;
    AFieldInfo.SubType := rdr.ReadUInt16;
    AFieldInfo.Flags := rdr.ReadUInt32;
    AFieldInfo.Offset := rdr.ReadUInt16;
  end
  else // FireBird
  begin
    { DType: Byte;
      Scale: Shortint;
      Length: Word;
      SubType: Word;  (codepage for CHAR) For BLOB/TEXT and integers
      Flags: Word;
      Address: DWORD_PTR; Position from begining of row }
    AFieldInfo.DType := rdr.ReadUInt8;
    AFieldInfo.Scale := rdr.ReadInt8;
    AFieldInfo.Length := rdr.ReadUInt16;
    AFieldInfo.SubType := rdr.ReadUInt16;
    AFieldInfo.Flags := rdr.ReadUInt16;
    AFieldInfo.Offset := rdr.ReadUInt32;
  end;

  //AFieldInfo.Size := FormatRec.Length;
  //AFieldInfo.FieldType := FormatRec.DType;
  //AFieldInfo.FieldSubType := FormatRec.SubType;
  //AFieldInfo.FieldLength := FormatRec.Length;

  if RelationID = Reader.DebugRelID then
  begin
    if AFieldIndex = 0 then
      Reader.LogInfo(SysUtils.Format('FORMAT BlobType=%d len=%d raw=%s', [Descriptor.BlobType, Length(sData), BufferToHex(sData[1], Length(sData))]));

    Reader.LogInfo(SysUtils.Format('FORMAT raw=%s  DType=%d(%d) Scale=%d Len=%d(%d) SubType=%d(%d) Flags=%d Offs=%d',
      [BufferToHex(sData[iPos+1], SizeOf(FormatRec)),
      AFieldInfo.DType, AFieldInfo.FieldType,
      AFieldInfo.Scale,
      AFieldInfo.Length, AFieldInfo.FieldLength,
      AFieldInfo.SubType, AFieldInfo.FieldSubType,
      AFieldInfo.Flags,
      AFieldInfo.Offset]
    ));
  end;
  Result := True;
end;

function TRDB_FormatsItem.GetAsText: string;
begin
  Result := '';
end;

procedure TRDB_FormatsItem.ReadData(const AData: AnsiString);
begin
  inherited;
  RelationID := ReadInt(2);       // SMALLINT
  Format := ReadInt(2);           // SMALLINT
  Descriptor := ReadBlob(6);      // BLOB SUB_TYPE 6

  RawData := '';
end;

{ TRDB_TableList }

constructor TRDB_RowsList.Create(AItemClass: TRDB_RowClass);
begin
  inherited Create();
  ItemClass := AItemClass;
end;

procedure TRDB_RowsList.FillSysTableInfo(ARelID: Integer);
begin
  FillSysTableInfo_fb11(ARelID);
end;

procedure TRDB_RowsList.FillSysTableInfo_fb11(ARelID: Integer);
var
  i: Integer;
  iAddr: Cardinal;
begin
  i := 0;
  case ARelID of
    0: // RDB$PAGES
    begin
      TableName := 'RDB$PAGES';
      SetLength(FieldsInfo, 4);
      iAddr := 4;
      SetFieldInfo(i, iAddr, 'PAGE_NUMBER', DTYPE_LONG, 4, 0);
      SetFieldInfo(i, iAddr, 'RELATION_ID', DTYPE_SHORT, 2, 0);
      SetFieldInfo(i, iAddr, 'PAGE_SEQUENCE', DTYPE_LONG, 4, 0);
      SetFieldInfo(i, iAddr, 'PAGE_TYPE', DTYPE_SHORT, 2, 0);
    end;
    2: // RDB$FIELDS
    begin
      TableName := 'RDB$FIELDS';
      SetLength(FieldsInfo, 28);
      iAddr := 4;
      SetFieldInfo(i, iAddr, 'FIELD_NAME', DTYPE_CSTRING, 31, 0);     // CHAR(31)
      SetFieldInfo(i, iAddr, 'QUERY_NAME', DTYPE_CSTRING, 31, 0);     // CHAR(31)
      SetFieldInfo(i, iAddr, 'VALIDATION_BLR', DTYPE_BLOB, 4, 2);     // BLOB 2
      SetFieldInfo(i, iAddr, 'VALIDATION_SOURCE', DTYPE_TEXT, 14, 1); // TEXT   14
      SetFieldInfo(i, iAddr, 'COMPUTED_BLR', DTYPE_BLOB, 4, 2);       // BLOB 2
      SetFieldInfo(i, iAddr, 'COMPUTED_SOURCE', DTYPE_TEXT, 14, 1);   // TEXT   14
      SetFieldInfo(i, iAddr, 'DEFAULT_VALUE', DTYPE_BLOB, 4, 2);      // BLOB 2
      SetFieldInfo(i, iAddr, 'DEFAULT_SOURCE', DTYPE_TEXT, 14, 1);    // TEXT    14
      SetFieldInfo(i, iAddr, 'FIELD_LENGHT', DTYPE_SHORT, 2, 0);      // SMALLINT
      SetFieldInfo(i, iAddr, 'FIELD_SCALE', DTYPE_SHORT, 2, 0);       // SMALLINT
      SetFieldInfo(i, iAddr, 'FIELD_TYPE', DTYPE_SHORT, 2, 0);        // SMALLINT
      SetFieldInfo(i, iAddr, 'FIELD_SUB_TYPE', DTYPE_SHORT, 2, 0);    // SMALLINT
      SetFieldInfo(i, iAddr, 'MISSING_VALUE', DTYPE_BLOB, 4, 2);      // BLOB 2
      SetFieldInfo(i, iAddr, 'MISSING_SOURCE', DTYPE_BLOB, 10, 1);    // BLOB TEXT  10
      SetFieldInfo(i, iAddr, 'DESCRIPTION', DTYPE_BLOB, 10, 1);       // BLOB TEXT     10
      SetFieldInfo(i, iAddr, 'SYSTEM_FLAG', DTYPE_SHORT, 2, 0);       // SMALLINT
      SetFieldInfo(i, iAddr, 'QUERY_HEADER', DTYPE_TEXT, 14, 1);      // TEXT    14
      SetFieldInfo(i, iAddr, 'SEGMENT_LENGTH', DTYPE_SHORT, 2, 0);    // SMALLINT
      SetFieldInfo(i, iAddr, 'EDIT_STRING', DTYPE_VARYNG, 125, 0);    // VARCHAR(125) + 3
      Inc(iAddr, 1);
      SetFieldInfo(i, iAddr, 'EXTERNAL_LENGTH', DTYPE_SHORT, 2, 0);   // SMALLINT
      SetFieldInfo(i, iAddr, 'EXTERNAL_SCALE', DTYPE_SHORT, 2, 0);    // SMALLINT
      SetFieldInfo(i, iAddr, 'EXTERNAL_TYPE', DTYPE_SHORT, 2, 0);     // SMALLINT
      SetFieldInfo(i, iAddr, 'DIMENSIONS', DTYPE_SHORT, 2, 0);        // SMALLINT
      SetFieldInfo(i, iAddr, 'NULL_FLAG', DTYPE_SHORT, 2, 0);         // SMALLINT
      SetFieldInfo(i, iAddr, 'CHARACTER_LENGTH', DTYPE_SHORT, 2, 0);  // SMALLINT
      SetFieldInfo(i, iAddr, 'COLLATION_ID', DTYPE_SHORT, 2, 0);      // SMALLINT
      SetFieldInfo(i, iAddr, 'CHARACTER_SET_ID', DTYPE_SHORT, 2, 0);  // SMALLINT
      SetFieldInfo(i, iAddr, 'FIELD_PRECISION', DTYPE_SHORT, 2, 0);   // SMALLINT
    end;

    5: // RDB$RELATION_FIELDS
    begin
      TableName := 'RDB$RELATION_FIELDS';
      SetLength(FieldsInfo, 19);
      iAddr := 4;
      SetFieldInfo(i, iAddr, 'FIELD_NAME', DTYPE_CSTRING, 31, 0);     // CHAR(31)
      SetFieldInfo(i, iAddr, 'RELATION_NAME', DTYPE_CSTRING, 31, 0);  // CHAR(31)
      SetFieldInfo(i, iAddr, 'FIELD_SOURCE', DTYPE_CSTRING, 31, 0);   // CHAR(31)
      SetFieldInfo(i, iAddr, 'QUERY_NAME', DTYPE_CSTRING, 31, 0);     // CHAR(31)
      SetFieldInfo(i, iAddr, 'BASE_FIELD', DTYPE_CSTRING, 31, 0);     // CHAR(31)
      SetFieldInfo(i, iAddr, 'EDIT_STRING', DTYPE_VARYNG, 127, 0);    // VARCHAR(125) + 4
      SetFieldInfo(i, iAddr, 'FIELD_POSITION', DTYPE_SHORT, 2, 0);    // SMALLINT
      SetFieldInfo(i, iAddr, 'QUERY_HEADER', DTYPE_TEXT, 14, 1);      // BLOB TEXT
      SetFieldInfo(i, iAddr, 'UPDATE_FLAG', DTYPE_SHORT, 2, 0);       // SMALLINT
      SetFieldInfo(i, iAddr, 'FIELD_ID', DTYPE_SHORT, 2, 0);          // SMALLINT
      SetFieldInfo(i, iAddr, 'VIEW_CONTEXT', DTYPE_SHORT, 2, 0);      // SMALLINT
      SetFieldInfo(i, iAddr, 'DESCRIPTION', DTYPE_TEXT, 14, 1);       // BLOB TEXT
      SetFieldInfo(i, iAddr, 'DEFAULT_VALUE', DTYPE_BLOB, 4, 2);      // BLOB BLR
      SetFieldInfo(i, iAddr, 'SYSTEM_FLAG', DTYPE_SHORT, 2, 0);       // SMALLINT
      SetFieldInfo(i, iAddr, 'SECURITY_CLASS', DTYPE_CSTRING, 31, 0); // CHAR(31)
      SetFieldInfo(i, iAddr, 'COMPLEX_NAME', DTYPE_CSTRING, 31, 0);   // CHAR(31)
      SetFieldInfo(i, iAddr, 'NULL_FLAG', DTYPE_SHORT, 2, 0);         // SMALLINT
      SetFieldInfo(i, iAddr, 'DEFAULT_SOURCE', DTYPE_TEXT, 14, 1);    // BLOB TEXT
      SetFieldInfo(i, iAddr, 'COLLATION_ID', DTYPE_SHORT, 2, 0);      // SMALLINT
    end;

    6: // RDB$RELATIONS
    begin
      TableName := 'RDB$RELATIONS';
      SetLength(FieldsInfo, 17);
      iAddr := 4;
      SetFieldInfo(i, iAddr, 'VIEW_BLR', DTYPE_BLOB, 4, 2);     // BLOB BLR
      SetFieldInfo(i, iAddr, 'VIEW_SOURCE', DTYPE_BLOB, 8, 1);  // BLOB TEXT
      SetFieldInfo(i, iAddr, 'DESCRIPTION', DTYPE_BLOB, 8, 1);  // BLOB TEXT
      Inc(iAddr, 8);               // skip 8
      SetFieldInfo(i, iAddr, 'RELATION_ID', DTYPE_SHORT, 2, 0);
      SetFieldInfo(i, iAddr, 'SYSTEM_FLAG', DTYPE_SHORT, 2, 0);
      SetFieldInfo(i, iAddr, 'DBKEY_LENGTH', DTYPE_SHORT, 2, 0);
      SetFieldInfo(i, iAddr, 'FORMAT', DTYPE_SHORT, 2, 0);
      SetFieldInfo(i, iAddr, 'FIELD_ID', DTYPE_SHORT, 2, 0);
      SetFieldInfo(i, iAddr, 'RELATION_NAME', DTYPE_CSTRING, 31, 0);
      SetFieldInfo(i, iAddr, 'SECURITY_CLASS', DTYPE_CSTRING, 31, 0);
      SetFieldInfo(i, iAddr, 'EXTERNAL_FILE', DTYPE_VARYNG, 255, 0); // CHAR(253)
      Inc(iAddr, 1);               // skip
      SetFieldInfo(i, iAddr, 'RUNTIME', DTYPE_BLOB, 8, 5);
      SetFieldInfo(i, iAddr, 'EXTERNAL_DESCRIPTION', DTYPE_BLOB, 8, 8);
      SetFieldInfo(i, iAddr, 'OWNER_NAME', DTYPE_CSTRING, 31, 0);
      SetFieldInfo(i, iAddr, 'DEFAULT_CLASS', DTYPE_CSTRING, 31, 0);
      SetFieldInfo(i, iAddr, 'FLAGS', DTYPE_SHORT, 2, 0);
      SetFieldInfo(i, iAddr, 'RELATION_TYPE', DTYPE_SHORT, 2, 0);
    end;
    8: // RDB$FORMATS
    begin
      TableName := 'RDB$FORMATS';
      SetLength(FieldsInfo, 3);
      iAddr := 4;
      SetFieldInfo(i, iAddr, 'RELATION_ID', DTYPE_SHORT, 2, 0);
      SetFieldInfo(i, iAddr, 'FORMAT', DTYPE_SHORT, 2, 0);
      SetFieldInfo(i, iAddr, 'DESCRIPTOR', DTYPE_BLOB, 8, 6);
    end;
  end;
end;

procedure TRDB_RowsList.FillSysTableInfo_ib10(ARelID: Integer);
var
  i: Integer;
  iAddr: Cardinal;
begin
  // InterBase (ODS 10)
  i := 0;
  case ARelID of
    0: // RDB$PAGES
    begin
      TableName := 'RDB$PAGES';
      SetLength(FieldsInfo, 4);
      iAddr := 4;
      SetFieldInfo(i, iAddr, 'PAGE_NUMBER', DTYPE_LONG, 4, 0);
      SetFieldInfo(i, iAddr, 'RELATION_ID', DTYPE_SHORT, 2, 0);
      SetFieldInfo(i, iAddr, 'PAGE_SEQUENCE', DTYPE_LONG, 4, 0);
      SetFieldInfo(i, iAddr, 'PAGE_TYPE', DTYPE_SHORT, 2, 0);
    end;
    2: // RDB$FIELDS
    begin
      TableName := 'RDB$FIELDS';
      SetLength(FieldsInfo, 28);
      iAddr := 4;
      SetFieldInfo(i, iAddr, 'FIELD_NAME', DTYPE_TEXT, 31, 3);        // CHAR(31)UTF8     t=14.3
      SetFieldInfo(i, iAddr, 'QUERY_NAME', DTYPE_TEXT, 31, 3);        // CHAR(31)UTF8     FIELD_NAME
      SetFieldInfo(i, iAddr, 'VALIDATION_BLR', DTYPE_BLOB, 8, 2);     // BLOB_2(8)        t=261 st=2
      SetFieldInfo(i, iAddr, 'VALIDATION_SOURCE', DTYPE_BLOB, 8, 1);  // BLOB_TEXT(8)     SOURCE  t=261 st=1
      SetFieldInfo(i, iAddr, 'COMPUTED_BLR', DTYPE_BLOB, 8, 2);       // BLOB_2(8)        VALUE   t=261 st=2
      SetFieldInfo(i, iAddr, 'COMPUTED_SOURCE', DTYPE_BLOB, 8, 1);    // BLOB_TEXT(8)     SOURCE
      SetFieldInfo(i, iAddr, 'DEFAULT_VALUE', DTYPE_BLOB, 8, 2);      // BLOB_2(8)        VALUE
      SetFieldInfo(i, iAddr, 'DEFAULT_SOURCE', DTYPE_TEXT, 8, 1);     // BLOB_TEXT(8)     SOURCE
      // pos
      iAddr := 116;
      SetFieldInfo(i, iAddr, 'FIELD_LENGHT', DTYPE_SHORT, 2, 0);      // SMALLINT
      SetFieldInfo(i, iAddr, 'FIELD_SCALE', DTYPE_SHORT, 2, 0);       // SMALLINT
      SetFieldInfo(i, iAddr, 'FIELD_TYPE', DTYPE_SHORT, 2, 0);        // SMALLINT
      SetFieldInfo(i, iAddr, 'FIELD_SUB_TYPE', DTYPE_SHORT, 2, 0);    // SMALLINT
      SetFieldInfo(i, iAddr, 'MISSING_VALUE', DTYPE_BLOB, 8, 2);      // BLOB_2
      SetFieldInfo(i, iAddr, 'MISSING_SOURCE', DTYPE_BLOB, 8, 1);     // BLOB_TEXT(8)
      SetFieldInfo(i, iAddr, 'DESCRIPTION', DTYPE_BLOB, 8, 1);        // BLOB_TEXT(8)
      SetFieldInfo(i, iAddr, 'SYSTEM_FLAG', DTYPE_SHORT, 2, 0);       // SMALLINT
      SetFieldInfo(i, iAddr, 'QUERY_HEADER', DTYPE_BLOB, 8, 1);       // BLOB_TEXT(8)
      SetFieldInfo(i, iAddr, 'SEGMENT_LENGTH', DTYPE_SHORT, 2, 0);    // SMALLINT
      SetFieldInfo(i, iAddr, 'EDIT_STRING', DTYPE_VARYNG, 125, 0);    // VARCHAR(125) + 3
      Inc(iAddr, 1);
      SetFieldInfo(i, iAddr, 'EXTERNAL_LENGTH', DTYPE_SHORT, 2, 0);   // SMALLINT
      SetFieldInfo(i, iAddr, 'EXTERNAL_SCALE', DTYPE_SHORT, 2, 0);    // SMALLINT
      SetFieldInfo(i, iAddr, 'EXTERNAL_TYPE', DTYPE_SHORT, 2, 0);     // SMALLINT
      SetFieldInfo(i, iAddr, 'DIMENSIONS', DTYPE_SHORT, 2, 0);        // SMALLINT
      SetFieldInfo(i, iAddr, 'NULL_FLAG', DTYPE_SHORT, 2, 0);         // SMALLINT
      SetFieldInfo(i, iAddr, 'CHARACTER_LENGTH', DTYPE_SHORT, 2, 0);  // SMALLINT
      SetFieldInfo(i, iAddr, 'COLLATION_ID', DTYPE_SHORT, 2, 0);      // SMALLINT
      SetFieldInfo(i, iAddr, 'CHARACTER_SET_ID', DTYPE_SHORT, 2, 0);  // SMALLINT
      SetFieldInfo(i, iAddr, 'FIELD_PRECISION', DTYPE_SHORT, 2, 0);   // SMALLINT
    end;

    5: // RDB$RELATION_FIELDS
    begin
      TableName := 'RDB$RELATION_FIELDS';
      SetLength(FieldsInfo, 19);
      iAddr := 4;
      SetFieldInfo(i, iAddr, 'FIELD_NAME', DTYPE_TEXT, 31, 3);        // CHAR(31)UTF8
      SetFieldInfo(i, iAddr, 'RELATION_NAME', DTYPE_TEXT, 31, 3);     // CHAR(31)UTF8
      SetFieldInfo(i, iAddr, 'FIELD_SOURCE', DTYPE_TEXT, 31, 3);      // CHAR(31)UTF8  FIELD_NAME
      SetFieldInfo(i, iAddr, 'QUERY_NAME', DTYPE_TEXT, 31, 3);        // CHAR(31)UTF8  FIELD_NAME
      SetFieldInfo(i, iAddr, 'BASE_FIELD', DTYPE_TEXT, 31, 3);        // CHAR(31)UTF8  FIELD_NAME
      SetFieldInfo(i, iAddr, 'EDIT_STRING', DTYPE_VARYNG, 127, 0);    // VARCHAR(125)
      SetFieldInfo(i, iAddr, 'FIELD_POSITION', DTYPE_SHORT, 2, 0);    // SMALLINT
      SetFieldInfo(i, iAddr, 'QUERY_HEADER', DTYPE_BLOB, 10, 1);      // BLOB_TEXT(8)  10
      SetFieldInfo(i, iAddr, 'UPDATE_FLAG', DTYPE_SHORT, 2, 0);       // SMALLINT
      SetFieldInfo(i, iAddr, 'FIELD_ID', DTYPE_SHORT, 2, 0);          // SMALLINT
      SetFieldInfo(i, iAddr, 'VIEW_CONTEXT', DTYPE_SHORT, 2, 0);      // SMALLINT
      SetFieldInfo(i, iAddr, 'DESCRIPTION', DTYPE_BLOB, 10, 1);       // BLOB_TEXT(8)  10
      SetFieldInfo(i, iAddr, 'DEFAULT_VALUE', DTYPE_BLOB, 8, 2);      // BLOB_BLR(8)
      SetFieldInfo(i, iAddr, 'SYSTEM_FLAG', DTYPE_SHORT, 2, 0);       // SMALLINT
      SetFieldInfo(i, iAddr, 'SECURITY_CLASS', DTYPE_TEXT, 31, 3);    // CHAR(31)UTF8
      SetFieldInfo(i, iAddr, 'COMPLEX_NAME', DTYPE_TEXT, 31, 3);      // CHAR(31)UTF8
      SetFieldInfo(i, iAddr, 'NULL_FLAG', DTYPE_SHORT, 2, 0);         // SMALLINT
      SetFieldInfo(i, iAddr, 'DEFAULT_SOURCE', DTYPE_BLOB, 8, 1);     // BLOB_TEXT(8)
      SetFieldInfo(i, iAddr, 'COLLATION_ID', DTYPE_SHORT, 2, 0);      // SMALLINT
    end;

    6: // RDB$RELATIONS
    begin
      TableName := 'RDB$RELATIONS';
      SetLength(FieldsInfo, 16);
      iAddr := 4;
      SetFieldInfo(i, iAddr, 'VIEW_BLR', DTYPE_BLOB, 8, 2);     // BLOB_BLR(8)
      SetFieldInfo(i, iAddr, 'VIEW_SOURCE', DTYPE_BLOB, 8, 1);  // BLOB_TEXT(8)
      SetFieldInfo(i, iAddr, 'DESCRIPTION', DTYPE_BLOB, 8, 1);  // BLOB_TEXT(8)
      //Inc(iAddr, 8);               // skip 8
      SetFieldInfo(i, iAddr, 'RELATION_ID', DTYPE_SHORT, 2, 0);
      SetFieldInfo(i, iAddr, 'SYSTEM_FLAG', DTYPE_SHORT, 2, 0);
      SetFieldInfo(i, iAddr, 'DBKEY_LENGTH', DTYPE_SHORT, 2, 0);
      SetFieldInfo(i, iAddr, 'FORMAT', DTYPE_SHORT, 2, 0);
      SetFieldInfo(i, iAddr, 'FIELD_ID', DTYPE_SHORT, 2, 0);
      SetFieldInfo(i, iAddr, 'RELATION_NAME', DTYPE_TEXT, 31, 3);     // CHAR(31)UTF8
      SetFieldInfo(i, iAddr, 'SECURITY_CLASS', DTYPE_TEXT, 31, 3);    // CHAR(31)UTF8
      SetFieldInfo(i, iAddr, 'EXTERNAL_FILE', DTYPE_VARYNG, 253, 0);  // VARCHAR(253)
      Inc(iAddr, 1);               // skip
      SetFieldInfo(i, iAddr, 'RUNTIME', DTYPE_BLOB, 8, 5);            // BLOB_5(8)
      SetFieldInfo(i, iAddr, 'EXTERNAL_DESCRIPTION', DTYPE_BLOB, 8, 8); // BLOB_8(8)
      SetFieldInfo(i, iAddr, 'OWNER_NAME', DTYPE_TEXT, 31, 3);        // CHAR(31)UTF8
      SetFieldInfo(i, iAddr, 'DEFAULT_CLASS', DTYPE_TEXT, 31, 3);     // CHAR(31)UTF8
      SetFieldInfo(i, iAddr, 'FLAGS', DTYPE_SHORT, 2, 0);             // unaligned ???
    end;
    8: // RDB$FORMATS
    begin
      TableName := 'RDB$FORMATS';
      SetLength(FieldsInfo, 3);
      iAddr := 4;
      SetFieldInfo(i, iAddr, 'RELATION_ID', DTYPE_SHORT, 2, 0);
      SetFieldInfo(i, iAddr, 'FORMAT', DTYPE_SHORT, 2, 0);
      SetFieldInfo(i, iAddr, 'DESCRIPTOR', DTYPE_BLOB, 8, 6);
    end;
  end;
end;

procedure TRDB_RowsList.FillSysTableInfo_ib11(ARelID: Integer);
var
  i: Integer;
  iAddr: Cardinal;
begin
  // InterBase (ODS 11)
  i := 0;
  case ARelID of
    0: // RDB$PAGES
    begin
      TableName := 'RDB$PAGES';
      SetLength(FieldsInfo, 4);
      iAddr := 4;
      SetFieldInfo(i, iAddr, 'PAGE_NUMBER', DTYPE_LONG, 4, 0);
      SetFieldInfo(i, iAddr, 'RELATION_ID', DTYPE_SHORT, 2, 0);
      SetFieldInfo(i, iAddr, 'PAGE_SEQUENCE', DTYPE_LONG, 4, 0);
      SetFieldInfo(i, iAddr, 'PAGE_TYPE', DTYPE_SHORT, 2, 0);
    end;
    2: // RDB$FIELDS
    begin
      TableName := 'RDB$FIELDS';
      SetLength(FieldsInfo, 28);
      iAddr := 4;
      SetFieldInfo(i, iAddr, 'FIELD_NAME', DTYPE_CSTRING, 67, 0);     // CHAR(67)     t=14 st=3
      Inc(iAddr, 1);
      SetFieldInfo(i, iAddr, 'QUERY_NAME', DTYPE_CSTRING, 67, 0);     // CHAR(67)     FIELD_NAME
      Inc(iAddr, 1);
      SetFieldInfo(i, iAddr, 'VALIDATION_BLR', DTYPE_BLOB, 8, 2);     // BLOB 2  8    t=261 st=2
      SetFieldInfo(i, iAddr, 'VALIDATION_SOURCE', DTYPE_TEXT, 8, 1); // TEXT   8      SOURCE  t=261 st=1
      SetFieldInfo(i, iAddr, 'COMPUTED_BLR', DTYPE_BLOB, 8, 2);       // BLOB 2  8    VALUE   t=261 st=2
      SetFieldInfo(i, iAddr, 'COMPUTED_SOURCE', DTYPE_TEXT, 8, 1);   // TEXT   14    SOURCE
      SetFieldInfo(i, iAddr, 'DEFAULT_VALUE', DTYPE_BLOB, 8, 2);      // BLOB 2       VALUE
      SetFieldInfo(i, iAddr, 'DEFAULT_SOURCE', DTYPE_TEXT, 8, 1);    // TEXT    14   SOURCE
      // pos 188
      iAddr := 188;
      SetFieldInfo(i, iAddr, 'FIELD_LENGHT', DTYPE_SHORT, 2, 0);      // SMALLINT
      SetFieldInfo(i, iAddr, 'FIELD_SCALE', DTYPE_SHORT, 2, 0);       // SMALLINT
      SetFieldInfo(i, iAddr, 'FIELD_TYPE', DTYPE_SHORT, 2, 0);        // SMALLINT
      SetFieldInfo(i, iAddr, 'FIELD_SUB_TYPE', DTYPE_SHORT, 2, 0);    // SMALLINT
      SetFieldInfo(i, iAddr, 'MISSING_VALUE', DTYPE_BLOB, 4, 2);      // BLOB 2
      SetFieldInfo(i, iAddr, 'MISSING_SOURCE', DTYPE_BLOB, 10, 1);    // BLOB TEXT  10
      SetFieldInfo(i, iAddr, 'DESCRIPTION', DTYPE_BLOB, 10, 1);       // BLOB TEXT     10
      SetFieldInfo(i, iAddr, 'SYSTEM_FLAG', DTYPE_SHORT, 2, 0);       // SMALLINT
      SetFieldInfo(i, iAddr, 'QUERY_HEADER', DTYPE_TEXT, 14, 1);      // TEXT    14
      SetFieldInfo(i, iAddr, 'SEGMENT_LENGTH', DTYPE_SHORT, 2, 0);    // SMALLINT
      SetFieldInfo(i, iAddr, 'EDIT_STRING', DTYPE_VARYNG, 125, 0);    // VARCHAR(125) + 3
      Inc(iAddr, 1);
      SetFieldInfo(i, iAddr, 'EXTERNAL_LENGTH', DTYPE_SHORT, 2, 0);   // SMALLINT
      SetFieldInfo(i, iAddr, 'EXTERNAL_SCALE', DTYPE_SHORT, 2, 0);    // SMALLINT
      SetFieldInfo(i, iAddr, 'EXTERNAL_TYPE', DTYPE_SHORT, 2, 0);     // SMALLINT
      SetFieldInfo(i, iAddr, 'DIMENSIONS', DTYPE_SHORT, 2, 0);        // SMALLINT
      SetFieldInfo(i, iAddr, 'NULL_FLAG', DTYPE_SHORT, 2, 0);         // SMALLINT
      SetFieldInfo(i, iAddr, 'CHARACTER_LENGTH', DTYPE_SHORT, 2, 0);  // SMALLINT
      SetFieldInfo(i, iAddr, 'COLLATION_ID', DTYPE_SHORT, 2, 0);      // SMALLINT
      SetFieldInfo(i, iAddr, 'CHARACTER_SET_ID', DTYPE_SHORT, 2, 0);  // SMALLINT
      SetFieldInfo(i, iAddr, 'FIELD_PRECISION', DTYPE_SHORT, 2, 0);   // SMALLINT
    end;

    5: // RDB$RELATION_FIELDS
    begin
      TableName := 'RDB$RELATION_FIELDS';
      SetLength(FieldsInfo, 19);
      iAddr := 4;
      SetFieldInfo(i, iAddr, 'FIELD_NAME', DTYPE_TEXT, 67, 3);        // CHAR(67)UTF8
      SetFieldInfo(i, iAddr, 'RELATION_NAME', DTYPE_TEXT, 67, 3);     // CHAR(67)UTF8
      SetFieldInfo(i, iAddr, 'FIELD_SOURCE', DTYPE_TEXT, 67, 3);      // CHAR(67)UTF8  FIELD_NAME
      SetFieldInfo(i, iAddr, 'QUERY_NAME', DTYPE_TEXT, 67, 3);        // CHAR(67)UTF8  FIELD_NAME
      SetFieldInfo(i, iAddr, 'BASE_FIELD', DTYPE_TEXT, 67, 3);        // CHAR(67)UTF8  FIELD_NAME
      SetFieldInfo(i, iAddr, 'EDIT_STRING', DTYPE_VARYNG, 127, 0);    // VARCHAR(127) CP_0
      SetFieldInfo(i, iAddr, 'FIELD_POSITION', DTYPE_SHORT, 2, 0);    // SMALLINT
      SetFieldInfo(i, iAddr, 'QUERY_HEADER', DTYPE_BLOB, 8, 1);       // BLOB_TEXT(8)
      SetFieldInfo(i, iAddr, 'UPDATE_FLAG', DTYPE_SHORT, 2, 0);       // SMALLINT
      SetFieldInfo(i, iAddr, 'FIELD_ID', DTYPE_SHORT, 2, 0);          // SMALLINT
      SetFieldInfo(i, iAddr, 'VIEW_CONTEXT', DTYPE_SHORT, 2, 0);      // SMALLINT
      SetFieldInfo(i, iAddr, 'DESCRIPTION', DTYPE_BLOB, 8, 1);        // BLOB_TEXT(8)
      SetFieldInfo(i, iAddr, 'DEFAULT_VALUE', DTYPE_BLOB, 8, 2);      // BLOB_BLR(8)
      SetFieldInfo(i, iAddr, 'SYSTEM_FLAG', DTYPE_SHORT, 2, 0);       // SMALLINT
      SetFieldInfo(i, iAddr, 'SECURITY_CLASS', DTYPE_TEXT, 67, 3);    // CHAR(67)UTF8
      SetFieldInfo(i, iAddr, 'COMPLEX_NAME', DTYPE_TEXT, 67, 3);      // CHAR(67)UTF8
      SetFieldInfo(i, iAddr, 'NULL_FLAG', DTYPE_SHORT, 2, 0);         // SMALLINT
      SetFieldInfo(i, iAddr, 'DEFAULT_SOURCE', DTYPE_BLOB, 8, 1);     // BLOB_TEXT(8)
      SetFieldInfo(i, iAddr, 'COLLATION_ID', DTYPE_SHORT, 2, 0);      // SMALLINT
    end;

    6: // RDB$RELATIONS
    begin
      TableName := 'RDB$RELATIONS';
      SetLength(FieldsInfo, 17);
      iAddr := 4;
      SetFieldInfo(i, iAddr, 'VIEW_BLR', DTYPE_BLOB, 8, 2);     // BLOB_BLR(8)
      SetFieldInfo(i, iAddr, 'VIEW_SOURCE', DTYPE_BLOB, 8, 1);  // BLOB_TEXT(8)
      SetFieldInfo(i, iAddr, 'DESCRIPTION', DTYPE_BLOB, 8, 1);  // BLOB_TEXT(8)
      //Inc(iAddr, 8);               // skip 8
      SetFieldInfo(i, iAddr, 'RELATION_ID', DTYPE_SHORT, 2, 0);
      SetFieldInfo(i, iAddr, 'SYSTEM_FLAG', DTYPE_SHORT, 2, 0);
      SetFieldInfo(i, iAddr, 'DBKEY_LENGTH', DTYPE_SHORT, 2, 0);
      SetFieldInfo(i, iAddr, 'FORMAT', DTYPE_SHORT, 2, 0);
      SetFieldInfo(i, iAddr, 'FIELD_ID', DTYPE_SHORT, 2, 0);
      SetFieldInfo(i, iAddr, 'RELATION_NAME', DTYPE_TEXT, 67, 3);     // CHAR(67)UTF8
      SetFieldInfo(i, iAddr, 'SECURITY_CLASS', DTYPE_TEXT, 67, 3);    // CHAR(67)UTF8
      SetFieldInfo(i, iAddr, 'EXTERNAL_FILE', DTYPE_VARYNG, 255, 0);  // VARCHAR(253)
      Inc(iAddr, 1);               // skip
      SetFieldInfo(i, iAddr, 'RUNTIME', DTYPE_BLOB, 8, 5);            // BLOB_5(8)
      SetFieldInfo(i, iAddr, 'EXTERNAL_DESCRIPTION', DTYPE_BLOB, 8, 8); // BLOB_8(8)
      SetFieldInfo(i, iAddr, 'OWNER_NAME', DTYPE_TEXT, 67, 3);        // CHAR(67)UTF8
      SetFieldInfo(i, iAddr, 'DEFAULT_CLASS', DTYPE_TEXT, 67, 3);     // CHAR(67)UTF8
      SetFieldInfo(i, iAddr, 'FLAGS', DTYPE_SHORT, 2, 0);
      SetFieldInfo(i, iAddr, 'RELATION_TYPE', DTYPE_CSTRING, 31, 0);  // CHAR(31)
    end;
    8: // RDB$FORMATS
    begin
      TableName := 'RDB$FORMATS';
      SetLength(FieldsInfo, 3);
      iAddr := 4;
      SetFieldInfo(i, iAddr, 'RELATION_ID', DTYPE_SHORT, 2, 0);
      SetFieldInfo(i, iAddr, 'FORMAT', DTYPE_SHORT, 2, 0);
      SetFieldInfo(i, iAddr, 'DESCRIPTOR', DTYPE_BLOB, 8, 6);
    end;
  end;
end;

function TRDB_RowsList.GetItem(AIndex: Integer): TRDB_RowItem;
begin
  Result := TRDB_RowItem(Get(AIndex));
end;

procedure TRDB_RowsList.SetFieldInfo(var AIndex: Integer; var AOffs: Cardinal; AName: string; AType, ALen, ASub: Word);
begin
  FieldsInfo[AIndex].Name := 'RDB$' + AName;
  FieldsInfo[AIndex].DType := Byte(AType);
  FieldsInfo[AIndex].Length := ALen;
  FieldsInfo[AIndex].SubType := ASub;
  FieldsInfo[AIndex].Offset := AOffs;

  // inherited FieldsDef
  if Length(FieldsDef) <= AIndex then
    SetLength(FieldsDef, AIndex+1);
  FieldsDef[AIndex].Name := FieldsInfo[AIndex].Name;
  FieldsDef[AIndex].TypeName := FieldsInfo[AIndex].AsString;
  FieldsDef[AIndex].FieldType := FieldTypeToDbFieldType(FieldsInfo[AIndex].FieldType, FieldsInfo[AIndex].Length, FieldsInfo[AIndex].SubType);
  FieldsDef[AIndex].Size := FieldsInfo[AIndex].GetSize;
  FieldsDef[AIndex].RawOffset := AOffs;

  case AType of
    DTYPE_VARYNG: Inc(ALen, 2);
  end;
  Inc(AIndex);
  Inc(AOffs, ALen);
end;

{ TRDB_TableRowItem }

function TRDB_TableRowItem.GetAsText: string;
var
  i: Integer;
begin
  Result := IntToHex(RowID, 8) + ' ';
  if not Assigned(TableInfo) then Exit;
  for i := Low(TableInfo._FieldsInfo) to High(TableInfo._FieldsInfo) do
  begin
    if Result <> '' then
      Result := Result + '  ';
    Result := Result + TableInfo._FieldsInfo[i].Name + '=' + VarToStrDef(Values[i], '');
  end;
end;

procedure TRDB_TableRowItem.ReadData(const AData: AnsiString);
var
  i, nSize, nLen, nSub: Integer;
  dType: Word;
  BlobRec: TRDB_Blob;
begin
  //StrToFile(AData, 'test_s1.data');
  inherited ReadData(AData);
  if not Assigned(TableInfo) then Exit;

  SetLength(Values, Length(TableInfo._FieldsInfo));
  for i := Low(TableInfo._FieldsInfo) to High(TableInfo._FieldsInfo) do
  begin
    if (not Reader.IsInterBase) and IsNullValue(i) then
    begin
      Values[i] := Null;
      // Interbase keep null values
      Continue;
    end;

    dType := TableInfo._FieldsInfo[i].DType;
    if dType = 0 then
    begin
      // no format descriptor, use field definition
      dType := TableInfo._FieldsInfo[i].FieldType;
      nSize := TableInfo._FieldsInfo[i].Size;
      nLen := TableInfo._FieldsInfo[i].FieldLength;
      nSub := TableInfo._FieldsInfo[i].FieldSubType;

      if (nSize = 0) then
      begin
        Values[i] := Null;
        Continue;
      end;

      case dType of
        7: Values[i] := ReadInt(2); // SMALLINT
        8: Values[i] := ReadInt(4); // INTEGER
        //8: Values[i] := ReadInt64(); // INTEGER
        10: Values[i] := ReadFloat(); // FLOAT
        14: Values[i] := ReadChar(nLen); // CHAR
        16:
        begin
          if nSub = 1 then
            Values[i] := ReadCurrency // CURRENCY
          else
            Values[i] := ReadInt64; // INT64
        end;
        27: Values[i] := ReadFloat(); // DOUBLE
        35: Values[i] := ReadDateTime(0, nSize); // DATETIME
        37: Values[i] := ReadVarChar(nLen, nSize); // VARCHAR
        261:
        begin
          BlobRec := ReadBlob(nSub, nLen); // BLOB
          if BlobRec.Data <> '' then
            Values[i] := BlobRec.Data
          else
          if (BlobRec.RelID <> 0) then
            Values[i] := BlobRec.RowID;
        end;
      else
        Values[i] := Null;
      end;
    end
    else
    begin
      // use format descriptor
      nSize := TableInfo._FieldsInfo[i].GetSize();
      nLen := TableInfo._FieldsInfo[i].Length;
      nSub := TableInfo._FieldsInfo[i].FieldSubType;
      
      FDataPos := TableInfo._FieldsInfo[i].Offset + 1;
      case dType of
        DTYPE_TEXT:
        begin
          // encoding?
          if (nSub = 0) or (nSub = 3) then
            Values[i] := ReadChar(nLen)
          else
          begin
            BlobRec := ReadBlob(nSub);
            if BlobRec.Data <> '' then
              Values[i] := BlobRec.Data
            else
            if (BlobRec.RelID <> 0) then
              Values[i] := BlobRec.RowID;
          end;
        end;
        DTYPE_CSTRING:   Values[i] := ReadChar(nLen);
        DTYPE_VARYNG:    Values[i] := ReadVarChar(nLen);
        //DTYPE_PACKED:    = 6;
        DTYPE_BYTE:      Values[i] := ReadInt(1);  // 1  Byte
        DTYPE_SHORT:     Values[i] := ReadInt(2);  // 2  Word
        DTYPE_LONG:      Values[i] := ReadInt(4);  // 4  Cardinal
        DTYPE_QUAD:      Values[i] := ReadInt64;  // 8  DWord
        DTYPE_REAL:      Values[i] := ReadFloat(4); // 4  Single
        DTYPE_DOUBLE:    Values[i] := ReadFloat(); // 8  Double
        DTYPE_DFLOAT:    Values[i] := ReadFloat(); //
        DTYPE_SQL_DATE:  Values[i] := ReadDateTime(1, 4); // 4  Integer
        DTYPE_SQL_TIME:  Values[i] := ReadDateTime(2, 4); // 4  Cardinal
        DTYPE_TIMESTAMP: Values[i] := ReadDateTime(0, 8); // 8  DateTime
        DTYPE_BLOB:
        begin
          BlobRec := ReadBlob(nSub, nLen);
          if BlobRec.Data <> '' then
          begin
            if Reader.IsInterBase then
            begin
              case nSub of
                1: Values[i] := Copy(BlobRec.Data, 3, MaxInt); // TEXT  skip first 2 bytes
              else
                Values[i] := BlobRec.Data
              end;
            end
            else
              Values[i] := BlobRec.Data
          end
          else
          if (BlobRec.RelID <> 0) then
            Values[i] := BlobRec.RowID;
        end;
        DTYPE_ARRAY:     Values[i] := '?ARRAY';
        DTYPE_INT64:
        begin
          if nSub = 1 then
            Values[i] := ReadCurrency // 8 Currency
          else
            Values[i] := ReadInt64; // 8 Int64
        end;
      else
        Values[i] := Null;
      end;
    end;

    if Reader.IsInterBase and IsNullValue(i) then
      Values[i] := Null;
  end;

  // leave only null flags at beginning of row
  //SetLength(FRawData, ((Length(TableInfo._FieldsInfo]) div 32) + 1) * 4);
  if Reader.IsClearRawData then
    RawData := '';
end;

{ TFBTable }
{
constructor TFBTable.Create(ARelationID: Integer; AName: string;
  AItemClass: TRDB_TableClass = TRDB_TableRowItem);
begin
  inherited Create();
  FDataList := TRDB_TableList.Create(AItemClass);
  FBlobList := TRDB_TableList.Create(AItemClass);
  FRelationID := ARelationID;
  FName := AName;
end;

destructor TFBTable.Destroy;
begin
  FreeAndNil(FBlobList);
  FreeAndNil(FDataList);
  inherited;
end;  }

{ TRDB_FieldInfoRec }

function TRDB_FieldInfoRec.AsString: string;
begin
  if DType <> 0 then
    Result := DataTypeToStr(DType, Length, SubType)
  else
    Result := FieldTypeToStr(FieldType, FieldLength, FieldSubType);
end;

function TRDB_FieldInfoRec.GetSize: Integer;
begin
  case DType of
    DTYPE_TEXT:      Result := 8;
    DTYPE_CSTRING:   Result := Length;  // Len
    DTYPE_VARYNG:    Result := 2 + Length;  // 2 + Len
    DTYPE_PACKED:    Result := 4;
    DTYPE_BYTE:      Result := 1; // 1  Byte
    DTYPE_SHORT:     Result := 2; // 2  Word
    DTYPE_LONG:      Result := 4; // 4  Cardinal
    DTYPE_QUAD:      Result := 8; // 8  DWord
    DTYPE_REAL:      Result := 4; // 4  Single
    DTYPE_DOUBLE:    Result := 8; // 8  Double
    DTYPE_DFLOAT:    Result := 8; //
    DTYPE_SQL_DATE:  Result := 4; // 4  Integer
    DTYPE_SQL_TIME:  Result := 4; // 4  Cardinal
    DTYPE_TIMESTAMP: Result := 8; // 8  DateTime
    DTYPE_BLOB:      Result := 8; // 8  RelID/RowID
    DTYPE_ARRAY:     Result := 8;
    DTYPE_INT64:     Result := 8; // 8 Int64
  else
    Result := Length;
  end;
end;

end.
