unit DBReaderDbf;

(*
DBF database file reader

Author: Sergey Bodrov, 2024 Minsk
License: MIT

*)

interface

uses
  Windows, {for OemToChar}
  SysUtils, Classes, Variants, DBReaderBase, DB;

type
  TDbfHeaderRec = packed record
    FileType: Byte;                   // DBF File type - See DBF_FILE_TYPE_
    LastUpdate: array [0..2] of Byte; // Last update (YYMMDD)
    RecCount: Cardinal;               // Number of records in file
    FirstRowOffs: Word;               // Position of first data record
    RecLength: Word;                  // Length of one data record, including delete flag
    Reserved12: array [12..27] of Byte;
    TableFlags: Byte;                 // see DBF_FILE_FLAG_
    CodePage: Byte;                   // Code page mark
    Reserved30: Word;                 // Reserved, contains 0x00
  end;

  // After header:
  //   Field subrecords
  //   Header record terminator (0x0D)
  //   (optional) Visual Foxpro only: A 263-byte range that contains the backlink, which is the relative
  //              path of an associated database (.dbc) file, information. If the first byte is 0x00,
  //              the file is not associated with a database. Therefore, database files always contain 0x00.

  TDbfFieldSubRec = packed record
    Name: array [0..10] of AnsiChar;  // Field name with a maximum of 10 characters. If less than 10, it is padded with null characters (0x00).
    FieldType: AnsiChar;              // Field type - see DBF_FIELD_TYPE_
    FieldOffs: Cardinal;              // Displacement of field in record
    FieldLen: Byte;                   // Length of field (in bytes)
    DecimalLen: Byte;                 // Number of decimal places
    FieldFlags: Byte;                 // Field flags - see DBF_FIELD_FLAG_
    AutoIncNext: Cardinal;            // Value of autoincrement Next value
    AutoIncStep: Byte;                // Value of autoincrement Step value
    Reserved24: array [24..31] of Byte;
  end;

const
  MaxFieldsDb3 = 128;  // for dBase III
  MaxFieldsVFP = 255;  // for Visual Foxpro

  DBF_FILE_TYPE_FB      = $02;  // FoxBASE
  DBF_FILE_TYPE_DB3     = $03;  // FoxBASE+/Dbase III plus, no memo
  DBF_FILE_TYPE_VFP     = $30;  // Visual FoxPro
  DBF_FILE_TYPE_VFP_A   = $31;  // Visual FoxPro, autoincrement enabled
  DBF_FILE_TYPE_VFP_V   = $32;  // Visual FoxPro with field type Varchar or Varbinary
  DBF_FILE_TYPE_DB4     = $43;  // dBASE IV SQL table files, no memo
  DBF_FILE_TYPE_DB4_S   = $63;  // dBASE IV SQL system files, no memo
  DBF_FILE_TYPE_DB3_M   = $83;  // FoxBASE+/dBASE III PLUS, with memo
  DBF_FILE_TYPE_DB4_M   = $8B;  // dBASE IV with memo
  DBF_FILE_TYPE_DB4_SM  = $CB;  // dBASE IV SQL table files, with memo
  DBF_FILE_TYPE_FP2     = $F5;  // FoxPro 2.x (or earlier) with memo
  DBF_FILE_TYPE_HP6     = $E5;  // HiPer-Six format with SMT memo file
  DBF_FILE_TYPE_FB2     = $FB;  // FoxBASE

  DBF_FILE_FLAG_CDX     = $01;  // file has a structural .cdx
  DBF_FILE_FLAG_MEMO    = $02;  // file has a Memo field
  DBF_FILE_FLAG_DBC     = $04;  // file is a database (.dbc)

  DBF_FIELD_TYPE_CHAR     = 'C';  // C - Character
  DBF_FIELD_TYPE_CURRENCY = 'Y';  // Y - Currency (Visual Foxpro)
  DBF_FIELD_TYPE_NUMERIC  = 'N';  // N - Numeric
  DBF_FIELD_TYPE_FLOAT    = 'F';  // F - Float
  DBF_FIELD_TYPE_DATE     = 'D';  // D - Date
  DBF_FIELD_TYPE_DATETIME = 'T';  // T - DateTime (Visual Foxpro)
  DBF_FIELD_TYPE_DOUBLE   = 'B';  // B - Double (Visual Foxpro)
  DBF_FIELD_TYPE_INTEGER  = 'I';  // I - Integer (Visual Foxpro)
  DBF_FIELD_TYPE_BOOL     = 'L';  // L - Logical
  DBF_FIELD_TYPE_MEMO     = 'M';  // M - Memo
  DBF_FIELD_TYPE_GENERAL  = 'G';  // G - General
  DBF_FIELD_TYPE_PICTURE  = 'P';  // P - Picture
  DBF_FIELD_TYPE_AUTOINC  = '+';  // + - Autoincrement (dBase Level 7)
  DBF_FIELD_TYPE_DOUBLE7  = 'O';  // O - Double (dBase Level 7)
  DBF_FIELD_TYPE_TIMESTAM = '@';  // @ - Timestamp (dBase Level 7)
  DBF_FIELD_TYPE_VARCHAR  = 'V';  // V - Varchar type (Visual Foxpro, character field with variable size, real size in the last byte of field)


  DBF_FIELD_FLAG_SYS      = $01;  // System Column (not visible to user)
  DBF_FIELD_FLAG_NULL     = $02;  // Column can store null values
  DBF_FIELD_FLAG_BIN      = $04;  // Binary column (for CHAR and MEMO only)
  //DBF_FIELD_FLAG_NBIN     = $06;  // When a field is NULL and binary (Integer, Currency, and Character/Memo fields)
  DBF_FIELD_FLAG_AUTOINC  = $0C;  // Column is autoincrementing

  DBF_REC_STATE_NORM      = ' ';
  DBF_REC_STATE_DELETED   = '*';

  DBF_CODEPAGE_866        = 101;  // Codepage 866 Russian MS-DOS
  DBF_CODEPAGE_1251       = 201;  // Codepage 866 Russian Windows

type
  TDBReaderDbf = class(TDBReader)
  private
    //FRowsList: TDbRowsList;
    //FDbVersion: Integer;
    //FIsMetadataReaded: Boolean;        // true after FillTableInfo()

    function ReadString(const ARaw: AnsiString; AStart, ALen: Word): string;

  public
    DbfHeader: TDbfHeaderRec;
    DbfFields: array of TDbfFieldSubRec;
    TableName: string;

    procedure AfterConstruction(); override;
    procedure BeforeDestruction(); override;

    function OpenFile(AFileName: string): Boolean; override;
    // Read table data from DB to AList
    // AName - table name
    // ACount - how many items read
    procedure ReadTable(AName: string; ACount: Int64 = MaxInt; AList: TDbRowsList = nil); override;
    // get detailed multi-line description of table
    function FillTableInfoText(ATableName: string; ALines: TStrings): Boolean; override;

    //property RowsList: TDbRowsList read FRowsList;
  end;

implementation

function DbfFieldTypeToDbFieldType(AValue: AnsiChar): TFieldType;
begin
  case AValue of
    DBF_FIELD_TYPE_CHAR:     Result := ftString;  // C - Character
    DBF_FIELD_TYPE_CURRENCY: Result := ftCurrency;  // Y - Currency (Visual Foxpro)
    DBF_FIELD_TYPE_NUMERIC:  Result := ftFloat;  // N - Numeric
    DBF_FIELD_TYPE_FLOAT:    Result := ftFloat;  // F - Float
    DBF_FIELD_TYPE_DATE:     Result := ftDate;  // D - Date
    DBF_FIELD_TYPE_DATETIME: Result := ftDateTime;  // T - DateTime (Visual Foxpro)
    DBF_FIELD_TYPE_DOUBLE:   Result := ftFloat;  // B - Double (Visual Foxpro)
    DBF_FIELD_TYPE_INTEGER:  Result := ftInteger;  // I - Integer (Visual Foxpro)
    DBF_FIELD_TYPE_BOOL:     Result := ftBoolean;  // L - Logical
    DBF_FIELD_TYPE_MEMO:     Result := ftMemo;  // M - Memo
    DBF_FIELD_TYPE_GENERAL:  Result := ftString;  // G - General
    DBF_FIELD_TYPE_PICTURE:  Result := ftBlob;  // P - Picture
    DBF_FIELD_TYPE_AUTOINC:  Result := ftInteger;  // + - Autoincrement (dBase Level 7)
    DBF_FIELD_TYPE_DOUBLE7:  Result := ftFloat;  // O - Double (dBase Level 7)
    DBF_FIELD_TYPE_TIMESTAM: Result := ftTimeStamp;  // @ - Timestamp (dBase Level 7)
    DBF_FIELD_TYPE_VARCHAR:  Result := ftString;  // V - Varchar type (Visual Foxpro, character field with variable size, real size in the last byte of field)
  else
    Result := ftUnknown;
  end;
end;

function DbfFieldTypeName(AType: AnsiChar; ASize, AFlags: Byte): string;
begin
  case AType of
    DBF_FIELD_TYPE_CHAR:     Result := 'Character';
    DBF_FIELD_TYPE_CURRENCY: Result := 'Currency';
    DBF_FIELD_TYPE_NUMERIC:  Result := 'Numeric';
    DBF_FIELD_TYPE_FLOAT:    Result := 'Float';
    DBF_FIELD_TYPE_DATE:     Result := 'Date';
    DBF_FIELD_TYPE_DATETIME: Result := 'DateTime';
    DBF_FIELD_TYPE_DOUBLE:   Result := 'Double';
    DBF_FIELD_TYPE_INTEGER:  Result := 'Integer';
    DBF_FIELD_TYPE_BOOL:     Result := 'Logical';
    DBF_FIELD_TYPE_MEMO:     Result := 'Memo';
    DBF_FIELD_TYPE_GENERAL:  Result := 'General';
    DBF_FIELD_TYPE_PICTURE:  Result := 'Picture';
    DBF_FIELD_TYPE_AUTOINC:  Result := 'Autoincrement';
    DBF_FIELD_TYPE_DOUBLE7:  Result := 'Double';
    DBF_FIELD_TYPE_TIMESTAM: Result := 'Timestamp';
    DBF_FIELD_TYPE_VARCHAR:  Result := 'Varchar';
  else
    Result := 'Unknown';
  end;

  Result := Result + '(' + IntToStr(ASize) + ')';

  if (AFlags and DBF_FIELD_FLAG_SYS) > 0 then
    Result := Result + ' SYS';
  if (AFlags and DBF_FIELD_FLAG_NULL) > 0 then
    Result := Result + ' NULL';
  if (AFlags and DBF_FIELD_FLAG_BIN) > 0 then
    Result := Result + ' BIN';
  if (AFlags and DBF_FIELD_FLAG_AUTOINC) > 0 then
    Result := Result + ' AINC';
end;

function DbfCodepageName(AValue: Byte): string;
begin
  case AValue of
    DBF_CODEPAGE_866:      Result := 'Codepage 866 Russian MS-DOS';
    DBF_CODEPAGE_1251:     Result := 'Codepage 866 Russian Windows';
  else
    Result := 'Unknown codepage (' + IntToStr(AValue) + ')';
  end;
end;

{ TDBReaderDbf }

procedure TDBReaderDbf.AfterConstruction;
begin
  inherited;
  FIsSingleTable := True;
  //FRowsList := TDbRowsList.Create();
end;

procedure TDBReaderDbf.BeforeDestruction;
begin
  //FreeAndNil(FRowsList);
  inherited;
end;

function TDBReaderDbf.FillTableInfoText(ATableName: string; ALines: TStrings): Boolean;
var
  i: Integer;
  s: string;
begin
  Result := False;
  ALines.Add(Format('== Table Name=%s  RecCount=%d', [TableName, DbfHeader.RecCount]));
  ALines.Add(Format('LastUpdate=%.2d-%.2d-%.2d', [1900 + DbfHeader.LastUpdate[0], DbfHeader.LastUpdate[1], DbfHeader.LastUpdate[2]]));
  ALines.Add(Format('Codepage=%s', [DbfCodepageName(DbfHeader.CodePage)]));
  ALines.Add(Format('== Fields  Count=%d', [Length(DbfFields)]));
  for i := Low(DbfFields) to High(DbfFields) do
  begin
    s := Format('%.2d Name=%s  Type=%s %s  Size=%d',
      [
        i,
        DbfFields[i].Name,
        DbfFields[i].FieldType,
        DbfFieldTypeName(DbfFields[i].FieldType, DbfFields[i].FieldLen, DbfFields[i].FieldFlags),
        DbfFields[i].FieldLen
      ]);
    ALines.Add(s);
  end;
end;

function TDBReaderDbf.OpenFile(AFileName: string): Boolean;
var
  DbfField: TDbfFieldSubRec;
  n, nOffs: Integer;
  FieldPos: Int64;
begin
  Result := inherited OpenFile(AFileName);
  if not Result then Exit;
  TableName := ExtractFileName(AFileName);

  // read header
  FFile.Position := 0;
  FFile.Read(DbfHeader, SizeOf(DbfHeader));
  // read field defs
  nOffs := 0;
  while FFile.Position + SizeOf(DbfField) < DbfHeader.FirstRowOffs do
  begin
    FieldPos := FFile.Position;
    FFile.Read(DbfField, SizeOf(DbfField));
    if DbfField.Name[0] = #$0D then
      Break;

    // copy field rec
    n := Length(DbfFields);
    SetLength(DbfFields, n+1);
    DbfFields[n] := DbfField;

    // update field offset
    if DbfFields[n].FieldOffs = 0 then
      DbfFields[n].FieldOffs := nOffs;
    Inc(nOffs, DbfField.FieldLen);
    // ???
    if (DbfFields[n].FieldType = DBF_FIELD_TYPE_CHAR) and (DbfField.FieldLen = 1) then
      Inc(nOffs);
  end;
end;

function TDBReaderDbf.ReadString(const ARaw: AnsiString; AStart, ALen: Word): string;
begin
  SetLength(Result, ALen);
  System.Move(ARaw[AStart+1], Result[1], ALen);
  Result := TrimRight(Result);

  if (DbfHeader.CodePage = DBF_CODEPAGE_866) and (Result <> '') then
    OemToChar(PChar(Result), PChar(Result));
end;

procedure TDBReaderDbf.ReadTable(AName: string; ACount: Int64; AList: TDbRowsList);
var
  i, n: Integer;
  TmpRow: TDbRowItem;
  s: string;
begin
  inherited;
  AList.Clear;

  // set field defs
  SetLength(AList.FieldsDef, Length(DbfFields));
  for i := 0 to Length(DbfFields) - 1 do
  begin
    AList.FieldsDef[i].Name := DbfFields[i].Name;
    AList.FieldsDef[i].TypeName := DbfFieldTypeName(DbfFields[i].FieldType, DbfFields[i].FieldLen, DbfFields[i].FieldFlags);
    AList.FieldsDef[i].FieldType := DbfFieldTypeToDbFieldType(DbfFields[i].FieldType);
  end;

  FFile.Position := DbfHeader.FirstRowOffs;
  for n := 1 to DbfHeader.RecCount do
  begin
    if FFile.Position + DbfHeader.RecLength >= FFile.Size then
      Break;
    TmpRow := TDbRowItem.Create(AList);
    AList.Add(TmpRow);
    SetLength(TmpRow.RawData, DbfHeader.RecLength);
    FFile.Read(TmpRow.RawData[1], DbfHeader.RecLength);

    SetLength(TmpRow.Values, Length(DbfFields));
    for i := 0 to Length(DbfFields) - 1 do
    begin
      // read field
      s := ReadString(TmpRow.RawData, DbfFields[i].FieldOffs, DbfFields[i].FieldLen);
      case DbfFields[i].FieldType of
        DBF_FIELD_TYPE_CHAR:     TmpRow.Values[i] := s;
        DBF_FIELD_TYPE_CURRENCY: TmpRow.Values[i] := StrToCurrDef(Trim(s), 0);
        DBF_FIELD_TYPE_NUMERIC,
        DBF_FIELD_TYPE_FLOAT,
        DBF_FIELD_TYPE_DOUBLE:   TmpRow.Values[i] := StrToFloatDef(Trim(s), 0);
        DBF_FIELD_TYPE_DATE:     TmpRow.Values[i] := StrToDateDef(Trim(s), 0);
        DBF_FIELD_TYPE_DATETIME: TmpRow.Values[i] := StrToDateTimeDef(Trim(s), 0);
        DBF_FIELD_TYPE_INTEGER:  TmpRow.Values[i] := StrToIntDef(Trim(s), 0);
        DBF_FIELD_TYPE_BOOL:     TmpRow.Values[i] := (Trim(s) = 'T');
        DBF_FIELD_TYPE_MEMO:     TmpRow.Values[i] := s;
        DBF_FIELD_TYPE_GENERAL:  TmpRow.Values[i] := s;
      end;
    end;

  end;
end;

end.
