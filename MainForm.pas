unit MainForm;

(*
Âatabase browser, main form

Author: Sergey Bodrov, 2024 Minsk
License: MIT
*)

interface

uses
  Windows, Messages, SysUtils, Variants, Classes, Graphics, Controls, Forms,  Menus,
  Dialogs, StdCtrls, ComCtrls, ExtCtrls, Grids, ValueViewForm, DB, RFUtils,
  DBReaderBase, DBReaderFirebird, DBReaderBerkley, DBReaderMidas, DBReaderParadox,
  DBReaderDbf, FSReaderMtf, DBReaderMdf, DBReaderMdb, DBReaderEdb,
  {$ifdef ENABLE_GSR}DBReaderGsr,{$endif}
  FSReaderBase, FSReaderPst;

type
  TMyTreeNode = class(TTreeNode)
  public
    TableName: string;
    FileName: string;
  end;

  TGridMode = (gmTable, gmRecord);

  TFormMain = class(TForm)
    memoLog: TMemo;
    panLeft: TPanel;
    spl1: TSplitter;
    tvMain: TTreeView;
    pgcMain: TPageControl;
    tsLog: TTabSheet;
    tsGrid: TTabSheet;
    dgItems: TDrawGrid;
    panFile: TPanel;
    lbFileName: TLabel;
    btnFileSelect: TButton;
    tsTableInfo: TTabSheet;
    memoInfo: TMemo;
    pmGrid: TPopupMenu;
    miExporttoCSV: TMenuItem;
    miDBGrid1: TMenuItem;
    miShowAsHex: TMenuItem;
    ProgressBar: TProgressBar;
    OpenDialog: TOpenDialog;
    procedure FormCreate(Sender: TObject);
    procedure dgItemsDrawCell(Sender: TObject; ACol, ARow: Integer; Rect: TRect;
      State: TGridDrawState);
    procedure tvMainChange(Sender: TObject; Node: TTreeNode);
    procedure btnFileSelectClick(Sender: TObject);
    procedure dgItemsDblClick(Sender: TObject);
    procedure dgItemsSelectCell(Sender: TObject; ACol, ARow: Integer; var CanSelect: Boolean);
    procedure miExporttoCSVClick(Sender: TObject);
    procedure miDBGrid1Click(Sender: TObject);
    procedure FormDestroy(Sender: TObject);
    procedure miShowAsHexClick(Sender: TObject);
  private
    { Private declarations }
    FLogStream: TStream;
    FSettings: TStringList;
    FDBReader: TDBReader;  // DataBase reader
    FFSReader: TFSReader;  // FileSystem reader
    FRowsList: TDbRowsList;
    FGridMode: TGridMode;
    FCurRowIndex: Integer;
    FCurColIndex: Integer;
    FDbFileName: string;
    FTableName: string;
    FShowAsHex: Boolean;  // show numbers as Hex
    FPrevUpdTC: Int64;  // for progress indicator
    function AddTreeNode(AParent: TTreeNode; AName, ATableName, AFileName: string): TMyTreeNode;
    function GetFieldColor(const AField: TDbFieldDefRec): TColor;

    procedure ExportGridToCSV();
    procedure TestDbGrid();

    procedure InitReader(AReader: TDBReader); // set reader options
    procedure FillTree();
    procedure FillTreeByFiles(AFileName: string);
    procedure ShowTable(ATableName: string);
    procedure OpenFB(AFileName: string);
    procedure OpenBDB(AFileName: string);  // BerkleyDB (not tested)
    procedure OpenCDS(AFileName: string);
    procedure OpenParadox(AFileName: string);
    procedure OpenGsr(AFileName: string);
    procedure OpenDbf(AFileName: string);
    procedure OpenTape(AFileName: string);
    procedure OpenMdf(AFileName: string; AStream: TStream = nil);
    procedure OpenMdb(AFileName: string);
    procedure OpenEdb(AFileName: string);
    procedure OpenPst(AFileName: string);

    procedure OnLogHandler(const S: string);
    procedure OnPageReadedHandler(Sender: TObject);
  public
    { Public declarations }
    MaxRows: Integer;
    procedure OpenDB(AFileName: string);
    procedure Test();
  end;

var
  FormMain: TFormMain;

implementation

{$R *.dfm}

{ TFormMain }

function TFormMain.AddTreeNode(AParent: TTreeNode; AName, ATableName, AFileName: string): TMyTreeNode;
begin
  Result := TMyTreeNode.Create(tvMain.Items);
  Result.TableName := ATableName;
  Result.FileName := AFileName;
  if Assigned(AParent) then
    tvMain.Items.AddNode(Result, AParent, AName, nil, naAddChild)
  else
    tvMain.Items.AddNode(Result, AParent, AName, nil, naAdd);
end;

function TFormMain.GetFieldColor(const AField: TDbFieldDefRec): TColor;
begin
  case AField.FieldType of
    ftString, ftWideString, ftFixedChar, ftFixedWideChar, ftMemo: Result := clGreen; // string
    ftInteger, ftSmallint, ftLargeint: Result := clNavy;  // integer
    ftFloat, ftCurrency: Result := clMaroon; // float
    ftDate, ftTime, ftDateTime, ftTimeStamp: Result := clTeal;     // date
    ftBytes, ftVarBytes, ftBlob: Result := clPurple;   // blob
  else // ftUnknown
    Result := clBlack;
  end;

end;

procedure TFormMain.InitReader(AReader: TDBReader);
begin
  AReader.OnLog := OnLogHandler;
  AReader.OnPageReaded := OnPageReadedHandler;
  AReader.IsDebugPages := (FSettings.Values['DebugPages'] <> '');
  AReader.IsDebugRows := (FSettings.Values['DebugRows'] <> '');
end;

procedure TFormMain.miDBGrid1Click(Sender: TObject);
begin
  TestDbGrid();
end;

procedure TFormMain.miExporttoCSVClick(Sender: TObject);
begin
  ExportGridToCSV();
end;

procedure TFormMain.miShowAsHexClick(Sender: TObject);
begin
  FShowAsHex := not FShowAsHex;
  miShowAsHex.Checked := FShowAsHex;
  dgItems.Invalidate();
end;

procedure TFormMain.btnFileSelectClick(Sender: TObject);
begin
  if OpenDialog.Execute then
  begin
    OpenDB(OpenDialog.FileName);
  end;
end;

procedure TFormMain.dgItemsDblClick(Sender: TObject);
var
  TmpField: TDbFieldDefRec;
  TmpRow: TDbRowItem;
  i, iOffs: Integer;
begin
  if (FCurRowIndex <= FRowsList.Count) and (FCurColIndex < Length(FRowsList.FieldsDef)) then
  begin
    TmpField := FRowsList.FieldsDef[FCurColIndex];
    TmpRow := FRowsList.GetItem(FCurRowIndex);
    if Assigned(TmpRow) then
    begin
      FormRawValue.Value := Null;
      if FCurColIndex < Length(TmpRow.Values) then
      begin
        //s := VarToStrDef(TmpRow.Values[ACol], 'null');
        //s := TmpRow.GetFieldAsStr(FCurRowIndex);
        FormRawValue.Value := TmpRow.Values[FCurColIndex];
      end;
      FormRawValue.FieldName := TmpField.Name;
      if TmpField.FieldType <> ftUnknown then
      begin
        FormRawValue.FieldTypeName := TmpField.TypeName;
        FormRawValue.IsBlob := TmpField.FieldType in [ftMemo, ftBlob, ftBytes, ftVarBytes];
        FormRawValue.ShowValue(TmpRow.RawData, TmpField.RawOffset, TmpField.Size);
      end
      else
      begin
        iOffs := TmpField.RawOffset;
        if iOffs = 0 then
        begin
          // calculate offset
          iOffs := ((Length(FRowsList.FieldsDef) div 32) + 1) * 4;
          for i := 0 to FCurColIndex-1 do
            iOffs := iOffs + FRowsList.FieldsDef[i].Size;
        end;
        FormRawValue.FieldTypeName := TmpField.TypeName;
        FormRawValue.IsBlob := TmpField.FieldType in [ftMemo, ftBlob, ftBytes, ftVarBytes];
        FormRawValue.ShowValue(TmpRow.RawData, iOffs, TmpField.Size);
      end;
    end;
  end;
end;

procedure TFormMain.dgItemsDrawCell(Sender: TObject; ACol, ARow: Integer; Rect: TRect;
  State: TGridDrawState);
var
  c: TCanvas;
  //n, ii, nLen, nEnum: Integer;
  s: string;
  clText, clBack: TColor;
  {$ifdef FPC}
  ts: TTextStyle;
  {$else}
  tf: TTextFormat;
  {$endif}
  TmpField: TDbFieldDefRec;
  //TmpParam: TParam;
  TmpRow: TDbRowItem;
  TmpInt64: Int64;
  //qr: TPrQuery;
begin
  c := dgItems.Canvas;
  clText := clWindowText;
  clBack := clWindow;
  s := '';
  {$ifdef FPC}
  ts := c.TextStyle;
  {$else}
  tf := [];
  {$endif}
  if not Assigned(FRowsList) then
    Exit;

  if FGridMode = gmRecord then
  begin
    // current rec fields
    if ARow = 0 then
    begin
      // captions
      clBack := clMenu;
      case ACol of
        0: s := 'Field';
        1: s := 'Value';
      else
        s := '';
      end;
    end
    else if (ARow <= Length(FRowsList.FieldsDef)) then
    begin
      // data
      TmpRow := FRowsList.GetItem(FCurRowIndex);
      TmpField := FRowsList.FieldsDef[ARow-1];

      if ACol = 0 then // name
      begin
        s := TmpField.Name;
      end
      else
      begin
        s := TmpRow.GetFieldAsStr(ARow-1);
        if Length(s) > 100 then
          s := Copy(s, 1, 100);
        clText := GetFieldColor(TmpField);
        // align numbers to right side
        if TmpField.FieldType in [ftInteger, ftSmallint, ftLargeint, ftFloat, ftCurrency] then
        //if VarIsOrdinal(TmpRow.Values[]) or VarIsFloat(TmpField.Value) then
        {$ifdef FPC}
          ts.Alignment := taRightJustify;
        {$else}
          tf := [tfRight];
        {$endif}
      end;
    end;
  end
  else if FGridMode = gmTable then
  begin
    // == table
    if ARow = 0 then
    begin
      // captions
      clBack := clMenu;
      if (ACol >= 0) and (ACol < Length(FRowsList.FieldsDef)) then
        s := FRowsList.FieldsDef[ACol].Name;
    end
    else if (ARow <= FRowsList.Count) and (ACol < Length(FRowsList.FieldsDef)) then
    begin
      TmpField := FRowsList.FieldsDef[ACol];
      TmpRow := FRowsList.GetItem(ARow-1);
      if Assigned(TmpRow) then
      begin
        if ACol < Length(TmpRow.Values) then
        begin
          //s := VarToStrDef(TmpRow.Values[ACol], 'null');
          s := TmpRow.GetFieldAsStr(ACol);

          if Length(s) > 100 then
            s := Copy(s, 1, 100);
          clText := GetFieldColor(TmpField);
          // align numbers to right side
          if TmpField.FieldType in [ftInteger, ftSmallint, ftLargeint, ftFloat, ftCurrency] then
          //if VarIsOrdinal(TmpRow.Data[ACol]) or VarIsFloat(TmpRow.Data[ACol]) then
          {$ifdef FPC}
            ts.Alignment := taRightJustify;
          {$else}
            tf := [tfRight];
          {$endif}
          // show as hex
          if FShowAsHex
          and (TmpField.FieldType in [ftInteger, ftSmallint, ftLargeint])
          and (not VarIsNull(TmpRow.Values[ACol]))
          then
          begin
            TmpInt64 := TmpRow.Values[ACol];
            s := Format('%x', [TmpInt64]);
          end;
        end;
      end;
    end;
  end;

  if c.Brush.Color <> clBack then
  begin
    c.Brush.Color := clBack;
    c.FillRect(Rect);
  end;
  c.Font.Color := clText;
  Rect.Left := Rect.Left + 2;
  Rect.Right := Rect.Right - 2;
  {$ifdef FPC}
  c.TextRect(Rect, Rect.Left, Rect.Top, s, ts);
  {$else}
  c.TextRect(Rect, s, tf);
  {$endif}
end;

procedure TFormMain.dgItemsSelectCell(Sender: TObject; ACol, ARow: Integer; var CanSelect: Boolean);
begin
  FCurRowIndex := ARow - 1;
  FCurColIndex := ACol;
end;

procedure TFormMain.ExportGridToCSV;
var
  dlg: TSaveDialog;
  sFileName: string;
  fs: TFileStream;
  i, ii: Integer;
  s, ss, sSeparator: string;
  TmpRow: TDbRowItem;
begin
  // select filename
  sFileName := '';
  sSeparator := #09; // TAB
  dlg := TSaveDialog.Create(Self);
  try
    dlg.DefaultExt := '.csv';
    dlg.FileName := FTableName + '.csv';
    dlg.Filter := 'CSV|*.csv';
    if dlg.Execute(Self.Handle) then
    begin
      sFileName := dlg.FileName;
    end;
  finally
    dlg.Free();
  end;
  if sFileName = '' then
    Exit;

  // export to stream
  fs := TFileStream.Create(sFileName, fmCreate);
  try
    // column names
    ss := '';
    for ii := 0 to Length(FRowsList.FieldsDef) - 1 do
    begin
      s := FRowsList.FieldsDef[ii].Name;
      if ss <> '' then
        ss := ss + sSeparator;
      ss := ss + s;
    end;
    StrToStream(ss, fs);

    // rows
    for i := 0 to FRowsList.Count - 1 do
    begin
      TmpRow := FRowsList.GetItem(i);
      ss := '';
      for ii := 0 to Length(FRowsList.FieldsDef) - 1 do
      begin
        s := TmpRow.GetFieldAsStr(ii);
        if (Pos(sSeparator, s) > 0) then
          s := QuotedStr(s);
        if ss <> '' then
          ss := ss + sSeparator;
        ss := ss + s;
      end;
      StrToStream(ss, fs);

    end;
  finally
    fs.Free();
  end;
end;

procedure TFormMain.FillTree();
var
  i: Integer;
  TmpTable: TDbRowsList;
  tnParent: TTreeNode;
begin
  // Fill tree
  tvMain.Items.BeginUpdate();
  tvMain.Items.Clear();
  // tables
  for i := 0 to FDBReader.GetTablesCount() - 1 do
  begin
    TmpTable := FDBReader.GetTableByIndex(i);
    if (not TmpTable.IsSystem) and (not TmpTable.IsEmpty) and (not TmpTable.IsGhost) then
      AddTreeNode(nil, TmpTable.TableName, TmpTable.TableName, '');
  end;
  // Empty tables
  tnParent := nil;
  for i := 0 to FDBReader.GetTablesCount() - 1 do
  begin
    TmpTable := FDBReader.GetTableByIndex(i);
    if (not TmpTable.IsSystem) and TmpTable.IsEmpty and (not TmpTable.IsGhost) then
    begin
      if not Assigned(tnParent) then
        tnParent := AddTreeNode(nil, 'Empty tables', '', '');
      AddTreeNode(tnParent, TmpTable.TableName, TmpTable.TableName, '');
    end;
  end;
  // Ghost tables
  tnParent := nil;
  for i := 0 to FDBReader.GetTablesCount() - 1 do
  begin
    TmpTable := FDBReader.GetTableByIndex(i);
    if (not TmpTable.IsSystem) and TmpTable.IsGhost then
    begin
      if not Assigned(tnParent) then
        tnParent := AddTreeNode(nil, 'Ghost tables', '', '');
      AddTreeNode(tnParent, TmpTable.TableName, TmpTable.TableName, '');
    end;
  end;
  // System tables
  tnParent := nil;
  for i := 0 to FDBReader.GetTablesCount() - 1 do
  begin
    TmpTable := FDBReader.GetTableByIndex(i);
    if TmpTable.IsSystem then
    begin
      if not Assigned(tnParent) then
        tnParent := AddTreeNode(nil, 'System tables', '', '');
      AddTreeNode(tnParent, TmpTable.TableName, TmpTable.TableName, '');
    end;
  end;
  tvMain.Items.EndUpdate();
end;

procedure TFormMain.FillTreeByFiles(AFileName: string);
var
  sr: TSearchRec;
  sPath, sName, sCurName: string;
  i: Integer;
begin
  sCurName := FTableName;
  if Assigned(tvMain.Selected) then
    sCurName := tvMain.Selected.Text;

  sPath := ExtractFilePath(AFileName);
  sName := sPath + '*' + ExtractFileExt(AFileName);
  tvMain.Items.BeginUpdate;
  tvMain.Items.Clear();
  if SysUtils.FindFirst(sName, faAnyFile, sr) = 0 then
  begin
    repeat
      if (sr.Attr and faAnyFile) > 0 then
      begin
        sName := sPath + sr.Name;
        AddTreeNode(nil, sr.Name, sr.Name, sName);
      end;
    until FindNext(sr) <> 0;
    FindClose(sr);
  end;

  for i := 0 to tvMain.Items.Count - 1 do
  begin
    if tvMain.Items[i].Text = sCurName then
    begin
      tvMain.OnChange := nil;
      tvMain.Selected := tvMain.Items[i];
      tvMain.OnChange := tvMainChange;
      Break;
    end;
  end;

  tvMain.Items.EndUpdate;
end;

procedure TFormMain.FormCreate(Sender: TObject);
var
  s, sLogName: string;
begin
  FSettings := TStringList.Create();
  if FileExists('DBReader.ini') then
  begin
    FSettings.LoadFromFile('DBReader.ini');
    if FSettings.Values['LogToFile'] <> '' then
    begin
      sLogName := 'DBReader.log';
      if not FileExists(sLogName) then
      begin
        FLogStream := TFileStream.Create(sLogName, fmCreate);
        FLogStream.Free();
      end;
      FLogStream := TFileStream.Create(sLogName, fmOpenWrite + fmShareDenyNone);
      FLogStream.Size := 0;
    end;
  end;
  memoLog.Clear();
  FRowsList := TDbRowsList.Create;
  MaxRows := MaxInt;
  //Test();
  if ParamCount > 0 then
  begin
    s := ExpandFileName(ParamStr(1));
    OpenDB(s);
  end;
end;

procedure TFormMain.FormDestroy(Sender: TObject);
begin
  FreeAndNil(FLogStream);
  FreeAndNil(FSettings);
end;

procedure TFormMain.OnLogHandler(const S: string);
begin
  if Assigned(FLogStream) then
    StrToStream(S, FLogStream, True);
  if memoLog.Lines.Count < 1000  then
    memoLog.Lines.Add(S);
end;

procedure TFormMain.OnPageReadedHandler(Sender: TObject);
begin
  if TickDiff(GetTickCount64, FPrevUpdTC) > 1000 then
  begin
    FPrevUpdTC := GetTickCount64;
    ProgressBar.Position := FDBReader.GetProgress();
    Application.ProcessMessages();
  end;
end;

procedure TFormMain.OpenBDB(AFileName: string);
begin
  if FileExists(AFileName) then
  begin
    FDbFileName := AFileName;
    lbFileName.Caption := ExtractFileName(FDbFileName);
  end
  else
  begin
    FDbFileName := '';
    lbFileName.Caption := '<file not selected>';
    Exit;
  end;

  memoLog.Lines.BeginUpdate();
  memoLog.Lines.Clear();
  FDBReader := TDBReaderBerkley.Create(Self);
  InitReader(FDBReader);
  FDBReader.OpenFile(AFileName);
  memoLog.Lines.EndUpdate();
end;

procedure TFormMain.OpenCDS(AFileName: string);
begin
  FDBReader := TDBReaderMidas.Create(Self);
  InitReader(FDBReader);
  try
    FDBReader.OpenFile(AFileName);
  except
    on E: Exception do
      memoInfo.Lines.Append(E.Message);
  end;

  ShowTable(ExtractFileName(AFileName));

  // show other files from same path
  FillTreeByFiles(AFileName);
end;

procedure TFormMain.OpenDB(AFileName: string);
var
  sExt: string;
begin
  if FileExists(AFileName) then
  begin
    FDbFileName := AFileName;
    lbFileName.Caption := ExtractFileName(FDbFileName);
  end
  else
  begin
    FDbFileName := '';
    lbFileName.Caption := '<file not selected>';
    Exit;
  end;

  sExt := LowerCase(ExtractFileExt(FDbFileName));

  ProgressBar.Visible := True;
  memoLog.Lines.BeginUpdate();
  memoLog.Lines.Clear();
  FreeAndNil(FDBReader);
  try
    if (sExt = '.gdb') or (sExt = '.fdb') then
      OpenFB(FDbFileName)
    else
    if (sExt = '.cds') then
      OpenCDS(FDbFileName)
    else
    if (sExt = '.db') then
      OpenParadox(FDbFileName)
    else
    if (sExt = '.gsr') then
      OpenGsr(FDbFileName)
    else
    if (sExt = '.dbf') then
      OpenDbf(FDbFileName)
    else
    if (sExt = '.bak') then
      OpenTape(FDbFileName)
    else
    if (sExt = '.mdf') then
      OpenMdf(FDbFileName)
    else
    if (sExt = '.mdb') or (sExt = '.accdb') then
      OpenMdb(FDbFileName)
    else
    if (sExt = '.edb') then
      OpenEdb(FDbFileName)
    else
    if (sExt = '.pst') then
      OpenPst(FDbFileName);
  finally
    memoLog.Lines.EndUpdate();
  end;
  ProgressBar.Visible := False;
end;

procedure TFormMain.OpenDbf(AFileName: string);
begin
  FDBReader := TDBReaderDbf.Create(Self);
  InitReader(FDBReader);
  try
    FDBReader.OpenFile(AFileName);
  except
    on E: Exception do
      memoInfo.Lines.Append(E.Message);
  end;

  ShowTable((FDBReader as TDBReaderDbf).TableName);

  // show other files from same path
  FillTreeByFiles(AFileName);
end;

procedure TFormMain.OpenEdb(AFileName: string);
var
  i: Integer;
  TmpTable: TEdbTableInfo;
  tnParent: TTreeNode;
begin
  FDBReader := TDBReaderEdb.Create(Self);
  InitReader(FDBReader);
  try
    FDBReader.OpenFile(AFileName);
  except
    on E: Exception do
      memoInfo.Lines.Append(E.Message);
  end;

  FillTree();
end;

procedure TFormMain.OpenFB(AFileName: string);
var
  tnParent: TMyTreeNode;
  i: Integer;
  //TmpRel: TRDB_RelationsItem;
  TmpTable: TRDBTable;
begin
  FDBReader := TDBReaderFB.Create(Self);
  InitReader(FDBReader);
  //FReader.IsLogBlobs := True;
  //FReader.DebugRelID := 12;
  try
    FDBReader.OpenFile(AFileName);
  except
    on E: Exception do
      memoInfo.Lines.Append(E.Message);
  end;

  FillTree();
end;

procedure TFormMain.OpenGsr(AFileName: string);
{$ifdef ENABLE_GSR}
var
  tnSys: TMyTreeNode;
  i: Integer;
  TmpTable: TGsrTable;
{$endif}
begin
{$ifdef ENABLE_GSR}
  FDBReader := TDBReaderGsr.Create(Self);
  InitReader(FDBReader);
  try
    FDBReader.OpenFile(AFileName);
  except
    on E: Exception do
      memoInfo.Lines.Append(E.Message);
  end;

  // Fill tree
  tvMain.Items.BeginUpdate();
  tvMain.Items.Clear();
  // tables
  for i := 0 to (FDBReader as TDBReaderGsr).TableList.Count - 1 do
  begin
    TmpTable := (FDBReader as TDBReaderGsr).TableList.GetItem(i);
    if TmpTable.RowCount > 0 then
      AddTreeNode(nil, TmpTable.TableName, TmpTable.TableName, '');
  end;
  // Empty tables
  tnSys := AddTreeNode(nil, 'Empty tables', '', '');
  for i := 0 to (FDBReader as TDBReaderGsr).TableList.Count - 1 do
  begin
    TmpTable := (FDBReader as TDBReaderGsr).TableList.GetItem(i);
    if TmpTable.RowCount > 0 then
      Continue;
    AddTreeNode(tnSys, TmpTable.TableName, TmpTable.TableName, '');
  end;
  tvMain.Items.EndUpdate();
{$endif}
end;

procedure TFormMain.OpenMdb(AFileName: string);
begin
  FDBReader := TDBReaderMdb.Create(Self);
  InitReader(FDBReader);
  try
    FDBReader.OpenFile(AFileName);
  except
    on E: Exception do
      memoInfo.Lines.Append(E.Message);
  end;

  // Fill tree
  FillTree();
end;

procedure TFormMain.OpenMdf(AFileName: string; AStream: TStream);
begin
  FDBReader := TDBReaderMdf.Create(Self);
  InitReader(FDBReader);
  try
    FDBReader.OpenFile(AFileName, AStream);
  except
    on E: Exception do
      memoInfo.Lines.Append(E.Message);
  end;

  FillTree();
end;

procedure TFormMain.OpenParadox(AFileName: string);
begin
  FDBReader := TDBReaderParadox.Create(Self);
  InitReader(FDBReader);
  FDBReader.OpenFile(AFileName);

  tvMain.Items.Clear();
  ShowTable((FDBReader as TDBReaderParadox).TableName);
end;

procedure TFormMain.OpenPst(AFileName: string);
var
  r: TFSReaderPst;
  i, ii: Integer;
  TmpNode: TPstNode;
  tn, tnParent: TMyTreeNode;
begin
  FreeAndNil(FFSReader);

  r := TFSReaderPst.Create(Self);
  FFSReader := r;
  r.OnLog := OnLogHandler;
  r.IsDebugPages := (FSettings.Values['DebugPages'] <> '');
  //r.IsSaveStreams := (FSettings.Values['PstSaveStreams'] <> '');
  r.OpenFile(AFileName);

  // fill tree
  tvMain.Items.BeginUpdate();
  tvMain.Items.Clear();
  // tables
  for i := 0 to r.NodeList.Count - 1 do
  begin
    TmpNode := r.NodeList.GetItem(i);
    if TmpNode.Name <> '' then
    begin
      // find parent
      tnParent := nil;
      for ii := 0 to tvMain.Items.Count - 1 do
      begin
        tnParent := (tvMain.Items.Item[ii] as TMyTreeNode);
        if tnParent.Data = TmpNode.ParentNode then
          Break;
        tnParent := nil;
      end;

      tn := AddTreeNode(tnParent, TmpNode.Name, TmpNode.Name, '');
      tn.Data := TmpNode;
    end;
  end;
  tvMain.Items.EndUpdate();

  ShowTable('Messages');
end;

procedure TFormMain.OpenTape(AFileName: string);
var
  r: TFSReaderMtf;
  ifs: TInnerFileStream;
begin
  r := TFSReaderMtf.Create(Self);
  try
    r.OnLog := OnLogHandler;
    r.IsSaveStreams := (FSettings.Values['MtfSaveStreams'] <> '');
    r.OpenFile(AFileName);
    if r.MdfPos > 0 then
    begin
      ifs := TInnerFileStream.Create(AFileName, r.MdfName, r.MdfPos, r.MdfSize);
      OpenMdf(r.MdfName, ifs);
    end;
  finally
    r.Free();
  end;
end;

procedure TFormMain.ShowTable(ATableName: string);
var
  i: Integer;
  TmpField: TDbFieldDefRec;
begin
  memoInfo.Clear();
  FTableName := ATableName;
  if ATableName = '' then
  begin
    tsGrid.Caption := 'Grid';
    dgItems.RowCount := 1;
    dgItems.ColCount := 0;
    Exit;
  end
  else
  begin
    if Assigned(FDBReader) then
    begin
      // table info
      FDBReader.FillTableInfoText(ATableName, memoInfo.Lines);
      // table rows
      tsGrid.Caption := ATableName;
      if FRowsList.TableName <> ATableName then
      begin
        FRowsList.Clear();
        ProgressBar.Visible := True;
        try
          FDBReader.ReadTable(ATableName, MaxRows, FRowsList);
        except
          on E: Exception do ShowException(E, nil);
        end;
        ProgressBar.Visible := False;
      end;
    end
    else if Assigned(FFSReader) then
    begin
      memoInfo.Lines.Clear();
      //FFSReader.FillNodeInfoText(ATableName, memoInfo.Lines);
      // node rows
      tsGrid.Caption := ATableName;
      if FRowsList.TableName <> ATableName then
      begin
        FRowsList.Clear();
        ProgressBar.Visible := True;
        try
          if (FFSReader is TFSReaderPst) then
            (FFSReader as TFSReaderPst).ReadTable(ATableName, MaxRows, FRowsList);
        except
          on E: Exception do ShowException(E, nil);
        end;
        ProgressBar.Visible := False;
      end;
    end
    else
    begin
      memoInfo.Lines.Clear();
      tsGrid.Caption := ATableName;
      FRowsList.Clear();
    end;

    tsGrid.Caption := ATableName + ' [' + IntToStr(FRowsList.Count) + ' rows]';

    // prepare grid (table)
    dgItems.DefaultRowHeight := dgItems.Font.Size + 8;
    dgItems.ColCount := Length(FRowsList.FieldsDef);
    dgItems.RowCount := FRowsList.Count + 1;
    for i := 0 to Length(FRowsList.FieldsDef) - 1 do
    begin
      TmpField := FRowsList.FieldsDef[0];
      //if (Pos('_name', FColNames[i]) > 0) or (ColItem.ValueSize > 40) then
      if (TmpField.Size > 40)
      or (TmpField.FieldType in [ftBlob, ftMemo])
      then
        dgItems.ColWidths[i] := 280
      {else if Length(ColItem.EnumValues) > 0 then
        dgItems.ColWidths[i] := 140
      else if ColItem.RefTableName <> '' then
        dgItems.ColWidths[i] := 40 }
      else
        dgItems.ColWidths[i] := 100;
    end;
  end;

  dgItems.FixedCols := 0;
  if dgItems.RowCount > 1 then
    dgItems.FixedRows := 1
  else
    dgItems.FixedRows := 0;
  dgItems.Invalidate();
end;

procedure TFormMain.Test;
var
  s1, s2: AnsiString;
begin
  s1 := #2 + 'ab' + #254 + 'c' + #3 +'def';
  s2 := RleDecompress(s1);
  OnLogHandler(s2);
end;

procedure TFormMain.TestDbGrid;
begin
{
  FormDbGrid.DataSet.AssignRowsList(FRowsList);
  FormDbGrid.dbgr.Columns.RebuildColumns();
  FormDbGrid.Show();
}  
end;

procedure TFormMain.tvMainChange(Sender: TObject; Node: TTreeNode);
begin
  if not Assigned(Node) then Exit;
  if not Assigned(FDBReader) then Exit;
  
  if FDBReader.IsSingleTable then
    FDBReader.OpenFile((Node as TMyTreeNode).FileName);
  ShowTable((Node as TMyTreeNode).TableName);
end;

end.
