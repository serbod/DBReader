unit MainForm;

(*
Âatabase browser, main form

Author: Sergey Bodrov, 2024 Minsk
License: MIT
*)

interface

uses
  Windows, Messages, SysUtils, Variants, Classes, Graphics, Controls, Forms,
  Dialogs, StdCtrls, ComCtrls, ExtCtrls, Grids, ValueViewForm, DB,
  DBReaderBase, DBReaderFirebird, DBReaderBerkley, DBReaderMidas, DBReaderParadox,
  DBReaderGsr, DBReaderDbf, FSReaderMtf, DBReaderMdf;

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
    FileOpenDialog: TFileOpenDialog;
    tsTableInfo: TTabSheet;
    memoInfo: TMemo;
    procedure FormCreate(Sender: TObject);
    procedure dgItemsDrawCell(Sender: TObject; ACol, ARow: Integer; Rect: TRect;
      State: TGridDrawState);
    procedure tvMainChange(Sender: TObject; Node: TTreeNode);
    procedure btnFileSelectClick(Sender: TObject);
    procedure dgItemsDblClick(Sender: TObject);
    procedure dgItemsSelectCell(Sender: TObject; ACol, ARow: Integer; var CanSelect: Boolean);
  private
    { Private declarations }
    FReader: TDBReader;
    FRowsList: TDbRowsList;
    FGridMode: TGridMode;
    FCurRowIndex: Integer;
    FCurColIndex: Integer;
    FDbFileName: string;
    FTableName: string;
    procedure OnLogHandler(const S: string);
    function AddTreeNode(AParent: TTreeNode; AName, ATableName, AFileName: string): TMyTreeNode;
    function GetFieldColor(const AField: TDbFieldDefRec): TColor;
    procedure FillTreeFB();
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
  tvMain.Items.AddNode(Result, AParent, AName, nil, naAddChild);
end;

function TFormMain.GetFieldColor(const AField: TDbFieldDefRec): TColor;
begin
  case AField.FieldType of
    ftString, ftWideString, ftMemo: Result := clGreen;       // string
    ftInteger, ftSmallint, ftLargeint: Result := clNavy;  // integer
    ftFloat, ftCurrency: Result := clMaroon; // float
    ftDate, ftTime, ftDateTime, ftTimeStamp: Result := clTeal;     // date
    ftBytes, ftVarBytes, ftBlob: Result := clPurple;   // blob
  else // ftUnknown
    Result := clBlack;
  end;

end;

procedure TFormMain.btnFileSelectClick(Sender: TObject);
begin
  if FileOpenDialog.Execute then
  begin
    OpenDB(FileOpenDialog.FileName);
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
  tf: TTextFormat;
  TmpField: TDbFieldDefRec;
  //TmpParam: TParam;
  TmpRow: TDbRowItem;
  //qr: TPrQuery;
begin
  c := dgItems.Canvas;
  clText := clWindowText;
  clBack := clWindow;
  s := '';
  tf := [];
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
          tf := [tfRight];
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
            tf := [tfRight];
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
  c.TextRect(Rect, s, tf);
end;

procedure TFormMain.dgItemsSelectCell(Sender: TObject; ACol, ARow: Integer; var CanSelect: Boolean);
begin
  FCurRowIndex := ARow - 1;
  FCurColIndex := ACol;
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

procedure TFormMain.FillTreeFB;
var
  tn, tnSys: TMyTreeNode;
  i: Integer;
  TmpRel: TRDB_RelationsItem;
begin
  if not (FReader is TDBReaderFB) then Exit;
  tvMain.Items.BeginUpdate();
  tvMain.Items.Clear();
  // System tables
  tnSys := AddTreeNode(nil, 'System tables', '', '');
  for i := 0 to (FReader as TDBReaderFB).RelationsList.Count - 1 do
  begin
    TmpRel := (FReader as TDBReaderFB).RelationsList.GetItem(i) as TRDB_RelationsItem;
    if TmpRel.RelationID > 50 then
      Continue;
    tn := AddTreeNode(tnSys, TmpRel.RelationName, TmpRel.RelationName, '');
  end;

  // User tables
  for i := 0 to (FReader as TDBReaderFB).RelationsList.Count - 1 do
  begin
    TmpRel := (FReader as TDBReaderFB).RelationsList.GetItem(i) as TRDB_RelationsItem;
    if TmpRel.RelationID < 50 then
      Continue;
    tn := AddTreeNode(nil, TmpRel.RelationName, TmpRel.RelationName, '');
  end;

  tvMain.Items.EndUpdate();
end;

procedure TFormMain.FormCreate(Sender: TObject);
var
  s: string;
begin
  memoLog.Clear();
  FRowsList := TRDB_RowsList.Create(TRDB_TableRowItem);
  MaxRows := MaxInt;
  //Test();
  if ParamCount > 0 then
  begin
    s := ExpandFileName(ParamStr(1));
    OpenDB(s);
  end;
end;

procedure TFormMain.OnLogHandler(const S: string);
begin
  memoLog.Lines.Add(S);
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
  FReader := TDBReaderBerkley.Create(Self);
  FReader.OnLog := OnLogHandler;
  FReader.IsLogPages := True;
  FReader.OpenFile(AFileName);
  memoLog.Lines.EndUpdate();
end;

procedure TFormMain.OpenCDS(AFileName: string);
begin
  FReader := TDBReaderMidas.Create(Self);
  FReader.OnLog := OnLogHandler;
  FReader.OpenFile(AFileName);

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

  memoLog.Lines.BeginUpdate();
  memoLog.Lines.Clear();
  FreeAndNil(FReader);
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
      OpenMdf(FDbFileName);
  finally
    memoLog.Lines.EndUpdate();
  end;
end;

procedure TFormMain.OpenDbf(AFileName: string);
begin
  FReader := TDBReaderDbf.Create(Self);
  FReader.OnLog := OnLogHandler;
  FReader.OpenFile(AFileName);

  ShowTable((FReader as TDBReaderDbf).TableName);

  // show other files from same path
  FillTreeByFiles(AFileName);
end;

procedure TFormMain.OpenFB(AFileName: string);
begin
  FReader := TDBReaderFB.Create(Self);
  FReader.OnLog := OnLogHandler;
  //FReader.IsLogPages := True;
  //FReader.IsLogBlobs := True;
  //FReader.DebugRelID := 12;
  FReader.OpenFile(AFileName);

  FillTreeFB();
end;

procedure TFormMain.OpenGsr(AFileName: string);
var
  tn, tnSys: TMyTreeNode;
  i: Integer;
  TmpTable: TGsrTable;
begin
  FReader := TDBReaderGsr.Create(Self);
  FReader.OnLog := OnLogHandler;
  try
    FReader.OpenFile(AFileName);
  except
    on E: Exception do
      memoInfo.Lines.Append(E.Message);
  end;

  // Fill tree
  tvMain.Items.BeginUpdate();
  tvMain.Items.Clear();
  // tables
  for i := 0 to (FReader as TDBReaderGsr).TableList.Count - 1 do
  begin
    TmpTable := (FReader as TDBReaderGsr).TableList.GetItem(i);
    if TmpTable.RowCount > 0 then
      tn := AddTreeNode(nil, TmpTable.TableName, TmpTable.TableName, '');
  end;
  // Empty tables
  tnSys := AddTreeNode(nil, 'Empty tables', '', '');
  for i := 0 to (FReader as TDBReaderGsr).TableList.Count - 1 do
  begin
    TmpTable := (FReader as TDBReaderGsr).TableList.GetItem(i);
    if TmpTable.RowCount > 0 then
      Continue;
    tn := AddTreeNode(tnSys, TmpTable.TableName, TmpTable.TableName, '');
  end;
  tvMain.Items.EndUpdate();
end;

procedure TFormMain.OpenMdf(AFileName: string; AStream: TStream);
var
  tn, tnSys: TMyTreeNode;
  i: Integer;
  TmpTable: TMdfTable;
begin
  FReader := TDBReaderMdf.Create(Self);
  FReader.OnLog := OnLogHandler;
  try
    FReader.OpenFile(AFileName, AStream);
  except
    on E: Exception do
      memoInfo.Lines.Append(E.Message);
  end;

  // Fill tree
  tvMain.Items.BeginUpdate();
  tvMain.Items.Clear();
  // tables
  for i := 0 to (FReader as TDBReaderMdf).TableList.Count - 1 do
  begin
    TmpTable := (FReader as TDBReaderMdf).TableList.GetItem(i);
    if TmpTable.ObjectID > 100 then
      tn := AddTreeNode(nil, TmpTable.TableName, TmpTable.TableName, '');
  end;
  // Empty tables
  tnSys := AddTreeNode(nil, 'System tables', '', '');
  for i := 0 to (FReader as TDBReaderMdf).TableList.Count - 1 do
  begin
    TmpTable := (FReader as TDBReaderMdf).TableList.GetItem(i);
    if TmpTable.ObjectID > 100 then
      Continue;
    tn := AddTreeNode(tnSys, TmpTable.TableName, TmpTable.TableName, '');
  end;
  tvMain.Items.EndUpdate();
end;

procedure TFormMain.OpenParadox(AFileName: string);
begin
  FReader := TDBReaderParadox.Create(Self);
  FReader.OnLog := OnLogHandler;
  FReader.OpenFile(AFileName);

  tvMain.Items.Clear();
  ShowTable((FReader as TDBReaderParadox).TableName);
end;

procedure TFormMain.OpenTape(AFileName: string);
var
  r: TFSReaderMtf;
  ifs: TInnerFileStream;
begin
  r := TFSReaderMtf.Create();
  try
    r.OnLog := OnLogHandler;
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
    // table info
    FReader.FillTableInfoText(ATableName, memoInfo.Lines);

    tsGrid.Caption := ATableName;
    if FRowsList.TableName <> ATableName then
    begin
      FRowsList.Clear();
      FReader.ReadTable(ATableName, MaxRows, FRowsList);
    end;

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

procedure TFormMain.tvMainChange(Sender: TObject; Node: TTreeNode);
begin
  if not Assigned(Node) then Exit;
  if FReader.IsSingleTable then
    FReader.OpenFile((Node as TMyTreeNode).FileName);
  ShowTable((Node as TMyTreeNode).TableName);
end;

end.
