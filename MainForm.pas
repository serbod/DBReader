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
  DBReaderBase, DBReaderFirebird, DBReaderBerkley, DBReaderMidas, DBReaderParadox;

type
  TMyTreeNode = class(TTreeNode)
  public
    TableName: string;
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
    procedure OnLogHandler(const S: string);
    function AddTreeNode(AParent: TTreeNode; AName, ATableName: string): TMyTreeNode;
    function GetFieldColor(const AField: TDbFieldDefRec): TColor;
    procedure FillTree();
    procedure ShowTable(ATableName: string);
    procedure OpenFB(AFileName: string);
    procedure OpenBDB(AFileName: string);
    procedure OpenCDS(AFileName: string);
    procedure OpenParadox(AFileName: string);
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

function TFormMain.AddTreeNode(AParent: TTreeNode; AName, ATableName: string): TMyTreeNode;
begin
  Result := TMyTreeNode.Create(tvMain.Items);
  Result.TableName := ATableName;
  tvMain.Items.AddNode(Result, AParent, AName, nil, naAddChild);
end;

function TFormMain.GetFieldColor(const AField: TDbFieldDefRec): TColor;
begin
  case AField.FieldType of
    ftString, ftWideString, ftMemo: Result := clGreen;       // string
    ftInteger, ftSmallint, ftLargeint: Result := clNavy;  // integer
    ftFloat: Result := clMaroon; // float
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
      if FCurColIndex < Length(TmpRow.Values) then
      begin
        //s := VarToStrDef(TmpRow.Values[ACol], 'null');
        //s := TmpRow.GetFieldAsStr(FCurRowIndex);
      end;
      FormRawValue.FieldName := TmpField.Name;
      if TmpField.FieldType <> ftUnknown then
      begin
        FormRawValue.FieldTypeName := TmpField.TypeName;
        FormRawValue.ShowValue(TmpRow.RawData, TmpField.RawOffset, TmpField.Size);
      end
      else
      begin
        // calculate offset
        iOffs := ((Length(FRowsList.FieldsDef) div 32) + 1) * 4;
        for i := 0 to FCurColIndex-1 do
          iOffs := iOffs + FRowsList.FieldsDef[i].Size;
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
        clText := GetFieldColor(TmpField);
        // align numbers to right side
        if TmpField.FieldType in [ftInteger, ftSmallint, ftLargeint, ftFloat] then
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
          clText := GetFieldColor(TmpField);
          // align numbers to right side
          if TmpField.FieldType in [ftInteger, ftSmallint, ftLargeint, ftFloat] then
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

procedure TFormMain.FillTree;
var
  tn, tnSys: TMyTreeNode;
  i: Integer;
  TmpRel: TRDB_RelationsItem;
begin
  if not (FReader is TDBReaderFB) then Exit;
  tvMain.Items.BeginUpdate();
  // System tables
  tnSys := AddTreeNode(nil, 'System tables', '');
  for i := 0 to (FReader as TDBReaderFB).RelationsList.Count - 1 do
  begin
    TmpRel := (FReader as TDBReaderFB).RelationsList.GetItem(i) as TRDB_RelationsItem;
    if TmpRel.RelationID > 50 then
      Continue;
    tn := AddTreeNode(tnSys, TmpRel.RelationName, TmpRel.RelationName);
  end;

  // User tables
  for i := 0 to (FReader as TDBReaderFB).RelationsList.Count - 1 do
  begin
    TmpRel := (FReader as TDBReaderFB).RelationsList.GetItem(i) as TRDB_RelationsItem;
    if TmpRel.RelationID < 50 then
      Continue;
    tn := AddTreeNode(nil, TmpRel.RelationName, TmpRel.RelationName);
  end;

  tvMain.Items.EndUpdate();
end;

procedure TFormMain.FormCreate(Sender: TObject);
begin
  memoLog.Clear();
  FRowsList := TRDB_RowsList.Create(TRDB_TableRowItem);
  MaxRows := MaxInt;
  //Test();
  if ParamCount > 0 then
    OpenDB(ParamStr(1));
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
  try
    if (sExt = '.gdb') or (sExt = '.fdb') then
      OpenFB(FDbFileName)
    else
    if (sExt = '.cds') then
      OpenCDS(FDbFileName)
    else
    if (sExt = '.db') then
      OpenParadox(FDbFileName);
  finally
    memoLog.Lines.EndUpdate();
  end;
end;

procedure TFormMain.OpenFB(AFileName: string);
begin
  FReader := TDBReaderFB.Create(Self);
  FReader.OnLog := OnLogHandler;
  //FReader.IsLogPages := True;
  //FReader.IsLogBlobs := True;
  //FReader.DebugRelID := 12;
  FReader.OpenFile(AFileName);

  FillTree();
end;

procedure TFormMain.OpenParadox(AFileName: string);
begin
  FReader := TDBReaderParadox.Create(Self);
  FReader.OnLog := OnLogHandler;
  FReader.OpenFile(AFileName);

  ShowTable((FReader as TDBReaderParadox).TableName);
end;

procedure TFormMain.ShowTable(ATableName: string);
var
  i: Integer;
  TmpField: TDbFieldDefRec;
begin
  memoInfo.Clear();
  if ATableName = '' then
  begin
    tsGrid.Caption := 'Grid';
    dgItems.RowCount := 1;
    dgItems.ColCount := 0;
    Exit;
  end
  else
  begin
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

    // table info
    FReader.FillTableInfoText(ATableName, memoInfo.Lines);
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
  ShowTable((Node as TMyTreeNode).TableName);
end;

end.
