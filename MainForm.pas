unit MainForm;

interface

uses
  Windows, Messages, SysUtils, Variants, Classes, Graphics, Controls, Forms,
  Dialogs, StdCtrls, FBReaderUnit, ComCtrls, ExtCtrls, Grids, ValueViewForm;

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
    procedure FormCreate(Sender: TObject);
    procedure dgItemsDrawCell(Sender: TObject; ACol, ARow: Integer; Rect: TRect;
      State: TGridDrawState);
    procedure tvMainChange(Sender: TObject; Node: TTreeNode);
    procedure btnFileSelectClick(Sender: TObject);
    procedure dgItemsDblClick(Sender: TObject);
    procedure dgItemsSelectCell(Sender: TObject; ACol, ARow: Integer; var CanSelect: Boolean);
  private
    { Private declarations }
    FReader: TDBReaderFB;
    FRowsList: TRDB_RowsList;
    FGridMode: TGridMode;
    FCurRowIndex: Integer;
    FCurColIndex: Integer;
    FDbFileName: string;
    procedure OnLogHandler(const S: string);
    function AddTreeNode(AParent: TTreeNode; AName, ATableName: string): TMyTreeNode;
    function GetFieldColor(const AField: TRDB_FieldInfoRec): TColor;
    procedure FillTree();
    procedure ShowTable(ATableName: string);
  public
    { Public declarations }
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

function TFormMain.GetFieldColor(const AField: TRDB_FieldInfoRec): TColor;
begin
  case AField.DType of
    1, 2, 3: Result := clGreen;       // string
    7, 8, 9, 10, 19: Result := clNavy;  // integer
    11, 12, 13: Result := clMaroon; // float
    14, 15, 16: Result := clTeal;     // date
    17: Result := clPurple;   // blob
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
  TmpField: TRDB_FieldInfoRec;
  TmpRow: TRDB_RowItem;
  i, iOffs: Integer;
begin
  if (FCurRowIndex <= FRowsList.Count) and (FCurColIndex < Length(FRowsList.FieldsInfo)) then
  begin
    TmpField := FRowsList.FieldsInfo[FCurColIndex];
    TmpRow := FRowsList.GetItem(FCurRowIndex);
    if Assigned(TmpRow) then
    begin
      if FCurColIndex < Length(TmpRow.Values) then
      begin
        //s := VarToStrDef(TmpRow.Values[ACol], 'null');
        //s := TmpRow.GetFieldAsStr(FCurRowIndex);
      end;
      FormRawValue.FieldName := TmpField.Name;
      if TmpField.DType <> 0 then
      begin
        FormRawValue.FieldType := TmpField.AsString;
        FormRawValue.ShowValue(TmpRow.RawData, TmpField.Offset, TmpField.Size);
      end
      else
      begin
        // calculate offset
        iOffs := ((Length(FRowsList.FieldsInfo) div 32) + 1) * 4;
        for i := 0 to FCurColIndex-1 do
          iOffs := iOffs + FRowsList.FieldsInfo[i].Size;
        FormRawValue.FieldType := TmpField.AsString;
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
  TmpField: TRDB_FieldInfoRec;
  //TmpParam: TParam;
  TmpRow: TRDB_RowItem;
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
    else if (ARow <= Length(FRowsList.FieldsInfo)) then
    begin
      // data
      TmpRow := FRowsList.GetItem(FCurRowIndex);
      TmpField := FRowsList.FieldsInfo[ARow-1];

      if ACol = 0 then // name
      begin
        s := TmpField.Name;
      end
      else
      begin
        s := TmpRow.GetFieldAsStr(ARow-1);
        clText := GetFieldColor(TmpField);
        // align numbers to right side
        if TmpField.DType in [7, 8, 9, 10, 11, 12, 13, 19] then
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
      if (ACol >= 0) and (ACol < Length(FRowsList.FieldsInfo)) then
        s := FRowsList.FieldsInfo[ACol].Name;
    end
    else if (ARow <= FRowsList.Count) then
    begin
      TmpField := FRowsList.FieldsInfo[ACol];
      TmpRow := FRowsList.GetItem(ARow-1);
      if Assigned(TmpRow) then
      begin
        if ACol < Length(TmpRow.Values) then
        begin
          //s := VarToStrDef(TmpRow.Values[ACol], 'null');
          s := TmpRow.GetFieldAsStr(ACol);
          clText := GetFieldColor(TmpField);
          // align numbers to right side
          if TmpField.DType in [7, 8, 9, 10, 11, 12, 13, 19] then
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
  tvMain.Items.BeginUpdate();
  // System tables
  tnSys := AddTreeNode(nil, 'System tables', '');
  for i := 0 to FReader.RelationsList.Count - 1 do
  begin
    TmpRel := FReader.RelationsList.GetItem(i) as TRDB_RelationsItem;
    if TmpRel.RelationID > 50 then
      Continue;
    tn := AddTreeNode(tnSys, TmpRel.RelationName, TmpRel.RelationName);
  end;

  // User tables
  for i := 0 to FReader.RelationsList.Count - 1 do
  begin
    TmpRel := FReader.RelationsList.GetItem(i) as TRDB_RelationsItem;
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
  //Test();
  if ParamCount > 0 then
    OpenDB(ParamStr(1));
end;

procedure TFormMain.OnLogHandler(const S: string);
begin
  memoLog.Lines.Add(S);
end;

procedure TFormMain.OpenDB(AFileName: string);
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
  FReader := TDBReaderFB.Create(Self);
  FReader.OnLog := OnLogHandler;
  //FReader.IsLogPages := True;
  FReader.IsLogBlobs := True;
  FReader.DebugRelID := 12;
  FReader.OpenFile(AFileName);
  //memoLog.Lines.SaveToFile('log.txt');
  //FReader.DumpSystemTables('sys_tables.txt');

  //memoLog.Lines.Clear();
  //FReader.ReadTable('ARM_CONFIG');
  //FReader.DumpTable('RDB$FORMATS', 'RDB$FORMATS.txt');
  //FReader.DumpTable('KART_PAC', 'KART_PAC.txt');
  //FReader.DumpTable('PROTOCOL', 'PROTOCOL.txt');
  //FReader.ReadTable('SP_MEDPERSONAL', 10);
  //FReader.ReadTable('SP_STUDY_TYPES', 1);
  //FReader.ReadTable('TM_CONSULTANT_LIST', 1);

  //memoLog.Lines.SaveToFile('log_table.txt');
  //memoLog.Lines.EndUpdate();
  FillTree();
end;

procedure TFormMain.ShowTable(ATableName: string);
var
  i: Integer;
  TmpField: TRDB_FieldInfoRec;
begin
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
      FReader.ReadTable(ATableName, MaxInt, FRowsList);
    end;

    // prepare grid (table)
    dgItems.DefaultRowHeight := dgItems.Font.Size + 8;
    dgItems.ColCount := Length(FRowsList.FieldsInfo);
    dgItems.RowCount := FRowsList.Count + 1;
    for i := 0 to Length(FRowsList.FieldsInfo) - 1 do
    begin
      TmpField := FRowsList.FieldsInfo[0];
      //if (Pos('_name', FColNames[i]) > 0) or (ColItem.ValueSize > 40) then
      if (TmpField.FieldLength > 40)
      or (TmpField.DType = 17)      // BLOB
      or ((TmpField.DType = 1) and (TmpField.SubType > 0)) then  // TEXT
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
  ShowTable((Node as TMyTreeNode).TableName);
end;

end.
