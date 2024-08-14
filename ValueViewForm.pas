unit ValueViewForm;

interface

uses
  Windows, Messages, SysUtils, Variants, Classes, Graphics, Controls, Forms,
  Dialogs, StdCtrls, ExtCtrls;

type
  TFormRawValue = class(TForm)
    panTop: TPanel;
    panDataView: TPanel;
    memoHex: TMemo;
    memoText: TMemo;
    lbTypeText: TLabel;
    lbType: TLabel;
    lbSize: TLabel;
    lbSizeText: TLabel;
    chkFullRaw: TCheckBox;
    lbRawOffsText: TLabel;
    lbRawOffs: TLabel;
    procedure chkFullRawClick(Sender: TObject);
  private
    { Private declarations }
  public
    { Public declarations }
    RawData: AnsiString;   // Whole data string
    RawOffs: Integer;      // value offset (0-based)
    RawLen: Integer;       // value length
    FieldName: string;
    FieldTypeName: string;

    procedure UpdateView();

    procedure ShowValue(ARawData: AnsiString; ARawOffs, ARawLen: Integer);
  end;

var
  FormRawValue: TFormRawValue;

implementation

{$R *.dfm}

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
  Result := '';
  pb := @Buffer;
  for i := 0 to BufferSize - 1 do
  begin
    if i > 0 then Result := Result + ' ';
    Result := Result + IntToHex(pb^, 2);
    Inc(pb);
  end;
end;

{ TFormRawValue }

procedure TFormRawValue.chkFullRawClick(Sender: TObject);
begin
  UpdateView();
end;

procedure TFormRawValue.ShowValue(ARawData: AnsiString; ARawOffs, ARawLen: Integer);
begin
  RawData := Copy(ARawData, 1, MaxInt);
  RawOffs := ARawOffs;
  RawLen := ARawLen;
  UpdateView();
  Show();
end;

procedure TFormRawValue.UpdateView;
var
  NewOffs, NewLen: Integer;
  s: string;
begin
  lbType.Caption := FieldTypeName;
  lbSize.Caption := IntToStr(RawLen);
  lbRawOffs.Caption := IntToStr(RawOffs);
  s := '';
  {NewOffs := RawOffs+1;
  while NewOffs > Length(RawData) do
  begin
    Dec(NewOffs);
    s := s + '   ';
  end;
  if (NewOffs + (NewLen-1)) < Length(RawData) then
    NewLen := Length(RawData) - NewOffs;   }

  if chkFullRaw.Checked then
  begin
    // full raw dump
    NewOffs := RawOffs+1;
    NewLen := RawLen;
    if NewOffs + NewLen > Length(RawData) then
      NewLen := Length(RawData) - NewOffs - 1;
    if (NewOffs <= Length(RawData)) and (NewLen > 0) then
    begin
      s := BufferToHex(RawData[1], RawOffs);
      s := s + '[' + BufferToHex(RawData[RawOffs+1], RawLen) + ']';
      // next line
      //if (Length(s) mod 23) = 0 then
      //  s := s + sLineBreak;
      NewOffs := RawOffs + 1 + RawLen;
      NewLen := Length(RawData) - NewOffs - 1;
      if (NewOffs <= Length(RawData)) and (NewLen > 0) then
        s := s + BufferToHex(RawData[NewOffs], NewLen);
    end;
  end
  else
  begin
    NewOffs := RawOffs+1;
    NewLen := RawLen;
    if NewOffs + NewLen > Length(RawData) then
      NewLen := Length(RawData) - NewOffs - 1;

    if NewOffs <= Length(RawData) then
      s := BufferToHex(RawData[NewOffs], NewLen);
  end;

  if RawLen > 0 then
  begin
    memoHex.Text := s;
    memoText.Text := DataAsStr(RawData[RawOffs+1], RawLen);
  end
  else
  begin
    memoHex.Text := '';
    memoText.Text := '';
  end;
end;

end.
