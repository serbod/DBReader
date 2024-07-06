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
  private
    { Private declarations }
  public
    { Public declarations }
    RawData: AnsiString;   // Whole data string
    RawOffs: Integer;      // value offset (0-based)
    RawLen: Integer;       // value length
    FieldName: string;
    FieldType: string;

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
  lbType.Caption := FieldType;
  lbSize.Caption := IntToStr(RawLen);
  s := '';
  NewOffs := RawOffs+1;
  while NewOffs > Length(RawData) do
  begin
    Dec(NewOffs);
    s := s + '   ';
  end;
  if (NewOffs + (NewLen-1)) < Length(RawData) then
    NewLen := Length(RawData) - NewOffs;
  if NewLen > 0 then
  begin
    memoHex.Text := BufferToHex(RawData[RawOffs+1], RawLen);
    memoText.Text := DataAsStr(RawData[RawOffs+1], RawLen);
  end
  else
  begin
    memoHex.Text := '';
    memoText.Text := '';
  end;
end;

end.
