program DbBrowser;

uses
  Forms,
  MainForm in 'MainForm.pas' {FormMain},
  DBReaderFirebird in 'DBReaderFirebird.pas',
  ValueViewForm in 'ValueViewForm.pas' {FormRawValue},
  DBReaderBerkley in 'DBReaderBerkley.pas',
  DBReaderMidas in 'DBReaderMidas.pas',
  DBReaderParadox in 'DBReaderParadox.pas',
  DBReaderBase in 'DBReaderBase.pas',
  DBReaderGsr in 'DBReaderGsr.pas',
  DBReaderDbf in 'DBReaderDbf.pas';

{$R *.res}

begin
  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TFormMain, FormMain);
  Application.CreateForm(TFormRawValue, FormRawValue);
  Application.Run;
end.
