program test_fb;

uses
  Forms,
  MainForm in 'MainForm.pas' {FormMain},
  FBReaderUnit in 'FBReaderUnit.pas',
  ValueViewForm in 'ValueViewForm.pas' {FormRawValue};

{$R *.res}

begin
  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TFormMain, FormMain);
  Application.CreateForm(TFormRawValue, FormRawValue);
  Application.Run;
end.
