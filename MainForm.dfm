object FormMain: TFormMain
  Left = 0
  Top = 0
  Caption = 'test FireBird database file reader'
  ClientHeight = 474
  ClientWidth = 792
  Color = clBtnFace
  Font.Charset = DEFAULT_CHARSET
  Font.Color = clWindowText
  Font.Height = -13
  Font.Name = 'Tahoma'
  Font.Style = []
  OldCreateOrder = False
  OnCreate = FormCreate
  PixelsPerInch = 96
  TextHeight = 16
  object spl1: TSplitter
    Left = 185
    Top = 0
    Width = 4
    Height = 474
  end
  object panLeft: TPanel
    Left = 0
    Top = 0
    Width = 185
    Height = 474
    Align = alLeft
    TabOrder = 0
    object tvMain: TTreeView
      Left = 1
      Top = 33
      Width = 183
      Height = 440
      Align = alClient
      Indent = 19
      ReadOnly = True
      RowSelect = True
      TabOrder = 0
      OnChange = tvMainChange
    end
    object panFile: TPanel
      Left = 1
      Top = 1
      Width = 183
      Height = 32
      Align = alTop
      TabOrder = 1
      DesignSize = (
        183
        32)
      object lbFileName: TLabel
        Left = 8
        Top = 8
        Width = 108
        Height = 16
        Caption = '<file not selected>'
      end
      object btnFileSelect: TButton
        Left = 156
        Top = 1
        Width = 22
        Height = 25
        Anchors = [akTop, akRight]
        Caption = '...'
        TabOrder = 0
        OnClick = btnFileSelectClick
      end
    end
  end
  object pgcMain: TPageControl
    Left = 189
    Top = 0
    Width = 603
    Height = 474
    ActivePage = tsGrid
    Align = alClient
    TabOrder = 1
    object tsLog: TTabSheet
      Caption = 'Log'
      object memoLog: TMemo
        AlignWithMargins = True
        Left = 4
        Top = 4
        Width = 587
        Height = 435
        Margins.Left = 4
        Margins.Top = 4
        Margins.Right = 4
        Margins.Bottom = 4
        Align = alClient
        Font.Charset = DEFAULT_CHARSET
        Font.Color = clWindowText
        Font.Height = -13
        Font.Name = 'Courier New'
        Font.Style = []
        Lines.Strings = (
          'memoLog')
        ParentFont = False
        ScrollBars = ssBoth
        TabOrder = 0
        WordWrap = False
      end
    end
    object tsGrid: TTabSheet
      Caption = 'Grid'
      ImageIndex = 1
      object dgItems: TDrawGrid
        Left = 0
        Top = 0
        Width = 595
        Height = 443
        Align = alClient
        Options = [goFixedVertLine, goFixedHorzLine, goVertLine, goHorzLine, goRangeSelect, goColSizing]
        TabOrder = 0
        OnDblClick = dgItemsDblClick
        OnDrawCell = dgItemsDrawCell
        OnSelectCell = dgItemsSelectCell
      end
    end
  end
  object FileOpenDialog: TFileOpenDialog
    FavoriteLinks = <>
    FileTypes = <
      item
        DisplayName = 'Interbase/Firebird files (*.gdb, *.fdb)'
        FileMask = '*.gdb;*.fdb'
      end
      item
        DisplayName = 'Firebird files (*.fdb)'
        FileMask = '*.fdb'
      end
      item
        DisplayName = 'All files (*.*)'
        FileMask = '*.*'
      end>
    Options = []
    Left = 100
    Top = 4
  end
end
