unit Horse.Logger.Provider.ElasticSearch;

{$IFDEF FPC }
{$MODE DELPHI}
{$ENDIF}

interface

uses
{$IFDEF FPC }
  Classes,
{$ELSE}
  System.Classes,
{$ENDIF}
  Horse.Logger;

type

  THorseLoggerElasticSearchConfig = class
  private
    { private declarations }
    FLogFormat: string;
    { protected declarations }
  public
    { public declarations }
    constructor Create;
    function SetLogFormat(ALogFormat: string): THorseLoggerElasticSearchConfig;
    function GetLogFormat(out ALogFormat: string): THorseLoggerElasticSearchConfig;
    function SendToElasticSearch( aValue: string): THorseLoggerElasticSearchConfig;
    class function New: THorseLoggerElasticSearchConfig;
  end;

  THorseLoggerProviderElasticSearchManager = class(THorseLoggerThread)
  private
    { private declarations }
    FConfig: THorseLoggerElasticSearchConfig;
  protected
    { protected declarations }
    procedure DispatchLogCache; override;
  public
    { public declarations }
    destructor Destroy; override;
    function SetConfig(AConfig: THorseLoggerElasticSearchConfig): THorseLoggerProviderElasticSearchManager;
  end;

  THorseLoggerProviderElasticSearch = class(TInterfacedObject, IHorseLoggerProvider)
  private
    { private declarations }
    FHorseLoggerProviderElasticSearchManager: THorseLoggerProviderElasticSearchManager;
  protected
    { protected declarations }
  public
    { public declarations }
    constructor Create(const AConfig: THorseLoggerElasticSearchConfig = nil);
    destructor Destroy; override;
    procedure DoReceiveLogCache(ALogCache: THorseLoggerCache);
    class function New(const AConfig: THorseLoggerElasticSearchConfig = nil): IHorseLoggerProvider;
  end;

implementation

uses
{$IFDEF FPC }
  SysUtils, fpJSON, SyncObjs;
{$ELSE}
  System.SysUtils, System.IOUtils, System.JSON, System.SyncObjs;
{$ENDIF}

{ THorseLoggerProviderElasticSearch }

const
  DEFAULT_HORSE_LOG_FORMAT =
    '${request_clientip} [${time}] ${request_user_agent}' +
    ' "${request_method} ${request_path_info} ${request_version}"' +
    ' ${response_status} ${response_content_length}';

constructor THorseLoggerProviderElasticSearch.Create(const AConfig: THorseLoggerElasticSearchConfig = nil);
begin
  FHorseLoggerProviderElasticSearchManager := THorseLoggerProviderElasticSearchManager.Create(True);
  FHorseLoggerProviderElasticSearchManager.SetConfig(AConfig);
  FHorseLoggerProviderElasticSearchManager.FreeOnTerminate := False;
  FHorseLoggerProviderElasticSearchManager.Start;
end;

destructor THorseLoggerProviderElasticSearch.Destroy;
begin
  FHorseLoggerProviderElasticSearchManager.Terminate;
  FHorseLoggerProviderElasticSearchManager.GetEvent.SetEvent;
  FHorseLoggerProviderElasticSearchManager.WaitFor;
  FHorseLoggerProviderElasticSearchManager.Free;
  inherited;
end;

procedure THorseLoggerProviderElasticSearch.DoReceiveLogCache(ALogCache: THorseLoggerCache);
var
  I: Integer;
begin
  for I := 0 to Pred(ALogCache.Count) do
    FHorseLoggerProviderElasticSearchManager.NewLog(THorseLoggerLog(ALogCache.Items[0].Clone));
end;

class function THorseLoggerProviderElasticSearch.New(const AConfig: THorseLoggerElasticSearchConfig = nil): IHorseLoggerProvider;
begin
  Result := THorseLoggerProviderElasticSearch.Create(AConfig);
end;

{ TTHorseLoggerProviderElasticSearchThread }

destructor THorseLoggerProviderElasticSearchManager.Destroy;
begin
  FreeAndNil(FConfig);
  inherited;
end;

procedure THorseLoggerProviderElasticSearchManager.DispatchLogCache;
var
  I: Integer;
  Z: Integer;
  LLogCache: THorseLoggerCache;
  LLog: THorseLoggerLog;
  LParams: TArray<string>;
  LValue: {$IFDEF FPC}THorseLoggerLogItemString{$ELSE}string{$ENDIF};
  LLogStr: string;
begin
  if FConfig = nil then
    FConfig := THorseLoggerElasticSearchConfig.New;

  LLogCache := ExtractLogCache;
  try
    if LLogCache.Count = 0 then
      Exit;

    try
      for I := 0 to Pred(LLogCache.Count) do
      begin
        LLog := LLogCache.Items[I] as THorseLoggerLog;
        LParams := THorseLoggerUtils.GetFormatParams(DEFAULT_HORSE_LOG_FORMAT);
        for Z := Low(LParams) to High(LParams) do
        begin
{$IFDEF FPC}
          if LLog.Find(LParams[Z], LValue) then
            LLogStr := LLogStr.Replace('${' + LParams[Z] + '}', LValue.AsString);
{$ELSE}
          if LLog.TryGetValue<string>(LParams[Z], LValue) then
            LLogStr := LLogStr.Replace('${' + LParams[Z] + '}', LValue);
{$ENDIF}
        end;
      end;
      FConfig.SendToElasticSearch(LLogStr);
    finally
      //
    end;
  finally
    LLogCache.Free;
  end;
end;

function THorseLoggerProviderElasticSearchManager.SetConfig(AConfig: THorseLoggerElasticSearchConfig): THorseLoggerProviderElasticSearchManager;
begin
  Result := Self;
  FConfig := AConfig;
end;

{ THorseLoggerConfig }

constructor THorseLoggerElasticSearchConfig.Create;
begin
  FLogFormat := DEFAULT_HORSE_LOG_FORMAT;
end;

function THorseLoggerElasticSearchConfig.GetLogFormat(out ALogFormat: string): THorseLoggerElasticSearchConfig;
begin
  Result := Self;
  ALogFormat := FLogFormat;
end;

class function THorseLoggerElasticSearchConfig.New: THorseLoggerElasticSearchConfig;
begin
  Result := THorseLoggerElasticSearchConfig.Create;
end;

function THorseLoggerElasticSearchConfig.SendToElasticSearch(aValue: string): THorseLoggerElasticSearchConfig;
begin
  Result := Self;
//RestRequest4D
end;


function THorseLoggerElasticSearchConfig.SetLogFormat(ALogFormat: string): THorseLoggerElasticSearchConfig;
begin
  Result := Self;
  FLogFormat := ALogFormat;
end;

end.
