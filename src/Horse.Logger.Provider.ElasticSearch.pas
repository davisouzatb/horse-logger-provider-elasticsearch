unit Horse.Logger.Provider.ElasticSearch;
{$IFDEF FPC }
{$MODE DELPHI}
{$ENDIF}
interface
uses
  REST.JSON,
  REST.Client,
  REST.Types,
{$IFDEF FPC }
  Classes,
{$ELSE}
  System.Classes,
{$ENDIF}
  Horse.Logger, System.Threading;
type
  THorseLoggerElasticSearchConfig = class
  private
    { private declarations }
    FLogFormat: string;
    FSubject : String;
    FBaseURL : String;
    { protected declarations }
  public
    { public declarations }
    constructor Create;
    destructor Destroy; override;
    function SetLogFormat(ALogFormat: string): THorseLoggerElasticSearchConfig;
    function SetSubject(ASubject: string): THorseLoggerElasticSearchConfig;
    function SetBaseURL(ABaseURL: string): THorseLoggerElasticSearchConfig;
    function GetLogFormat(out ALogFormat: string): THorseLoggerElasticSearchConfig;
    function GetSubject(out ASubject: string): THorseLoggerElasticSearchConfig;
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
    '"${request_clientip}" "${time}" "${request_user_agent}"' +
    ' "${request_method}" "${request_path_info}" "${request_version}"' +
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
  LLogStr, LSubject, LAuxLogStr: string;
begin
  if FConfig = nil then
    FConfig := THorseLoggerElasticSearchConfig.New;
  LLogCache := ExtractLogCache;
  try
    if LLogCache.Count = 0 then
      Exit;
    try
      FConfig.GetLogFormat(LLogStr);
      FConfig.GetSubject(LSubject);
      for I := 0 to Pred(LLogCache.Count) do
      begin
        LLog := LLogCache.Items[I] as THorseLoggerLog;
        LParams := THorseLoggerUtils.GetFormatParams(FConfig.FLogFormat);
        for Z := Low(LParams) to High(LParams) do
        begin
          if LLogStr.Contains('"${' + LParams[Z] + '}"') then
            LAuxLogStr := LAuxLogStr + '"'+ LParams[Z] + '":"${' + LParams[Z] + '}",'
          else
            LAuxLogStr := LAuxLogStr + '"'+ LParams[Z] + '":${' + LParams[Z] + '},';
        end;
        LLogStr := LAuxLogStr;
        for Z := Low(LParams) to High(LParams) do
        begin
{$IFDEF FPC}
          if LLog.Find(LParams[Z], LValue) then
            LLogStr := LLogStr.Replace('${' + LParams[Z] + '},', LValue.AsString);
{$ELSE}
          if LLog.TryGetValue<string>(LParams[Z], LValue) then
            LLogStr := LLogStr.Replace('${' + LParams[Z] + '}', LValue);
{$ENDIF}
        end;
      end;
      LLogStr := Copy(LLogStr,1,LLogStr.Length -1);
      LLogStr := '{'+LLogStr + ',"Subject":"'+LSubject+'"}';
      FConfig.SendToElasticSearch(LLogStr.Replace('\','/'));
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
  FSubject := '';
  FBaseURL := 'http://localhost:9200/apidelphi/_doc';
end;
destructor THorseLoggerElasticSearchConfig.Destroy;
begin
  inherited;
end;

function THorseLoggerElasticSearchConfig.GetLogFormat(out ALogFormat: string): THorseLoggerElasticSearchConfig;
begin
  Result := Self;
  ALogFormat := FLogFormat;
end;
function THorseLoggerElasticSearchConfig.GetSubject(out ASubject: string): THorseLoggerElasticSearchConfig;
begin
  Result := Self;
  ASubject := FSubject;
end;

class function THorseLoggerElasticSearchConfig.New: THorseLoggerElasticSearchConfig;
begin
  Result := THorseLoggerElasticSearchConfig.Create;
end;
function THorseLoggerElasticSearchConfig.SendToElasticSearch(aValue: string): THorseLoggerElasticSearchConfig;

begin
  Result := Self;
  try
    TTask.Run(
      procedure ()
      var
        FRESTClient: TRESTClient;
        FRESTRequest: TRESTRequest;
        FRESTResponse: TRESTResponse;
    begin
      try
        FRESTClient := TRESTClient.Create('');
        FRESTRequest := TRESTRequest.Create(nil);
        FRESTResponse := TRESTResponse.Create(nil);
        FRESTRequest.Client := FRESTClient;
        FRESTRequest.Response := FRESTResponse;

        FRESTClient.BaseURL := FBaseURL;
        FRESTRequest.Method := rmPOST;
        FRESTRequest.AddBody(aValue, TRESTContentType.ctAPPLICATION_JSON);
        FRESTRequest.Accept := 'application/json';
        FRESTRequest.Execute;
      finally
        FreeAndNil(FRESTClient);
        FreeAndNil(FRESTRequest);
        FreeAndNil(FRESTResponse);
      end;
    end);
  except
  //
  end;

end;

function THorseLoggerElasticSearchConfig.SetBaseURL(ABaseURL: string): THorseLoggerElasticSearchConfig;
begin
  Result := Self;
  FBaseURL := ABaseURL;
end;

function THorseLoggerElasticSearchConfig.SetLogFormat(ALogFormat: string): THorseLoggerElasticSearchConfig;
begin
  Result := Self;
  FLogFormat := ALogFormat;
end;
function THorseLoggerElasticSearchConfig.SetSubject(ASubject: string): THorseLoggerElasticSearchConfig;
begin
  Result := Self;
  FSubject := ASubject;
end;

end.
