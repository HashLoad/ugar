unit ugar.db.mongo.Imp;

interface

uses
  ugar.db.Mongo,
  ugar.db.mongo.Enum, ugar.db.mongo.Protocol;

type
  TUgarClientSettings = record
  private const
    C_DEFAULT_TIMEOUT = 5000;
  public
    ConnectionTimeout: Integer;
    ReplyTimeout: Integer;
  public
    class function Create: TUgarClientSettings; static;
  end;

type
  TUgarClient = class(TInterfacedObject, IUgarClient)
  public const
    DEFAULT_HOST = 'localhost';
    DEFAULT_PORT = 27017;
  private
    FProtocol: TUgarMongoProtocol;
  protected
    function ListDatabaseNames: TArray<String>;
    function ListDatabases: TArray<TUgarBsonDocument>;
    procedure DropDatabase(const AName: String);
    function GetDatabase(const AName: String): IUgarDatabase;
  protected
    property Protocol: TUgarMongoProtocol read FProtocol;
  public
    constructor Create(const AHost: String = DEFAULT_HOST; const APort: Integer = DEFAULT_PORT); overload;
    constructor Create(const AHost: String; const APort: Integer; const ASettings: TUgarClientSettings); overload;
    constructor Create(const ASettings: TUgarClientSettings); overload;
    destructor Destroy; override;
  end;

implementation

uses
  System.Math, ugar.db.mongo.Func, ugar.db.mongo.internals;

constructor TUgarClient.Create(const AHost: String; const APort: Integer);
begin
  Create(AHost, APort, TUgarClientSettings.Create);
end;

constructor TUgarClient.Create(const AHost: String; const APort: Integer;
  const ASettings: TUgarClientSettings);
var
  LSettings: TUgarMongoProtocolSettings;
begin
  inherited Create;
  LSettings.ConnectionTimeout := ASettings.ConnectionTimeout;
  LSettings.ReplyTimeout := ASettings.ReplyTimeout;
  FProtocol := TUgarMongoProtocol.Create(AHost, APort, LSettings);
end;

constructor TUgarClient.Create(const ASettings: TUgarClientSettings);
begin
  Create(DEFAULT_HOST, DEFAULT_PORT, ASettings);
end;

destructor TUgarClient.Destroy;
begin
  FProtocol.Free;
  inherited;
end;

procedure TUgarClient.DropDatabase(const AName: String);
var
  Writer: IUgarBsonWriter;
  Reply: IUgarMongoReply;
begin
  Writer := TUgarBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteInt32('dropDatabase', 1);
  Writer.WriteEndDocument;
  Reply := FProtocol.OpQuery(UTF8String(AName + '.' + COLLECTION_COMMAND), [], 0, -1, Writer.ToBson, nil);
  HandleCommandReply(Reply);
end;

function TUgarClient.GetDatabase(const AName: String): IUgarDatabase;
begin
  Result := TUgarDatabase.Create(Self, AName);
end;

function TUgarClient.ListDatabaseNames: TArray<String>;
begin

end;

function TUgarClient.ListDatabases: TArray<TUgarBsonDocument>;
begin

end;

{ TUgarClientSettings }

class function TUgarClientSettings.Create: TUgarClientSettings;
begin
  Result.ConnectionTimeout := C_DEFAULT_TIMEOUT;
  Result.ReplyTimeout := C_DEFAULT_TIMEOUT;
end;

end.
