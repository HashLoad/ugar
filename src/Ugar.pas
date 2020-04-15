unit Ugar;

interface

uses
  System.Generics.Collections, Ugar.db.mongo.Enum, Ugar.db.mongo.Query, System.JSON, Ugar.db.mongo;

type

  TUgarBsonValue = Ugar.db.mongo.Enum.TUgarBsonValue;
  TUgarBsonDocument = Ugar.db.mongo.Enum.TUgarBsonDocument;
  TUgarDatabase = TUgarDatabaseFunction;
  UgarQuery = Ugar.db.mongo.Query.TUgarTextSearchOption;
  TUgarTextSearchOptions = Ugar.db.mongo.Query.TUgarTextSearchOptions;
  UgarFilter = Ugar.db.mongo.Query.TUgarFilter;
  Projection = Ugar.db.mongo.Query.TUgarProjection;
  TUgarSortDirection = Ugar.db.mongo.Query.TUgarSortDirection;
  UgarSort = Ugar.db.mongo.Query.TUgarSort;
  TUgarCurrentDateType = Ugar.db.mongo.Query.TUgarCurrentDateType;
  UgarUpdate = Ugar.db.mongo.Query.TUgarUpdate;

  TUgar = class
  private
    FConnection: TDictionary<string, IUgarConnection>;
    class var FInstance: TUgar;
    class function GetDefaultInstance: TUgar;
  public
    constructor Create;
    destructor Destroy; override;
    class destructor UnInitialize;
    class function Init(AHost: string; APort: Integer; ADatabase: String): TUgarDatabase;
  end;

implementation

uses
  System.SysUtils, Ugar.Connection.Imp;

{ TUgar }

constructor TUgar.Create;
begin
  FConnection := TDictionary<string, IUgarConnection>.Create;
end;

destructor TUgar.Destroy;
begin
  FConnection.DisposeOf;
  inherited;
end;

class function TUgar.GetDefaultInstance: TUgar;
begin
  if FInstance = nil then
    FInstance := TUgar.Create;
  Result := FInstance;
end;

class function TUgar.Init(AHost: string; APort: Integer; ADatabase: String): TUgarDatabase;
var
  LConnection: IUgarConnection;
  LKey: string;
begin
  LKey := AHost + APort.ToString;
  if not GetDefaultInstance.FConnection.TryGetValue(LKey, LConnection) then
  begin
    LConnection := TUgarConnection.Create(AHost, APort);
    GetDefaultInstance.FConnection.Add(LKey, LConnection);
  end;
  Result := LConnection.Database[ADatabase];
end;

class destructor TUgar.UnInitialize;
begin
  if FInstance <> nil then
    FInstance.Free;
end;

end.
