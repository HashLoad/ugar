unit Ugar;

interface

uses
  System.Generics.Collections, ugar.db.mongo.Enum, ugar.db.mongo.Query, System.JSON, ugar.db.Mongo;

type

  TUgarBsonValue = ugar.db.mongo.Enum.TUgarBsonValue;
  TUgarBsonDocument = ugar.db.mongo.Enum.TUgarBsonDocument;
  TUgarDatabase = TUgarDatabaseFunction;
  UgarQuery = ugar.db.mongo.Query.TUgarTextSearchOption;
  TUgarTextSearchOptions = ugar.db.mongo.Query.TUgarTextSearchOptions;
  UgarFilter = ugar.db.mongo.Query.TUgarFilter;
  Projection = ugar.db.mongo.Query.TUgarProjection;
  TUgarSortDirection = ugar.db.mongo.Query.TUgarSortDirection;
  UgarSort = ugar.db.mongo.Query.TUgarSort;
  TUgarCurrentDateType = ugar.db.mongo.Query.TUgarCurrentDateType;
  UgarUpdate = ugar.db.mongo.Query.TUgarUpdate;



  TUgar = class
  private
    FConnection: TDictionary<string, IUgarConnection>;

    constructor Create;
  public
    destructor Destroy; override;
    class function Init(AHost: string; APort: Integer; ADatabase: String): TUgarDatabase;
  end;

implementation

uses
  System.SysUtils, Ugar.Connection.Imp;

var
  _Instance: TUgar;

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

class function TUgar.Init(AHost: string; APort: Integer; ADatabase: String): TUgarDatabase;
var
  LConnection: IUgarConnection;
  LKey: string;
begin
  Lkey := AHost + APort.ToString;
  if not _Instance.FConnection.TryGetValue(LKey, LConnection) then
  begin
    LConnection := TUgarConnection.Create(AHost, APort);
    _Instance.FConnection.Add(LKey, LConnection);
  end;
  Result := LConnection.Database[ADatabase];
end;


initialization

_Instance := TUgar.Create;

finalization

_Instance.DisposeOf;

end.
