unit ugar.connection.Imp;

interface

uses
  ugar.db.Mongo;

type
  TUgarConnection = class(TInterfacedObject, IUgarConnection)
  private
    FMongo: IUgarClient;
    function GetDatabase(ADatabaseName: string): TUgarDatabaseFunction;
  public
    constructor Create(AHost: string; APort: Integer);
    property Database[ADatabaseName: string]: TUgarDatabaseFunction read GetDatabase;
  end;

implementation

uses
  ugar.db.Mongo.Imp;

{ TUgarConnection }

constructor TUgarConnection.Create(AHost: string; APort: Integer);
begin
  FMongo := TUgarClient.Create(AHost, APort);
end;

function TUgarConnection.GetDatabase(ADatabaseName: string): TUgarDatabaseFunction;
begin
  result := function(AName: String): IUgarCollection
    begin
      result := IUgarCollection(FMongo.GetDatabase(ADatabaseName).GetCollection(AName))
    end;
end;

end.
