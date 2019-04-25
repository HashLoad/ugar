unit ugar.db.Mongo;

interface

uses
  Grijjy.Bson, System.Generics.Collections, System.JSON, System.SysUtils, ugar.db.Mongo.Enum,
  ugar.db.Mongo.Query;

type

  IUgarDatabase = interface;
  IUgarCollection = interface;

  IUgarCursor = interface
    ['{18813F27-1B41-453C-86FE-E98AFEB3D905}']
    function GetEnumerator: TEnumerator<TUgarBsonDocument>;
    function ToArray: TArray<TUgarBsonDocument>;
  end;

  IUgarClient = interface
    ['{66FF5346-48F6-44E1-A46F-D8B958F06EA0}']
    function ListDatabaseNames: TArray<String>;
    function ListDatabases: TArray<TUgarBsonDocument>;
    procedure DropDatabase(const AName: String);
    function GetDatabase(const AName: String): IUgarDatabase;
  end;

  IUgarDatabase = interface
    ['{5164D7B1-74F5-45F1-AE22-AB5FFC834590}']
    function _GetClient: IUgarClient;
    function _GetName: String;
    function ListCollectionNames: TArray<String>;
    function ListCollections: TArray<TUgarBsonDocument>;

    procedure DropCollection(const AName: String);
    function RunCommand(const ACommand: string): IUgarCursor; overload;
    function RunCommand(const ACommand: TUgarBsonDocument): IUgarCursor; overload;
    procedure DropDatabase();

    function GetCollection(const AName: String): IUgarCollection;

    property Client: IUgarClient read _GetClient;

    property Name: String read _GetName;
  end;

  IUgarCollection = interface
    ['{9822579B-1682-4FAC-81CF-A4B239777812}']
    function _GetDatabase: IUgarDatabase;
    function _GetName: String;
    function InsertOne(const ADocument: TUgarBsonDocument): Boolean; overload;
    function InsertOne(const ADocument: TJsonObject): TJsonObject; overload;
    function InsertOne(const ADocument: string): Boolean; overload;

    function InsertMany(const ADocuments: array of TUgarBsonDocument; const AOrdered: Boolean = True): Integer;
      overload;
    function InsertMany(const ADocuments: array of TJsonObject; const AOrdered: Boolean = True): Integer; overload;
    function InsertMany(const ADocuments: array of string; const AOrdered: Boolean = True): Integer; overload;

    function InsertMany(const ADocuments: TArray<TUgarBsonDocument>; const AOrdered: Boolean = True): Integer; overload;
    function InsertMany(const ADocuments: TArray<TJsonObject>; const AOrdered: Boolean = True): Integer; overload;
    function InsertMany(const ADocuments: TArray<string>; const AOrdered: Boolean = True): Integer; overload;

    function InsertMany(const ADocuments: TEnumerable<TUgarBsonDocument>; const AOrdered: Boolean = True)
      : Integer; overload;
    function InsertMany(const ADocuments: TEnumerable<TJsonObject>; const AOrdered: Boolean = True): Integer; overload;
    function InsertMany(const ADocuments: TEnumerable<string>; const AOrdered: Boolean = True): Integer; overload;

    function DeleteOne(const AFilter: TUgarFilter): Boolean;

    function DeleteMany(const AFilter: TUgarFilter; const AOrdered: Boolean = True): Integer;

    function UpdateOne(const AFilter: TUgarFilter; const AUpdate: TUgarUpdate; const AUpsert: Boolean = False): Boolean;

    function UpdateMany(const AFilter: TUgarFilter; const AUpdate: TUgarUpdate; const AUpsert: Boolean = False;
      const AOrdered: Boolean = True): Integer;

    function Find(const AFilter: TUgarFilter; const AProjection: TUgarProjection): IUgarCursor; overload;
    function Find(const AFilter: TUgarFilter): IUgarCursor; overload;
    function Find(const AProjection: TUgarProjection): IUgarCursor; overload;
    function Find: TJSONArray; overload;
    function Find(const AFilter: TUgarFilter; const ASort: TUgarSort): IUgarCursor; overload;
    function Find(const AFilter: TUgarFilter; const AProjection: TUgarProjection; const ASort: TUgarSort)
      : IUgarCursor; overload;

    function FindOne(const AFilter: TUgarFilter; const AProjection: TUgarProjection): TUgarBsonDocument; overload;
    function FindOne(const AFilter: TUgarFilter): TUgarBsonDocument; overload;

    function Count: Integer; overload;
    function Count(const AFilter: TUgarFilter): Integer; overload;

    property Database: IUgarDatabase read _GetDatabase;

    property Name: String read _GetName;
  end;

  TUgarDatabaseFunction = reference to function(AName: string = '_'): IUgarCollection;

  IUgarConnection = Interface
    function GetDatabase(ADatabaseName: string): TUgarDatabaseFunction;
    property Database[ADatabaseName: string]: TUgarDatabaseFunction read GetDatabase;
  End;

implementation

end.
