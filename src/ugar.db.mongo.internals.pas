unit ugar.db.mongo.internals;

interface

{$POINTERMATH ON}

uses
  ugar.db.mongo, ugar.db.mongo.Imp,
  ugar.db.mongo.Enum, ugar.db.mongo.Query, System.SysUtils, System.JSON,
  System.Generics.Collections, ugar.db.mongo.Protocol;

type
  TUgarClientHack = class(TUgarClient)
    property Protocol;
  end;

  TUgarDatabase = class(TInterfacedObject, IUgarDatabase)
  private
    FClient: IUgarClient;
    FProtocol: TUgarMongoProtocol; // Reference
    FName: String;
    FFullCommandCollectionName: UTF8String;
  protected
    function _GetClient: IUgarClient;
    function _GetName: String;

    function ListCollectionNames: TArray<String>;
    function ListCollections: TArray<TUgarBsonDocument>;
    procedure DropCollection(const AName: String);
    procedure DropDatabase;
    function GetCollection(const AName: String): IUgarCollection;

    function RunCommand(const ACommand: string): IUgarCursor; overload;
    function RunCommand(const ACommand: TUgarBsonDocument): IUgarCursor; overload;
  protected
    property Protocol: TUgarMongoProtocol read FProtocol;
    property Name: String read FName;
    property FullCommandCollectionName: UTF8String read FFullCommandCollectionName;
  public
    constructor Create(const AClient: TUgarClient; const AName: String);
  end;

  TUgarCursor = class(TInterfacedObject, IUgarCursor)
  private type
    TEnumerator = class(TEnumerator<TUgarBsonDocument>)
    private
      FProtocol: TUgarMongoProtocol; // Reference
      FFullCollectionName: UTF8String;
      FPage: TArray<TBytes>;
      FCursorId: Int64;
      FIndex: Integer;
    private
      procedure GetMore;
    protected
      function DoGetCurrent: TUgarBsonDocument; override;
      function DoMoveNext: Boolean; override;
    public
      constructor Create(const AProtocol: TUgarMongoProtocol; const AFullCollectionName: UTF8String;
        const APage: TArray<TBytes>; const ACursorId: Int64);
    end;
  private
    FProtocol: TUgarMongoProtocol; // Reference
    FFullCollectionName: UTF8String;
    FInitialPage: TArray<TBytes>;
    FInitialCursorId: Int64;
  public
    function GetEnumerator: TEnumerator<TUgarBsonDocument>;
    function ToArray: TArray<TUgarBsonDocument>;
  public
    constructor Create(const AProtocol: TUgarMongoProtocol; const AFullCollectionName: UTF8String;
      const AInitialPage: TArray<TBytes>; const AInitialCursorId: Int64);
  end;

  TUgarCollection = class(TInterfacedObject, IUgarCollection)
  private type
    PUgarBsonDocument = ^TUgarBsonDocument;
  private
    FDatabase: IUgarDatabase;
    FProtocol: TUgarMongoProtocol; // Reference
    FName: String;
    FFullName: UTF8String;
    FFullCommandCollectionName: UTF8String;
  private
    procedure AddWriteConcern(const AWriter: IUgarBsonWriter);
    function InsertMany(const ADocuments: PUgarBsonDocument; const ACount: Integer; const AOrdered: Boolean)
      : Integer; overload;
    function Delete(const AFilter: TUgarFilter; const AOrdered: Boolean; const ALimit: Integer): Integer;
    function Update(const AFilter: TUgarFilter; const AUpdate: TUgarUpdate;
      const AUpsert, AOrdered, AMulti: Boolean): Integer;

    function Find(const AFilter, AProjection: TBytes): IUgarCursor; overload;
    function FindOne(const AFilter, AProjection: TBytes): TUgarBsonDocument; overload;
  private
    class function AddModifier(const AFilter: TUgarFilter; const ASort: TUgarSort): TBytes; static;
  protected
    function _GetDatabase: IUgarDatabase;
    function _GetName: String;

    function InsertOne(const ADocument: TUgarBsonDocument): Boolean; overload;
    function InsertOne(const ADocument: TJsonObject): TJSONObject; overload;
    function InsertOne(const ADocument: string): Boolean; overload;

    function InsertMany(const ADocuments: array of TUgarBsonDocument; const AOrdered: Boolean = True): Integer; overload;
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
  public
    constructor Create(const ADatabase: TUgarDatabase; const AName: String);
  end;

implementation

uses
  ugar.db.mongo.Func, System.Math, Grijjy.Bson, Grijjy.Bson.IO;

{ TUgarDatabase }

constructor TUgarDatabase.Create(const AClient: TUgarClient; const AName: String);
begin
  Assert(AClient <> nil);
  Assert(AName <> '');
  inherited Create;
  FClient := AClient;
  FName := AName;
  FFullCommandCollectionName := UTF8String(AName + '.' + COLLECTION_COMMAND);
  FProtocol := TUgarClientHack(AClient).Protocol;
  Assert(FProtocol <> nil);
end;

procedure TUgarDatabase.DropCollection(const AName: String);
var
  Writer: IUgarBsonWriter;
  Reply: IUgarMongoReply;
begin
  Writer := TUgarBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteString('drop', AName);
  Writer.WriteEndDocument;
  Reply := FProtocol.OpQuery(FFullCommandCollectionName, [], 0, -1, Writer.ToBson, nil);
  HandleCommandReply(Reply, TUgarErrorCode.NamespaceNotFound);
end;

procedure TUgarDatabase.DropDatabase;
begin
  _GetClient.DropDatabase(Name);
end;

function TUgarDatabase.GetCollection(const AName: String): IUgarCollection;
begin
  Result := TUgarCollection.Create(Self, AName);
end;

function TUgarDatabase.ListCollectionNames: TArray<String>;
var
  LDocs: TArray<TUgarBsonDocument>;
  LIndex: Integer;
begin
  LDocs := ListCollections;
  SetLength(Result, Length(LDocs));
  for LIndex := 0 to Length(LDocs) - 1 do
    Result[LIndex] := LDocs[LIndex]['name'];
end;

function TUgarDatabase.ListCollections: TArray<TUgarBsonDocument>;
var
  LWriter: IUgarBsonWriter;
  LReply: IUgarMongoReply;
  LDoc, LCursor: TUgarBsonDocument;
  LValue: TUgarBsonValue;
  LDocs: TUgarBsonArray;
  LIndex: Integer;
begin
  LWriter := TUgarBsonWriter.Create;
  LWriter.WriteStartDocument;
  LWriter.WriteInt32('listCollections', 1);
  LWriter.WriteEndDocument;
  LReply := FProtocol.OpQuery(FFullCommandCollectionName, [], 0, -1, LWriter.ToBson, nil);
  HandleCommandReply(LReply);
  if (LReply.Documents = nil) then
    Exit(nil);

  LDoc := TUgarBsonDocument.Load(LReply.Documents[0]);
  if (not LDoc.TryGetValue('cursor', LValue)) then
    Exit(nil);
  LCursor := LValue.AsBsonDocument;

  if (not LCursor.TryGetValue('firstBatch', LValue)) then
    Exit(nil);

  LDocs := LValue.AsBsonArray;
  SetLength(Result, LDocs.Count);
  for LIndex := 0 to LDocs.Count - 1 do
    Result[LIndex] := LDocs[LIndex].AsBsonDocument;
end;

function TUgarDatabase.RunCommand(const ACommand: TUgarBsonDocument): IUgarCursor;
var
  Reply: IUgarMongoReply;
begin
  Reply := FProtocol.OpQuery(FFullCommandCollectionName, [], 0, -1, ACommand.ToBson, nil);
  HandleCommandReply(Reply);
  Result := TUgarCursor.Create(FProtocol, UTF8String(FName), Reply.Documents, Reply.CursorId);
end;

function TUgarDatabase.RunCommand(const ACommand: string): IUgarCursor;
begin
  Result := RunCommand(TgoBsonDocument.Parse(ACommand));
end;

function TUgarDatabase._GetClient: IUgarClient;
begin
  Result := FClient;
end;

function TUgarDatabase._GetName: String;
begin
  Result := FName;
end;

{ TUgarCollection }

class function TUgarCollection.AddModifier(const AFilter: TUgarFilter; const ASort: TUgarSort): TBytes;
var
  Writer: IUgarBsonWriter;
begin
  Writer := TUgarBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteName('$query');
  Writer.WriteRawBsonDocument(AFilter.ToBson);
  Writer.WriteName('$orderby');
  Writer.WriteRawBsonDocument(ASort.ToBson);
  Writer.WriteEndDocument;
  Result := Writer.ToBson;
end;

procedure TUgarCollection.AddWriteConcern(const AWriter: IUgarBsonWriter);
begin
  { TODO -oROB -cFeature : Write concerns are currently not supported }
end;

function TUgarCollection.Count(const AFilter: TUgarFilter): Integer;
var
  Writer: IUgarBsonWriter;
  Reply: IUgarMongoReply;
begin
  Writer := TUgarBsonWriter.Create;

  Writer.WriteStartDocument;
  Writer.WriteString('count', FName);
  Writer.WriteName('query');
  Writer.WriteRawBsonDocument(AFilter.ToBson);
  Writer.WriteEndDocument;

  Reply := FProtocol.OpQuery(FFullCommandCollectionName, [], 0, -1, Writer.ToBson, nil);
  Result := HandleCommandReply(Reply);
end;

function TUgarCollection.Count: Integer;
begin
  Result := Count(TUgarFilter.Empty);
end;

constructor TUgarCollection.Create(const ADatabase: TUgarDatabase; const AName: String);
begin
  Assert(Assigned(ADatabase));
  Assert(AName <> '');
  inherited Create;
  FDatabase := ADatabase;
  FName := AName;
  FFullName := UTF8String(ADatabase.Name + '.' + AName);
  FFullCommandCollectionName := ADatabase.FullCommandCollectionName;
  FProtocol := ADatabase.Protocol;
  Assert(FProtocol <> nil);
end;

function TUgarCollection.Delete(const AFilter: TUgarFilter; const AOrdered: Boolean; const ALimit: Integer): Integer;
var
  Writer: IUgarBsonWriter;
  Reply: IUgarMongoReply;
begin
  Writer := TUgarBsonWriter.Create;
  Writer.WriteStartDocument;

  Writer.WriteString('delete', FName);

  Writer.WriteStartArray('deletes');
  Writer.WriteStartDocument;
  Writer.WriteName('q');
  Writer.WriteRawBsonDocument(AFilter.ToBson);
  Writer.WriteInt32('limit', ALimit);
  Writer.WriteEndDocument;
  Writer.WriteEndArray;

  AddWriteConcern(Writer);
  Writer.WriteEndDocument;

  Reply := FProtocol.OpQuery(FFullCommandCollectionName, [], 0, -1, Writer.ToBson, nil);
  Result := HandleCommandReply(Reply);
end;

function TUgarCollection.DeleteMany(const AFilter: TUgarFilter; const AOrdered: Boolean): Integer;
begin
  Result := Delete(AFilter, AOrdered, 0);
end;

function TUgarCollection.DeleteOne(const AFilter: TUgarFilter): Boolean;
begin
  Result := (Delete(AFilter, True, 1) = 1);
end;

function TUgarCollection.Find(const AFilter: TUgarFilter; const ASort: TUgarSort): IUgarCursor;
begin
  Result := Find(AddModifier(AFilter, ASort), nil);
end;

function TUgarCollection.Find(const AFilter: TUgarFilter; const AProjection: TUgarProjection; const ASort: TUgarSort)
  : IUgarCursor;
begin
  Result := Find(AddModifier(AFilter, ASort), AProjection.ToBson);
end;

function TUgarCollection.Find(const AFilter: TUgarFilter; const AProjection: TUgarProjection): IUgarCursor;
begin
  Result := Find(AFilter.ToBson, AProjection.ToBson);
end;

function TUgarCollection.Find(const AFilter: TUgarFilter): IUgarCursor;
begin
  Result := Find(AFilter.ToBson, nil);
end;

function TUgarCollection.Find(const AProjection: TUgarProjection): IUgarCursor;
begin
  Result := Find(nil, AProjection.ToBson);
end;

function TUgarCollection.Find(const AFilter, AProjection: TBytes): IUgarCursor;
var
  Reply: IUgarMongoReply;
begin
  Reply := FProtocol.OpQuery(FFullName, [], 0, 0, AFilter, AProjection);
  HandleTimeout(Reply);
  Result := TUgarCursor.Create(FProtocol, FFullName, Reply.Documents, Reply.CursorId);
end;

function TUgarCollection.Find: TJSONArray;
var
  LBSON: TEnumerator<TUgarBsonDocument>;
begin
  Result := TJSONArray.Create;

  LBSON := Find(nil, nil).GetEnumerator;

  while LBSON.MoveNext do
  begin
    Result.AddElement(TJSONObject.ParseJSONValue(LBSON.Current.ToJson));
  end;

end;

function TUgarCollection.FindOne(const AFilter, AProjection: TBytes): TUgarBsonDocument;
var
  LReply: IUgarMongoReply;
begin
  LReply := FProtocol.OpQuery(FFullName, [], 0, 1, AFilter, AProjection);
  HandleTimeout(LReply);
  if (LReply.Documents = nil) then
    Result.SetNil
  else
    Result := TUgarBsonDocument.Load(LReply.Documents[0]);
end;

function TUgarCollection.FindOne(const AFilter: TUgarFilter): TUgarBsonDocument;
begin
  Result := FindOne(AFilter.ToBson, nil);
end;

function TUgarCollection.FindOne(const AFilter: TUgarFilter; const AProjection: TUgarProjection): TUgarBsonDocument;
begin
  Result := FindOne(AFilter.ToBson, AProjection.ToBson);
end;

function TUgarCollection.InsertMany(const ADocuments: TArray<TUgarBsonDocument>; const AOrdered: Boolean): Integer;
begin
  if (Length(ADocuments) > 0) then
    Result := InsertMany(@ADocuments[0], Length(ADocuments), AOrdered)
  else
    Result := 0;
end;

function TUgarCollection.InsertMany(const ADocuments: TEnumerable<TUgarBsonDocument>; const AOrdered: Boolean): Integer;
begin
  Result := InsertMany(ADocuments.ToArray, AOrdered);
end;

function TUgarCollection.InsertMany(const ADocuments: PUgarBsonDocument; const ACount: Integer;
  const AOrdered: Boolean): Integer;
var
  LWriter: IUgarBsonWriter;
  LReply: IUgarMongoReply;
  LI, LRemaining, LItemsInBatch, LIndex: Integer;
begin
  LRemaining := ACount;
  LIndex := 0;
  Result := 0;
  while (LRemaining > 0) do
  begin
    LWriter := TUgarBsonWriter.Create;

    LWriter.WriteStartDocument;

    LWriter.WriteString('insert', FName);

    LWriter.WriteStartArray('documents');

    LItemsInBatch := Min(LRemaining, MAX_BULK_SIZE);

    for LI := 0 to LItemsInBatch - 1 do
    begin
      LWriter.WriteValue(ADocuments[LIndex]);
      Inc(LIndex);
    end;

    Dec(LRemaining, LItemsInBatch);
    LWriter.WriteEndArray;

    LWriter.WriteBoolean('ordered', AOrdered);

    AddWriteConcern(LWriter);

    LWriter.WriteEndDocument;

    LReply := FProtocol.OpQuery(FFullCommandCollectionName, [], 0, -1, LWriter.ToBson, nil);
    Inc(Result, HandleCommandReply(LReply));
  end;
  Assert(LIndex = ACount);
end;

function TUgarCollection.InsertMany(const ADocuments: array of TUgarBsonDocument; const AOrdered: Boolean): Integer;
begin
  if (Length(ADocuments) > 0) then
    Result := InsertMany(@ADocuments[0], Length(ADocuments), AOrdered)
  else
    Result := 0;
end;

function TUgarCollection.InsertOne(const ADocument: TUgarBsonDocument): Boolean;
var
  Writer: IUgarBsonWriter;
  Reply: IUgarMongoReply;
begin
  Writer := TUgarBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteString('insert', FName);

  Writer.WriteStartArray('documents');
  Writer.WriteValue(ADocument);
  Writer.WriteEndArray;

  AddWriteConcern(Writer);

  Writer.WriteEndDocument;

  Reply := FProtocol.OpQuery(FFullCommandCollectionName, [], 0, -1, Writer.ToBson, nil);
  Result := (HandleCommandReply(Reply) = 1);
end;

function TUgarCollection.InsertOne(const ADocument: TJsonObject): TJSONObject;
begin
  InsertOne(TUgarBsonDocument.Parse(ADocument.ToJSON));
  Result := ADocument;
end;

function TUgarCollection.InsertMany(const ADocuments: TArray<TJsonObject>; const AOrdered: Boolean): Integer;
var
  LDocuments: TArray<string>;
  LIndex: Integer;
begin
  SetLength(LDocuments, Length(ADocuments));

  for LIndex := 0 to Length(ADocuments) - 1 do
    LDocuments[LIndex] := ADocuments[LIndex].ToJSON;

  Result := InsertMany(LDocuments, AOrdered);
end;

function TUgarCollection.InsertMany(const ADocuments: array of string; const AOrdered: Boolean): Integer;
var
  LDocuments: TArray<TUgarBsonDocument>;
  LIndex: Integer;
begin
  SetLength(LDocuments, Length(ADocuments));

  for LIndex := 0 to Length(ADocuments) - 1 do
    LDocuments[LIndex] := TUgarBsonDocument.Parse(ADocuments[LIndex]);

  Result := InsertMany(LDocuments, AOrdered);
end;

function TUgarCollection.InsertMany(const ADocuments: array of TJsonObject; const AOrdered: Boolean): Integer;
var
  LDocuments: TArray<string>;
  LIndex: Integer;
begin
  SetLength(LDocuments, Length(ADocuments));

  for LIndex := 0 to Length(ADocuments) - 1 do
    LDocuments[LIndex] := ADocuments[LIndex].ToJSON;

  Result := InsertMany(LDocuments, AOrdered);
end;

function TUgarCollection.InsertMany(const ADocuments: TEnumerable<string>; const AOrdered: Boolean): Integer;
begin
  Result := InsertMany(ADocuments.ToArray, AOrdered);
end;

function TUgarCollection.InsertMany(const ADocuments: TEnumerable<TJsonObject>; const AOrdered: Boolean): Integer;
begin
  Result := InsertMany(ADocuments.ToArray, AOrdered);
end;

function TUgarCollection.InsertMany(const ADocuments: TArray<string>; const AOrdered: Boolean): Integer;
var
  LDocuments: TArray<TUgarBsonDocument>;
  LIndex: Integer;
begin
  SetLength(LDocuments, Length(ADocuments));

  for LIndex := 0 to Length(ADocuments) - 1 do
    LDocuments[LIndex] := TUgarBsonDocument.Parse(ADocuments[LIndex]);

  Result := InsertMany(LDocuments, AOrdered);
end;

function TUgarCollection.InsertOne(const ADocument: string): Boolean;
begin
  Result := InsertOne(TUgarBsonDocument.Parse(ADocument));
end;

function TUgarCollection.Update(const AFilter: TUgarFilter; const AUpdate: TUgarUpdate;
  const AUpsert, AOrdered, AMulti: Boolean): Integer;
var
  Writer: IUgarBsonWriter;
  Reply: IUgarMongoReply;
begin
  Writer := TUgarBsonWriter.Create;
  Writer.WriteStartDocument;
  Writer.WriteString('update', FName);

  Writer.WriteStartArray('updates');

  Writer.WriteStartDocument;
  Writer.WriteName('q');
  Writer.WriteRawBsonDocument(AFilter.ToBson);
  Writer.WriteName('u');
  Writer.WriteRawBsonDocument(AUpdate.ToBson);
  Writer.WriteBoolean('upsert', AUpsert);
  Writer.WriteBoolean('multi', AMulti);
  Writer.WriteEndDocument;

  Writer.WriteEndArray;

  Writer.WriteBoolean('ordered', AOrdered);

  AddWriteConcern(Writer);

  Writer.WriteEndDocument;

  Reply := FProtocol.OpQuery(FFullCommandCollectionName, [], 0, -1, Writer.ToBson, nil);
  Result := HandleCommandReply(Reply);
end;

function TUgarCollection.UpdateMany(const AFilter: TUgarFilter; const AUpdate: TUgarUpdate;
  const AUpsert, AOrdered: Boolean): Integer;
begin
  Result := Update(AFilter, AUpdate, AUpsert, AOrdered, True);
end;

function TUgarCollection.UpdateOne(const AFilter: TUgarFilter; const AUpdate: TUgarUpdate;
  const AUpsert: Boolean): Boolean;
begin
  Result := (Update(AFilter, AUpdate, AUpsert, False, False) = 1);
end;

function TUgarCollection._GetDatabase: IUgarDatabase;
begin
  Result := FDatabase;
end;

function TUgarCollection._GetName: String;
begin
  Result := FName;
end;

{ TUgarCursor }

constructor TUgarCursor.Create(const AProtocol: TUgarMongoProtocol; const AFullCollectionName: UTF8String;
  const AInitialPage: TArray<TBytes>; const AInitialCursorId: Int64);
begin
  inherited Create;
  FProtocol := AProtocol;
  FFullCollectionName := AFullCollectionName;
  FInitialPage := AInitialPage;
  FInitialCursorId := AInitialCursorId;
end;

function TUgarCursor.GetEnumerator: TEnumerator<TUgarBsonDocument>;
begin
  Result := TEnumerator.Create(FProtocol, FFullCollectionName, FInitialPage, FInitialCursorId);
end;

function TUgarCursor.ToArray: TArray<TUgarBsonDocument>;
var
  LCount, LCapacity: Integer;
  LDoc: TUgarBsonDocument;
begin
  LCount := 0;
  LCapacity := 16;
  SetLength(Result, LCapacity);

  for LDoc in Self do
  begin
    if (LCount >= LCapacity) then
    begin
      LCapacity := LCapacity * 2;
      SetLength(Result, LCapacity);
    end;
    Result[LCount] := LDoc;
    Inc(LCount);
  end;
  SetLength(Result, LCount);
end;

{ TUgarCursor.TEnumerator }

constructor TUgarCursor.TEnumerator.Create(const AProtocol: TUgarMongoProtocol; const AFullCollectionName: UTF8String;
  const APage: TArray<TBytes>; const ACursorId: Int64);
begin
  inherited Create;
  FProtocol := AProtocol;
  FFullCollectionName := AFullCollectionName;
  FPage := APage;
  FCursorId := ACursorId;
  FIndex := -1;
end;

function TUgarCursor.TEnumerator.DoGetCurrent: TUgarBsonDocument;
begin
  Result := TUgarBsonDocument.Load(FPage[FIndex]);
end;

function TUgarCursor.TEnumerator.DoMoveNext: Boolean;
begin
  Result := (FIndex < (Length(FPage) - 1));
  if Result then
    Inc(FIndex)
  else if (FCursorId <> 0) then
  begin
    GetMore;
    Result := (FPage <> nil);
  end;
end;

procedure TUgarCursor.TEnumerator.GetMore;
var
  LReply: IUgarMongoReply;
begin
  LReply := FProtocol.OpGetMore(FFullCollectionName, Length(FPage), FCursorId);
  HandleTimeout(LReply);
  FPage := LReply.Documents;
  FCursorId := LReply.CursorId;
  FIndex := 0;
end;

end.
