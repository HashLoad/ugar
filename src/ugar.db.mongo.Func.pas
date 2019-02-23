unit ugar.db.mongo.Func;

interface

uses
  ugar.db.mongo.Enum, ugar.db.mongo, System.SysUtils, ugar.db.mongo.protocol.Types;

type
  EUgarDBError = class(Exception);

  EUgarDBConnectionError = class(EUgarDBError);

  EUgarDBWriteError = class(EUgarDBError)
  private
    FErrorCode: TUgarErrorCode;
  public
    constructor Create(const AErrorCode: TUgarErrorCode; const AErrorMsg: String);
    property ErrorCode: TUgarErrorCode read FErrorCode;
  end;

procedure HandleTimeout(const AReply: IUgarMongoReply);
function HandleCommandReply(const AReply: IUgarMongoReply;
  const AErrorToIgnore: TUgarErrorCode = TUgarErrorCode.OK): Integer;

implementation

uses
  Grijjy.Bson;

resourcestring
  RS_MONGODB_CONNECTION_ERROR = 'Error connecting to the MongoDB database';
  RS_MONGODB_GENERIC_ERROR = 'Unspecified error while performing MongoDB operation';

procedure HandleTimeout(const AReply: IUgarMongoReply);
begin
  if (AReply = nil) then
    raise EUgarDBConnectionError.Create(RS_MONGODB_CONNECTION_ERROR);
end;

function HandleCommandReply(const AReply: IUgarMongoReply;
  const AErrorToIgnore: TUgarErrorCode = TUgarErrorCode.OK): Integer;
var
  LDoc, LErrorDoc: TUgarBsonDocument;
  LValue: TgoBsonValue;
  LValues: TgoBsonArray;
  LOK: Boolean;
  LErrorCode: TUgarErrorCode;
  LErrorMsg: String;
begin
  if (AReply = nil) then
    raise EUgarDBConnectionError.Create(RS_MONGODB_CONNECTION_ERROR);

  if (AReply.Documents = nil) then
    Exit(0);

  LDoc := TUgarBsonDocument.Load(AReply.Documents[0]);
  Result := LDoc['n'];

  LOK := LDoc['ok'];
  if (not LOK) then
  begin
    Word(LErrorCode) := LDoc['code'];

    if (AErrorToIgnore <> TUgarErrorCode.OK) and (LErrorCode = AErrorToIgnore) then
      Exit;

    if (LErrorCode <> TUgarErrorCode.OK) then
    begin
      LErrorMsg := LDoc['errmsg'];
      raise EUgarDBWriteError.Create(LErrorCode, LErrorMsg);
    end;

    if (LDoc.TryGetValue('writeErrors', LValue)) then
    begin
      LValues := LValue.AsBsonArray;
      if (LValues.Count > 0) then
      begin
        LErrorDoc := LValues.Items[0].AsBsonDocument;
        Word(LErrorCode) := LErrorDoc['code'];
        LErrorMsg := LErrorDoc['errmsg'];
        raise EUgarDBWriteError.Create(LErrorCode, LErrorMsg);
      end;
    end;

    if (LDoc.TryGetValue('writeConcernError', LValue)) then
    begin
      LErrorDoc := LValue.AsBsonDocument;
      Word(LErrorCode) := LErrorDoc['code'];
      LErrorMsg := LErrorDoc['errmsg'];
      raise EUgarDBWriteError.Create(LErrorCode, LErrorMsg);
    end;

    raise EUgarDBError.Create(RS_MONGODB_GENERIC_ERROR);
  end;
end;

{ EUgarDBWriteError }

constructor EUgarDBWriteError.Create(const AErrorCode: TUgarErrorCode; const AErrorMsg: String);
begin
  inherited Create(AErrorMsg + Format(' (error %d)', [Ord(AErrorCode)]));
  FErrorCode := AErrorCode;
end;

end.
