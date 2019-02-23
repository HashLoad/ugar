unit ugar.db.mongo.protocol.Types;

interface

uses
  ugar.db.mongo.Enum, System.SysUtils;

type
  IUgarMongoReply = interface
    ['{25CEF8E1-B023-4232-BE9A-1FBE9E51CE57}']
    function _GetResponseFlags: TUgarMongoResponseFlags;
    function _GetCursorId: Int64;
    function _GetStartingFrom: Integer;
    function _GetResponseTo: Integer;
    function _GetDocuments: TArray<TBytes>;
    property ReponseFlags: TUgarMongoResponseFlags read _GetResponseFlags;
    property CursorId: Int64 read _GetCursorId;
    property StartingFrom: Integer read _GetStartingFrom;
    property ResponseTo: Integer read _GetResponseTo;
    property Documents: TArray<TBytes> read _GetDocuments;
  end;

  TUgarMongoProtocolSettings = record
    ConnectionTimeout: Integer;
    ReplyTimeout: Integer;
    PoolSize: Integer;
  end;

type
  TMsgHeader = packed record
    MessageLength: Int32;
    RequestID: Int32;
    ResponseTo: Int32;
    OpCode: Int32;
  end;

  PMsgHeader = ^TMsgHeader;
                                            
type
  TOpReplyHeader = packed record
    Header: TMsgHeader;
    ResponseFlags: Int32;
    CursorId: Int64;
    StartingFrom: Int32;
    NumberReturned: Int32;
    { Documents: Documents }
  end;

  POpReplyHeader = ^TOpReplyHeader;

  TUgarMongoReply = class(TInterfacedObject, IUgarMongoReply)
  private
    FHeader: TOpReplyHeader;
    FDocuments: TArray<TBytes>;
  protected
    function _GetResponseFlags: TUgarMongoResponseFlags;
    function _GetCursorId: Int64;
    function _GetStartingFrom: Integer;
    function _GetResponseTo: Integer;
    function _GetDocuments: TArray<TBytes>;
  public
    constructor Create(const ABuffer: TBytes; const ASize: Integer);
  end;

implementation

constructor TUgarMongoReply.Create(const ABuffer: TBytes; const ASize: Integer);
var
  I, Index, Count: Integer;
  Size: Int32;
  Document: TBytes;
begin
  inherited Create;
  if (ASize >= SizeOf(TOpReplyHeader)) then
  begin
    FHeader := POpReplyHeader(@ABuffer[0])^;
    if (FHeader.NumberReturned > 0) then
    begin
      Index := SizeOf(TOpReplyHeader);
      Count := 0;
      SetLength(FDocuments, FHeader.NumberReturned);

      for I := 0 to FHeader.NumberReturned - 1 do
      begin
        Move(ABuffer[Index], Size, SizeOf(Int32));
        if (ASize < (Index + Size)) then
          Break;

        SetLength(Document, Size);
        Move(ABuffer[Index], Document[0], Size);
        FDocuments[Count] := Document;
        Inc(Index, Size);
        Inc(Count);
      end;

      SetLength(FDocuments, Count);
    end;
  end
  else
    FHeader.CursorId := -1;
end;

function TUgarMongoReply._GetCursorId: Int64;
begin
  Result := FHeader.CursorId;
end;

function TUgarMongoReply._GetDocuments: TArray<TBytes>;
begin
  Result := FDocuments;
end;

function TUgarMongoReply._GetResponseFlags: TUgarMongoResponseFlags;
begin
  Byte(Result) := FHeader.ResponseFlags;
end;

function TUgarMongoReply._GetResponseTo: Integer;
begin
  Result := FHeader.Header.ResponseTo;
end;

function TUgarMongoReply._GetStartingFrom: Integer;
begin
  Result := FHeader.StartingFrom;
end;

end.
