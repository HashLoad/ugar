unit ugar.db.mongo.Protocol;

{$INCLUDE 'Grijjy.inc'}

interface

uses
  System.SyncObjs,
  System.SysUtils,
  System.Generics.Collections,
{$IF Defined(MSWINDOWS)}
  Grijjy.SocketPool.Win,
{$ELSEIF Defined(LINUX)}
  Grijjy.SocketPool.Linux,
{$ELSE}
{$MESSAGE Error 'The MongoDB driver is only supported on Windows and Linux'}
{$ENDIF}
  Grijjy.Bson, ugar.db.mongo.Enum, ugar.db.mongo.Protocol.Types;

type
  TUgarMongoProtocolSettings = ugar.db.mongo.Protocol.Types.TUgarMongoProtocolSettings;
  IUgarMongoReply = ugar.db.mongo.Protocol.Types.IUgarMongoReply;

  TUgarMongoProtocol = class
  private const
    OP_QUERY = 2004;
    OP_GET_MORE = 2005;
    RECV_BUFFER_SIZE = 32768;
    EMPTY_DOCUMENT: array [0 .. 4] of Byte = (5, 0, 0, 0, 0);
  private
    FHost: String;
    FPort: Integer;
    FSettings: TUgarMongoProtocolSettings;
    FNextRequestId: Integer;
    FConnection: TgoSocketConnection;
    FConnectionLock: TCriticalSection;
    FCompletedReplies: TDictionary<Integer, IUgarMongoReply>;
    FPartialReplies: TDictionary<Integer, TDateTime>;
    FRepliesLock: TCriticalSection;
    FRecvBuffer: TBytes;
    FRecvSize: Integer;
    FRecvBufferLock: TCriticalSection;
  private
    procedure Send(const AData: TBytes);
    function WaitForReply(const ARequestId: Integer): IUgarMongoReply;
    function TryGetReply(const ARequestId: Integer; out AReply: IUgarMongoReply): Boolean; inline;
    function LastPartialReply(const ARequestId: Integer; out ALastRecv: TDateTime): Boolean;
    function OpReplyValid(out AIndex: Integer): Boolean;
    function OpReplyMsgHeader(out AMsgHeader): Boolean;
  private
    { Connection }
    function Connect: Boolean;
    function IsConnected: Boolean;
    function ConnectionState: TgoConnectionState; inline;
  private
    { Socket events }
    procedure SocketConnected;
    procedure SocketDisconnected;
    procedure SocketRecv(const ABuffer: Pointer; const ASize: Integer);
  public
    constructor Create(const AHost: String; const APort: Integer; const ASettings: TUgarMongoProtocolSettings);
    destructor Destroy; override;

    function OpQuery(
      const AFullCollectionName: UTF8String;
      const AFlags: TUgarMongoQueryFlags;
      const ANumberToSkip,
      ANumberToReturn: Integer;
      const AQuery: TBytes;
      const AReturnFieldsSelector: TBytes = nil): IUgarMongoReply;

    function OpGetMore(
      const AFullCollectionName: UTF8String;
      const ANumberToReturn: Integer;
      const ACursorId: Int64): IUgarMongoReply;
  end;

implementation

uses
  System.DateUtils,
  Grijjy.SysUtils;

var
  FClientSocketManager: TgoClientSocketManager;

function TUgarMongoProtocol.Connect: Boolean;
var
  Connection: TgoSocketConnection;

  procedure WaitForConnected;
  var
    Start: TDateTime;
  begin
    Start := Now;
    while (MillisecondsBetween(Now, Start) < FSettings.ConnectionTimeout) and
      (FConnection.State <> TgoConnectionState.Connected) do
      Sleep(5);
  end;

begin
  FConnectionLock.Acquire;
  try
    Connection := FConnection;
    FConnection := FClientSocketManager.Request(FHost, FPort);
    FConnection.OnConnected := SocketConnected;
    FConnection.OnDisconnected := SocketDisconnected;
    FConnection.OnRecv := SocketRecv;
  finally
    FConnectionLock.Release;
  end;

  if (Connection <> nil) then
    FClientSocketManager.Release(Connection);

  Result := (ConnectionState = TgoConnectionState.Connected);
  if (not Result) then
  begin
    FConnectionLock.Acquire;
    try
      if FConnection.Connect then
        WaitForConnected;
    finally
      FConnectionLock.Release;
    end;
    Result := (ConnectionState = TgoConnectionState.Connected);
  end;
end;

function TUgarMongoProtocol.ConnectionState: TgoConnectionState;
begin
  FConnectionLock.Acquire;
  try
    if (FConnection <> nil) then
      Result := FConnection.State
    else
      Result := TgoConnectionState.Disconnected;
  finally
    FConnectionLock.Release;
  end;
end;

constructor TUgarMongoProtocol.Create(const AHost: String; const APort: Integer;
  const ASettings: TUgarMongoProtocolSettings);
begin
  Assert(AHost <> '');
  Assert(APort <> 0);
  inherited Create;
  FHost := AHost;
  FPort := APort;
  FSettings := ASettings;
  FConnectionLock := TCriticalSection.Create;
  FRepliesLock := TCriticalSection.Create;
  FRecvBufferLock := TCriticalSection.Create;
  FCompletedReplies := TDictionary<Integer, IUgarMongoReply>.Create;
  FPartialReplies := TDictionary<Integer, TDateTime>.Create;
  SetLength(FRecvBuffer, RECV_BUFFER_SIZE);
end;

destructor TUgarMongoProtocol.Destroy;
var
  Connection: TgoSocketConnection;
begin
  if (FConnectionLock <> nil) then
  begin
    FConnectionLock.Acquire;
    try
      Connection := FConnection;
      FConnection := nil;
    finally
      FConnectionLock.Release;
    end;
  end
  else
  begin
    Connection := FConnection;
    FConnection := nil;
  end;

  if (Connection <> nil) and (FClientSocketManager <> nil) then
    FClientSocketManager.Release(Connection);

  if (FRepliesLock <> nil) then
  begin
    FRepliesLock.Acquire;
    try
      FCompletedReplies.Free;
      FPartialReplies.Free;
    finally
      FRepliesLock.Release;
    end;
  end;

  FRepliesLock.Free;
  FConnectionLock.Free;
  FRecvBufferLock.Free;
  inherited;
end;

function TUgarMongoProtocol.IsConnected: Boolean;
begin
  Result := (ConnectionState = TgoConnectionState.Connected);
  if (not Result) then
    Result := Connect;
end;

function TUgarMongoProtocol.LastPartialReply(const ARequestId: Integer; out ALastRecv: TDateTime): Boolean;
begin
  FRepliesLock.Acquire;
  try
    Result := FPartialReplies.TryGetValue(ARequestId, ALastRecv);
  finally
    FRepliesLock.Release;
  end;
end;

function TUgarMongoProtocol.OpGetMore(const AFullCollectionName: UTF8String; const ANumberToReturn: Integer;
  const ACursorId: Int64): IUgarMongoReply;
{ https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#op-get-more }
var
  Header: TMsgHeader;
  Data: TgoByteBuffer;
  I: Integer;
begin
  Header.MessageLength := SizeOf(TMsgHeader) + 16 + Length(AFullCollectionName) + 1;
  Header.RequestID := AtomicIncrement(FNextRequestId);
  Header.ResponseTo := 0;
  Header.OpCode := OP_GET_MORE;

  Data := TgoByteBuffer.Create(Header.MessageLength);
  try
    Data.AppendBuffer(Header, SizeOf(TMsgHeader));
    I := 0;
    Data.AppendBuffer(I, SizeOf(Int32)); // Reserved
    Data.AppendBuffer(AFullCollectionName[Low(UTF8String)], Length(AFullCollectionName) + 1);
    Data.AppendBuffer(ANumberToReturn, SizeOf(Int32));
    Data.AppendBuffer(ACursorId, SizeOf(Int64));
    Send(Data.ToBytes);
  finally
    Data.Free;
  end;
  Result := WaitForReply(Header.RequestID);
end;

function TUgarMongoProtocol.OpQuery(const AFullCollectionName: UTF8String; const AFlags: TUgarMongoQueryFlags;
  const ANumberToSkip, ANumberToReturn: Integer; const AQuery, AReturnFieldsSelector: TBytes): IUgarMongoReply;
{ https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#wire-op-query }
var
  Header: TMsgHeader;
  Data: TgoByteBuffer;
  I: Int32;
begin
  Header.MessageLength := SizeOf(TMsgHeader) + 12 + Length(AFullCollectionName) + 1 + Length(AQuery) +
    Length(AReturnFieldsSelector);
  if (AQuery = nil) then
    Inc(Header.MessageLength, Length(EMPTY_DOCUMENT));
  Header.RequestID := AtomicIncrement(FNextRequestId);
  Header.ResponseTo := 0;
  Header.OpCode := OP_QUERY;

  Data := TgoByteBuffer.Create(Header.MessageLength);
  try
    Data.AppendBuffer(Header, SizeOf(TMsgHeader));
    I := Byte(AFlags);
    Data.AppendBuffer(I, SizeOf(Int32));
    Data.AppendBuffer(AFullCollectionName[Low(UTF8String)], Length(AFullCollectionName) + 1);
    Data.AppendBuffer(ANumberToSkip, SizeOf(Int32));
    Data.AppendBuffer(ANumberToReturn, SizeOf(Int32));
    if (AQuery <> nil) then
      Data.Append(AQuery)
    else
      Data.Append(EMPTY_DOCUMENT);
    if (AReturnFieldsSelector <> nil) then
      Data.Append(AReturnFieldsSelector);

    TMonitor.Enter(Self);
    Send(Data.ToBytes);
  finally
    Data.Free;
  end;
  Result := WaitForReply(Header.RequestID);
  TMonitor.Exit(Self);
end;

function TUgarMongoProtocol.OpReplyMsgHeader(out AMsgHeader): Boolean;
begin
  Result := (FRecvSize >= SizeOf(TMsgHeader));
  if (Result) then
    Move(FRecvBuffer[0], AMsgHeader, SizeOf(TMsgHeader));
end;

function TUgarMongoProtocol.OpReplyValid(out AIndex: Integer): Boolean;
// https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#wire-op-reply
var
  Header: POpReplyHeader;
  Size: Int32;
  NumberReturned: Integer;
begin
  AIndex := 0;
  if (FRecvSize >= SizeOf(TOpReplyHeader)) then { minimum size }
  begin
    Header := @FRecvBuffer[0];
    if (Header.NumberReturned = 0) then
    begin
      AIndex := SizeOf(TOpReplyHeader);
      Result := True; { no documents, ok }
    end
    else
    begin
      { Make sure we have all the documents }
      NumberReturned := Header.NumberReturned;
      AIndex := SizeOf(TOpReplyHeader);
      repeat
        if (FRecvSize >= (AIndex + SizeOf(Int32))) then
        begin
          Move(FRecvBuffer[AIndex], Size, SizeOf(Int32));
          if (FRecvSize >= (AIndex + Size)) then
          begin
            Dec(NumberReturned);
            AIndex := AIndex + Size; { next }
          end
          else
            Break;
        end
        else
          Break;
      until (NumberReturned = 0);
      Result := (NumberReturned = 0); { all documents, ok }
    end;
  end
  else
    Result := False;
end;

procedure TUgarMongoProtocol.Send(const AData: TBytes);
begin
  if IsConnected then
  begin
    FConnectionLock.Acquire;
    try
      if (FConnection <> nil) then
        FConnection.Send(AData);
    finally
      FConnectionLock.Release;
    end;
  end;
end;

procedure TUgarMongoProtocol.SocketConnected;
begin
  { Not interested (yet) }
end;

procedure TUgarMongoProtocol.SocketDisconnected;
begin
  { Not interested (yet) }
end;

procedure TUgarMongoProtocol.SocketRecv(const ABuffer: Pointer; const ASize: Integer);
var
  MongoReply: IUgarMongoReply;
  Index: Integer;
  MsgHeader: TMsgHeader;
begin
  FRecvBufferLock.Enter;
  try
    { Expand the buffer if we are at capacity }
    if (FRecvSize + ASize >= Length(FRecvBuffer)) then
      SetLength(FRecvBuffer, (FRecvSize + ASize) * 2);

    { Append the new buffer }
    Move(ABuffer^, FRecvBuffer[FRecvSize], ASize);
    FRecvSize := FRecvSize + ASize;

    { Is there one or more valid replies pending? }
    while True do
    begin
      if OpReplyValid(Index) then
      begin
        MongoReply := TUgarMongoReply.Create(FRecvBuffer, FRecvSize);

        FRepliesLock.Acquire;
        try
          { Remove the partial reply timestamp }
          FPartialReplies.Remove(MongoReply.ResponseTo);

          { Add the completed reply to the dictionary }
          FCompletedReplies.Add(MongoReply.ResponseTo, MongoReply);
        finally
          FRepliesLock.Release;
        end;

        { Shift the receive buffer, if needed }
        if (Index = FRecvSize) then
          FRecvSize := 0
        else
          Move(FRecvBuffer[Index], FRecvBuffer[0], FRecvSize - Index);
      end
      else
      begin
        { Update the partial reply timestamp }
        if OpReplyMsgHeader(MsgHeader) then
        begin
          FRepliesLock.Acquire;
          try
            FPartialReplies.AddOrSetValue(MsgHeader.ResponseTo, Now);
          finally
            FRepliesLock.Release;
          end;
        end;
        Break;
      end;
    end;
  finally
    FRecvBufferLock.Leave;
  end;
end;

function TUgarMongoProtocol.TryGetReply(const ARequestId: Integer; out AReply: IUgarMongoReply): Boolean;
begin
  FRepliesLock.Acquire;
  try
    Result := FCompletedReplies.TryGetValue(ARequestId, AReply);
  finally
    FRepliesLock.Release;
  end;
end;

function TUgarMongoProtocol.WaitForReply(const ARequestId: Integer): IUgarMongoReply;
var
  LastRecv: TDateTime;
begin
  Result := nil;
  while (ConnectionState = TgoConnectionState.Connected) and (not TryGetReply(ARequestId, Result)) do
  begin
    if LastPartialReply(ARequestId, LastRecv) and (MillisecondsBetween(Now, LastRecv) > FSettings.ReplyTimeout) then
      Break;
    Sleep(5);
  end;

  if (Result = nil) then
    TryGetReply(ARequestId, Result);
end;

initialization
  FClientSocketManager := TgoClientSocketManager.Create(TgoSocketOptimization.Scale,
    TgoSocketPoolBehavior.PoolAndReuse, 100);

finalization

FreeAndNil(FClientSocketManager);

end.
