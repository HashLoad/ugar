unit ugar.struct;

{
     See https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/ for more info
}

interface

uses
  ugar.bson, ugar.optional;

type
  document = TUgarBsonDocument;


  MsgHeader = record
    messageLength: Int32; // total message size, including this
    requestID    : int32; // identifier for this message
    responseTo   : int32; // requestID from the original request
                          //   (used in responses from db)
    opCode       : int32; // request type
  end;

  OP_UPDATE = record
    header             : MsgHeader; // standard message header
    ZERO               : int32;     // 0 - reserved for future use
    fullCollectionName : PChar;     // "dbname.collectionname"
    flags              : int32;     // bit vector.
    selector           : document;  // the query to select the document
    update             : document;  // specification of the update to perform
  end;

  OP_INSERT = record
    header             : MsgHeader; // standard message header
    flags              : int32;     // bit vector
    fullCollectionName : PChar;     // "dbname.collectionname"
    documents          : document;  // one or more documents to insert into the collection
  end;

  OP_QUERY = record
    header                  : MsgHeader; // standard message header
    flags                   : int32;     // bit vector of query options.
    fullCollectionName      : PChar;     // "dbname.collectionname"
    numberToSkip            : Int32;     // number of documents to skip
    numberToReturn          : int32;     // number of documents to return
                                         //  in the first OP_REPLY batch
    query                   : document;  // query object.
    returnFieldsSelector    : document;  // Optional. Selector indicating the fields
                                         //  to return.
  end;

  OP_GET_MORE = record
    header                  : MsgHeader; // standard message header
    ZERO                    : Int32;     // 0 - reserved for future use
    fullCollectionName      : PChar;     // "dbname.collectionname"
    numberToReturn          : int32;     // number of documents to return
    cursorID                : int64;     // cursorID from the OP_REPLY
  end;

  OP_DELETE = record
    header                  : MsgHeader; // standard message header
    ZERO                    : Int32;     // 0 - reserved for future use
    fullCollectionName      : PChar;     // "dbname.collectionname"
    flags                   : Int32;     // bit vector.
    selector                : document;  // query object.
  end;

  OP_KILL_CURSORS = record
    header                  : MsgHeader;      // standard message header
    ZERO                    : Int32;          // 0 - reserved for future use
    numberOfCursorIDs       : int32;          // number of cursorIDs in message
    cursorIDs               : array of Int64; // sequence of cursorIDs to close
  end;

  OP_MSG = record
    header                  : MsgHeader;         // standard message header
    flagBits                : UInt32;            // message flags
//    sections                : Sections[];      // data sections
    checksum                : TOptional<UInt32>; // optional CRC-32C checksum
  end;


  OP_REPLY = record
    header                  : MsgHeader; // standard message header
    responseFlags           : Int32;     // bit vector - see details below
    cursorID                : Int64;     // cursor id if client needs to do get more's
    startingFrom            : Int32;     // where in the cursor this reply is starting
    numberReturned          : int32;     // number of documents in the reply
    documents               : document;  // documents
  end;



implementation

end.
