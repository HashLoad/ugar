unit ugar.db.mongo.Query;

interface

uses
  System.SysUtils, ugar.db.mongo.Enum;

type
  TUgarTextSearchOption = (CaseSensitive, DiacriticSensitive);
  TUgarTextSearchOptions = set of TUgarTextSearchOption;

  TUgarFilter = record
  private type
    IFilter = interface
      ['{BAE9502F-7FC3-4AB4-B35F-AEA09F8BC0DB}']
      function Render: TUgarBsonDocument;
      function ToBson: TBytes;
      function ToJson(const ASettings: TUgarJsonWriterSettings): String;
    end;
  private
    class var FEmpty: TUgarFilter;
  private
    FImpl: IFilter;
  public
    class constructor Create;
  public
    class operator Implicit(const AJson: String): TUgarFilter; static;
    class operator Implicit(const ADocument: TUgarBsonDocument): TUgarFilter; static;
    function IsNil: Boolean; inline;
    procedure SetNil; inline;
    function Render: TUgarBsonDocument; inline;
    function ToBson: TBytes; inline;
    function ToJson: String; overload; inline;
    function ToJson(const ASettings: TUgarJsonWriterSettings): String; overload; inline;
    class property Empty: TUgarFilter read FEmpty;
  public
    class function Eq(const AFieldName: String; const AValue: TUgarBsonValue): TUgarFilter; static;
    class function Ne(const AFieldName: String; const AValue: TUgarBsonValue): TUgarFilter; static;
    class function Lt(const AFieldName: String; const AValue: TUgarBsonValue): TUgarFilter; static;
    class function Lte(const AFieldName: String; const AValue: TUgarBsonValue): TUgarFilter; static;
    class function Gt(const AFieldName: String; const AValue: TUgarBsonValue): TUgarFilter; static;
    class function Gte(const AFieldName: String; const AValue: TUgarBsonValue): TUgarFilter; static;
  public
    class operator LogicalAnd(const ALeft, ARight: TUgarFilter): TUgarFilter; static;
    class operator LogicalOr(const ALeft, ARight: TUgarFilter): TUgarFilter; static;
    class operator LogicalNot(const AOperand: TUgarFilter): TUgarFilter; static;
    class function &And(const AFilter1, AFilter2: TUgarFilter): TUgarFilter; overload; static;
    class function &And(const AFilters: array of TUgarFilter): TUgarFilter; overload; static;
    class function &Or(const AFilter1, AFilter2: TUgarFilter): TUgarFilter; overload; static;
    class function &Or(const AFilters: array of TUgarFilter): TUgarFilter; overload; static;
    class function &Not(const AOperand: TUgarFilter): TUgarFilter; static;
  public
    class function Exists(const AFieldName: String; const AExists: Boolean = True): TUgarFilter; static;
    class function &Type(const AFieldName: String; const AType: TUgarBsonType): TUgarFilter; overload; static;
    class function &Type(const AFieldName, AType: String): TUgarFilter; overload; static;
  public
    class function &Mod(const AFieldName: String; const ADivisor, ARemainder: Int64): TUgarFilter; static;
    class function Regex(const AFieldName: String; const ARegex: TUgarBsonRegularExpression): TUgarFilter; static;
    class function Text(const AText: String; const AOptions: TUgarTextSearchOptions = []; const ALanguage: String = ''): TUgarFilter; static;
  public
    class function AnyEq(const AFieldName: String; const AValue: TUgarBsonValue): TUgarFilter; static;
    class function AnyNe(const AFieldName: String; const AValue: TUgarBsonValue): TUgarFilter; static;
    class function AnyLt(const AFieldName: String; const AValue: TUgarBsonValue): TUgarFilter; static;
    class function AnyLte(const AFieldName: String; const AValue: TUgarBsonValue): TUgarFilter; static;
    class function AnyGt(const AFieldName: String; const AValue: TUgarBsonValue): TUgarFilter; static;
    class function AnyGte(const AFieldName: String; const AValue: TUgarBsonValue): TUgarFilter; static;
    class function All(const AFieldName: String; const AValues: TArray<TUgarBsonValue>): TUgarFilter; overload; static;
    class function All(const AFieldName: String; const AValues: array of TUgarBsonValue): TUgarFilter; overload; static;
    class function All(const AFieldName: String; const AValues: TUgarBsonArray): TUgarFilter; overload; static;
    class function &In(const AFieldName: String; const AValues: TArray<TUgarBsonValue>): TUgarFilter; overload; static;
    class function &In(const AFieldName: String; const AValues: array of TUgarBsonValue): TUgarFilter; overload; static;
    class function &In(const AFieldName: String; const AValues: TUgarBsonArray): TUgarFilter; overload; static;
    class function Nin(const AFieldName: String; const AValues: TArray<TUgarBsonValue>): TUgarFilter; overload; static;
    class function Nin(const AFieldName: String; const AValues: array of TUgarBsonValue): TUgarFilter; overload; static;
    class function Nin(const AFieldName: String; const AValues: TUgarBsonArray): TUgarFilter; overload; static;
    class function ElemMatch(const AFieldName: String; const AFilter: TUgarFilter): TUgarFilter; overload; static;
    class function Size(const AFieldName: String; const ASize: Integer): TUgarFilter; overload; static;
    class function SizeGt(const AFieldName: String; const ASize: Integer): TUgarFilter; overload; static;
    class function SizeGte(const AFieldName: String; const ASize: Integer): TUgarFilter; overload; static;
    class function SizeLt(const AFieldName: String; const ASize: Integer): TUgarFilter; overload; static;
    class function SizeLte(const AFieldName: String; const ASize: Integer): TUgarFilter; overload; static;
  public
    class function BitsAllClear(const AFieldName: String; const ABitMask: UInt64): TUgarFilter; static;
    class function BitsAllSet(const AFieldName: String; const ABitMask: UInt64): TUgarFilter; static;
    class function BitsAnyClear(const AFieldName: String; const ABitMask: UInt64): TUgarFilter; static;
    class function BitsAnySet(const AFieldName: String; const ABitMask: UInt64): TUgarFilter; static;
  end;

  TUgarProjection = record
  private type
    IProjection = interface
      ['{060E413F-6B4E-4FFE-83EF-5A124BC914DB}']
      function Render: TUgarBsonDocument;
      function ToBson: TBytes;
      function ToJson(const ASettings: TUgarJsonWriterSettings): String;
    end;
  private
    FImpl: IProjection;
    class function GetEmpty: TUgarProjection; static; inline;
  public
    class operator Implicit(const AJson: String): TUgarProjection; static;
    class operator Implicit(const ADocument: TUgarBsonDocument): TUgarProjection; static;
    class operator Add(const ALeft, ARight: TUgarProjection): TUgarProjection; static;
    function IsNil: Boolean; inline;
    procedure SetNil; inline;
    function Render: TUgarBsonDocument; inline;
    function ToBson: TBytes; inline;
    function ToJson: String; overload; inline;
    function ToJson(const ASettings: TUgarJsonWriterSettings): String; overload; inline;
    class property Empty: TUgarProjection read GetEmpty;
  public
    class function Combine(const AProjection1, AProjection2: TUgarProjection): TUgarProjection;
      overload; static;
    class function Combine(const AProjections: array of TUgarProjection): TUgarProjection; overload; static;
    class function Include(const AFieldName: String): TUgarProjection; overload; static;
    class function Include(const AFieldNames: array of String): TUgarProjection; overload; static;
    class function Exclude(const AFieldName: String): TUgarProjection; overload; static;
    class function Exclude(const AFieldNames: array of String): TUgarProjection; overload; static;
    class function ElemMatch(const AFieldName: String; const AFilter: TUgarFilter): TUgarProjection; static;
    class function MetaTextScore(const AFieldName: String): TUgarProjection; static;
    class function Slice(const AFieldName: String; const ALimit: Integer): TUgarProjection; overload; static;
    class function Slice(const AFieldName: String; const ASkip, ALimit: Integer): TUgarProjection;
      overload; static;
  end;

  TUgarSortDirection = (Ascending, Descending);

  TUgarSort = record
  private type
    ISort = interface
      ['{FB526276-76F3-4F67-90C9-F09010FE8F37}']
      function Render: TUgarBsonDocument;
      function ToBson: TBytes;
      function ToJson(const ASettings: TUgarJsonWriterSettings): String;
    end;
  private
    FImpl: ISort;
  public
    class operator Implicit(const AJson: String): TUgarSort; static;
    class operator Implicit(const ADocument: TUgarBsonDocument): TUgarSort; static;
    class operator Add(const ALeft, ARight: TUgarSort): TUgarSort; static;
    function IsNil: Boolean; inline;
    procedure SetNil; inline;
    function Render: TUgarBsonDocument; inline;
    function ToBson: TBytes; inline;
    function ToJson: String; overload; inline;
    function ToJson(const ASettings: TUgarJsonWriterSettings): String; overload; inline;
  public
    class function Combine(const ASort1, ASort2: TUgarSort): TUgarSort; overload; static;
    class function Combine(const ASorts: array of TUgarSort): TUgarSort; overload; static;
    class function Ascending(const AFieldName: String): TUgarSort; static;
    class function Descending(const AFieldName: String): TUgarSort; static;
    class function MetaTextScore(const AFieldName: String): TUgarSort; static;
  end;

  TUgarCurrentDateType = (Default, Date, Timestamp);

  TUgarUpdate = record
  public const
    NO_SLICE = Integer.MaxValue;
    NO_POSITION = Integer.MaxValue;
  private type
    IUpdate = interface
      ['{9FC6C8B5-B4BA-445F-A960-67FBDF8613F4}']
      function Render: TUgarBsonDocument;
      function ToBson: TBytes;
      function ToJson(const ASettings: TUgarJsonWriterSettings): String;
      function IsCombine: Boolean;
    end;
  private
    FImpl: IUpdate;
  private
    function SetOrCombine(const AUpdate: IUpdate): IUpdate;
  public
    class function Init: TUgarUpdate; inline; static;
    class operator Implicit(const AJson: String): TUgarUpdate; static;
    class operator Implicit(const ADocument: TUgarBsonDocument): TUgarUpdate; static;
    function IsNil: Boolean; inline;
    procedure SetNil; inline;
    function Render: TUgarBsonDocument; inline;
    function ToBson: TBytes; inline;
    function ToJson: String; overload; inline;
    function ToJson(const ASettings: TUgarJsonWriterSettings): String; overload; inline;
  public
    function &Set(const AFieldName: String; const AValue: TUgarBsonValue): TUgarUpdate;
    function SetOnInsert(const AFieldName: String; const AValue: TUgarBsonValue): TUgarUpdate;
    function Unset(const AFieldName: String): TUgarUpdate;
    function Inc(const AFieldName: String; const AAmount: TUgarBsonValue): TUgarUpdate; overload;
    function Inc(const AFieldName: String): TUgarUpdate; overload;
    function Mul(const AFieldName: String; const AAmount: TUgarBsonValue): TUgarUpdate;
    function Max(const AFieldName: String; const AValue: TUgarBsonValue): TUgarUpdate;
    function Min(const AFieldName: String; const AValue: TUgarBsonValue): TUgarUpdate;
    function CurrentDate(const AFieldName: String; const AType: TUgarCurrentDateType = TUgarCurrentDateType.
      Default): TUgarUpdate;
    function Rename(const AFieldName, ANewName: String): TUgarUpdate;
  public
    function AddToSet(const AFieldName: String; const AValue: TUgarBsonValue): TUgarUpdate;
    function AddToSetEach(const AFieldName: String; const AValues: array of TUgarBsonValue): TUgarUpdate;
    function PopFirst(const AFieldName: String): TUgarUpdate;
    function PopLast(const AFieldName: String): TUgarUpdate;
    function Pull(const AFieldName: String; const AValue: TUgarBsonValue): TUgarUpdate;
    function PullFilter(const AFieldName: String; const AFilter: TUgarFilter): TUgarUpdate;
    function PullAll(const AFieldName: String; const AValues: array of TUgarBsonValue): TUgarUpdate;
    function Push(const AFieldName: String; const AValue: TUgarBsonValue): TUgarUpdate;
    function PushEach(const AFieldName: String; const AValues: array of TUgarBsonValue;
      const ASlice: Integer = NO_SLICE; const APosition: Integer = NO_POSITION): TUgarUpdate; overload;
    function PushEach(const AFieldName: String; const AValues: array of TUgarBsonValue; const ASort: TUgarSort;
      const ASlice: Integer = NO_SLICE; const APosition: Integer = NO_POSITION): TUgarUpdate; overload;
  public
    function BitwiseAnd(const AFieldName: String; const AValue: TUgarBsonValue): TUgarUpdate;
    function BitwiseOr(const AFieldName: String; const AValue: TUgarBsonValue): TUgarUpdate;
    function BitwiseXor(const AFieldName: String; const AValue: TUgarBsonValue): TUgarUpdate;
  end;

implementation

uses
  Grijjy.Bson, Grijjy.Bson.IO;

type
  TBuilder = class abstract(TInterfacedObject)
  protected
    class function SupportsWriter: Boolean; virtual;
    procedure Write(const AWriter: IUgarBsonBaseWriter); virtual;
    function Build: TUgarBsonDocument; virtual;
  protected
    function Render: TUgarBsonDocument;
    function ToBson: TBytes;
    function ToJson(const ASettings: TUgarJsonWriterSettings): String;
  end;

type
  TFilter = class abstract(TBuilder, TUgarFilter.IFilter)
  end;

type
  TFilterEmpty = class(TFilter)
  protected
    function Build: TUgarBsonDocument; override;
  end;

type
  TFilterJson = class(TFilter)
  private
    FJson: String;
  protected
    function Build: TUgarBsonDocument; override;
  public
    constructor Create(const AJson: String);
  end;

type
  TFilterBsonDocument = class(TFilter)
  private
    FDocument: TUgarBsonDocument;
  protected
    function Build: TUgarBsonDocument; override;
  public
    constructor Create(const ADocument: TUgarBsonDocument);
  end;

type
  TFilterSimple = class(TFilter)
  private
    FFieldName: String;
    FValue: TUgarBsonValue;
  protected
    class function SupportsWriter: Boolean; override;
    procedure Write(const AWriter: IUgarBsonBaseWriter); override;
  public
    constructor Create(const AFieldName: String; const AValue: TUgarBsonValue);
  end;

type
  TFilterOperator = class(TFilter)
  private
    FFieldName: String;
    FOperator: String;
    FValue: TUgarBsonValue;
  protected
    class function SupportsWriter: Boolean; override;
    procedure Write(const AWriter: IUgarBsonBaseWriter); override;
  public
    constructor Create(const AFieldName, AOperator: String; const AValue: TUgarBsonValue);
  end;

type
  TFilterArrayOperator = class(TFilter)
  private
    FFieldName: String;
    FOperator: String;
    FValues: TUgarBsonArray;
  protected
    class function SupportsWriter: Boolean; override;
    procedure Write(const AWriter: IUgarBsonBaseWriter); override;
  public
    constructor Create(const AFieldName, AOperator: String; const AValues: TUgarBsonArray);
  end;

type
  TFilterArrayIndexExists = class(TFilter)
  private
    FFieldName: String;
    FIndex: Integer;
    FExists: Boolean;
  protected
    class function SupportsWriter: Boolean; override;
    procedure Write(const AWriter: IUgarBsonBaseWriter); override;
  public
    constructor Create(const AFieldName: String; const AIndex: Integer; const AExists: Boolean);
  end;

type
  TFilterAnd = class(TFilter)
  private
    FFilters: TArray<TUgarFilter.IFilter>;
  private
    class procedure AddClause(const ADocument: TUgarBsonDocument; const AClause: TUgarBsonElement); static;
    class procedure PromoteFilterToDollarForm(const ADocument: TUgarBsonDocument;
      const AClause: TUgarBsonElement); static;
  protected
    function Build: TUgarBsonDocument; override;
  public
    constructor Create(const AFilter1, AFilter2: TUgarFilter); overload;
    constructor Create(const AFilters: array of TUgarFilter); overload;
  end;

type
  TFilterOr = class(TFilter)
  private
    FFilters: TArray<TUgarFilter.IFilter>;
  private
    class procedure AddClause(const AClauses: TUgarBsonArray; const AFilter: TUgarBsonDocument); static;
  protected
    function Build: TUgarBsonDocument; override;
  public
    constructor Create(const AFilter1, AFilter2: TUgarFilter); overload;
    constructor Create(const AFilters: array of TUgarFilter); overload;
  end;

type
  TFilterNot = class(TFilter)
  private
    FFilter: TUgarFilter.IFilter;
  private
    class function NegateArbitraryFilter(const AFilter: TUgarBsonDocument): TUgarBsonDocument; static;
    class function NegateSingleElementFilter(const AFilter: TUgarBsonDocument; const AElement: TUgarBsonElement)
      : TUgarBsonDocument; static;
    class function NegateSingleElementTopLevelOperatorFilter(const AFilter: TUgarBsonDocument;
      const AElement: TUgarBsonElement): TUgarBsonDocument; static;
    class function NegateSingleFieldOperatorFilter(const AFieldName: String; const AElement: TUgarBsonElement)
      : TUgarBsonDocument; static;
  protected
    function Build: TUgarBsonDocument; override;
  public
    constructor Create(const AOperand: TUgarFilter);
  end;

type
  TFilterElementMatch = class(TFilter)
  private
    FFieldName: String;
    FFilter: TUgarFilter.IFilter;
  protected
    function Build: TUgarBsonDocument; override;
  public
    constructor Create(const AFieldName: String; const AFilter: TUgarFilter);
  end;

type
  TProjection = class abstract(TBuilder, TUgarProjection.IProjection)
  end;

type
  TProjectionJson = class(TProjection)
  private
    FJson: String;
  protected
    function Build: TUgarBsonDocument; override;
  public
    constructor Create(const AJson: String);
  end;

type
  TProjectionBsonDocument = class(TProjection)
  private
    FDocument: TUgarBsonDocument;
  protected
    function Build: TUgarBsonDocument; override;
  public
    constructor Create(const ADocument: TUgarBsonDocument);
  end;

type
  TProjectionSingleField = class(TProjection)
  private
    FFieldName: String;
    FValue: TUgarBsonValue;
  protected
    class function SupportsWriter: Boolean; override;
    procedure Write(const AWriter: IUgarBsonBaseWriter); override;
  public
    constructor Create(const AFieldName: String; const AValue: TUgarBsonValue);
  end;

type
  TProjectionMultipleFields = class(TProjection)
  private
    FFieldNames: TArray<String>;
    FValue: Integer;
  protected
    class function SupportsWriter: Boolean; override;
    procedure Write(const AWriter: IUgarBsonBaseWriter); override;
  public
    constructor Create(const AFieldNames: array of String; const AValue: Integer);
  end;

type
  TProjectionCombine = class(TProjection)
  private
    FProjections: TArray<TUgarProjection.IProjection>;
  protected
    function Build: TUgarBsonDocument; override;
  public
    constructor Create(const AProjection1, AProjection2: TUgarProjection); overload;
    constructor Create(const AProjections: array of TUgarProjection); overload;
  end;

type
  TProjectionElementMatch = class(TProjection)
  private
    FFieldName: String;
    FFilter: TUgarFilter.IFilter;
  protected
    function Build: TUgarBsonDocument; override;
  public
    constructor Create(const AFieldName: String; const AFilter: TUgarFilter);
  end;

type
  TSort = class abstract(TBuilder, TUgarSort.ISort)
  end;

type
  TSortJson = class(TSort)
  private
    FJson: String;
  protected
    function Build: TUgarBsonDocument; override;
  public
    constructor Create(const AJson: String);
  end;

type
  TSortBsonDocument = class(TSort)
  private
    FDocument: TUgarBsonDocument;
  protected
    function Build: TUgarBsonDocument; override;
  public
    constructor Create(const ADocument: TUgarBsonDocument);
  end;

type
  TSortCombine = class(TSort)
  private
    FSorts: TArray<TUgarSort.ISort>;
  protected
    function Build: TUgarBsonDocument; override;
  public
    constructor Create(const ASort1, ASort2: TUgarSort); overload;
    constructor Create(const ASorts: array of TUgarSort); overload;
  end;

type
  TSortDirectional = class(TSort)
  private
    FFieldName: String;
    FDirection: TUgarSortDirection;
  protected
    class function SupportsWriter: Boolean; override;
    procedure Write(const AWriter: IUgarBsonBaseWriter); override;
  public
    constructor Create(const AFieldName: String; const ADirection: TUgarSortDirection);
  end;

type
  TUpdate = class abstract(TBuilder, TUgarUpdate.IUpdate)
  protected
    { TUgarUpdate.IUpdate }
    function IsCombine: Boolean; virtual;
  end;

type
  TUpdateJson = class(TUpdate)
  private
    FJson: String;
  protected
    function Build: TUgarBsonDocument; override;
  public
    constructor Create(const AJson: String);
  end;

type
  TUpdateBsonDocument = class(TUpdate)
  private
    FDocument: TUgarBsonDocument;
  protected
    function Build: TUgarBsonDocument; override;
  public
    constructor Create(const ADocument: TUgarBsonDocument);
  end;

type
  TUpdateOperator = class(TUpdate)
  private
    FOperator: String;
    FFieldName: String;
    FValue: TUgarBsonValue;
  protected
    class function SupportsWriter: Boolean; override;
    procedure Write(const AWriter: IUgarBsonBaseWriter); override;
  public
    constructor Create(const AOperator, AFieldName: String; const AValue: TUgarBsonValue);
  end;

type
  TUpdateBitwiseOperator = class(TUpdate)
  private
    FOperator: String;
    FFieldName: String;
    FValue: TUgarBsonValue;
  protected
    class function SupportsWriter: Boolean; override;
    procedure Write(const AWriter: IUgarBsonBaseWriter); override;
  public
    constructor Create(const AOperator, AFieldName: String; const AValue: TUgarBsonValue);
  end;

type
  TUpdateAddToSet = class(TUpdate)
  private
    FFieldName: String;
    FValues: TArray<TUgarBsonValue>;
  protected
    class function SupportsWriter: Boolean; override;
    procedure Write(const AWriter: IUgarBsonBaseWriter); override;
  public
    constructor Create(const AFieldName: String; const AValue: TUgarBsonValue); overload;
    constructor Create(const AFieldName: String; const AValues: array of TUgarBsonValue); overload;
  end;

type
  TUpdatePull = class(TUpdate)
  private
    FFieldName: String;
    FFilter: TUgarFilter;
    FValues: TArray<TUgarBsonValue>;
  protected
    class function SupportsWriter: Boolean; override;
    procedure Write(const AWriter: IUgarBsonBaseWriter); override;
  public
    constructor Create(const AFieldName: String; const AValue: TUgarBsonValue); overload;
    constructor Create(const AFieldName: String; const AValues: array of TUgarBsonValue); overload;
    constructor Create(const AFieldName: String; const AFilter: TUgarFilter); overload;
  end;

type
  TUpdatePush = class(TUpdate)
  private
    FFieldName: String;
    FValues: TArray<TUgarBsonValue>;
    FSlice: Integer;
    FPosition: Integer;
    FSort: TUgarSort;
  protected
    class function SupportsWriter: Boolean; override;
    procedure Write(const AWriter: IUgarBsonBaseWriter); override;
  public
    constructor Create(const AFieldName: String; const AValue: TUgarBsonValue); overload;
    constructor Create(const AFieldName: String; const AValues: array of TUgarBsonValue;
      const ASlice, APosition: Integer; const ASort: TUgarSort); overload;
  end;

type
  TUpdateCombine = class(TUpdate)
  private
    FUpdates: TArray<TUgarUpdate.IUpdate>;
    FCount: Integer;
  protected
    { TUgarUpdate.IUpdate }
    function IsCombine: Boolean; override;
  protected
    function Build: TUgarBsonDocument; override;
  public
    constructor Create(const AUpdate1, AUpdate2: TUgarUpdate.IUpdate); overload;
    constructor Create(const AUpdate1, AUpdate2: TUgarUpdate); overload;
    constructor Create(const AUpdates: array of TUgarUpdate); overload;
    procedure Add(const AUpdate: TUgarUpdate.IUpdate);
  end;

  { TUgarFilter }

class function TUgarFilter.All(const AFieldName: String; const AValues: TArray<TUgarBsonValue>): TUgarFilter;
begin
  Result.FImpl := TFilterArrayOperator.Create(AFieldName, '$all', TUgarBsonArray.Create(AValues));
end;

class function TUgarFilter.All(const AFieldName: String; const AValues: array of TUgarBsonValue): TUgarFilter;
begin
  Result.FImpl := TFilterArrayOperator.Create(AFieldName, '$all', TUgarBsonArray.Create(AValues));
end;

class function TUgarFilter.&Mod(const AFieldName: String; const ADivisor, ARemainder: Int64): TUgarFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$mod', TUgarBsonArray.Create([ADivisor, ARemainder]));
end;

class function TUgarFilter.Ne(const AFieldName: String; const AValue: TUgarBsonValue): TUgarFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$ne', AValue);
end;

class function TUgarFilter.Nin(const AFieldName: String; const AValues: TArray<TUgarBsonValue>): TUgarFilter;
begin
  Result.FImpl := TFilterArrayOperator.Create(AFieldName, '$nin', TUgarBsonArray.Create(AValues));
end;

class function TUgarFilter.Nin(const AFieldName: String; const AValues: array of TUgarBsonValue): TUgarFilter;
begin
  Result.FImpl := TFilterArrayOperator.Create(AFieldName, '$nin', TUgarBsonArray.Create(AValues));
end;

class function TUgarFilter.Nin(const AFieldName: String; const AValues: TUgarBsonArray): TUgarFilter;
begin
  Result.FImpl := TFilterArrayOperator.Create(AFieldName, '$nin', AValues);
end;

class function TUgarFilter.&Not(const AOperand: TUgarFilter): TUgarFilter;
begin
  Result.FImpl := TFilterNot.Create(AOperand);
end;

class function TUgarFilter.&Or(const AFilter1, AFilter2: TUgarFilter): TUgarFilter;
begin
  Result.FImpl := TFilterOr.Create(AFilter1, AFilter2);
end;

class function TUgarFilter.&Or(const AFilters: array of TUgarFilter): TUgarFilter;
begin
  Result.FImpl := TFilterOr.Create(AFilters);
end;

class function TUgarFilter.&Type(const AFieldName: String; const AType: TUgarBsonType): TUgarFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$type', Ord(AType));
end;

class function TUgarFilter.&Type(const AFieldName, AType: String): TUgarFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$type', AType);
end;

class function TUgarFilter.All(const AFieldName: String; const AValues: TUgarBsonArray): TUgarFilter;
begin
  Result.FImpl := TFilterArrayOperator.Create(AFieldName, '$all', AValues);
end;

class function TUgarFilter.&And(const AFilter1, AFilter2: TUgarFilter): TUgarFilter;
begin
  Result.FImpl := TFilterAnd.Create(AFilter1, AFilter2);
end;

class function TUgarFilter.&And(const AFilters: array of TUgarFilter): TUgarFilter;
begin
  Result.FImpl := TFilterAnd.Create(AFilters);
end;

class function TUgarFilter.AnyEq(const AFieldName: String; const AValue: TUgarBsonValue): TUgarFilter;
begin
  Result.FImpl := TFilterSimple.Create(AFieldName, AValue);
end;

class function TUgarFilter.AnyGt(const AFieldName: String; const AValue: TUgarBsonValue): TUgarFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$gt', AValue);
end;

class function TUgarFilter.AnyGte(const AFieldName: String; const AValue: TUgarBsonValue): TUgarFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$gte', AValue);
end;

class function TUgarFilter.AnyLt(const AFieldName: String; const AValue: TUgarBsonValue): TUgarFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$lt', AValue);
end;

class function TUgarFilter.AnyLte(const AFieldName: String; const AValue: TUgarBsonValue): TUgarFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$lte', AValue);
end;

class function TUgarFilter.AnyNe(const AFieldName: String; const AValue: TUgarBsonValue): TUgarFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$ne', AValue);
end;

class function TUgarFilter.BitsAllClear(const AFieldName: String; const ABitMask: UInt64): TUgarFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$bitsAllClear', ABitMask);
end;

class function TUgarFilter.BitsAllSet(const AFieldName: String; const ABitMask: UInt64): TUgarFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$bitsAllSet', ABitMask);
end;

class function TUgarFilter.BitsAnyClear(const AFieldName: String; const ABitMask: UInt64): TUgarFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$bitsAnyClear', ABitMask);
end;

class function TUgarFilter.BitsAnySet(const AFieldName: String; const ABitMask: UInt64): TUgarFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$bitsAnySet', ABitMask);
end;

class constructor TUgarFilter.Create;
begin
  FEmpty.FImpl := TFilterEmpty.Create;
end;

class function TUgarFilter.ElemMatch(const AFieldName: String; const AFilter: TUgarFilter): TUgarFilter;
begin
  Result.FImpl := TFilterElementMatch.Create(AFieldName, AFilter);
end;

class function TUgarFilter.Eq(const AFieldName: String; const AValue: TUgarBsonValue): TUgarFilter;
begin
  Result.FImpl := TFilterSimple.Create(AFieldName, AValue);
end;

class function TUgarFilter.Exists(const AFieldName: String; const AExists: Boolean): TUgarFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$exists', AExists);
end;

class function TUgarFilter.Gt(const AFieldName: String; const AValue: TUgarBsonValue): TUgarFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$gt', AValue);
end;

class function TUgarFilter.Gte(const AFieldName: String; const AValue: TUgarBsonValue): TUgarFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$gte', AValue);
end;

class operator TUgarFilter.Implicit(const AJson: String): TUgarFilter;
begin
  Result.FImpl := TFilterJson.Create(AJson);
end;

class operator TUgarFilter.Implicit(const ADocument: TUgarBsonDocument): TUgarFilter;
begin
  if (ADocument.IsNil) then
    Result.FImpl := nil
  else
    Result.FImpl := TFilterBsonDocument.Create(ADocument);
end;

class function TUgarFilter.&In(const AFieldName: String; const AValues: TArray<TUgarBsonValue>): TUgarFilter;
begin
  Result.FImpl := TFilterArrayOperator.Create(AFieldName, '$in', TUgarBsonArray.Create(AValues));
end;

class function TUgarFilter.&In(const AFieldName: String; const AValues: array of TUgarBsonValue): TUgarFilter;
begin
  Result.FImpl := TFilterArrayOperator.Create(AFieldName, '$in', TUgarBsonArray.Create(AValues));
end;

class function TUgarFilter.&In(const AFieldName: String; const AValues: TUgarBsonArray): TUgarFilter;
begin
  Result.FImpl := TFilterArrayOperator.Create(AFieldName, '$in', AValues);
end;

function TUgarFilter.IsNil: Boolean;
begin
  Result := (FImpl = nil);
end;

class operator TUgarFilter.LogicalAnd(const ALeft, ARight: TUgarFilter): TUgarFilter;
begin
  Result.FImpl := TFilterAnd.Create(ALeft, ARight);
end;

class operator TUgarFilter.LogicalNot(const AOperand: TUgarFilter): TUgarFilter;
begin
  Result.FImpl := TFilterNot.Create(AOperand);
end;

class operator TUgarFilter.LogicalOr(const ALeft, ARight: TUgarFilter): TUgarFilter;
begin
  Result.FImpl := TFilterOr.Create(ALeft, ARight);
end;

class function TUgarFilter.Lt(const AFieldName: String; const AValue: TUgarBsonValue): TUgarFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$lt', AValue);
end;

class function TUgarFilter.Lte(const AFieldName: String; const AValue: TUgarBsonValue): TUgarFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$lte', AValue);
end;

class function TUgarFilter.Regex(const AFieldName: String; const ARegex: TUgarBsonRegularExpression)
  : TUgarFilter;
begin
  Result.FImpl := TFilterSimple.Create(AFieldName, ARegex);
end;

function TUgarFilter.Render: TUgarBsonDocument;
begin
  Assert(Assigned(FImpl));
  Result := FImpl.Render;
end;

procedure TUgarFilter.SetNil;
begin
  FImpl := nil;
end;

class function TUgarFilter.Size(const AFieldName: String; const ASize: Integer): TUgarFilter;
begin
  Result.FImpl := TFilterOperator.Create(AFieldName, '$size', ASize);
end;

class function TUgarFilter.SizeGt(const AFieldName: String; const ASize: Integer): TUgarFilter;
begin
  Result.FImpl := TFilterArrayIndexExists.Create(AFieldName, ASize, True);
end;

class function TUgarFilter.SizeGte(const AFieldName: String; const ASize: Integer): TUgarFilter;
begin
  Result.FImpl := TFilterArrayIndexExists.Create(AFieldName, ASize - 1, True);
end;

class function TUgarFilter.SizeLt(const AFieldName: String; const ASize: Integer): TUgarFilter;
begin
  Result.FImpl := TFilterArrayIndexExists.Create(AFieldName, ASize - 1, False);
end;

class function TUgarFilter.SizeLte(const AFieldName: String; const ASize: Integer): TUgarFilter;
begin
  Result.FImpl := TFilterArrayIndexExists.Create(AFieldName, ASize, False);
end;

class function TUgarFilter.Text(const AText: String; const AOptions: TUgarTextSearchOptions;
  const ALanguage: String): TUgarFilter;
var
  Settings: TUgarBsonDocument;
begin
  Settings := TUgarBsonDocument.Create;
  Settings.Add('$search', AText);
  if (ALanguage <> '') then
    Settings.Add('$language', ALanguage);
  if (TUgarTextSearchOption.CaseSensitive in AOptions) then
    Settings.Add('$caseSensitive', True);
  if (TUgarTextSearchOption.DiacriticSensitive in AOptions) then
    Settings.Add('$diacriticSensitive', True);

  Result.FImpl := TFilterBsonDocument.Create(TUgarBsonDocument.Create('$text', Settings));
end;

function TUgarFilter.ToJson: String;
begin
  Assert(Assigned(FImpl));
  Result := FImpl.ToJson(TUgarJsonWriterSettings.Default);
end;

function TUgarFilter.ToBson: TBytes;
begin
  Assert(Assigned(FImpl));
  Result := FImpl.ToBson;
end;

function TUgarFilter.ToJson(const ASettings: TUgarJsonWriterSettings): String;
begin
  Assert(Assigned(FImpl));
  Result := FImpl.ToJson(ASettings);
end;

{ TUgarProjection }

class operator TUgarProjection.Implicit(const AJson: String): TUgarProjection;
begin
  Result.FImpl := TProjectionJson.Create(AJson);
end;

class function TUgarProjection.Combine(const AProjection1, AProjection2: TUgarProjection)
  : TUgarProjection;
begin
  Result.FImpl := TProjectionCombine.Create(AProjection1, AProjection2);
end;

class function TUgarProjection.Combine(const AProjections: array of TUgarProjection): TUgarProjection;
begin
  Result.FImpl := TProjectionCombine.Create(AProjections);
end;

class function TUgarProjection.ElemMatch(const AFieldName: String; const AFilter: TUgarFilter)
  : TUgarProjection;
begin
  Result.FImpl := TProjectionElementMatch.Create(AFieldName, AFilter);
end;

class function TUgarProjection.Exclude(const AFieldNames: array of String): TUgarProjection;
begin
  Result.FImpl := TProjectionMultipleFields.Create(AFieldNames, 0);
end;

class function TUgarProjection.Exclude(const AFieldName: String): TUgarProjection;
begin
  Result.FImpl := TProjectionSingleField.Create(AFieldName, 0);
end;

class function TUgarProjection.GetEmpty: TUgarProjection;
begin
  Result.FImpl := nil;
end;

class operator TUgarProjection.Implicit(const ADocument: TUgarBsonDocument): TUgarProjection;
begin
  if (ADocument.IsNil) then
    Result.FImpl := nil
  else
    Result.FImpl := TProjectionBsonDocument.Create(ADocument);
end;

class operator TUgarProjection.Add(const ALeft, ARight: TUgarProjection): TUgarProjection;
begin
  Result.FImpl := TProjectionCombine.Create(ALeft, ARight);
end;

class function TUgarProjection.Include(const AFieldName: String): TUgarProjection;
begin
  Result.FImpl := TProjectionSingleField.Create(AFieldName, 1);
end;

class function TUgarProjection.Include(const AFieldNames: array of String): TUgarProjection;
begin
  Result.FImpl := TProjectionMultipleFields.Create(AFieldNames, 1);
end;

function TUgarProjection.IsNil: Boolean;
begin
  Result := (FImpl = nil);
end;

class function TUgarProjection.MetaTextScore(const AFieldName: String): TUgarProjection;
begin
  Result.FImpl := TProjectionSingleField.Create(AFieldName, TUgarBsonDocument.Create('$meta', 'textScore'));
end;

function TUgarProjection.Render: TUgarBsonDocument;
begin
  Assert(Assigned(FImpl));
  Result := FImpl.Render;
end;

class function TUgarProjection.Slice(const AFieldName: String; const ALimit: Integer): TUgarProjection;
begin
  Result.FImpl := TProjectionSingleField.Create(AFieldName, TUgarBsonDocument.Create('$slice', ALimit));
end;

procedure TUgarProjection.SetNil;
begin
  FImpl := nil;
end;

class function TUgarProjection.Slice(const AFieldName: String; const ASkip, ALimit: Integer): TUgarProjection;
begin
  Result.FImpl := TProjectionSingleField.Create(AFieldName, TUgarBsonDocument.Create('$slice',
    TUgarBsonArray.Create([ASkip, ALimit])));
end;

function TUgarProjection.ToBson: TBytes;
begin
  Assert(Assigned(FImpl));
  Result := FImpl.ToBson;
end;

function TUgarProjection.ToJson: String;
begin
  Assert(Assigned(FImpl));
  Result := FImpl.ToJson(TUgarJsonWriterSettings.Default);
end;

function TUgarProjection.ToJson(const ASettings: TUgarJsonWriterSettings): String;
begin
  Assert(Assigned(FImpl));
  Result := FImpl.ToJson(ASettings);
end;

{ TUgarSort }

class operator TUgarSort.Add(const ALeft, ARight: TUgarSort): TUgarSort;
begin
  Result.FImpl := TSortCombine.Create(ALeft, ARight);
end;

class function TUgarSort.Ascending(const AFieldName: String): TUgarSort;
begin
  Result.FImpl := TSortDirectional.Create(AFieldName, TUgarSortDirection.Ascending);
end;

class function TUgarSort.Combine(const ASorts: array of TUgarSort): TUgarSort;
begin
  Result.FImpl := TSortCombine.Create(ASorts);
end;

class function TUgarSort.Descending(const AFieldName: String): TUgarSort;
begin
  Result.FImpl := TSortDirectional.Create(AFieldName, TUgarSortDirection.Descending);
end;

class function TUgarSort.Combine(const ASort1, ASort2: TUgarSort): TUgarSort;
begin
  Result.FImpl := TSortCombine.Create(ASort1, ASort2);
end;

class operator TUgarSort.Implicit(const ADocument: TUgarBsonDocument): TUgarSort;
begin
  if (ADocument.IsNil) then
    Result.FImpl := nil
  else
    Result.FImpl := TSortBsonDocument.Create(ADocument);
end;

class operator TUgarSort.Implicit(const AJson: String): TUgarSort;
begin
  Result.FImpl := TSortJson.Create(AJson);
end;

function TUgarSort.IsNil: Boolean;
begin
  Result := (FImpl = nil);
end;

class function TUgarSort.MetaTextScore(const AFieldName: String): TUgarSort;
begin
  Result.FImpl := TSortBsonDocument.Create(TUgarBsonDocument.Create(AFieldName, TUgarBsonDocument.Create('$meta',
    'textScore')));
end;

function TUgarSort.Render: TUgarBsonDocument;
begin
  Assert(Assigned(FImpl));
  Result := FImpl.Render;
end;

procedure TUgarSort.SetNil;
begin
  FImpl := nil;
end;

function TUgarSort.ToBson: TBytes;
begin
  Assert(Assigned(FImpl));
  Result := FImpl.ToBson;
end;

function TUgarSort.ToJson: String;
begin
  Assert(Assigned(FImpl));
  Result := FImpl.ToJson(TUgarJsonWriterSettings.Default);
end;

function TUgarSort.ToJson(const ASettings: TUgarJsonWriterSettings): String;
begin
  Assert(Assigned(FImpl));
  Result := FImpl.ToJson(ASettings);
end;

{ TUgarUpdate }

function TUgarUpdate.&Set(const AFieldName: String; const AValue: TUgarBsonValue): TUgarUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdateOperator.Create('$set', AFieldName, AValue));
end;

procedure TUgarUpdate.SetNil;
begin
  FImpl := nil;
end;

function TUgarUpdate.SetOnInsert(const AFieldName: String; const AValue: TUgarBsonValue): TUgarUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdateOperator.Create('$setOnInsert', AFieldName, AValue));
end;

function TUgarUpdate.SetOrCombine(const AUpdate: IUpdate): IUpdate;
begin
  if (FImpl = nil) then
    FImpl := AUpdate
  else if (FImpl.IsCombine) then
    TUpdateCombine(FImpl).Add(AUpdate)
  else
    FImpl := TUpdateCombine.Create(FImpl, AUpdate);
  Result := FImpl;
end;

function TUgarUpdate.AddToSet(const AFieldName: String; const AValue: TUgarBsonValue): TUgarUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdateAddToSet.Create(AFieldName, AValue));
end;

function TUgarUpdate.AddToSetEach(const AFieldName: String; const AValues: array of TUgarBsonValue)
  : TUgarUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdateAddToSet.Create(AFieldName, AValues));
end;

function TUgarUpdate.BitwiseAnd(const AFieldName: String; const AValue: TUgarBsonValue): TUgarUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdateBitwiseOperator.Create('and', AFieldName, AValue));
end;

function TUgarUpdate.BitwiseOr(const AFieldName: String; const AValue: TUgarBsonValue): TUgarUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdateBitwiseOperator.Create('or', AFieldName, AValue));
end;

function TUgarUpdate.BitwiseXor(const AFieldName: String; const AValue: TUgarBsonValue): TUgarUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdateBitwiseOperator.Create('xor', AFieldName, AValue));
end;

function TUgarUpdate.CurrentDate(const AFieldName: String; const AType: TUgarCurrentDateType)
  : TUgarUpdate;
var
  Value: TUgarBsonValue;
begin
  case AType of
    TUgarCurrentDateType.Date:
      Value := TUgarBsonDocument.Create('$type', 'date');

    TUgarCurrentDateType.Timestamp:
      Value := TUgarBsonDocument.Create('$type', 'timestamp');
  else
    Value := True;
  end;

  Result.FImpl := SetOrCombine(TUpdateOperator.Create('$currentDate', AFieldName, Value));
end;

class operator TUgarUpdate.Implicit(const ADocument: TUgarBsonDocument): TUgarUpdate;
begin
  if (ADocument.IsNil) then
    Result.FImpl := nil
  else
    Result.FImpl := TUpdateBsonDocument.Create(ADocument);
end;

function TUgarUpdate.Inc(const AFieldName: String; const AAmount: TUgarBsonValue): TUgarUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdateOperator.Create('$inc', AFieldName, AAmount));
end;

function TUgarUpdate.Inc(const AFieldName: String): TUgarUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdateOperator.Create('$inc', AFieldName, 1));
end;

class function TUgarUpdate.Init: TUgarUpdate;
begin
  Result.FImpl := nil;
end;

class operator TUgarUpdate.Implicit(const AJson: String): TUgarUpdate;
begin
  Result.FImpl := TUpdateJson.Create(AJson);
end;

function TUgarUpdate.IsNil: Boolean;
begin
  Result := (FImpl = nil);
end;

function TUgarUpdate.Max(const AFieldName: String; const AValue: TUgarBsonValue): TUgarUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdateOperator.Create('$max', AFieldName, AValue));
end;

function TUgarUpdate.Min(const AFieldName: String; const AValue: TUgarBsonValue): TUgarUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdateOperator.Create('$min', AFieldName, AValue));
end;

function TUgarUpdate.Mul(const AFieldName: String; const AAmount: TUgarBsonValue): TUgarUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdateOperator.Create('$mul', AFieldName, AAmount));
end;

function TUgarUpdate.PopFirst(const AFieldName: String): TUgarUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdateOperator.Create('$pop', AFieldName, -1));
end;

function TUgarUpdate.PopLast(const AFieldName: String): TUgarUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdateOperator.Create('$pop', AFieldName, 1));
end;

function TUgarUpdate.Pull(const AFieldName: String; const AValue: TUgarBsonValue): TUgarUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdatePull.Create(AFieldName, AValue));
end;

function TUgarUpdate.PullAll(const AFieldName: String; const AValues: array of TUgarBsonValue): TUgarUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdatePull.Create(AFieldName, AValues));
end;

function TUgarUpdate.PullFilter(const AFieldName: String; const AFilter: TUgarFilter): TUgarUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdatePull.Create(AFieldName, AFilter));
end;

function TUgarUpdate.Push(const AFieldName: String; const AValue: TUgarBsonValue): TUgarUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdatePush.Create(AFieldName, AValue));
end;

function TUgarUpdate.PushEach(const AFieldName: String; const AValues: array of TUgarBsonValue;
  const ASlice, APosition: Integer): TUgarUpdate;
var
  Sort: TUgarSort;
begin
  Sort.SetNil;
  Result := PushEach(AFieldName, AValues, Sort, ASlice, APosition);
end;

function TUgarUpdate.PushEach(const AFieldName: String; const AValues: array of TUgarBsonValue;
  const ASort: TUgarSort; const ASlice, APosition: Integer): TUgarUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdatePush.Create(AFieldName, AValues, ASlice, APosition, ASort));
end;

function TUgarUpdate.Rename(const AFieldName, ANewName: String): TUgarUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdateOperator.Create('$rename', AFieldName, ANewName));
end;

function TUgarUpdate.Render: TUgarBsonDocument;
begin
  Assert(Assigned(FImpl));
  Result := FImpl.Render;
end;

function TUgarUpdate.ToBson: TBytes;
begin
  Assert(Assigned(FImpl));
  Result := FImpl.ToBson;
end;

function TUgarUpdate.ToJson: String;
begin
  Assert(Assigned(FImpl));
  Result := FImpl.ToJson(TUgarJsonWriterSettings.Default);
end;

function TUgarUpdate.ToJson(const ASettings: TUgarJsonWriterSettings): String;
begin
  Assert(Assigned(FImpl));
  Result := FImpl.ToJson(ASettings);
end;

function TUgarUpdate.Unset(const AFieldName: String): TUgarUpdate;
begin
  Result.FImpl := SetOrCombine(TUpdateOperator.Create('$unset', AFieldName, 1));
end;

{ TBuilder }

function TBuilder.Build: TUgarBsonDocument;
begin
  Result := TUgarBsonDocument.Create;
end;

function TBuilder.Render: TUgarBsonDocument;
var
  Writer: IUgarBsonDocumentWriter;
begin
  if (SupportsWriter) then
  begin
    Result := TUgarBsonDocument.Create;
    Writer := TUgarBsonDocumentWriter.Create(Result);
    Write(Writer);
  end
  else
    Result := Build();
end;

class function TBuilder.SupportsWriter: Boolean;
begin
  Result := False;
end;

function TBuilder.ToBson: TBytes;
var
  Writer: IUgarBsonWriter;
begin
  if (SupportsWriter) then
  begin
    Writer := TUgarBsonWriter.Create;
    Write(Writer);
    Result := Writer.ToBson;
  end
  else
    Result := Build().ToBson;
end;

function TBuilder.ToJson(const ASettings: TUgarJsonWriterSettings): String;
var
  Writer: IUgarJsonWriter;
begin
  if (SupportsWriter) then
  begin
    Writer := TUgarJsonWriter.Create(ASettings);
    Write(Writer);
    Result := Writer.ToJson;
  end
  else
    Result := Build().ToJson(ASettings);
end;

procedure TBuilder.Write(const AWriter: IUgarBsonBaseWriter);
begin
  { No default implementation }
end;

{ TFilterEmpty }

function TFilterEmpty.Build: TUgarBsonDocument;
begin
  Result := TUgarBsonDocument.Create;
end;

{ TFilterJson }

function TFilterJson.Build: TUgarBsonDocument;
begin
  Result := TUgarBsonDocument.Parse(FJson);
end;

constructor TFilterJson.Create(const AJson: String);
begin
  inherited Create;
  FJson := AJson;
end;

{ TFilterBsonDocument }

function TFilterBsonDocument.Build: TUgarBsonDocument;
begin
  Result := FDocument;
end;

constructor TFilterBsonDocument.Create(const ADocument: TUgarBsonDocument);
begin
  inherited Create;
  FDocument := ADocument;
end;

{ TFilterSimple }

constructor TFilterSimple.Create(const AFieldName: String; const AValue: TUgarBsonValue);
begin
  inherited Create;
  FFieldName := AFieldName;
  FValue := AValue;
end;

class function TFilterSimple.SupportsWriter: Boolean;
begin
  Result := True;
end;

procedure TFilterSimple.Write(const AWriter: IUgarBsonBaseWriter);
begin
  AWriter.WriteStartDocument;
  AWriter.WriteName(FFieldName);
  AWriter.WriteValue(FValue);
  AWriter.WriteEndDocument;
end;

{ TFilterOperator }

constructor TFilterOperator.Create(const AFieldName, AOperator: String; const AValue: TUgarBsonValue);
begin
  inherited Create;
  FFieldName := AFieldName;
  FOperator := AOperator;
  FValue := AValue;
end;

class function TFilterOperator.SupportsWriter: Boolean;
begin
  Result := True;
end;

procedure TFilterOperator.Write(const AWriter: IUgarBsonBaseWriter);
begin
  AWriter.WriteStartDocument;
  AWriter.WriteName(FFieldName);

  AWriter.WriteStartDocument;
  AWriter.WriteName(FOperator);
  AWriter.WriteValue(FValue);
  AWriter.WriteEndDocument;

  AWriter.WriteEndDocument;
end;

{ TFilterArrayOperator }

constructor TFilterArrayOperator.Create(const AFieldName, AOperator: String; const AValues: TUgarBsonArray);
begin
  inherited Create;
  FFieldName := AFieldName;
  FOperator := AOperator;
  FValues := AValues;
end;

class function TFilterArrayOperator.SupportsWriter: Boolean;
begin
  Result := True;
end;

procedure TFilterArrayOperator.Write(const AWriter: IUgarBsonBaseWriter);
begin
  AWriter.WriteStartDocument;
  AWriter.WriteName(FFieldName);

  AWriter.WriteStartDocument;
  AWriter.WriteName(FOperator);
  AWriter.WriteValue(FValues);
  AWriter.WriteEndDocument;

  AWriter.WriteEndDocument;
end;

{ TFilterAnd }

class procedure TFilterAnd.AddClause(const ADocument: TUgarBsonDocument; const AClause: TUgarBsonElement);
var
  Item, Value: TUgarBsonValue;
  ExistingClauseValue, ClauseValue: TUgarBsonDocument;
  Element: TUgarBsonElement;
  I: Integer;
begin
  if (AClause.Name = '$and') then
  begin
    for Item in AClause.Value.AsBsonArray do
    begin
      for Element in Item.AsBsonDocument do
        AddClause(ADocument, Element);
    end;
  end
  else if (ADocument.Count = 1) and (ADocument.Elements[0].Name = '$and') then
    ADocument.Values[0].AsBsonArray.Add(TUgarBsonDocument.Create(AClause))
  else if (ADocument.TryGetValue(AClause.Name, Value)) then
  begin
    if (Value.IsBsonDocument) and (AClause.Value.IsBsonDocument) then
    begin
      ClauseValue := AClause.Value.AsBsonDocument;
      ExistingClauseValue := Value.AsBsonDocument;

      for I := 0 to ExistingClauseValue.Count - 1 do
      begin
        if (ClauseValue.Contains(ExistingClauseValue.Elements[I].Name)) then
        begin
          PromoteFilterToDollarForm(ADocument, AClause);
          Exit;
        end;
      end;

      for Element in ClauseValue do
        ExistingClauseValue.Add(Element);
    end
    else
      PromoteFilterToDollarForm(ADocument, AClause);
  end
  else
    ADocument.Add(AClause);
end;

function TFilterAnd.Build: TUgarBsonDocument;
var
  I, J: Integer;
  RenderedFilter: TUgarBsonDocument;
begin
  Result := TUgarBsonDocument.Create;
  for I := 0 to Length(FFilters) - 1 do
  begin
    RenderedFilter := FFilters[I].Render;
    for J := 0 to RenderedFilter.Count - 1 do
      AddClause(Result, RenderedFilter.Elements[J]);
  end;
end;

constructor TFilterAnd.Create(const AFilter1, AFilter2: TUgarFilter);
begin
  Assert(not AFilter1.IsNil);
  Assert(not AFilter2.IsNil);
  inherited Create;
  SetLength(FFilters, 2);
  FFilters[0] := AFilter1.FImpl;
  FFilters[1] := AFilter2.FImpl;
end;

constructor TFilterAnd.Create(const AFilters: array of TUgarFilter);
var
  I: Integer;
begin
  inherited Create;
  SetLength(FFilters, Length(AFilters));
  for I := 0 to Length(AFilters) - 1 do
  begin
    Assert(not AFilters[I].IsNil);
    FFilters[I] := AFilters[I].FImpl;
  end;
end;

class procedure TFilterAnd.PromoteFilterToDollarForm(const ADocument: TUgarBsonDocument;
  const AClause: TUgarBsonElement);
var
  Clauses: TUgarBsonArray;
  QueryElement: TUgarBsonElement;
begin
  Clauses := TUgarBsonArray.Create(ADocument.Count);
  for QueryElement in ADocument do
    Clauses.Add(TUgarBsonDocument.Create(QueryElement));
  Clauses.Add(TUgarBsonDocument.Create(AClause));
  ADocument.Clear;
  ADocument.Add('$and', Clauses)
end;

{ TFilterOr }

class procedure TFilterOr.AddClause(const AClauses: TUgarBsonArray; const AFilter: TUgarBsonDocument);
begin
  if (AFilter.Count = 1) and (AFilter.Elements[0].Name = '$or') then
    { Flatten nested $or }
    AClauses.AddRange(AFilter.Values[0].AsBsonArray)
  else
    { We could shortcut the user's query if there are no elements in the filter,
      but I'd rather be literal and let them discover the problem on their own. }
    AClauses.Add(AFilter);
end;

function TFilterOr.Build: TUgarBsonDocument;
var
  I: Integer;
  Clauses: TUgarBsonArray;
  RenderedFilter: TUgarBsonDocument;
begin
  Clauses := TUgarBsonArray.Create;
  for I := 0 to Length(FFilters) - 1 do
  begin
    RenderedFilter := FFilters[I].Render;
    AddClause(Clauses, RenderedFilter);
  end;
  Result := TUgarBsonDocument.Create('$or', Clauses);
end;

constructor TFilterOr.Create(const AFilter1, AFilter2: TUgarFilter);
begin
  Assert(not AFilter1.IsNil);
  Assert(not AFilter2.IsNil);
  inherited Create;
  SetLength(FFilters, 2);
  FFilters[0] := AFilter1.FImpl;
  FFilters[1] := AFilter2.FImpl;
end;

constructor TFilterOr.Create(const AFilters: array of TUgarFilter);
var
  I: Integer;
begin
  inherited Create;
  SetLength(FFilters, Length(AFilters));
  for I := 0 to Length(AFilters) - 1 do
  begin
    Assert(not AFilters[I].IsNil);
    FFilters[I] := AFilters[I].FImpl;
  end;
end;

{ TFilterNot }

function TFilterNot.Build: TUgarBsonDocument;
var
  RenderedFilter: TUgarBsonDocument;
begin
  RenderedFilter := FFilter.Render;
  if (RenderedFilter.Count = 1) then
    Result := NegateSingleElementFilter(RenderedFilter, RenderedFilter.Elements[0])
  else
    Result := NegateArbitraryFilter(RenderedFilter);
end;

constructor TFilterNot.Create(const AOperand: TUgarFilter);
begin
  Assert(not AOperand.IsNil);
  inherited Create;
  FFilter := AOperand.FImpl;
end;

class function TFilterNot.NegateArbitraryFilter(const AFilter: TUgarBsonDocument): TUgarBsonDocument;
begin
  // $not only works as a meta operator on a single operator so simulate Not using $nor
  Result := TUgarBsonDocument.Create('$nor', TUgarBsonArray.Create([AFilter]));
end;

class function TFilterNot.NegateSingleElementFilter(const AFilter: TUgarBsonDocument; const AElement: TUgarBsonElement)
  : TUgarBsonDocument;
var
  Selector: TUgarBsonDocument;
  OperatorName: String;
begin
  if (AElement.Name.Chars[0] = '$') then
    Exit(NegateSingleElementTopLevelOperatorFilter(AFilter, AElement));

  if (AElement.Value.IsBsonDocument) then
  begin
    Selector := AElement.Value.AsBsonDocument;
    if (Selector.Count > 0) then
    begin
      OperatorName := Selector.Elements[0].Name;
      Assert(OperatorName <> '');
      if (OperatorName.Chars[0] = '$') and (OperatorName <> '$ref') then
      begin
        if (Selector.Count = 1) then
          Exit(NegateSingleFieldOperatorFilter(AElement.Name, Selector.Elements[0]))
        else
          Exit(NegateArbitraryFilter(AFilter));
      end;
    end;
  end;

  if (AElement.Value.IsBsonRegularExpression) then
    Exit(TUgarBsonDocument.Create(AElement.Name, TUgarBsonDocument.Create('$not', AElement.Value)));

  Result := TUgarBsonDocument.Create(AElement.Name, TUgarBsonDocument.Create('$ne', AElement.Value));
end;

class function TFilterNot.NegateSingleElementTopLevelOperatorFilter(const AFilter: TUgarBsonDocument;
  const AElement: TUgarBsonElement): TUgarBsonDocument;
begin
  if (AElement.Name = '$or') then
    Result := TUgarBsonDocument.Create('$nor', AElement.Value)
  else if (AElement.Name = '$nor') then
    Result := TUgarBsonDocument.Create('$or', AElement.Value)
  else
    Result := NegateArbitraryFilter(AFilter);
end;

class function TFilterNot.NegateSingleFieldOperatorFilter(const AFieldName: String; const AElement: TUgarBsonElement)
  : TUgarBsonDocument;
var
  S: String;
begin
  S := AElement.Name;
  if (S = '$exists') then
    Result := TUgarBsonDocument.Create(AFieldName, TUgarBsonDocument.Create('$exists', not AElement.Value.AsBoolean))
  else if (S = '$in') then
    Result := TUgarBsonDocument.Create(AFieldName, TUgarBsonDocument.Create('$nin', AElement.Value.AsBsonArray))
  else if (S = '$ne') or (S = '$not') then
    Result := TUgarBsonDocument.Create(AFieldName, AElement.Value)
  else if (S = '$nin') then
    Result := TUgarBsonDocument.Create(AFieldName, TUgarBsonDocument.Create('$in', AElement.Value.AsBsonArray))
  else
    Result := TUgarBsonDocument.Create(AFieldName, TUgarBsonDocument.Create('$not',
      TUgarBsonDocument.Create(AElement)));
end;

{ TFilterElementMatch }

function TFilterElementMatch.Build: TUgarBsonDocument;
begin
  Result := TUgarBsonDocument.Create(FFieldName, TUgarBsonDocument.Create('$elemMatch', FFilter.Render));
end;

constructor TFilterElementMatch.Create(const AFieldName: String; const AFilter: TUgarFilter);
begin
  Assert(not AFilter.IsNil);
  inherited Create;
  FFieldName := AFieldName;
  FFilter := AFilter.FImpl;
end;

{ TFilterArrayIndexExists }

constructor TFilterArrayIndexExists.Create(const AFieldName: String; const AIndex: Integer; const AExists: Boolean);
begin
  inherited Create;
  FFieldName := AFieldName;
  FIndex := AIndex;
  FExists := AExists;
end;

class function TFilterArrayIndexExists.SupportsWriter: Boolean;
begin
  Result := True;
end;

procedure TFilterArrayIndexExists.Write(const AWriter: IUgarBsonBaseWriter);
begin
  AWriter.WriteStartDocument;
  AWriter.WriteName(FFieldName + '.' + FIndex.ToString);
  AWriter.WriteStartDocument;
  AWriter.WriteName('$exists');
  AWriter.WriteBoolean(FExists);
  AWriter.WriteEndDocument;
  AWriter.WriteEndDocument;
end;

{ TProjectionJson }

function TProjectionJson.Build: TUgarBsonDocument;
begin
  Result := TUgarBsonDocument.Parse(FJson);
end;

constructor TProjectionJson.Create(const AJson: String);
begin
  inherited Create;
  FJson := AJson;
end;

{ TProjectionBsonDocument }

function TProjectionBsonDocument.Build: TUgarBsonDocument;
begin
  Result := FDocument;
end;

constructor TProjectionBsonDocument.Create(const ADocument: TUgarBsonDocument);
begin
  inherited Create;
  FDocument := ADocument;
end;

{ TProjectionCombine }

function TProjectionCombine.Build: TUgarBsonDocument;
var
  Projection: TUgarProjection.IProjection;
  RenderedProjection: TUgarBsonDocument;
  Element: TUgarBsonElement;
begin
  Result := TUgarBsonDocument.Create;
  for Projection in FProjections do
  begin
    RenderedProjection := Projection.Render;
    for Element in RenderedProjection do
    begin
      Result.Remove(Element.Name);
      Result.Add(Element)
    end;
  end;
end;

constructor TProjectionCombine.Create(const AProjection1, AProjection2: TUgarProjection);
begin
  Assert(not AProjection1.IsNil);
  Assert(not AProjection2.IsNil);
  inherited Create;
  SetLength(FProjections, 2);
  FProjections[0] := AProjection1.FImpl;
  FProjections[1] := AProjection2.FImpl;
end;

constructor TProjectionCombine.Create(const AProjections: array of TUgarProjection);
var
  I: Integer;
begin
  inherited Create;
  SetLength(FProjections, Length(AProjections));
  for I := 0 to Length(AProjections) - 1 do
  begin
    Assert(not AProjections[I].IsNil);
    FProjections[I] := AProjections[I].FImpl;
  end;
end;

{ TProjectionSingleField }

constructor TProjectionSingleField.Create(const AFieldName: String; const AValue: TUgarBsonValue);
begin
  inherited Create;
  FFieldName := AFieldName;
  FValue := AValue;
end;

class function TProjectionSingleField.SupportsWriter: Boolean;
begin
  Result := True;
end;

procedure TProjectionSingleField.Write(const AWriter: IUgarBsonBaseWriter);
begin
  AWriter.WriteStartDocument;
  AWriter.WriteName(FFieldName);
  AWriter.WriteValue(FValue);
  AWriter.WriteEndDocument;
end;

{ TProjectionMultipleFields }

constructor TProjectionMultipleFields.Create(const AFieldNames: array of String; const AValue: Integer);
var
  I: Integer;
begin
  inherited Create;
  FValue := AValue;
  SetLength(FFieldNames, Length(AFieldNames));
  for I := 0 to Length(AFieldNames) - 1 do
    FFieldNames[I] := AFieldNames[I];
end;

class function TProjectionMultipleFields.SupportsWriter: Boolean;
begin
  Result := True;
end;

procedure TProjectionMultipleFields.Write(const AWriter: IUgarBsonBaseWriter);
var
  I: Integer;
begin
  AWriter.WriteStartDocument;
  for I := 0 to Length(FFieldNames) - 1 do
  begin
    AWriter.WriteName(FFieldNames[I]);
    AWriter.WriteInt32(FValue);
  end;
  AWriter.WriteEndDocument;
end;

{ TProjectionElementMatch }

function TProjectionElementMatch.Build: TUgarBsonDocument;
begin
  Result := TUgarBsonDocument.Create(FFieldName, TUgarBsonDocument.Create('$elemMatch', FFilter.Render));
end;

constructor TProjectionElementMatch.Create(const AFieldName: String; const AFilter: TUgarFilter);
begin
  Assert(not AFilter.IsNil);
  inherited Create;
  FFieldName := AFieldName;
  FFilter := AFilter.FImpl;
end;

{ TSortJson }

function TSortJson.Build: TUgarBsonDocument;
begin
  Result := TUgarBsonDocument.Parse(FJson);
end;

constructor TSortJson.Create(const AJson: String);
begin
  inherited Create;
  FJson := AJson;
end;

{ TSortBsonDocument }

function TSortBsonDocument.Build: TUgarBsonDocument;
begin
  Result := FDocument;
end;

constructor TSortBsonDocument.Create(const ADocument: TUgarBsonDocument);
begin
  inherited Create;
  FDocument := ADocument;
end;

{ TSortCombine }

function TSortCombine.Build: TUgarBsonDocument;
var
  Sort: TUgarSort.ISort;
  RenderedSort: TUgarBsonDocument;
  Element: TUgarBsonElement;
begin
  Result := TUgarBsonDocument.Create;
  for Sort in FSorts do
  begin
    RenderedSort := Sort.Render;
    for Element in RenderedSort do
    begin
      Result.Remove(Element.Name);
      Result.Add(Element)
    end;
  end;
end;

constructor TSortCombine.Create(const ASort1, ASort2: TUgarSort);
begin
  Assert(not ASort1.IsNil);
  Assert(not ASort2.IsNil);
  inherited Create;
  SetLength(FSorts, 2);
  FSorts[0] := ASort1.FImpl;
  FSorts[1] := ASort2.FImpl;
end;

constructor TSortCombine.Create(const ASorts: array of TUgarSort);
var
  I: Integer;
begin
  inherited Create;
  SetLength(FSorts, Length(ASorts));
  for I := 0 to Length(ASorts) - 1 do
  begin
    Assert(not ASorts[I].IsNil);
    FSorts[I] := ASorts[I].FImpl;
  end;
end;

{ TSortDirectional }

constructor TSortDirectional.Create(const AFieldName: String; const ADirection: TUgarSortDirection);
begin
  inherited Create;
  FFieldName := AFieldName;
  FDirection := ADirection;
end;

class function TSortDirectional.SupportsWriter: Boolean;
begin
  Result := True;
end;

procedure TSortDirectional.Write(const AWriter: IUgarBsonBaseWriter);
begin
  AWriter.WriteStartDocument;
  AWriter.WriteName(FFieldName);
  case FDirection of
    TUgarSortDirection.Ascending:
      AWriter.WriteInt32(1);

    TUgarSortDirection.Descending:
      AWriter.WriteInt32(-1);
  else
    Assert(False);
  end;
  AWriter.WriteEndDocument;
end;

{ TUpdate }

function TUpdate.IsCombine: Boolean;
begin
  Result := False;
end;

{ TUpdateJson }

function TUpdateJson.Build: TUgarBsonDocument;
begin
  Result := TUgarBsonDocument.Parse(FJson);
end;

constructor TUpdateJson.Create(const AJson: String);
begin
  inherited Create;
  FJson := AJson;
end;

{ TUpdateBsonDocument }

function TUpdateBsonDocument.Build: TUgarBsonDocument;
begin
  Result := FDocument;
end;

constructor TUpdateBsonDocument.Create(const ADocument: TUgarBsonDocument);
begin
  inherited Create;
  FDocument := ADocument;
end;

{ TUpdateOperator }

constructor TUpdateOperator.Create(const AOperator, AFieldName: String; const AValue: TUgarBsonValue);
begin
  inherited Create;
  FOperator := AOperator;
  FFieldName := AFieldName;
  FValue := AValue;
end;

class function TUpdateOperator.SupportsWriter: Boolean;
begin
  Result := True;
end;

procedure TUpdateOperator.Write(const AWriter: IUgarBsonBaseWriter);
begin
  AWriter.WriteStartDocument;
  AWriter.WriteName(FOperator);

  AWriter.WriteStartDocument;
  AWriter.WriteName(FFieldName);
  AWriter.WriteValue(FValue);
  AWriter.WriteEndDocument;

  AWriter.WriteEndDocument;
end;

{ TUpdateBitwiseOperator }

constructor TUpdateBitwiseOperator.Create(const AOperator, AFieldName: String; const AValue: TUgarBsonValue);
begin
  inherited Create;
  FOperator := AOperator;
  FFieldName := AFieldName;
  FValue := AValue;
end;

class function TUpdateBitwiseOperator.SupportsWriter: Boolean;
begin
  Result := True;
end;

procedure TUpdateBitwiseOperator.Write(const AWriter: IUgarBsonBaseWriter);
begin
  AWriter.WriteStartDocument;
  AWriter.WriteName('$bit');

  AWriter.WriteStartDocument;
  AWriter.WriteName(FFieldName);

  AWriter.WriteStartDocument;
  AWriter.WriteName(FOperator);
  AWriter.WriteValue(FValue);
  AWriter.WriteEndDocument;

  AWriter.WriteEndDocument;

  AWriter.WriteEndDocument;
end;

{ TUpdateAddToSet }

constructor TUpdateAddToSet.Create(const AFieldName: String; const AValue: TUgarBsonValue);
begin
  inherited Create;
  FFieldName := AFieldName;
  SetLength(FValues, 1);
  FValues[0] := AValue;
end;

constructor TUpdateAddToSet.Create(const AFieldName: String; const AValues: array of TUgarBsonValue);
var
  I: Integer;
begin
  inherited Create;
  FFieldName := AFieldName;
  SetLength(FValues, Length(AValues));
  for I := 0 to Length(AValues) - 1 do
    FValues[I] := AValues[I];
end;

class function TUpdateAddToSet.SupportsWriter: Boolean;
begin
  Result := True;
end;

procedure TUpdateAddToSet.Write(const AWriter: IUgarBsonBaseWriter);
var
  I: Integer;
begin
  AWriter.WriteStartDocument;

  AWriter.WriteName('$addToSet');
  AWriter.WriteStartDocument;

  AWriter.WriteName(FFieldName);

  if (Length(FValues) = 1) then
    AWriter.WriteValue(FValues[0])
  else
  begin
    AWriter.WriteStartDocument;
    AWriter.WriteName('$each');
    AWriter.WriteStartArray;

    for I := 0 to Length(FValues) - 1 do
      AWriter.WriteValue(FValues[I]);

    AWriter.WriteEndArray;
    AWriter.WriteEndDocument;
  end;

  AWriter.WriteEndDocument;

  AWriter.WriteEndDocument;
end;

{ TUpdatePull }

constructor TUpdatePull.Create(const AFieldName: String; const AValue: TUgarBsonValue);
begin
  inherited Create;
  FFieldName := AFieldName;
  SetLength(FValues, 1);
  FValues[0] := AValue;
end;

constructor TUpdatePull.Create(const AFieldName: String; const AValues: array of TUgarBsonValue);
var
  I: Integer;
begin
  inherited Create;
  FFieldName := AFieldName;
  SetLength(FValues, Length(AValues));
  for I := 0 to Length(AValues) - 1 do
    FValues[I] := AValues[I];
end;

constructor TUpdatePull.Create(const AFieldName: String; const AFilter: TUgarFilter);
begin
  inherited Create;
  FFieldName := AFieldName;
  FFilter := AFilter;
end;

class function TUpdatePull.SupportsWriter: Boolean;
begin
  Result := True;
end;

procedure TUpdatePull.Write(const AWriter: IUgarBsonBaseWriter);
var
  RenderedFilter: TUgarBsonDocument;
  I: Integer;
begin
  AWriter.WriteStartDocument;
  if (FFilter.IsNil) then
  begin
    if (Length(FValues) = 1) then
      AWriter.WriteName('$pull')
    else
      AWriter.WriteName('$pullAll');
    AWriter.WriteStartDocument;

    AWriter.WriteName(FFieldName);
    if (Length(FValues) = 1) then
      AWriter.WriteValue(FValues[0])
    else
    begin
      AWriter.WriteStartArray;
      for I := 0 to Length(FValues) - 1 do
        AWriter.WriteValue(FValues[I]);
      AWriter.WriteEndArray;
    end;

    AWriter.WriteEndDocument;
  end
  else
  begin
    RenderedFilter := FFilter.Render;

    AWriter.WriteStartDocument('$pull');

    AWriter.WriteName(FFieldName);
    AWriter.WriteValue(RenderedFilter);

    AWriter.WriteEndDocument;
  end;
  AWriter.WriteEndDocument;
end;

{ TUpdatePush }

constructor TUpdatePush.Create(const AFieldName: String; const AValue: TUgarBsonValue);
begin
  inherited Create;
  FFieldName := AFieldName;
  SetLength(FValues, 1);
  FValues[0] := AValue;
  FSlice := TUgarUpdate.NO_SLICE;
  FPosition := TUgarUpdate.NO_POSITION;
end;

constructor TUpdatePush.Create(const AFieldName: String; const AValues: array of TUgarBsonValue;
  const ASlice, APosition: Integer; const ASort: TUgarSort);
var
  I: Integer;
begin
  inherited Create;
  FFieldName := AFieldName;
  SetLength(FValues, Length(AValues));
  for I := 0 to Length(AValues) - 1 do
    FValues[I] := AValues[I];
  FSlice := ASlice;
  FPosition := APosition;
  FSort := ASort;
end;

class function TUpdatePush.SupportsWriter: Boolean;
begin
  Result := True;
end;

procedure TUpdatePush.Write(const AWriter: IUgarBsonBaseWriter);
var
  I: Integer;
  RenderedSort: TUgarBsonDocument;
begin
  AWriter.WriteStartDocument;
  AWriter.WriteStartDocument('$push');

  AWriter.WriteName(FFieldName);
  if (FSlice = TUgarUpdate.NO_SLICE) and (FPosition = TUgarUpdate.NO_POSITION) and (FSort.IsNil) and
    (Length(FValues) = 1) then
    AWriter.WriteValue(FValues[0])
  else
  begin
    AWriter.WriteStartDocument;

    AWriter.WriteStartArray('$each');
    for I := 0 to Length(FValues) - 1 do
      AWriter.WriteValue(FValues[I]);
    AWriter.WriteEndArray;

    if (FSlice <> TUgarUpdate.NO_SLICE) then
      AWriter.WriteInt32('$slice', FSlice);

    if (FPosition <> TUgarUpdate.NO_POSITION) then
      AWriter.WriteInt32('$position', FPosition);

    if (not FSort.IsNil) then
    begin
      RenderedSort := FSort.Render;
      AWriter.WriteName('$sort');
      AWriter.WriteValue(RenderedSort);
    end;
    AWriter.WriteEndDocument;
  end;

  AWriter.WriteEndDocument;
  AWriter.WriteEndDocument;
end;

{ TUpdateCombine }

procedure TUpdateCombine.Add(const AUpdate: TUgarUpdate.IUpdate);
var
  NewCapacity: Integer;
begin
  if (FCount >= Length(FUpdates)) then
  begin
    if (FCount = 0) then
      NewCapacity := 2
    else
      NewCapacity := FCount * 2;
    SetLength(FUpdates, NewCapacity);
  end;
  FUpdates[FCount] := AUpdate;
  Inc(FCount);
end;

function TUpdateCombine.Build: TUgarBsonDocument;
var
  I: Integer;
  Update: TUgarUpdate.IUpdate;
  RenderedUpdate: TUgarBsonDocument;
  Element: TUgarBsonElement;
  CurrentOperatorValue: TUgarBsonValue;
begin
  Result := TUgarBsonDocument.Create;
  for I := 0 to FCount - 1 do
  begin
    Update := FUpdates[I];
    RenderedUpdate := Update.Render;
    for Element in RenderedUpdate do
    begin
      if (Result.TryGetValue(Element.Name, CurrentOperatorValue)) then
        Result[Element.Name] := CurrentOperatorValue.AsBsonDocument.Merge(Element.Value.AsBsonDocument, True)
      else
        Result.Add(Element);
    end;
  end;
end;

constructor TUpdateCombine.Create(const AUpdate1, AUpdate2: TUgarUpdate.IUpdate);
begin
  Assert(Assigned(AUpdate1));
  Assert(Assigned(AUpdate2));
  inherited Create;
  FCount := 2;
  SetLength(FUpdates, 2);
  FUpdates[0] := AUpdate1;
  FUpdates[1] := AUpdate2;
end;

constructor TUpdateCombine.Create(const AUpdate1, AUpdate2: TUgarUpdate);
begin
  Assert(not AUpdate1.IsNil);
  Assert(not AUpdate2.IsNil);
  inherited Create;
  FCount := 2;
  SetLength(FUpdates, 2);
  FUpdates[0] := AUpdate1.FImpl;
  FUpdates[1] := AUpdate2.FImpl;
end;

constructor TUpdateCombine.Create(const AUpdates: array of TUgarUpdate);
var
  I: Integer;
begin
  inherited Create;
  FCount := Length(AUpdates);
  SetLength(FUpdates, FCount);
  for I := 0 to FCount - 1 do
  begin
    Assert(not AUpdates[I].IsNil);
    FUpdates[I] := AUpdates[I].FImpl;
  end;
end;

function TUpdateCombine.IsCombine: Boolean;
begin
  Result := True;
end;

end.
