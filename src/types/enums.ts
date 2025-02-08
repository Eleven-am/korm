export enum StringFunction {
    SUBSTRING = 'SUBSTRING',
    CONCAT = 'CONCAT',
    TRIM = 'TRIM',
    UPPER = 'UPPER',
    LOWER = 'LOWER',
    REPLACE = 'REPLACE',
    SPLIT = 'SPLIT',
    LEN = 'LEN',
    LTRIM = 'LTRIM',
    RTRIM = 'RTRIM',
    INITCAP = 'INITCAP',
    POSITION = 'POSITION',
    SPLIT_PART = 'SPLIT_PART',
    CHAR_LENGTH = 'CHAR_LENGTH',
    LCASE = 'LCASE',
    UCASE = 'UCASE'
}

export enum NumericFunction {
    ABS = 'ABS',
    CEIL = 'CEIL',
    FLOOR = 'FLOOR',
    ROUND = 'ROUND',
    RANDOM = 'RANDOM',
    SIGN = 'SIGN',
    SQRT = 'SQRT',
    EXP = 'EXP',
    LN = 'LN',
    LOG10 = 'LOG10',
    POWER = 'POWER',
    MOD = 'MOD',
    ROUND_DECIMAL = 'ROUND_DECIMAL',
    GCD = 'GCD',
    LCM = 'LCM'
}

export enum DateFunction {
    DATEADD = 'DATEADD',
    DATEDIFF = 'DATEDIFF',
    DATETRUNC = 'DATETRUNC',
    UNIX_DATE = 'UNIX_DATE',
    UNIX_TIMESTAMP = 'UNIX_TIMESTAMP',
    TIMESTAMPTOSTRING = 'TIMESTAMPTOSTRING',
    STRINGTOTIMESTAMP = 'STRINGTOTIMESTAMP',
    FORMAT_DATE = 'FORMAT_DATE',
    PARSE_TIMESTAMP = 'PARSE_TIMESTAMP',
    FORMAT_TIMESTAMP = 'FORMAT_TIMESTAMP',
}

export enum AggregateFunction {
    COUNT = 'COUNT',
    SUM = 'SUM',
    AVG = 'AVG',
    MIN = 'MIN',
    MAX = 'MAX',
    TOPK = 'TOPK',
    TOPKDISTINCT = 'TOPKDISTINCT',
    COLLECT_LIST = 'COLLECT_LIST',
    COLLECT_SET = 'COLLECT_SET',
    COUNT_DISTINCT = 'COUNT_DISTINCT'
}

export enum ArithmeticOperator {
    ADD = '+',
    SUBTRACT = '-',
    MULTIPLY = '*',
    DIVIDE = '/',
    MODULO = '%'
}

export enum ComparisonOperator {
    EQUAL = '=',
    NOT_EQUAL = '!=',
    LESS_THAN = '<',
    LESS_THAN_OR_EQUAL = '<=',
    GREATER_THAN = '>',
    GREATER_THAN_OR_EQUAL = '>=',
    IS_NULL = 'IS NULL',
    IS_NOT_NULL = 'IS NOT NULL',
    IN = 'IN',
    NOT_IN = 'NOT IN',
    LIKE = 'LIKE',
    NOT_LIKE = 'NOT LIKE',
    BETWEEN = 'BETWEEN',
    NOT_BETWEEN = 'NOT BETWEEN'
}

export enum LogicalOperator {
    AND = 'AND',
    OR = 'OR',
    NOT = 'NOT'
}

export enum InsertType {
    VALUES = 'VALUES',
    SELECT = 'SELECT'
}

export enum QueryType {
    PROPERTY = 'PROPERTY',
    CREATE = 'CREATE',
    INSERT = 'INSERT',
    DROP = 'DROP',
    DESCRIBE = 'DESCRIBE',
    EXPLAIN = 'EXPLAIN',
    TERMINATE = 'TERMINATE',
    LIST = 'LIST',
    SHOW = 'SHOW',
}

export enum DataSourceType {
    STREAM = 'STREAM',
    TABLE = 'TABLE'
}

export enum JoinType {
    INNER = 'INNER',
    LEFT_OUTER = 'LEFT OUTER',
    FULL_OUTER = 'FULL OUTER',
}

export enum SourceType {
    DIRECT = 'DIRECT',
    SUBQUERY = 'SUBQUERY'
}

export enum TransformType {
    STRING = 'STRING',
    NUMERIC = 'NUMERIC',
    DATE = 'DATE',
    CAST = 'CAST',
    CASE = 'CASE',
    ARITHMETIC = 'ARITHMETIC',
    COMPARISON = 'COMPARISON',
    LOGICAL = 'LOGICAL',
    AGGREGATE = 'AGGREGATE',
    COLLECTION = 'COLLECTION',
    WINDOW = 'WINDOW',
    STRUCT_ACCESS = 'STRUCT_ACCESS',
    ARRAY_ACCESS = 'ARRAY_ACCESS',
    MAP_ACCESS = 'MAP_ACCESS',
    EXTRACT = 'EXTRACT',
    WINDOW_BOUNDARY = 'WINDOW_BOUNDARY',
}

export enum WindowFunction {
    LAG = 'LAG',
    LEAD = 'LEAD'
}

export enum ExpressionType {
    COLUMN = 'COLUMN',
    LITERAL = 'LITERAL',
    TRANSFORMATION = 'TRANSFORMATION',
}

export enum CollectionFunction {
    ARRAY_LENGTH = 'ARRAY_LENGTH',
    ARRAY_CONTAINS = 'ARRAY_CONTAINS',
    ARRAY = 'ARRAY',
    ARRAY_JOIN = 'ARRAY_JOIN',
    ARRAY_DISTINCT = 'ARRAY_DISTINCT',
    ARRAY_REMOVE = 'ARRAY_REMOVE',
    ARRAY_POSITION = 'ARRAY_POSITION',
    EXTRACT = 'EXTRACT',
    AS_MAP = 'AS_MAP',
    TO_STRUCT = 'TO_STRUCT',
    GET = 'GET',
    GET_FIELD = 'GET_FIELD',
    AS_ARRAY = 'AS_ARRAY',
    COLLECT_SET = 'COLLECT_SET',
    COLLECT_LIST = 'COLLECT_LIST',
}

export enum WhereType {
    LOGICAL = 'LOGICAL',
    COMPARISON = 'COMPARISON'
}

export enum SelectType {
    COLUMN = 'COLUMN',
    STAR = 'STAR'
}

export enum WindowType {
    TUMBLING = 'TUMBLING',
    HOPPING = 'HOPPING',
    SESSION = 'SESSION'
}

export enum WindowTimeUnit {
    MILLISECONDS = 'MILLISECONDS',
    SECONDS = 'SECONDS',
    MINUTES = 'MINUTES',
    HOURS = 'HOURS',
    DAYS = 'DAYS'
}

export enum WindowBoundary {
    WINDOWSTART = 'WINDOWSTART',
    WINDOWEND = 'WINDOWEND'
}

export enum TimeExtractField {
    YEAR = 'YEAR',
    MONTH = 'MONTH',
    DAY = 'DAY',
    HOUR = 'HOUR',
    MINUTE = 'MINUTE',
    SECOND = 'SECOND',
    MILLISECOND = 'MILLISECOND',
}

export enum HavingConditionType {
    AGGREGATE = 'AGGREGATE',
    COMPARISON = 'COMPARISON',
    LOGICAL = 'LOGICAL'
}

export enum SerializationFormat {
    JSON = 'JSON',
    AVRO = 'AVRO',
    PROTOBUF = 'PROTOBUF',
    KAFKA = 'KAFKA',
    DELIMITED = 'DELIMITED'
}

export enum CreateType {
    SOURCE = 'SOURCE',
    AS_SELECT = 'AS_SELECT'
}

export enum PropertyAction {
    SET = 'SET',
    SHOW = 'SHOW'
}

export enum OrderDirection {
    ASC = 'ASC',
    DESC = 'DESC'
}

export enum NullHandling {
    NULLS_FIRST = 'NULLS FIRST',
    NULLS_LAST = 'NULLS LAST'
}

export enum OrderByType {
    SIMPLE = 'SIMPLE',
    GROUP_BY = 'GROUP_BY',
    WINDOWED = 'WINDOWED'
}

export enum EmitType {
    CHANGES = 'CHANGES',
    FINAL = 'FINAL'
}

export enum DataType {
    BOOLEAN = 'BOOLEAN',
    INTEGER = 'INTEGER',
    BIGINT = 'BIGINT',
    DOUBLE = 'DOUBLE',
    STRING = 'STRING',
    DATE = 'DATE',
    TIME = 'TIME',
    TIMESTAMP = 'TIMESTAMP',
    DECIMAL = 'DECIMAL',
    INTERVAL = 'INTERVAL'
}

export enum CastType {
    ARRAY = 'ARRAY',
    MAP = 'MAP',
    STRUCT = 'STRUCT'
}
