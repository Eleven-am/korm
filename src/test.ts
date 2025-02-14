// Target KSQL Query:
/*
SELECT
    p.product_id,
    p.name AS product_name,
    COUNT(*) AS total_purchases,
    SUM(o.quantity) AS total_quantity,
    SUM(o.quantity * p.price) AS total_revenue,
    COLLECT_LIST(o.user_id) AS buyer_list
FROM orders_stream o
    INNER JOIN products_table p ON o.product_id = p.product_id
    LEFT OUTER JOIN inventory_table i ON p.product_id = i.product_id
WHERE o.status = 'COMPLETED'
    AND o.quantity > 0
    AND p.price > 10.00
WINDOW TUMBLING (SIZE 1 HOURS, RETENTION 24 HOURS, GRACE PERIOD 10 MINUTES)
GROUP BY p.product_id, p.name
HAVING COUNT(*) > 5
    AND SUM(o.quantity) >= 100
PARTITION BY p.category
ORDER BY total_revenue DESC
EMIT CHANGES;
*/

import { KSQLStatementBuilder } from './builders';
import { extractZodErrorsWithPaths } from './client/error';
import { ksqlStatementSchema } from './schemas';
import {
    OrderByType,
    SelectType,
    ExpressionType,
    TransformType,
    AggregateFunction,
    ArithmeticOperator,
    DataSourceType,
    SourceType,
    JoinType,
    WhereType,
    LogicalOperator,
    ComparisonOperator,
    WindowType,
    WindowTimeUnit,
    HavingConditionType,
    OrderDirection,
    EmitType,
    DateFunction,
    CollectionFunction,
    WindowedSelectQuery,
    WindowFunction,
    NumericFunction,
    KSQLStatement,
} from './types';

const complexQuery: KSQLStatement = {
    type: SelectType.COLUMN,
    columns: [
        {
            type: SelectType.COLUMN,
            expression: {
                type: ExpressionType.COLUMN,
                sourceColumn: 'p.product_id',
            },
        },
        {
            type: SelectType.COLUMN,
            expression: {
                type: ExpressionType.COLUMN,
                sourceColumn: 'p.name',
            },
            alias: 'product_name',
        },
        {
            type: SelectType.COLUMN,
            expression: {
                type: ExpressionType.TRANSFORMATION,
                value: {
                    type: TransformType.AGGREGATE,
                    'function': AggregateFunction.COUNT,
                    parameters: [],
                },
            },
            alias: 'total_purchases',
        },
        {
            type: SelectType.COLUMN,
            expression: {
                type: ExpressionType.TRANSFORMATION,
                value: {
                    type: TransformType.AGGREGATE,
                    'function': AggregateFunction.SUM,
                    parameters: [
                        {
                            type: ExpressionType.COLUMN,
                            sourceColumn: 'o.quantity',
                        },
                    ],
                },
            },
            alias: 'total_quantity',
        },
        {
            type: SelectType.COLUMN,
            expression: {
                type: ExpressionType.TRANSFORMATION,
                value: {
                    type: TransformType.AGGREGATE,
                    'function': AggregateFunction.SUM,
                    parameters: [
                        {
                            type: ExpressionType.TRANSFORMATION,
                            value: {
                                type: TransformType.ARITHMETIC,
                                operator: ArithmeticOperator.MULTIPLY,
                                left: {
                                    type: ExpressionType.COLUMN,
                                    sourceColumn: 'o.quantity',
                                },
                                right: {
                                    type: ExpressionType.COLUMN,
                                    sourceColumn: 'p.price',
                                },
                            },
                        },
                    ],
                },
            },
            alias: 'total_revenue',
        },
        {
            type: SelectType.COLUMN,
            expression: {
                type: ExpressionType.TRANSFORMATION,
                value: {
                    type: TransformType.AGGREGATE,
                    'function': AggregateFunction.COLLECT_LIST,
                    parameters: [
                        {
                            type: ExpressionType.COLUMN,
                            sourceColumn: 'o.user_id',
                        },
                    ],
                },
            },
            alias: 'buyer_list',
        },
    ],
    from: {
        sourceType: DataSourceType.STREAM,
        source: {
            type: SourceType.DIRECT,
            name: 'orders_stream',
            alias: 'o',
            sourceType: DataSourceType.STREAM,
        },
        joins: [
            {
                type: JoinType.INNER,
                source: {
                    name: 'products_table',
                    alias: 'p',
                    sourceType: DataSourceType.TABLE,
                },
                conditions: [
                    {
                        leftField: 'o.product_id',
                        rightField: 'p.product_id',
                    },
                ],
            },
            {
                type: JoinType.LEFT_OUTER,
                source: {
                    name: 'inventory_table',
                    alias: 'i',
                    sourceType: DataSourceType.TABLE,
                },
                conditions: [
                    {
                        leftField: 'p.product_id',
                        rightField: 'i.product_id',
                    },
                ],
            },
        ],
    },
    where: {
        type: WhereType.LOGICAL,
        operator: LogicalOperator.AND,
        conditions: [
            {
                type: WhereType.COMPARISON,
                operator: ComparisonOperator.EQUAL,
                left: {
                    type: ExpressionType.COLUMN,
                    sourceColumn: 'o.status',
                },
                right: {
                    type: ExpressionType.LITERAL,
                    value: 'COMPLETED',
                },
            },
            {
                type: WhereType.COMPARISON,
                operator: ComparisonOperator.GREATER_THAN,
                left: {
                    type: ExpressionType.COLUMN,
                    sourceColumn: 'o.quantity',
                },
                right: {
                    type: ExpressionType.LITERAL,
                    value: 0,
                },
            },
            {
                type: WhereType.COMPARISON,
                operator: ComparisonOperator.GREATER_THAN,
                left: {
                    type: ExpressionType.COLUMN,
                    sourceColumn: 'p.price',
                },
                right: {
                    type: ExpressionType.LITERAL,
                    value: 10.00,
                },
            },
        ],
    },
    groupBy: {
        columns: [
            {
                expression: {
                    type: ExpressionType.COLUMN,
                    sourceColumn: 'p.product_id',
                },
            },
            {
                expression: {
                    type: ExpressionType.COLUMN,
                    sourceColumn: 'p.name',
                },
            },
        ],
        window: {
            name: 'W1',
            spec: {
                type: WindowType.TUMBLING,
                size: {
                    value: 1,
                    unit: WindowTimeUnit.HOURS,
                },
                retention: {
                    value: 24,
                    unit: WindowTimeUnit.HOURS,
                },
                gracePeriod: {
                    value: 10,
                    unit: WindowTimeUnit.MINUTES,
                },
            },
        },
        having: {
            type: HavingConditionType.LOGICAL,
            operator: LogicalOperator.AND,
            conditions: [
                {
                    type: HavingConditionType.AGGREGATE,
                    'function': AggregateFunction.COUNT,
                    parameters: [],
                    operator: ComparisonOperator.GREATER_THAN,
                    right: {
                        type: ExpressionType.LITERAL,
                        value: 5,
                    },
                },
                {
                    type: HavingConditionType.AGGREGATE,
                    'function': AggregateFunction.SUM,
                    parameters: [
                        {
                            type: ExpressionType.COLUMN,
                            sourceColumn: 'o.quantity',
                        },
                    ],
                    operator: ComparisonOperator.GREATER_THAN_OR_EQUAL,
                    right: {
                        type: ExpressionType.LITERAL,
                        value: 100,
                    },
                },
            ],
        },
    },
    partitionBy: {
        columns: [
            {
                type: ExpressionType.COLUMN,
                sourceColumn: 'p.category',
            },
        ],
    },
    orderBy: {
        columns: [
            {
                type: OrderByType.WINDOWED,
                expression: {
                    type: ExpressionType.TRANSFORMATION,
                    value: {
                        type: TransformType.AGGREGATE,
                        'function': AggregateFunction.SUM,
                        parameters: [
                            {
                                type: ExpressionType.TRANSFORMATION,
                                value: {
                                    type: TransformType.ARITHMETIC,
                                    operator: ArithmeticOperator.MULTIPLY,
                                    left: {
                                        type: ExpressionType.COLUMN,
                                        sourceColumn: 'o.quantity',
                                    },
                                    right: {
                                        type: ExpressionType.COLUMN,
                                        sourceColumn: 'p.price',
                                    },
                                },
                            },
                        ],
                    },
                },
                direction: OrderDirection.DESC,
            },
        ],
    },
    emit: EmitType.CHANGES,
};

const builder = new KSQLStatementBuilder();
const query = builder.build(complexQuery);

console.log(query);

// Target KSQL Query:
/*
SELECT
    customer_id,
    TIMESTAMPTOSTRING(order_timestamp, 'yyyy-MM-dd HH:mm:ss') as order_time,
    COLLECT_SET(product_name) as unique_products,
    ARRAY_LENGTH(COLLECT_LIST(product_id)) as total_items,
    MAX(unit_price) as highest_price,
    CASE
        WHEN SUM(quantity * unit_price) > 1000 THEN 'HIGH'
        WHEN SUM(quantity * unit_price) > 500 THEN 'MEDIUM'
        ELSE 'LOW'
    END as order_value_category
FROM customer_orders_stream c
    LEFT JOIN product_details p ON c.product_id = p.id
WHERE c.status NOT IN ('CANCELLED', 'REJECTED')
    AND EXTRACT(HOUR FROM order_timestamp) BETWEEN 9 AND 17
WINDOW HOPPING (
    SIZE 30 MINUTES,
    ADVANCE BY 10 MINUTES,
    RETENTION 24 HOURS,
    GRACE PERIOD 10 MINUTES
)
GROUP BY customer_id
HAVING COUNT(*) > 3
ORDER BY MAX(unit_price) DESC
EMIT CHANGES;
*/

const complexQuery2: KSQLStatement = {
    type: SelectType.COLUMN,
    columns: [
        {
            type: SelectType.COLUMN,
            expression: {
                type: ExpressionType.COLUMN,
                sourceColumn: 'customer_id',
            },
        },
        {
            type: SelectType.COLUMN,
            expression: {
                type: ExpressionType.TRANSFORMATION,
                value: {
                    type: TransformType.DATE,
                    'function': DateFunction.TIMESTAMPTOSTRING,
                    parameters: [
                        {
                            type: ExpressionType.COLUMN,
                            sourceColumn: 'order_timestamp',
                        },
                        {
                            type: ExpressionType.LITERAL,
                            value: 'yyyy-MM-dd HH:mm:ss',
                        },
                    ],
                },
            },
            alias: 'order_time',
        },
        {
            type: SelectType.COLUMN,
            expression: {
                type: ExpressionType.TRANSFORMATION,
                value: {
                    type: TransformType.COLLECTION,
                    'function': CollectionFunction.COLLECT_SET,
                    parameters: [
                        {
                            type: ExpressionType.COLUMN,
                            sourceColumn: 'product_name',
                        },
                    ],
                },
            },
            alias: 'unique_products',
        },
        {
            type: SelectType.COLUMN,
            expression: {
                type: ExpressionType.TRANSFORMATION,
                value: {
                    type: TransformType.COLLECTION,
                    'function': CollectionFunction.ARRAY_LENGTH,
                    parameters: [
                        {
                            type: ExpressionType.TRANSFORMATION,
                            value: {
                                type: TransformType.COLLECTION,
                                'function': CollectionFunction.COLLECT_LIST,
                                parameters: [
                                    {
                                        type: ExpressionType.COLUMN,
                                        sourceColumn: 'product_id',
                                    },
                                ],
                            },
                        },
                    ],
                },
            },
            alias: 'total_items',
        },
        {
            type: SelectType.COLUMN,
            expression: {
                type: ExpressionType.TRANSFORMATION,
                value: {
                    type: TransformType.AGGREGATE,
                    'function': AggregateFunction.MAX,
                    parameters: [
                        {
                            type: ExpressionType.COLUMN,
                            sourceColumn: 'unit_price',
                        },
                    ],
                },
            },
            alias: 'highest_price',
        },
        {
            type: SelectType.COLUMN,
            expression: {
                type: ExpressionType.TRANSFORMATION,
                value: {
                    type: TransformType.CASE,
                    conditions: [
                        {
                            when: {
                                type: ExpressionType.TRANSFORMATION,
                                value: {
                                    type: TransformType.COMPARISON,
                                    operator: ComparisonOperator.GREATER_THAN,
                                    left: {
                                        type: ExpressionType.TRANSFORMATION,
                                        value: {
                                            type: TransformType.AGGREGATE,
                                            'function': AggregateFunction.SUM,
                                            parameters: [
                                                {
                                                    type: ExpressionType.TRANSFORMATION,
                                                    value: {
                                                        type: TransformType.ARITHMETIC,
                                                        operator: ArithmeticOperator.MULTIPLY,
                                                        left: {
                                                            type: ExpressionType.COLUMN,
                                                            sourceColumn: 'quantity',
                                                        },
                                                        right: {
                                                            type: ExpressionType.COLUMN,
                                                            sourceColumn: 'unit_price',
                                                        },
                                                    },
                                                },
                                            ],
                                        },
                                    },
                                    right: {
                                        type: ExpressionType.LITERAL,
                                        value: 1000,
                                    },
                                },
                            },
                            then: {
                                type: ExpressionType.LITERAL,
                                value: 'HIGH',
                            },
                        },
                        {
                            when: {
                                type: ExpressionType.TRANSFORMATION,
                                value: {
                                    type: TransformType.COMPARISON,
                                    operator: ComparisonOperator.GREATER_THAN,
                                    left: {
                                        type: ExpressionType.TRANSFORMATION,
                                        value: {
                                            type: TransformType.AGGREGATE,
                                            'function': AggregateFunction.SUM,
                                            parameters: [
                                                {
                                                    type: ExpressionType.TRANSFORMATION,
                                                    value: {
                                                        type: TransformType.ARITHMETIC,
                                                        operator: ArithmeticOperator.MULTIPLY,
                                                        left: {
                                                            type: ExpressionType.COLUMN,
                                                            sourceColumn: 'quantity',
                                                        },
                                                        right: {
                                                            type: ExpressionType.COLUMN,
                                                            sourceColumn: 'unit_price',
                                                        },
                                                    },
                                                },
                                            ],
                                        },
                                    },
                                    right: {
                                        type: ExpressionType.LITERAL,
                                        value: 500,
                                    },
                                },
                            },
                            then: {
                                type: ExpressionType.LITERAL,
                                value: 'MEDIUM',
                            },
                        },
                    ],
                    'else': {
                        type: ExpressionType.LITERAL,
                        value: 'LOW',
                    },
                },
            },
            alias: 'order_value_category',
        },
    ],
    from: {
        sourceType: DataSourceType.STREAM,
        source: {
            type: SourceType.DIRECT,
            name: 'customer_orders_stream',
            alias: 'c',
            sourceType: DataSourceType.STREAM,
        },
        joins: [
            {
                type: JoinType.LEFT_OUTER,
                source: {
                    name: 'product_details',
                    alias: 'p',
                    sourceType: DataSourceType.TABLE,
                },
                conditions: [
                    {
                        leftField: 'c.product_id',
                        rightField: 'p.id',
                    },
                ],
            },
        ],
    },
    where: {
        type: WhereType.LOGICAL,
        operator: LogicalOperator.AND,
        conditions: [
            {
                type: WhereType.COMPARISON,
                operator: ComparisonOperator.NOT_IN,
                left: {
                    type: ExpressionType.COLUMN,
                    sourceColumn: 'c.status',
                },
                right: [
                    {
                        type: ExpressionType.LITERAL,
                        value: 'CANCELLED',
                    },
                    {
                        type: ExpressionType.LITERAL,
                        value: 'REJECTED',
                    },
                ],
            },
            {
                type: WhereType.COMPARISON,
                operator: ComparisonOperator.BETWEEN,
                left: {
                    type: ExpressionType.TRANSFORMATION,
                    value: {
                        type: TransformType.EXTRACT,
                        field: 'HOUR',
                        source: {
                            type: ExpressionType.COLUMN,
                            sourceColumn: 'order_timestamp',
                        },
                    },
                },
                right: {
                    start: {
                        type: ExpressionType.LITERAL,
                        value: 9,
                    },
                    end: {
                        type: ExpressionType.LITERAL,
                        value: 17,
                    },
                },
            },
        ],
    },
    groupBy: {
        window: {
            name: 'W1',
            spec: {
                type: WindowType.HOPPING,
                size: {
                    value: 30,
                    unit: WindowTimeUnit.MINUTES,
                },
                advance: {
                    value: 10,
                    unit: WindowTimeUnit.MINUTES,
                },
                retention: {
                    value: 24,
                    unit: WindowTimeUnit.HOURS,
                },
                gracePeriod: {
                    value: 10,
                    unit: WindowTimeUnit.MINUTES,
                },
            },
        },
        columns: [
            {
                expression: {
                    type: ExpressionType.COLUMN,
                    sourceColumn: 'customer_id',
                },
            },
        ],
        having: {
            type: HavingConditionType.AGGREGATE,
            'function': AggregateFunction.COUNT,
            parameters: [],
            operator: ComparisonOperator.GREATER_THAN,
            right: {
                type: ExpressionType.LITERAL,
                value: 3,
            },
        },
    },
    orderBy: {
        columns: [
            {
                type: OrderByType.WINDOWED,
                expression: {
                    type: ExpressionType.TRANSFORMATION,
                    value: {
                        type: TransformType.AGGREGATE,
                        'function': AggregateFunction.MAX,
                        parameters: [
                            {
                                type: ExpressionType.COLUMN,
                                sourceColumn: 'unit_price',
                            },
                        ],
                    },
                },
                direction: OrderDirection.DESC,
            },
        ],
    },
    emit: EmitType.CHANGES,
};

const query2 = builder.build(complexQuery2);

console.log(query2);

// Target KSQL Query:
/*
SELECT
    symbol,
    price,
    (price - LAG(price, 1) OVER w) / LAG(price, 1) OVER w * 100 AS price_change_pct
FROM stock_prices
WINDOW price_analysis_window HOPPING (SIZE 15 MINUTES, ADVANCE 1 MINUTES)
WINDOW w AS (PARTITION BY symbol ORDER BY ROWTIME)
GROUP BY symbol
HAVING ABS(AVG(price) - LAG(AVG(price)) OVER w) > 2;
*/

const anomalyDetectionQuery: WindowedSelectQuery = {
    type: SelectType.COLUMN,
    columns: [
        // symbol
        {
            type: SelectType.COLUMN,
            expression: {
                type: ExpressionType.COLUMN,
                sourceColumn: 'symbol',
            },
        },
        // price
        {
            type: SelectType.COLUMN,
            expression: {
                type: ExpressionType.COLUMN,
                sourceColumn: 'price',
            },
        },
        // price_change_pct calculation
        {
            type: SelectType.COLUMN,
            expression: {
                type: ExpressionType.TRANSFORMATION,
                value: {
                    type: TransformType.ARITHMETIC,
                    operator: ArithmeticOperator.MULTIPLY,
                    left: {
                        type: ExpressionType.TRANSFORMATION,
                        value: {
                            type: TransformType.ARITHMETIC,
                            operator: ArithmeticOperator.DIVIDE,
                            left: {
                                type: ExpressionType.TRANSFORMATION,
                                value: {
                                    type: TransformType.ARITHMETIC,
                                    operator: ArithmeticOperator.SUBTRACT,
                                    left: {
                                        type: ExpressionType.COLUMN,
                                        sourceColumn: 'price',
                                    },
                                    right: {
                                        type: ExpressionType.TRANSFORMATION,
                                        value: {
                                            type: TransformType.WINDOW,
                                            'function': WindowFunction.LAG,
                                            parameters: [
                                                {
                                                    type: ExpressionType.COLUMN,
                                                    sourceColumn: 'price',
                                                },
                                                {
                                                    type: ExpressionType.LITERAL,
                                                    value: 1,
                                                },
                                            ],
                                            over: {
                                                partitionBy: [
                                                    {
                                                        type: ExpressionType.COLUMN,
                                                        sourceColumn: 'symbol',
                                                    },
                                                ],
                                                orderBy: [
                                                    {
                                                        expression: {
                                                            type: ExpressionType.COLUMN,
                                                            sourceColumn: 'ROWTIME',
                                                        },
                                                    },
                                                ],
                                            },
                                        },
                                    },
                                },
                            },
                            right: {
                                type: ExpressionType.TRANSFORMATION,
                                value: {
                                    type: TransformType.WINDOW,
                                    'function': WindowFunction.LAG,
                                    parameters: [
                                        {
                                            type: ExpressionType.COLUMN,
                                            sourceColumn: 'price',
                                        },
                                        {
                                            type: ExpressionType.LITERAL,
                                            value: 1,
                                        },
                                    ],
                                    over: {
                                        partitionBy: [
                                            {
                                                type: ExpressionType.COLUMN,
                                                sourceColumn: 'symbol',
                                            },
                                        ],
                                        orderBy: [
                                            {
                                                expression: {
                                                    type: ExpressionType.COLUMN,
                                                    sourceColumn: 'ROWTIME',
                                                },
                                            },
                                        ],
                                    },
                                },
                            },
                        },
                    },
                    right: {
                        type: ExpressionType.LITERAL,
                        value: 100,
                    },
                },
            },
            alias: 'price_change_pct',
        },
    ],
    from: {
        sourceType: DataSourceType.STREAM,
        source: {
            type: SourceType.DIRECT,
            name: 'stock_prices',
            sourceType: DataSourceType.STREAM,
        },
    },
    groupBy: {
        columns: [
            {
                expression: {
                    type: ExpressionType.COLUMN,
                    sourceColumn: 'symbol',
                },
            },
        ],
        window: {
            spec: {
                type: WindowType.HOPPING,
                size: {
                    value: 15,
                    unit: WindowTimeUnit.MINUTES,
                },
                advance: {
                    value: 1,
                    unit: WindowTimeUnit.MINUTES,
                },
            },
        },
        having: {
            type: HavingConditionType.COMPARISON,
            operator: ComparisonOperator.GREATER_THAN,
            left: {
                type: ExpressionType.TRANSFORMATION,
                value: {
                    type: TransformType.NUMERIC,
                    'function': NumericFunction.ABS,
                    parameters: [
                        {
                            type: ExpressionType.TRANSFORMATION,
                            value: {
                                type: TransformType.AGGREGATE,
                                'function': AggregateFunction.AVG,
                                parameters: [
                                    {
                                        type: ExpressionType.TRANSFORMATION,
                                        value: {
                                            type: TransformType.ARITHMETIC,
                                            operator: ArithmeticOperator.MULTIPLY,
                                            left: {
                                                type: ExpressionType.TRANSFORMATION,
                                                value: {
                                                    type: TransformType.ARITHMETIC,
                                                    operator: ArithmeticOperator.DIVIDE,
                                                    left: {
                                                        type: ExpressionType.TRANSFORMATION,
                                                        value: {
                                                            type: TransformType.ARITHMETIC,
                                                            operator: ArithmeticOperator.SUBTRACT,
                                                            left: {
                                                                type: ExpressionType.COLUMN,
                                                                sourceColumn: 'price',
                                                            },
                                                            right: {
                                                                type: ExpressionType.TRANSFORMATION,
                                                                value: {
                                                                    type: TransformType.WINDOW,
                                                                    'function': WindowFunction.LAG,
                                                                    parameters: [
                                                                        {
                                                                            type: ExpressionType.COLUMN,
                                                                            sourceColumn: 'price',
                                                                        },
                                                                        {
                                                                            type: ExpressionType.LITERAL,
                                                                            value: 1,
                                                                        },
                                                                    ],
                                                                    over: {
                                                                        partitionBy: [
                                                                            {
                                                                                type: ExpressionType.COLUMN,
                                                                                sourceColumn: 'symbol',
                                                                            },
                                                                        ],
                                                                        orderBy: [
                                                                            {
                                                                                expression: {
                                                                                    type: ExpressionType.COLUMN,
                                                                                    sourceColumn: 'ROWTIME',
                                                                                },
                                                                            },
                                                                        ],
                                                                    },
                                                                },
                                                            },
                                                        },
                                                    },
                                                    right: {
                                                        type: ExpressionType.TRANSFORMATION,
                                                        value: {
                                                            type: TransformType.WINDOW,
                                                            'function': WindowFunction.LAG,
                                                            parameters: [
                                                                {
                                                                    type: ExpressionType.COLUMN,
                                                                    sourceColumn: 'price',
                                                                },
                                                                {
                                                                    type: ExpressionType.LITERAL,
                                                                    value: 1,
                                                                },
                                                            ],
                                                            over: {
                                                                partitionBy: [
                                                                    {
                                                                        type: ExpressionType.COLUMN,
                                                                        sourceColumn: 'symbol',
                                                                    },
                                                                ],
                                                                orderBy: [
                                                                    {
                                                                        expression: {
                                                                            type: ExpressionType.COLUMN,
                                                                            sourceColumn: 'ROWTIME',
                                                                        },
                                                                    },
                                                                ],
                                                            },
                                                        },
                                                    },
                                                },
                                            },
                                            right: {
                                                type: ExpressionType.LITERAL,
                                                value: 100,
                                            },
                                        },
                                    },
                                ],
                            },
                        },
                    ],
                },
            },
            right: {
                type: ExpressionType.LITERAL,
                value: 2,
            },
        },
    },
    emit: EmitType.CHANGES,
};

const query3 = builder.build(anomalyDetectionQuery);

console.log(query3);

const complexHavingQuery: KSQLStatement = {
    type: SelectType.COLUMN,
    columns: [
        {
            type: SelectType.COLUMN,
            expression: {
                type: ExpressionType.COLUMN,
                sourceColumn: 'category',
            },
        },
        {
            type: SelectType.COLUMN,
            expression: {
                type: ExpressionType.TRANSFORMATION,
                value: {
                    type: TransformType.AGGREGATE,
                    'function': AggregateFunction.COUNT,
                    parameters: [],
                },
            },
            alias: 'total_events',
        },
        {
            type: SelectType.COLUMN,
            expression: {
                type: ExpressionType.TRANSFORMATION,
                value: {
                    type: TransformType.AGGREGATE,
                    'function': AggregateFunction.AVG,
                    parameters: [
                        {
                            type: ExpressionType.COLUMN,
                            sourceColumn: 'value',
                        },
                    ],
                },
            },
            alias: 'avg_value',
        },
    ],
    from: {
        sourceType: DataSourceType.STREAM,
        source: {
            type: SourceType.DIRECT,
            name: 'events_stream',
            sourceType: DataSourceType.STREAM,
        },
    },
    groupBy: {
        columns: [
            {
                expression: {
                    type: ExpressionType.COLUMN,
                    sourceColumn: 'category',
                },
            },
        ],
        having: {
            type: HavingConditionType.LOGICAL,
            operator: LogicalOperator.AND,
            conditions: [
                // Check if any values are NULL
                {
                    type: HavingConditionType.COMPARISON,
                    operator: ComparisonOperator.GREATER_THAN,
                    left: {
                        type: ExpressionType.TRANSFORMATION,
                        value: {
                            type: TransformType.AGGREGATE,
                            'function': AggregateFunction.COUNT,
                            parameters: [
                                {
                                    type: ExpressionType.TRANSFORMATION,
                                    value: {
                                        type: TransformType.CASE,
                                        conditions: [
                                            {
                                                when: {
                                                    type: ExpressionType.TRANSFORMATION,
                                                    value: {
                                                        type: TransformType.COMPARISON,
                                                        operator: ComparisonOperator.IS_NULL,
                                                        expression: {
                                                            type: ExpressionType.COLUMN,
                                                            sourceColumn: 'value',
                                                        },
                                                    },
                                                },
                                                then: {
                                                    type: ExpressionType.LITERAL,
                                                    value: 1,
                                                },
                                            },
                                        ],
                                        'else': {
                                            type: ExpressionType.LITERAL,
                                            value: 0,
                                        },
                                    },
                                },
                            ],
                        },
                    },
                    right: {
                        type: ExpressionType.LITERAL,
                        value: 5,
                    },
                },
                // Complex condition checking ratio of nulls to total
                {
                    type: HavingConditionType.COMPARISON,
                    operator: ComparisonOperator.GREATER_THAN,
                    left: {
                        type: ExpressionType.TRANSFORMATION,
                        value: {
                            type: TransformType.ARITHMETIC,
                            operator: ArithmeticOperator.DIVIDE,
                            left: {
                                type: ExpressionType.TRANSFORMATION,
                                value: {
                                    type: TransformType.AGGREGATE,
                                    'function': AggregateFunction.COUNT,
                                    parameters: [
                                        {
                                            type: ExpressionType.TRANSFORMATION,
                                            value: {
                                                type: TransformType.CASE,
                                                conditions: [
                                                    {
                                                        when: {
                                                            type: ExpressionType.TRANSFORMATION,
                                                            value: {
                                                                type: TransformType.COMPARISON,
                                                                operator: ComparisonOperator.IS_NULL,
                                                                expression: {
                                                                    type: ExpressionType.COLUMN,
                                                                    sourceColumn: 'value',
                                                                },
                                                            },
                                                        },
                                                        then: {
                                                            type: ExpressionType.LITERAL,
                                                            value: 1,
                                                        },
                                                    },
                                                ],
                                                'else': {
                                                    type: ExpressionType.LITERAL,
                                                    value: 0,
                                                },
                                            },
                                        },
                                    ],
                                },
                            },
                            right: {
                                type: ExpressionType.TRANSFORMATION,
                                value: {
                                    type: TransformType.AGGREGATE,
                                    'function': AggregateFunction.COUNT,
                                    parameters: [],
                                },
                            },
                        },
                    },
                    right: {
                        type: ExpressionType.LITERAL,
                        value: 0.1,
                    },
                },
            ],
        },
    },
    emit: EmitType.CHANGES,
};

const query4 = builder.build(complexHavingQuery);

console.log(query4);

const data = ksqlStatementSchema.safeParse(complexQuery);

console.log(data.success);

if (!data.success) {
    extractZodErrorsWithPaths(data.error).forEach((error) => {
        console.log(error);
    });
}

const data2 = ksqlStatementSchema.safeParse(complexQuery2);

console.log(data2.success);

if (!data2.success) {
    console.log(JSON.stringify(data2.error));
}

const data3 = ksqlStatementSchema.safeParse(anomalyDetectionQuery);

console.log(data3.success);

if (!data3.success) {
    console.log(JSON.stringify(data3.error));
}

const data4 = ksqlStatementSchema.safeParse(complexHavingQuery);

console.log(data4.success);

if (!data4.success) {
    console.log(JSON.stringify(data4.error));
}
