# KORM (KSQL Object Relational Mapping)

KORM is a sophisticated TypeScript Object-Relational Mapping (ORM) library designed specifically for KSQL stream processing. It enables developers to build and execute KSQL queries using type-safe TypeScript objects instead of raw SQL strings, providing robust compile-time safety, excellent IDE support, and comprehensive validation.

## Key Features

### Type Safety & Validation
- 🛡️ Complete TypeScript type checking for query construction
- ✅ Comprehensive Zod schema validation
- 🔍 Runtime validation of complex query relationships
- 🚫 Prevention of SQL injection vulnerabilities

### Query Building
- 🔄 Full support for KSQL streams and tables
- 🛠️ Rich set of transformation functions (string, numeric, date, aggregate)
- 📦 Composable query components
- 🔌 Seamless integration with existing KSQL deployments

### Developer Experience
- 💡 Full IDE IntelliSense support
- 🔧 Refactoring-friendly design
- 📝 Clear version control diffs
- 📚 Extensive inline documentation

## Installation

```bash
npm install @eleven-am/korm
```

## Basic Usage

### Client Setup
```typescript
import { KsqlDBClient } from '@eleven-am/korm';

const client = new KsqlDBClient({
  host: 'localhost',
  port: 8088,
  protocol: 'http',
  auth: {
    username: 'user',
    password: 'pass'
  }
});
```

### Simple Query Example
```typescript
// Building a query to count orders
const countOrdersQuery = {
  type: SelectType.COLUMN,
  columns: [{
    type: SelectType.COLUMN,
    expression: {
      type: ExpressionType.TRANSFORMATION,
      value: {
        type: TransformType.AGGREGATE,
        function: AggregateFunction.COUNT,
        parameters: []
      }
    },
    alias: 'total_orders'
  }],
  from: {
    sourceType: DataSourceType.STREAM,
    source: {
      type: SourceType.DIRECT,
      name: 'orders',
      sourceType: DataSourceType.STREAM
    }
  },
  groupBy: {
    columns: []
  },
  emit: EmitType.CHANGES
};

// Execute the query
const result = await client.executeStatement(countOrdersQuery);
```

## Advanced Features

### Complex Transformations
```typescript
// Example of string and numeric transformations
const transformationExample = {
  type: TransformType.STRING,
  function: StringFunction.CONCAT,
  parameters: [/* parameters */]
};
```

### Window Operations
```typescript
// Example of a tumbling window
const windowedQuery = {
  window: {
    type: WindowType.TUMBLING,
    size: {
      value: 5,
      unit: WindowTimeUnit.MINUTES
    }
  }
};
```

### Join Operations
```typescript
// Example of stream-table join
const joinExample = {
  type: JoinType.INNER,
  source: {
    name: 'order_details',
    sourceType: DataSourceType.TABLE
  },
  conditions: [
    {
      leftField: 'order_id',
      rightField: 'id'
    }
  ]
};
```

## Error Handling

KORM provides detailed error information through two main error types:

```typescript
try {
  await client.executeStatement(query);
} catch (error) {
  if (error instanceof KsqlDBError) {
    // Handle server-side errors
    console.error('KSQL Error:', error.message);
  } else if (error instanceof ValidationError) {
    // Handle validation failures
    console.error('Validation Error:', error.details);
  }
}
```

## Configuration Options

```typescript
interface KsqlDBConfig {
  host: string;
  port: number;
  protocol?: 'http' | 'https';
  auth?: {
    username: string;
    password: string;
  };
  defaultStreamProperties?: {
    'ksql.streams.auto.offset.reset'?: 'earliest' | 'latest';
    'ksql.streams.cache.max.bytes.buffering'?: number;
    [key: string]: string | number | boolean | undefined;
  };
}
```

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details on:
- Code of Conduct
- Development setup
- Testing guidelines
- Pull request process

## License

MIT

## Support

- 📚 [Documentation](docs/README.md)
- 💬 [Discussions](https://github.com/eleven-am/korm/discussions)
- 🐛 [Issue Tracker](https://github.com/eleven-am/korm/issues)

