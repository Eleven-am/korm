import { z } from 'zod';

import { nonAggregateExpressionSchema } from './expressionSchema';

export const partitionBySchema = z.object({
    columns: z.array(z.lazy(() => nonAggregateExpressionSchema)),
});
