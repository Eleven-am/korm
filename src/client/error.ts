import { ZodError } from 'zod';

interface ZodErrorWithPath {
    path: string[];
    message: string;
}

export function extractZodErrorsWithPaths (error: ZodError): ZodErrorWithPath[] {
    const errorsWithPath: ZodErrorWithPath[] = [];

    error.errors.forEach((err) => {
        const fullPath = err.path as string[];
        let errorMessage = err.message;

        if (err.code === 'invalid_union') {
            const unionErrors = err.unionErrors.flatMap(extractZodErrorsWithPaths);

            errorsWithPath.push(...unionErrors);

            return;
        }

        if (err.code === 'invalid_type') {
            errorMessage = `Invalid type: Expected ${err.expected}, received ${err.received}`;
        } else if (err.code === 'invalid_literal') {
            errorMessage = `Invalid literal: Expected ${err.expected}, received ${err.received}`;
        } else if (err.code === 'invalid_enum_value') {
            errorMessage = `Invalid enum value: Expected one of ${err.options.join(', ')}, received ${err.received}`;
        }

        errorsWithPath.push({
            path: fullPath,
            message: errorMessage,
        });
    });

    return errorsWithPath;
}
