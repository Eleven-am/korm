import {
    HoppingWindowSpec,
    WindowType,
    SessionWindowSpec,
    TumblingWindowSpec,
    WindowReference,
    WindowSpec,
    WindowDuration,
} from '../types';
import { SQLBuilder } from './base';

export class WindowSpecBuilder implements SQLBuilder<WindowSpec> {
    validate (window: WindowSpec): string | null {
        switch (window.type) {
            case WindowType.TUMBLING:
                return this.validateTumblingWindow(window);
            case WindowType.HOPPING:
                return this.validateHoppingWindow(window);
            case WindowType.SESSION:
                return this.validateSessionWindow(window);
            default:
                return `Invalid window type: ${(window as any).type}, expected TUMBLING, HOPPING, or SESSION at ${JSON.stringify(window)}`;
        }
    }

    build (window: WindowSpec): string {
        const validation = this.validate(window);

        if (validation) {
            throw new Error(validation);
        }

        switch (window.type) {
            case WindowType.TUMBLING:
                return this.buildTumblingWindow(window);
            case WindowType.HOPPING:
                return this.buildHoppingWindow(window);
            case WindowType.SESSION:
                return this.buildSessionWindow(window);
            default:
                throw new Error(`Unknown window type: ${(window as any).type} at ${JSON.stringify(window)}`);
        }
    }

    private validateDuration (duration: WindowDuration): string | null {
        return duration.value > 0 ? null : `Invalid window duration: ${duration.value} ${duration.unit} at ${JSON.stringify(duration)}`;
    }

    private buildDuration (duration: WindowDuration): string {
        return `${duration.value} ${duration.unit}`;
    }

    private buildRetentionAndGrace (window: WindowSpec): string {
        let sql = '';

        if (window.retention) {
            sql += `, RETENTION ${this.buildDuration(window.retention)}`;
        }
        if (window.gracePeriod) {
            sql += `, GRACE PERIOD ${this.buildDuration(window.gracePeriod)}`;
        }

        return sql;
    }

    private validateTumblingWindow (window: TumblingWindowSpec): string | null {
        return this.validateDuration(window.size) &&
            (!window.retention || this.validateDuration(window.retention)) &&
            (!window.gracePeriod || this.validateDuration(window.gracePeriod)) &&
            null;
    }

    private validateHoppingWindow (window: HoppingWindowSpec): string | null {
        const sizeValidation = this.validateDuration(window.size);
        const advanceValidation = this.validateDuration(window.advance);
        const retentionValidation = !window.retention ? null : this.validateDuration(window.retention);
        const gracePeriodValidation = !window.gracePeriod ? null : this.validateDuration(window.gracePeriod);
        const errs = [sizeValidation, advanceValidation, retentionValidation, gracePeriodValidation].filter((v) => v !== null);

        return errs.length === 0 ? null : errs.join(', ');
    }

    private validateSessionWindow (window: SessionWindowSpec): string | null {
        return this.validateDuration(window.inactivityGap) &&
            (!window.retention || this.validateDuration(window.retention)) &&
            (!window.gracePeriod || this.validateDuration(window.gracePeriod)) &&
            !window.retention && !window.gracePeriod
            ? null
            : 'Session windows do not support retention or grace periods, only inactivity gap';
    }

    private buildTumblingWindow (window: TumblingWindowSpec): string {
        return `TUMBLING (SIZE ${this.buildDuration(window.size)}${this.buildRetentionAndGrace(window)})`;
    }

    private buildHoppingWindow (window: HoppingWindowSpec): string {
        return `HOPPING (SIZE ${this.buildDuration(window.size)}, ` +
               `ADVANCE BY ${this.buildDuration(window.advance)}${this.buildRetentionAndGrace(window)})`;
    }

    private buildSessionWindow (window: SessionWindowSpec): string {
        let sql = `SESSION (${this.buildDuration(window.inactivityGap)}`;

        if (window.sessionConfig) {
            const configs: string[] = [];

            if (window.sessionConfig.includeStart !== undefined) {
                configs.push(`INCLUDE_START := ${window.sessionConfig.includeStart}`);
            }
            if (window.sessionConfig.includeEnd !== undefined) {
                configs.push(`INCLUDE_END := ${window.sessionConfig.includeEnd}`);
            }
            if (configs.length > 0) {
                sql += `, CONFIG(${configs.join(', ')})`;
            }
        }

        sql += this.buildRetentionAndGrace(window);
        sql += ')';

        return sql;
    }
}

export class WindowReferenceBuilder implements SQLBuilder<WindowReference> {
    validate (windowReference: WindowReference): string | null {
        if (!windowReference.name || windowReference.name.trim() === '') {
            return 'Window reference name is required';
        }

        if (windowReference.boundaries) {
            return windowReference.boundaries.length > 0 ? null : 'Window boundaries are required';
        }

        return null;
    }

    build (windowReference: WindowReference): string {
        const validation = this.validate(windowReference);

        if (validation) {
            throw new Error(validation);
        }

        let sql = windowReference.name;

        if (windowReference.boundaries) {
            sql += ` WITH (${windowReference.boundaries.join(', ')})`;
        }

        return sql;
    }
}
