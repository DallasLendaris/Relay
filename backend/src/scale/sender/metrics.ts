/** One CloudWatch EMF line for a dimensionless metric in the sender namespace. */
export const METRIC_NAMESPACE = 'Relay/Sender';

export function emfLine(name: string, value: number, now: number = Date.now()): string {
  return JSON.stringify({
    _aws: {
      Timestamp: now,
      CloudWatchMetrics: [
        {
          Namespace: METRIC_NAMESPACE,
          Dimensions: [[]],
          Metrics: [{ Name: name, Unit: 'Count' }],
        },
      ],
    },
    [name]: value,
  });
}

export type MetricWriter = (line: string) => void;
export const consoleMetricWriter: MetricWriter = (line) => console.log(line);
