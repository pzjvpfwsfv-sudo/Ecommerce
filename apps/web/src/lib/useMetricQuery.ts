import { useEffect, useRef, useState } from "react";

export interface MetricQueryState<T> {
  data: T | undefined;
  loading: boolean;
  error: Error | null;
  retry: () => void;
}

export function useMetricQuery<T>(
  loader: (signal: AbortSignal) => Promise<T>,
  key: string,
): MetricQueryState<T> {
  const loaderRef = useRef(loader);
  loaderRef.current = loader;
  const [revision, setRevision] = useState(0);
  const [state, setState] = useState<Omit<MetricQueryState<T>, "retry">>({
    data: undefined, loading: true, error: null,
  });

  useEffect(() => {
    const controller = new AbortController();
    setState((previous) => ({ ...previous, loading: true, error: null }));
    loaderRef.current(controller.signal).then(
      (data) => { if (!controller.signal.aborted) setState({ data, loading: false, error: null }); },
      (error: unknown) => {
        if (!controller.signal.aborted) {
          setState((previous) => ({ ...previous, loading: false, error: error instanceof Error ? error : new Error(String(error)) }));
        }
      },
    );
    return () => controller.abort();
  }, [key, revision]);

  return { ...state, retry: () => setRevision((value) => value + 1) };
}
