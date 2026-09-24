import { render, screen, waitFor } from "@testing-library/react";
import { act } from "react";
import { expect, it } from "vitest";

import { useMetricQuery } from "./useMetricQuery";

function Probe({ queryKey, loader }: {
  queryKey: string;
  loader: (signal: AbortSignal) => Promise<string>;
}) {
  const state = useMetricQuery(loader, queryKey);
  return <div><span data-testid="value">{state.data ?? "empty"}</span><span data-testid="loading">{String(state.loading)}</span></div>;
}

it("keeps old data while loading and ignores a late older response", async () => {
  let resolveOld!: (value: string) => void;
  let resolveNew!: (value: string) => void;
  const oldLoader = () => new Promise<string>((resolve) => { resolveOld = resolve; });
  const newLoader = () => new Promise<string>((resolve) => { resolveNew = resolve; });
  const view = render(<Probe queryKey="old" loader={oldLoader} />);
  view.rerender(<Probe queryKey="new" loader={newLoader} />);
  await act(async () => { resolveNew("new-run"); });
  expect(screen.getByTestId("value")).toHaveTextContent("new-run");
  await act(async () => { resolveOld("old-run"); });
  expect(screen.getByTestId("value")).toHaveTextContent("new-run");
  expect(screen.getByTestId("loading")).toHaveTextContent("false");
});

it("marks an old result as loading without clearing it before replacement", async () => {
  let finish!: (value: string) => void;
  const view = render(<Probe queryKey="first" loader={() => Promise.resolve("first-run")} />);
  await waitFor(() => expect(screen.getByTestId("value")).toHaveTextContent("first-run"));
  view.rerender(<Probe queryKey="second" loader={() => new Promise((resolve) => { finish = resolve; })} />);
  expect(screen.getByTestId("value")).toHaveTextContent("first-run");
  expect(screen.getByTestId("loading")).toHaveTextContent("true");
  await act(async () => { finish("second-run"); });
  expect(screen.getByTestId("value")).toHaveTextContent("second-run");
});
