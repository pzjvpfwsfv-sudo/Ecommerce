import { expect, it } from "vitest";

import { metricLinkParam } from "./metricNavigation";

it("accepts only linked metric parameters for the intended route", () => {
  expect(metricLinkParam("#/orders?window=full", "/orders", "window", ["day", "month", "full"])).toBe("full");
  expect(metricLinkParam("#/rankings?window=day&source=behavior", "/rankings", "source", ["orders", "behavior"])).toBe("behavior");
  expect(metricLinkParam("#/rankings?window=day&source=behavior", "/rankings", "window", ["day", "full"])).toBe("day");
  expect(metricLinkParam("#/fulfillment?window=month&view=reviews", "/fulfillment", "view", ["delivery", "reviews"])).toBe("reviews");
  expect(metricLinkParam("#/orders?window=bogus", "/orders", "window", ["day", "month", "full"])).toBeNull();
  expect(metricLinkParam("#/behavior?window=full", "/orders", "window", ["day", "month", "full"])).toBeNull();
});
