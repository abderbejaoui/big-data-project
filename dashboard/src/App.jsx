import React, { useMemo } from "react";
import LiveFeed from "./LiveFeed.jsx";
import MetricsChart from "./MetricsChart.jsx";
import BatchStats from "./BatchStats.jsx";
import { useStream } from "./useStream.js";

function formatTime(iso) {
  if (!iso) return "—";
  return new Date(iso).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

export default function App() {
  const { connected, feed, rates, totals, lastBatchAt } = useStream();

  const currentRate = useMemo(() => {
    if (rates.length === 0) return 0;
    const last = rates[rates.length - 1];
    return (last.orders || 0) + (last.clicks || 0);
  }, [rates]);

  const totalEvents = totals.orders + totals.clicks;

  return (
    <div className="app">
      <header className="header">
        <h1>
          <span className="pulse" />
          Spark Structured Streaming • Live Demo
        </h1>
        <div className="header-stats">
          <div className="stat">
            <span className="label">Events (session)</span>
            <span className="value">{totalEvents.toLocaleString()}</span>
          </div>
          <div className="stat">
            <span className="label">Now • rec/s</span>
            <span className="value">{currentRate}</span>
          </div>
          <div className="stat">
            <span className="label">Last batch</span>
            <span className="value">{formatTime(lastBatchAt)}</span>
          </div>
          <div className="stat">
            <span className="label">Orders / Clicks</span>
            <span className="value" style={{ fontSize: 14 }}>
              <span style={{ color: "#4ea1ff" }}>{totals.orders}</span>
              {" / "}
              <span style={{ color: "#4ef5a8" }}>{totals.clicks}</span>
            </span>
          </div>
        </div>
      </header>

      <div className="layout">
        <LiveFeed feed={feed} connected={connected} />
        <div className="right-col">
          <MetricsChart rates={rates} />
          <BatchStats />
        </div>
      </div>
    </div>
  );
}
