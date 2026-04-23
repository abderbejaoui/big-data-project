import React from "react";

function formatTime(iso) {
  if (!iso) return "--:--:--";
  const d = new Date(iso);
  return d.toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

export default function LiveFeed({ feed, connected }) {
  return (
    <div className="panel" style={{ height: "100%" }}>
      <h2>
        Streaming Layer • Live Feed
        <span className="connection">
          <span className={`dot ${connected ? "ok" : ""}`} />
          {connected ? "connected" : "disconnected"}
        </span>
      </h2>

      <div className="feed">
        {feed.length === 0 && (
          <div className="empty">
            Waiting for the first Spark micro-batch…
            <br />
            (events flow every ~5 s once the producer + streaming job are up)
          </div>
        )}

        {feed.map((row) => (
          <div key={row.id} className={`feed-row ${row.kind}`}>
            <span className="time">{formatTime(row.timestamp)}</span>
            <span className="topic">{row.topic}</span>
            <span className="batch">batch #{row.batch_id}</span>
            <span className="records">{row.records}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
