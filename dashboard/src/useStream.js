import { useEffect, useRef, useState } from "react";

const WS_URL =
  import.meta.env.VITE_WS_URL || `ws://${window.location.hostname}:8000/ws/stream`;

export function useStream({ maxFeed = 50, rateWindowSec = 60 } = {}) {
  const [connected, setConnected] = useState(false);
  const [feed, setFeed] = useState([]);
  const [rates, setRates] = useState([]);
  const [totals, setTotals] = useState({ orders: 0, clicks: 0 });
  const [lastBatchAt, setLastBatchAt] = useState(null);

  const bucketsRef = useRef(new Map());

  useEffect(() => {
    let ws;
    let stopped = false;
    let retry = 0;

    const connect = () => {
      if (stopped) return;
      ws = new WebSocket(WS_URL);

      ws.onopen = () => {
        retry = 0;
        setConnected(true);
      };

      ws.onclose = () => {
        setConnected(false);
        if (!stopped) {
          const delay = Math.min(1000 * 2 ** retry++, 8000);
          setTimeout(connect, delay);
        }
      };

      ws.onerror = () => {
        ws && ws.close();
      };

      ws.onmessage = (evt) => {
        try {
          const msg = JSON.parse(evt.data);
          if (msg.type !== "batch") return;
          handleBatch(msg);
        } catch (e) {
          console.warn("bad ws message", e);
        }
      };
    };

    const handleBatch = (msg) => {
      const topic = msg.topic || "unknown";
      const kind = topic.includes("orders") ? "orders" : topic.includes("clicks") ? "clicks" : "other";
      const ts = new Date(msg.timestamp || Date.now()).getTime();

      setLastBatchAt(msg.timestamp);

      setFeed((prev) => {
        const next = [
          {
            id: `${msg.batch_id}-${topic}-${ts}`,
            topic,
            kind,
            batch_id: msg.batch_id,
            records: msg.records,
            timestamp: msg.timestamp,
            sample: msg.sample || [],
          },
          ...prev,
        ];
        if (next.length > maxFeed) next.length = maxFeed;
        return next;
      });

      setTotals((prev) => ({
        ...prev,
        [kind]: (prev[kind] || 0) + (msg.records || 0),
      }));

      const secondBucket = Math.floor(ts / 1000);
      const m = bucketsRef.current;
      const cur = m.get(secondBucket) || { t: secondBucket, orders: 0, clicks: 0 };
      if (kind === "orders") cur.orders += msg.records || 0;
      if (kind === "clicks") cur.clicks += msg.records || 0;
      m.set(secondBucket, cur);

      const cutoff = secondBucket - rateWindowSec;
      for (const k of m.keys()) if (k < cutoff) m.delete(k);

      setRates(
        [...m.values()]
          .sort((a, b) => a.t - b.t)
          .map((b) => ({
            time: new Date(b.t * 1000).toLocaleTimeString([], {
              minute: "2-digit",
              second: "2-digit",
            }),
            orders: b.orders,
            clicks: b.clicks,
          }))
      );
    };

    connect();

    return () => {
      stopped = true;
      ws && ws.close();
    };
  }, [maxFeed, rateWindowSec]);

  return { connected, feed, rates, totals, lastBatchAt };
}
