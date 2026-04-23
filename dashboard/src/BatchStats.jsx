import React, { useEffect, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

const API_BASE =
  import.meta.env.VITE_API_BASE_URL || `http://${window.location.hostname}:8000`;

const COLORS = ["#4ea1ff", "#4ef5a8", "#f5d74e", "#ff6b6b", "#b084ff", "#5bd3ff"];

function useGold(pollMs = 10000) {
  const [orders, setOrders] = useState(null);
  const [clicks, setClicks] = useState(null);
  const [counts, setCounts] = useState(null);

  useEffect(() => {
    let alive = true;
    const fetchAll = async () => {
      try {
        const [o, c, s] = await Promise.all([
          fetch(`${API_BASE}/stats/orders`).then((r) => r.json()),
          fetch(`${API_BASE}/stats/clicks`).then((r) => r.json()),
          fetch(`${API_BASE}/layers/summary`).then((r) => r.json()),
        ]);
        if (!alive) return;
        setOrders(o);
        setClicks(c);
        setCounts(s);
      } catch (e) {
        // silent: gold layer not ready yet
      }
    };
    fetchAll();
    const id = setInterval(fetchAll, pollMs);
    return () => {
      alive = false;
      clearInterval(id);
    };
  }, [pollMs]);

  return { orders, clicks, counts };
}

export default function BatchStats() {
  const { orders, clicks, counts } = useGold();

  const revenuePerProduct = (orders?.revenue_per_product || []).slice(0, 6);
  const statusDist = orders?.orders_by_status || [];
  const topPages = clicks?.top_pages || [];

  return (
    <div className="panel" style={{ flex: 1, minHeight: 0 }}>
      <h2>
        Batch Layer • Gold Aggregates
        <span className="badge">
          bronze {counts?.bronze_total ?? "—"} · silver {counts?.silver_total ?? "—"}
        </span>
      </h2>

      <div className="grid-2">
        <div className="chart-block">
          <div className="title">Revenue per product</div>
          {revenuePerProduct.length === 0 ? (
            <div className="empty">Run batch jobs to populate.</div>
          ) : (
            <div className="chart">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={revenuePerProduct} margin={{ left: 0, right: 12, top: 8 }}>
                  <CartesianGrid stroke="#25306a" strokeDasharray="3 3" />
                  <XAxis
                    dataKey="product"
                    stroke="#8a98cf"
                    fontSize={10}
                    angle={-18}
                    textAnchor="end"
                    height={60}
                    interval={0}
                  />
                  <YAxis stroke="#8a98cf" fontSize={10} />
                  <Tooltip
                    contentStyle={{
                      background: "#151e45",
                      border: "1px solid #2a3570",
                      borderRadius: 8,
                      color: "#e6ecff",
                      fontSize: 12,
                    }}
                  />
                  <Bar dataKey="total_revenue" fill="#4ea1ff" radius={[4, 4, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          )}
        </div>

        <div className="chart-block">
          <div className="title">Order status distribution</div>
          {statusDist.length === 0 ? (
            <div className="empty">Run batch jobs to populate.</div>
          ) : (
            <div className="chart">
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie
                    data={statusDist}
                    dataKey="order_count"
                    nameKey="status"
                    innerRadius={45}
                    outerRadius={75}
                    paddingAngle={3}
                  >
                    {statusDist.map((_, i) => (
                      <Cell key={i} fill={COLORS[i % COLORS.length]} />
                    ))}
                  </Pie>
                  <Tooltip
                    contentStyle={{
                      background: "#151e45",
                      border: "1px solid #2a3570",
                      borderRadius: 8,
                      color: "#e6ecff",
                      fontSize: 12,
                    }}
                  />
                  <Legend wrapperStyle={{ fontSize: 11 }} />
                </PieChart>
              </ResponsiveContainer>
            </div>
          )}
        </div>

        <div className="chart-block" style={{ gridColumn: "1 / -1" }}>
          <div className="title">Top pages (clicks)</div>
          {topPages.length === 0 ? (
            <div className="empty">Run batch jobs to populate.</div>
          ) : (
            <table className="table">
              <thead>
                <tr>
                  <th>Page</th>
                  <th style={{ textAlign: "right" }}>Views</th>
                </tr>
              </thead>
              <tbody>
                {topPages.map((row) => (
                  <tr key={row.page}>
                    <td>{row.page}</td>
                    <td className="num">{row.views}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>
    </div>
  );
}
