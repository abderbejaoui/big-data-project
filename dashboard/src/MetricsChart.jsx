import React from "react";
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

export default function MetricsChart({ rates }) {
  return (
    <div className="panel" style={{ minHeight: 260 }}>
      <h2>
        Real-time throughput (records / second)
        <span className="badge">last 60 s</span>
      </h2>

      {rates.length === 0 ? (
        <div className="empty">Collecting data…</div>
      ) : (
        <ResponsiveContainer width="100%" height={220}>
          <LineChart data={rates}>
            <CartesianGrid stroke="#25306a" strokeDasharray="3 3" />
            <XAxis dataKey="time" stroke="#8a98cf" fontSize={11} minTickGap={24} />
            <YAxis stroke="#8a98cf" fontSize={11} allowDecimals={false} />
            <Tooltip
              contentStyle={{
                background: "#151e45",
                border: "1px solid #2a3570",
                borderRadius: 8,
                color: "#e6ecff",
                fontSize: 12,
              }}
            />
            <Legend wrapperStyle={{ fontSize: 12 }} />
            <Line
              type="monotone"
              dataKey="orders"
              stroke="#4ea1ff"
              strokeWidth={2}
              dot={false}
              isAnimationActive={false}
            />
            <Line
              type="monotone"
              dataKey="clicks"
              stroke="#4ef5a8"
              strokeWidth={2}
              dot={false}
              isAnimationActive={false}
            />
          </LineChart>
        </ResponsiveContainer>
      )}
    </div>
  );
}
