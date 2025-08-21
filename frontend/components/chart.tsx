"use client"

import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer } from "recharts"
import { AlertCircle } from "lucide-react"

export function Chart({ data, color, title }) {
  // Vérifier si les données sont valides
  if (!data || !Array.isArray(data) || data.length === 0) {
    return (
      <div className="h-[140px] flex flex-col items-center justify-center text-center">
        <AlertCircle className="h-8 w-8 text-muted-foreground mb-1" />
        <p className="text-sm text-muted-foreground">(vide)</p>
      </div>
    )
  }

  return (
    <div className="h-[140px]">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data}>
          <XAxis
            dataKey="name"
            fontSize={12}
            tickLine={false}
            axisLine={false}
            tick={{ fill: "#888888" }}
            tickFormatter={(value) => (value ? (value.length > 10 ? `${value.substring(0, 10)}...` : value) : "")}
          />
          <YAxis hide />
          <Tooltip
            formatter={(value) => [`${value}%`, "Pourcentage"]}
            contentStyle={{
              backgroundColor: "white",
              border: "1px solid #e2e8f0",
              borderRadius: "6px",
              boxShadow: "0 4px 6px -1px rgba(0, 0, 0, 0.1)",
            }}
          />
          <Bar dataKey="value" fill={color} radius={[4, 4, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
