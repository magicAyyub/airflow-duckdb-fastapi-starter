"use client"

import { useEffect, useState } from "react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { getStats } from "@/lib/data"
import { Chart } from "./chart"
import { AlertCircle } from "lucide-react"

export function DataStats({ type }) {
  const [stats, setStats] = useState([])
  const [error, setError] = useState(null)
  const [noData, setNoData] = useState(false)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    async function fetchStats() {
      try {
        const response = await getStats(type)

        // Vérifier si nous avons un message indiquant qu'il n'y a pas de données
        if (response.message === "no_data" || response.message === "timeout") {
          setNoData(true)
        } else if (response.error) {
          setError(response.error)
        } else {
          setStats(response)
        }
      } catch (e) {
        setError("Erreur lors du chargement des statistiques")
      } finally {
        setLoading(false)
      }
    }

    fetchStats()
  }, [type])

  const getTitle = () => {
    switch (type) {
      case "operators":
        return "Top 5 Opérateurs"
      case "status":
        return "Répartition par Statut"
      case "2fa":
        return "Répartition par 2FA Statut"
      default:
        return "Statistiques"
    }
  }

  const getColor = () => {
    switch (type) {
      case "operators":
        return "#3b82f6"
      case "status":
        return "#10b981"
      case "2fa":
        return "#f59e0b"
      default:
        return "#6366f1"
    }
  }

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-base font-medium">{getTitle()}</CardTitle>
      </CardHeader>
      <CardContent>
        {loading ? (
          <div className="h-[140px] flex flex-col items-center justify-center text-center">
            <div className="w-8 h-8 border-4 border-primary border-t-transparent rounded-full animate-spin mb-2"></div>
            <p className="text-sm text-muted-foreground">Chargement...</p>
          </div>
        ) : noData ? (
          <div className="h-[140px] flex flex-col items-center justify-center text-center">
            <AlertCircle className="h-8 w-8 text-muted-foreground mb-1" />
            <p className="text-sm text-muted-foreground">Aucune donnée disponible</p>
            <p className="text-xs text-muted-foreground mt-1">Importez les fichiers pour commencer</p>
          </div>
        ) : error ? (
          <div className="h-[140px] flex flex-col items-center justify-center text-center">
            <AlertCircle className="h-8 w-8 text-red-500 mb-1" />
            <p className="text-sm text-muted-foreground">{error}</p>
          </div>
        ) : (
          <Chart data={stats} color={getColor()} title={getTitle()} />
        )}
      </CardContent>
    </Card>
  )
}
