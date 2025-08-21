"use client"

import { useState, useEffect } from "react"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent } from "@/components/ui/card"
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip"
import { Database, Clock, CheckCircle, AlertCircle, RefreshCw } from "lucide-react"

interface ServerStatus {
  status: "online" | "offline" | "loading"
  lastUpdate: string | null
  recordCount: number
  etlStatus: string
}

interface ServerStatusIndicatorProps {
  backendUrl?: string
}

export function ServerStatusIndicator({
  backendUrl = process.env.NEXT_PUBLIC_BACKEND_URL || "http://localhost:8000",
}: ServerStatusIndicatorProps) {
  const [serverStatus, setServerStatus] = useState<ServerStatus>({
    status: "loading",
    lastUpdate: null,
    recordCount: 0,
    etlStatus: "unknown"
  })

  const checkServerStatus = async () => {
    try {
      // Check if server is responding first
      const response = await fetch(`${backendUrl}/api/csv/stats?type=operators`, {
        method: "GET",
        headers: { "Content-Type": "application/json" }
      })

      if (response.ok) {
        // Get ETL synchronization information (this is the correct data!)
        const etlResponse = await fetch(`${backendUrl}/api/csv/etl-status`)
        const etlData = await etlResponse.json()
        
        setServerStatus({
          status: "online",
          lastUpdate: etlData.last_sync, // This is the actual ETL sync time
          recordCount: etlData.total_records, // Total records in DuckDB
          etlStatus: etlData.status || "running"
        })
      } else {
        setServerStatus({
          status: "offline",
          lastUpdate: null,
          recordCount: 0,
          etlStatus: "stopped"
        })
      }
    } catch (error) {
      console.error("Error checking server status:", error)
      setServerStatus({
        status: "offline",
        lastUpdate: null,
        recordCount: 0,
        etlStatus: "error"
      })
    }
  }

  useEffect(() => {
    checkServerStatus()
    
    // Check status every 30 seconds
    const interval = setInterval(checkServerStatus, 30000)
    
    return () => clearInterval(interval)
  }, []) // Remove backendUrl dependency to avoid infinite re-renders

  const formatLastUpdate = (dateString: string | null) => {
    if (!dateString) return "Aucune donnée"
    
    const date = new Date(dateString)
    const now = new Date()
    const diffMs = now.getTime() - date.getTime()
    const diffMinutes = Math.floor(diffMs / 60000)
    const diffHours = Math.floor(diffMinutes / 60)
    const diffDays = Math.floor(diffHours / 24)

    if (diffMinutes < 1) return "À l'instant"
    if (diffMinutes < 60) return `Il y a ${diffMinutes} min`
    if (diffHours < 24) return `Il y a ${diffHours}h`
    if (diffDays < 7) return `Il y a ${diffDays}j`
    
    return date.toLocaleDateString('fr-FR', { 
      day: '2-digit', 
      month: '2-digit', 
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    })
  }

  const getStatusIcon = () => {
    switch (serverStatus.status) {
      case "online":
        return <CheckCircle className="h-4 w-4 text-green-500" />
      case "offline":
        return <AlertCircle className="h-4 w-4 text-red-500" />
      case "loading":
        return <RefreshCw className="h-4 w-4 text-blue-500 animate-spin" />
    }
  }

  const getStatusColor = () => {
    switch (serverStatus.status) {
      case "online":
        return "bg-green-500/10 text-green-600 border-green-200"
      case "offline":
        return "bg-red-500/10 text-red-600 border-red-200"
      case "loading":
        return "bg-blue-500/10 text-blue-600 border-blue-200"
    }
  }

  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger asChild>
          <Card className="w-auto">
            <CardContent>
              <div className="flex items-center space-x-4 px-2">
                {/* Server Status */}
                <div className="flex items-center space-x-2">
                  {getStatusIcon()}
                  <Badge variant="outline" className={getStatusColor()}>
                    {serverStatus.status === "online" ? "Live" : 
                     serverStatus.status === "offline" ? "Hors ligne" : "Connexion..."}
                  </Badge>
                </div>

                {/* Last Update */}
                <div className="flex items-center space-x-2 text-sm --color-foreground">
                  <Clock className="h-4 w-4" />
                  <span>Dernière MAJ: {formatLastUpdate(serverStatus.lastUpdate)}</span>
                </div>

                {/* Record Count */}
                <div className="flex items-center space-x-2 text-sm --color-foreground">
                  <Database className="h-4 w-4" />
                  <span>{serverStatus.recordCount.toLocaleString()} enregistrements</span>
                </div>
              </div>
            </CardContent>
          </Card>
        </TooltipTrigger>
        <TooltipContent>
          <div className="space-y-1">
            <p><strong>Statut API:</strong> {serverStatus.status}</p>
            <p><strong>ETL:</strong> {serverStatus.etlStatus}</p>
            <p><strong>Dernière synchronisation:</strong> {formatLastUpdate(serverStatus.lastUpdate)}</p>
            <p><strong>Total d&apos;enregistrements:</strong> {serverStatus.recordCount.toLocaleString()}</p>
          </div>
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  )
}
