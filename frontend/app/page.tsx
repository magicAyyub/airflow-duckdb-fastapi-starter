import { Suspense } from "react"
import { DataTable } from "@/components/data-table"
import { DataFilter } from "@/components/data-filter"
import { DataStats } from "@/components/data-stats"
import { ServerStatusIndicator } from "@/components/server-status-indicator"

// Loading components for Suspense fallbacks
function DataFilterSkeleton() {
  return (
    <div className="bg-white rounded-xl shadow-sm border p-6 mb-6">
      <div className="animate-pulse">
        <div className="h-6 bg-gray-200 rounded w-24 mb-4"></div>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {[...Array(6)].map((_, i) => (
            <div key={i} className="space-y-2">
              <div className="h-4 bg-gray-200 rounded w-20"></div>
              <div className="h-10 bg-gray-200 rounded"></div>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

function DataTableSkeleton() {
  return (
    <div className="bg-white rounded-xl shadow-sm border p-6">
      <div className="animate-pulse">
        <div className="h-6 bg-gray-200 rounded w-32 mb-4"></div>
        <div className="space-y-3">
          {[...Array(5)].map((_, i) => (
            <div key={i} className="h-12 bg-gray-200 rounded"></div>
          ))}
        </div>
      </div>
    </div>
  )
}

export default function Home() {
  return (
    <main className="flex min-h-screen flex-col">
      <div className="bg-gradient-to-b from-zinc-900 to-zinc-800 text-white">
        <div className="container mx-auto py-8">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-3xl font-bold tracking-tight mb-2">Tableau de Bord Opérateur</h1>
              <p className="text-zinc-300">Analysez et filtrez les données des opérateurs en temps réel</p>
            </div>
            <ServerStatusIndicator />
          </div>
        </div>
      </div>

      <div className="container mx-auto py-8 space-y-8">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <DataStats type="operators" />
          <DataStats type="status" />
          <DataStats type="2fa" />
        </div>

        <Suspense fallback={<DataFilterSkeleton />}>
          <DataFilter />
        </Suspense>
        
        <Suspense fallback={<DataTableSkeleton />}>
          <DataTable />
        </Suspense>
      </div>
    </main>
  )
}
