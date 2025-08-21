import { DataTable } from "@/components/data-table"
import { DataFilter } from "@/components/data-filter"
import { DataStats } from "@/components/data-stats"
import { ServerStatusIndicator } from "@/components/server-status-indicator"

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

        <div className="bg-white rounded-xl shadow-sm border p-6">
          <DataFilter />
          <DataTable />
        </div>
      </div>
    </main>
  )
}
