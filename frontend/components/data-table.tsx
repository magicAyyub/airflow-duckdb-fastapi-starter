"use client"

import { useState, useEffect } from "react"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Button } from "@/components/ui/button"
import { useSearchParams } from "next/navigation"
import { useData } from "@/hooks/use-data"
import {
  Download,
  ChevronLeft,
  ChevronRight,
  AlertCircle,
  Database,
  FilterX,
  TrendingDown,
  TrendingUp,
  Minus,
} from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip"
import { useToast } from "@/hooks/use-toast"

export function DataTable() {
  const [page, setPage] = useState(1)
  const searchParams = useSearchParams()
  const { data, isLoading, totalPages, message, isFiltered } = useData(page)
  const { toast } = useToast()

  // Fonction pour exporter les données
  const handleExport = async () => {
    const params = new URLSearchParams(searchParams)

    try {
      // Ouvrir une nouvelle fenêtre avec l'URL d'exportation
      const exportUrl = `http://localhost:8000/api/csv/export?${params.toString()}`
      window.open(exportUrl, "_blank")
    } catch (error) {
      console.error("Erreur lors de l'exportation:", error)
    }
  }

  // Fonction pour formater les critères de filtrage de manière lisible
  const getFilterDescription = () => {
    const filters = []

    if (searchParams.get("statut") && searchParams.get("statut") !== "all") {
      filters.push(`Statut: ${searchParams.get("statut")}`)
    }

    if (searchParams.get("fa_statut") && searchParams.get("fa_statut") !== "all") {
      filters.push(`2FA Statut: ${searchParams.get("fa_statut")}`)
    }

    if (
      searchParams.get("limite_type") &&
      searchParams.get("limite_valeur") &&
      searchParams.get("limite_type") !== "none"
    ) {
      const operator = searchParams.get("limite_type") === "lt" ? "<" : ">"
      const baseText = searchParams.get("filtre_global") === "true" ? "(sur parc global)" : "(sur données filtrées)"
      filters.push(`Pourcentage IN ${operator} ${searchParams.get("limite_valeur")}% ${baseText}`)
    }

    if (searchParams.get("date_min")) {
      filters.push(`Date min: ${new Date(searchParams.get("date_min")).toLocaleDateString()}`)
    }

    if (searchParams.get("date_max")) {
      filters.push(`Date max: ${new Date(searchParams.get("date_max")).toLocaleDateString()}`)
    }

    if (searchParams.get("annee") && searchParams.get("annee") !== "all") {
      filters.push(`Année: ${searchParams.get("annee")}`)
    }

    return filters
  }

  // Fonction pour afficher l'indicateur de variation
  const renderVariationIndicator = (global, filtered) => {
    if (!isFiltered) return null

    // Calculer la variation
    const variation = filtered - global

    // Arrondir à 2 décimales
    const roundedVariation = Math.round(variation * 100) / 100

    if (roundedVariation > 0.1) {
      return (
        <TooltipProvider>
          <Tooltip>
            <TooltipTrigger asChild>
              <div className="inline-flex items-center text-green-600 ml-2">
                <TrendingUp className="h-4 w-4 mr-1" />
                <span className="text-xs">+{roundedVariation.toFixed(2)}%</span>
              </div>
            </TooltipTrigger>
            <TooltipContent>
              <p>Augmentation de {roundedVariation.toFixed(2)}% avec les filtres appliqués</p>
            </TooltipContent>
          </Tooltip>
        </TooltipProvider>
      )
    } else if (roundedVariation < -0.1) {
      return (
        <TooltipProvider>
          <Tooltip>
            <TooltipTrigger asChild>
              <div className="inline-flex items-center text-red-600 ml-2">
                <TrendingDown className="h-4 w-4 mr-1" />
                <span className="text-xs">{roundedVariation.toFixed(2)}%</span>
              </div>
            </TooltipTrigger>
            <TooltipContent>
              <p>Diminution de {Math.abs(roundedVariation).toFixed(2)}% avec les filtres appliqués</p>
            </TooltipContent>
          </Tooltip>
        </TooltipProvider>
      )
    } else {
      return (
        <TooltipProvider>
          <Tooltip>
            <TooltipTrigger asChild>
              <div className="inline-flex items-center text-gray-500 ml-2">
                <Minus className="h-4 w-4 mr-1" />
                <span className="text-xs">0%</span>
              </div>
            </TooltipTrigger>
            <TooltipContent>
              <p>Pas de changement significatif avec les filtres appliqués</p>
            </TooltipContent>
          </Tooltip>
        </TooltipProvider>
      )
    }
  }

  // Vérifier si nous avons un message indiquant qu'il n'y a pas de données
  const noData = message === "no_data"

  // Fonction pour formater les grands nombres avec des séparateurs de milliers
  const formatNumber = (num) => {
    return new Intl.NumberFormat("fr-FR").format(num)
  }
  // pour vérifier si des données ont été chargées après un rechargement
  useEffect(() => {
    // Vérifier si nous venons de charger des données (après un rechargement)
    const justLoaded = sessionStorage.getItem("justLoadedData")
    if (justLoaded) {
      // Supprimer le marqueur
      sessionStorage.removeItem("justLoadedData")

      // Afficher une notification de succès
      toast({
        title: "Données chargées avec succès",
        description: "Les données ont été importées et sont maintenant visibles dans le tableau.",
        variant: "default",
      })
    }
  }, [])

  return (
    <div className="space-y-4">
      <div className="flex justify-between items-center">
        <div>
          <h2 className="text-xl font-semibold">Résultats</h2>
          {isFiltered && (
            <p className="text-sm text-muted-foreground mt-1">
              Les filtres sont appliqués. Le pourcentage global reste calculé sur l'ensemble des données.
            </p>
          )}
        </div>
        <div className="flex gap-2">
          <Button
            onClick={handleExport}
            variant="outline"
            className="flex items-center gap-2"
            disabled={noData || data.length === 0}
          >
            <Download className="h-4 w-4" />
            Exporter résumé
          </Button>
        </div>
      </div>

      <div className="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="w-[200px]">Opérateur</TableHead>
              <TableHead>
                Nombre d'IN
                {isFiltered && (
                  <Badge variant="outline" className="ml-2 font-normal bg-blue-50 text-blue-700 border-blue-200">
                    Filtré
                  </Badge>
                )}
              </TableHead>
              <TableHead>
                % IN
                <Badge variant="outline" className="ml-2 font-normal bg-green-50 text-green-700 border-green-200">
                  Global
                </Badge>
              </TableHead>
              {isFiltered && (
                <TableHead>
                  % IN
                  <Badge variant="outline" className="ml-2 font-normal bg-purple-50 text-purple-700 border-purple-200">
                    Filtré
                  </Badge>
                </TableHead>
              )}
            </TableRow>
          </TableHeader>
          <TableBody>
            {isLoading ? (
              <TableRow>
                <TableCell colSpan={isFiltered ? 4 : 3} className="h-24 text-center">
                  <div className="flex flex-col items-center justify-center">
                    <div className="w-8 h-8 border-4 border-primary border-t-transparent rounded-full animate-spin mb-2"></div>
                    <p className="text-sm text-muted-foreground">Chargement des données...</p>
                  </div>
                </TableCell>
              </TableRow>
            ) : noData ? (
              <TableRow>
                <TableCell colSpan={isFiltered ? 4 : 3} className="h-64">
                  <div className="flex flex-col items-center justify-center text-center p-6">
                    <AlertCircle className="h-12 w-12 text-muted-foreground mb-4" />
                    <h3 className="text-lg font-medium mb-2">Aucune donnée disponible</h3>
                    <p className="text-muted-foreground mb-4">
                      Commencez par importer un fichier CSV pour visualiser les données.
                    </p>

                    <Button
                      variant="outline"
                      className="flex items-center gap-2"
                      onClick={() => {
                        // Trouver et cliquer sur le bouton d'importation dans la barre d'action
                        const importButton = document.querySelector(".action-bar-import-button")
                        if (importButton) {
                          ;(importButton as HTMLButtonElement).click()
                        }
                      }}
                    >
                      <Database className="h-4 w-4" />
                      Importer des données
                    </Button>
                  </div>
                </TableCell>
              </TableRow>
            ) : data.length === 0 ? (
              <TableRow>
                <TableCell colSpan={isFiltered ? 4 : 3} className="h-64">
                  <div className="flex flex-col items-center justify-center text-center p-6">
                    <AlertCircle className="h-12 w-12 text-muted-foreground mb-4" />
                    <h3 className="text-lg font-medium mb-2">Aucune donnée trouvée</h3>

                    {searchParams.toString() ? (
                      <>
                        <p className="text-muted-foreground mb-2">
                          Aucune donnée ne correspond aux critères de filtrage suivants:
                        </p>

                        <div className="bg-gray-50 p-3 rounded-md mb-4 text-sm">
                          {getFilterDescription().map((filter, index) => (
                            <div key={index} className="mb-1 last:mb-0">
                              {filter}
                            </div>
                          ))}
                        </div>

                        <p className="text-muted-foreground mb-4">
                          Essayez d'élargir vos critères de recherche pour obtenir plus de résultats.
                        </p>

                        <Button
                          variant="outline"
                          onClick={() => (window.location.href = "/")}
                          className="flex items-center gap-2"
                        >
                          <FilterX className="h-4 w-4" />
                          Réinitialiser les filtres
                        </Button>
                      </>
                    ) : (
                      <>
                        <p className="text-muted-foreground mb-4">
                          Aucune donnée n'est disponible. Commencez par charger des données.
                        </p>

                        <Button
                          variant="outline"
                          className="flex items-center gap-2"
                          onClick={() => {
                            // Trouver et cliquer sur le bouton d'importation dans la barre d'action
                            const importButton = document.querySelector(".action-bar-import-button")
                            if (importButton) {
                              ;(importButton as HTMLButtonElement).click()
                            }
                          }}
                        >
                          <Database className="h-4 w-4" />
                          Charger les données
                        </Button>
                      </>
                    )}
                  </div>
                </TableCell>
              </TableRow>
            ) : (
              data.map((row) => (
                <TableRow key={row.id}>
                  <TableCell className="font-medium">{row.operateur}</TableCell>
                  <TableCell>{formatNumber(row.nombre_in)}</TableCell>
                  <TableCell>
                    {row.pourcentage_in}%
                    {isFiltered && renderVariationIndicator(row.pourcentage_in, row.pourcentage_filtre)}
                  </TableCell>
                  {isFiltered && <TableCell>{row.pourcentage_filtre}%</TableCell>}
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </div>

      <div className="flex items-center justify-between">
        <div className="text-sm text-muted-foreground">
          {data.length > 0 ? `Affichage de ${Math.min(10, data.length)} résultats par page` : "Aucun résultat"}
        </div>
        <div className="flex items-center space-x-2">
          <Button
            variant="outline"
            size="sm"
            onClick={() => setPage(page > 1 ? page - 1 : 1)}
            disabled={page === 1 || data.length === 0 || noData}
          >
            <ChevronLeft className="h-4 w-4" />
          </Button>
          <div className="text-sm font-medium">
            Page {page} sur {totalPages || 1}
          </div>
          <Button
            variant="outline"
            size="sm"
            onClick={() => setPage(page + 1)}
            disabled={page === totalPages || totalPages === 0 || data.length === 0 || noData}
          >
            <ChevronRight className="h-4 w-4" />
          </Button>
        </div>
      </div>
    </div>
  )
}