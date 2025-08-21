"use client"

import { useState, useEffect } from "react"
import { useRouter, useSearchParams } from "next/navigation"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"
import { Calendar } from "@/components/ui/calendar"
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover"
import { CalendarIcon, Filter, FilterX } from "lucide-react"
import { cn } from "@/lib/utils"
import { getFilterOptions } from "@/lib/data"
import type { DateRange } from "react-day-picker"
import { format, startOfYear, endOfYear } from "date-fns"
import { fr } from "date-fns/locale"
import { Card, CardContent } from "@/components/ui/card"
import { Checkbox } from "@/components/ui/checkbox"

export function DataFilter() {
  const router = useRouter()
  const searchParams = useSearchParams()

  // États pour les filtres
  const [statut, setStatut] = useState(searchParams.get("statut") || "all")
  const [faStatut, setFaStatut] = useState(searchParams.get("fa_statut") || "all")
  const [limiteType, setLimiteType] = useState(searchParams.get("limite_type") || "none")
  const [limiteValeur, setLimiteValeur] = useState(searchParams.get("limite_valeur") || "")
  const [filtrerParGlobal, setFiltrerParGlobal] = useState(searchParams.get("filtre_global") === "true")
  const [annee, setAnnee] = useState(searchParams.get("annee") || "all")

  // État pour la plage de dates
  const [dateRange, setDateRange] = useState<DateRange | undefined>(() => {
    const dateMin = searchParams.get("date_min")
    const dateMax = searchParams.get("date_max")

    if (dateMin && dateMax) {
      return {
        from: new Date(dateMin),
        to: new Date(dateMax),
      }
    } else if (dateMin) {
      return {
        from: new Date(dateMin),
        to: undefined,
      }
    } else if (dateMax) {
      return {
        from: undefined,
        to: new Date(dateMax),
      }
    }

    return undefined
  })

  // État pour les options de filtrage
  const [filterOptions, setFilterOptions] = useState({
    statuts: [],
    fa_statuts: [],
    annees: [],
  })

  // État pour les limites de dates
  const [minDate, setMinDate] = useState<Date | undefined>(undefined)
  const [maxDate, setMaxDate] = useState<Date | undefined>(undefined)

  // Chargement des options de filtrage
  useEffect(() => {
    async function fetchFilterOptions() {
      const options = await getFilterOptions()
      setFilterOptions(options)

      // Déterminer les dates min et max à partir des années disponibles
      if (options.annees && options.annees.length > 0) {
        const years = options.annees.map((y) => Number.parseInt(y)).sort((a, b) => a - b)
        if (years.length > 0) {
          setMinDate(startOfYear(new Date(years[0], 0, 1)))
          setMaxDate(endOfYear(new Date(years[years.length - 1], 0, 1)))
        }
      }
    }

    fetchFilterOptions()
  }, [])

  // Fonction pour appliquer les filtres
  const handleFilter = () => {
    const params = new URLSearchParams()

    if (statut !== "all") {
      params.set("statut", statut)
    }

    if (faStatut !== "all") {
      params.set("fa_statut", faStatut)
    }

    if (limiteType !== "none" && limiteValeur) {
      params.set("limite_type", limiteType)
      params.set("limite_valeur", limiteValeur)

      if (filtrerParGlobal) {
        params.set("filtre_global", "true")
      }
    }

    if (dateRange?.from) {
      params.set("date_min", dateRange.from.toISOString().split("T")[0])
    }

    if (dateRange?.to) {
      params.set("date_max", dateRange.to.toISOString().split("T")[0])
    }

    if (annee !== "all") {
      params.set("annee", annee)
    }

    router.push(`/?${params.toString()}`)
  }

  // Fonction pour réinitialiser les filtres
  const handleReset = () => {
    setStatut("all")
    setFaStatut("all")
    setLimiteType("none")
    setLimiteValeur("")
    setFiltrerParGlobal(false)
    setDateRange(undefined)
    setAnnee("all")
    router.push("/")
  }

  return (
    <Card className="mb-6">
      <CardContent>
        <div className="flex justify-between items-center mb-4">
          <h2 className="text-xl font-semibold">Filtres</h2>
          <Button variant="secondary" onClick={handleReset} className="flex items-center gap-2 text-muted-foreground cursor-pointer hover:text-primary">
            <FilterX className="h-4 w-4" />
            Réinitialiser
          </Button>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {/* Groupe 1: Statuts */}
          <div className="space-y-4">
            <div>
              <Label htmlFor="statut" className="text-sm font-medium">
                Statut utilisateur
              </Label>
              <Select value={statut} onValueChange={setStatut}>
                <SelectTrigger id="statut" className="mt-1.5">
                  <SelectValue placeholder="Tous les statuts" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">Tous les statuts</SelectItem>
                  {filterOptions.statuts.map((s) => (
                    <SelectItem key={s} value={s}>
                      {s}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div>
              <Label htmlFor="fa_statut" className="text-sm font-medium">
                Statut 2FA
              </Label>
              <Select value={faStatut} onValueChange={setFaStatut}>
                <SelectTrigger id="fa_statut" className="mt-1.5">
                  <SelectValue placeholder="Tous les statuts 2FA" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">Tous les statuts 2FA</SelectItem>
                  {filterOptions.fa_statuts.map((fa) => (
                    <SelectItem key={fa} value={fa}>
                      {fa}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          {/* Groupe 2: Dates */}
          <div className="space-y-4">
            <div>
              <Label className="text-sm font-medium">Période</Label>
              <Popover>
                <PopoverTrigger asChild>
                  <Button
                    variant={"outline"}
                    className={cn(
                      "w-full justify-start text-left font-normal mt-1.5",
                      !dateRange && "text-muted-foreground",
                    )}
                  >
                    <CalendarIcon className="mr-2 h-4 w-4" />
                    {dateRange?.from ? (
                      dateRange.to ? (
                        <>
                          {format(dateRange.from, "dd MMM yyyy", { locale: fr })} -{" "}
                          {format(dateRange.to, "dd MMM yyyy", { locale: fr })}
                        </>
                      ) : (
                        format(dateRange.from, "dd MMM yyyy", { locale: fr })
                      )
                    ) : (
                      <span>Sélectionner une période</span>
                    )}
                  </Button>
                </PopoverTrigger>
                <PopoverContent className="w-auto p-0" align="start">
                  <Calendar
                    initialFocus
                    mode="range"
                    defaultMonth={minDate}
                    selected={dateRange}
                    onSelect={setDateRange}
                    numberOfMonths={2}
                    disabled={(date) => (minDate && date < minDate) || (maxDate && date > maxDate)}
                    locale={fr}
                  />
                </PopoverContent>
              </Popover>
            </div>

            <div>
              <Label htmlFor="annee" className="text-sm font-medium">
                Année
              </Label>
              <Select value={annee} onValueChange={setAnnee}>
                <SelectTrigger id="annee" className="mt-1.5">
                  <SelectValue placeholder="Toutes les années" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">Toutes les années</SelectItem>
                  {filterOptions.annees.map((a) => (
                    <SelectItem key={a} value={a}>
                      {a}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          {/* Groupe 3: Limites et Actions */}
          <div className="space-y-4">
            <div>
              <Label className="text-sm font-medium">Limite pourcentage</Label>
              <div className="flex gap-2 mt-1.5">
                <Select value={limiteType} onValueChange={setLimiteType} className="w-1/3">
                  <SelectTrigger>
                    <SelectValue placeholder="Op." />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="none">Aucun</SelectItem>
                    <SelectItem value="lt">&lt;</SelectItem>
                    <SelectItem value="gt">&gt;</SelectItem>
                  </SelectContent>
                </Select>

                <Input
                  type="number"
                  placeholder="Valeur %"
                  value={limiteValeur}
                  onChange={(e) => setLimiteValeur(e.target.value)}
                  min="0"
                  max="100"
                  className="w-2/3"
                  disabled={limiteType === "none"}
                />
              </div>

              <div className="flex items-center space-x-2 mt-2">
                <Checkbox
                  id="filtre-global"
                  checked={filtrerParGlobal}
                  onCheckedChange={(checked) => setFiltrerParGlobal(checked === true)}
                  disabled={limiteType === "none"}
                />
                <label
                  htmlFor="filtre-global"
                  className={`text-sm font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70 ${limiteType === "none" ? "text-muted-foreground" : ""}`}
                >
                  Filtrer par pourcentage global
                </label>
              </div>
            </div>

            <div className="pt-4">
              <Button onClick={handleFilter} className="w-full flex items-center justify-center gap-2 cursor-pointer">
                <Filter className="h-4 w-4" />
                Appliquer les filtres
              </Button>
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  )
}
