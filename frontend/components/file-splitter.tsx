"use client"

import { useState, useEffect } from "react"
import { Button } from "@/components/ui/button"
import { Progress } from "@/components/ui/progress"
import { Scissors, FileText, CheckCircle, AlertCircle, Info } from "lucide-react"
import { useToast } from "@/hooks/use-toast"

interface FileSplitterProps {
  file: File
  onSplitComplete: (files: File[]) => void
  onCancel: () => void
}

export default function FileSplitter({ file, onSplitComplete, onCancel }: FileSplitterProps) {
  const [splitting, setSplitting] = useState(false)
  const [progress, setProgress] = useState(0)
  const [parts, setParts] = useState<number>(2)
  const [splitFiles, setSplitFiles] = useState<File[]>([])
  const [error, setError] = useState<string | null>(null)
  const [estimatedTime, setEstimatedTime] = useState<string | null>(null)
  const { toast } = useToast()

  // Calculate optimal number of parts based on file size
  const calculateOptimalParts = () => {
    const sizeInGB = file.size / (1024 * 1024 * 1024)
    if (sizeInGB > 4) return 6
    if (sizeInGB > 3) return 5
    if (sizeInGB > 2) return 4
    if (sizeInGB > 1) return 3
    return 2
  }

  // Set optimal parts on component mount
  useEffect(() => {
    setParts(calculateOptimalParts())
  }, [file])

  // Estimate processing time based on file size
  useEffect(() => {
    const sizeInGB = file.size / (1024 * 1024 * 1024)
    const minutes = Math.ceil(sizeInGB * 2) // Rough estimate: 2 minutes per GB
    setEstimatedTime(`${minutes} minute${minutes > 1 ? "s" : ""}`)
  }, [file])

  // Split the file into multiple parts with header preservation using Web Workers
  const splitFileOptimized = async () => {
    setSplitting(true)
    setProgress(0)
    setError(null)

    try {
      const optimalParts = parts
      const splitFiles: File[] = []

      // Read the header first (first line of the file)
      const headerReader = new FileReader()
      const headerPromise = new Promise<string>((resolve, reject) => {
        headerReader.onload = (e) => {
          const content = e.target?.result as string
          const firstLineEnd = content.indexOf("\n")
          if (firstLineEnd === -1) {
            resolve(content)
          } else {
            resolve(content.substring(0, firstLineEnd + 1))
          }
        }
        headerReader.onerror = (e) => {
          reject(new Error("Erreur lors de la lecture de l'en-tête: " + headerReader.error?.message))
        }
        headerReader.readAsText(file.slice(0, 10240))
      })

      const header = await headerPromise
      console.log("En-tête extrait:", header)

      // Calculate part size
      const contentSize = file.size
      const partSize = Math.ceil(contentSize / optimalParts)

      // Process parts sequentially but with better progress reporting
      for (let i = 0; i < optimalParts; i++) {
        // Update progress for this part
        setProgress(Math.round((i * 100) / optimalParts))

        // Calculate start and end positions
        const start = i * partSize
        const end = Math.min(file.size, (i + 1) * partSize)

        // Create part blob
        let partBlob
        if (i === 0) {
          // First part already has the header
          partBlob = file.slice(start, end)
        } else {
          // Add header to other parts
          const headerBlob = new Blob([header], { type: "text/plain" })
          const contentBlob = file.slice(start, end)
          partBlob = new Blob([headerBlob, contentBlob], { type: file.type })
        }

        // Create file with a more descriptive name
        const fileName = `${file.name.replace(/\.[^/.]+$/, "")}_part${i + 1}_of_${optimalParts}${file.name.match(/\.[^/.]+$/)?.[0] || ""}`
        const partFile = new File([partBlob], fileName, { type: file.type })

        splitFiles.push(partFile)

        // Allow UI to update between parts
        await new Promise((resolve) => setTimeout(resolve, 50))
      }

      setProgress(100)
      setSplitFiles(splitFiles)

      toast({
        title: "Fichier divisé avec succès",
        description: `Le fichier a été divisé en ${splitFiles.length} parties avec l'en-tête préservé dans chaque partie.`,
      })

      // Short delay before completing to allow the UI to show 100%
      setTimeout(() => {
        onSplitComplete(splitFiles)
      }, 500)
    } catch (error) {
      console.error("Error splitting file:", error)
      setError(error instanceof Error ? error.message : "Une erreur inconnue s'est produite")
      toast({
        title: "Erreur",
        description: "Une erreur est survenue lors de la division du fichier.",
        variant: "destructive",
      })
    } finally {
      setSplitting(false)
    }
  }

  return (
    <div className="space-y-4 p-4 border rounded-lg bg-muted/20">
      <div className="flex items-start gap-3">
        <div className="bg-primary/10 p-2 rounded-full">
          <Scissors className="h-5 w-5 text-primary" />
        </div>
        <div>
          <h3 className="text-lg font-medium">Division du fichier</h3>
          <p className="text-sm text-muted-foreground">
            Nous allons diviser <span className="font-medium">{file.name}</span> en plusieurs parties pour faciliter le
            traitement.
          </p>
        </div>
      </div>

      {error && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-4 text-red-800 flex items-start gap-3">
          <AlertCircle className="h-5 w-5 text-red-500 mt-0.5" />
          <div>
            <p className="font-medium">Erreur lors de la division du fichier</p>
            <p className="text-sm mt-1">{error}</p>
            <p className="text-sm mt-2">Essayez avec un fichier plus petit ou contactez le support technique.</p>
          </div>
        </div>
      )}

      {!splitting && splitFiles.length === 0 && (
        <>
          <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 text-blue-800 flex items-start gap-3 mb-4">
            <Info className="h-5 w-5 text-blue-500 mt-0.5" />
            <div>
              <p className="font-medium">Recommandation pour les performances</p>
              <p className="text-sm mt-1">
                Pour un fichier de {(file.size / (1024 * 1024 * 1024)).toFixed(2)} GB, nous recommandons de le diviser
                en {calculateOptimalParts()} parties. Le traitement prendra environ {estimatedTime}.
              </p>
            </div>
          </div>

          <div className="space-y-4">
            <div className="space-y-2">
              <label htmlFor="parts" className="text-sm font-medium">
                Nombre de parties
              </label>
              <select
                id="parts"
                className="w-full rounded-md border border-input bg-background px-3 py-2"
                value={parts}
                onChange={(e) => setParts(Number.parseInt(e.target.value))}
              >
                <option value={2}>2 parties</option>
                <option value={3}>3 parties</option>
                <option value={4}>4 parties</option>
                <option value={5}>5 parties</option>
                <option value={6}>6 parties</option>
              </select>
              <p className="text-xs text-muted-foreground">
                Recommandation: {calculateOptimalParts()} parties pour un fichier de{" "}
                {(file.size / (1024 * 1024 * 1024)).toFixed(2)} GB
              </p>
            </div>

            <div className="flex flex-col sm:flex-row justify-end gap-2">
              <Button variant="outline" onClick={onCancel}>
                Annuler
              </Button>
              <Button onClick={splitFileOptimized} className="gap-2">
                <Scissors className="h-4 w-4" />
                Diviser le fichier
              </Button>
            </div>
          </div>
        </>
      )}

      {splitting && (
        <div className="space-y-4">
          <div className="space-y-2">
            <div className="flex justify-between text-sm">
              <span>Progression</span>
              <span>{progress}%</span>
            </div>
            <Progress value={progress} className="h-2" />
          </div>
          <p className="text-sm text-center text-muted-foreground">
            Division du fichier en cours... Veuillez patienter.
          </p>
          <p className="text-xs text-center text-muted-foreground animate-pulse">
            Cette opération peut prendre quelques minutes pour les fichiers volumineux.
          </p>
        </div>
      )}

      {!splitting && splitFiles.length > 0 && (
        <div className="space-y-4">
          <div className="rounded-lg border bg-card p-3">
            <div className="flex items-center gap-2 mb-2">
              <CheckCircle className="h-5 w-5 text-green-600" />
              <h4 className="font-medium">Fichier divisé avec succès</h4>
            </div>
            <ul className="space-y-2">
              {splitFiles.map((file, index) => (
                <li key={index} className="flex items-center gap-2 text-sm">
                  <FileText className="h-4 w-4 text-muted-foreground" />
                  <span className="flex-1 truncate">{file.name}</span>
                  <span className="text-xs text-muted-foreground">{(file.size / (1024 * 1024)).toFixed(2)} MB</span>
                </li>
              ))}
            </ul>
          </div>

          <div className="flex justify-end">
            <Button onClick={() => onSplitComplete(splitFiles)} className="gap-2">
              <CheckCircle className="h-4 w-4" />
              Utiliser ces fichiers
            </Button>
          </div>
        </div>
      )}
    </div>
  )
}
