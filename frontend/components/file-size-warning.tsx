"use client"

import { AlertCircle, FileText, Scissors } from "lucide-react"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"

interface FileSizeWarningProps {
  fileName: string
  fileSize: number
  onSplit: () => void
  onContinue: () => void
}

export function FileSizeWarning({ fileName, fileSize, onSplit, onContinue }: FileSizeWarningProps) {
  // Convert bytes to GB with 2 decimal places
  const fileSizeGB = (fileSize / (1024 * 1024 * 1024)).toFixed(2)

  return (
    <Alert variant="warning" className="bg-amber-50 border-amber-200 text-amber-800">
      <AlertCircle className="h-5 w-5 text-amber-600" />
      <div className="flex-1">
        <AlertTitle className="text-amber-800 flex items-center gap-2">
          <FileText className="h-4 w-4" /> Fichier volumineux détecté
        </AlertTitle>
        <AlertDescription className="text-amber-700">
          <p className="mt-2">
            Le fichier <span className="font-medium">{fileName}</span> a une taille de{" "}
            <span className="font-medium">{fileSizeGB} GB</span>.
          </p>
          <p className="mt-2">
            Les fichiers de plus de 1 GB peuvent causer des problèmes lors du traitement via le navigateur. Nous
            recommandons de diviser ce fichier en parties plus petites pour un traitement plus fiable.
          </p>
          <div className="mt-4 flex flex-col sm:flex-row gap-2">
            <Button
              variant="outline"
              className="bg-white border-amber-300 text-amber-800 hover:bg-amber-100 hover:text-amber-900 flex items-center gap-2"
              onClick={onSplit}
            >
              <Scissors className="h-4 w-4" />
              Diviser le fichier
            </Button>
            <Button
              variant="ghost"
              className="text-amber-800 hover:bg-amber-100 hover:text-amber-900"
              onClick={onContinue}
            >
              Continuer sans diviser
            </Button>
          </div>
        </AlertDescription>
      </div>
    </Alert>
  )
}
