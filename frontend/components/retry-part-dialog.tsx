"use client"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { AlertCircle, RefreshCw, Loader2 } from 'lucide-react'

interface RetryPartDialogProps {
  isOpen: boolean
  onClose: () => void
  partName: string
  partIndex: number
  totalParts: number
  onRetry: () => void
  isRetrying: boolean
}

export function RetryPartDialog({
  isOpen,
  onClose,
  partName,
  partIndex,
  totalParts,
  onRetry,
  isRetrying,
}: RetryPartDialogProps) {
  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="sm:max-w-[425px]">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <AlertCircle className="h-5 w-5 text-red-500" />
            Échec du traitement
          </DialogTitle>
          <DialogDescription>
            La partie {partIndex + 1} sur {totalParts} n'a pas pu être traitée correctement.
          </DialogDescription>
        </DialogHeader>

        <div className="py-4">
          <div className="rounded-lg border border-red-100 bg-red-50 p-3 text-red-800">
            <p className="text-sm font-medium mb-1">Détails de l'erreur :</p>
            <p className="text-sm">
              Le fichier <span className="font-medium">{partName}</span> n'a pas pu être traité. Cela peut être dû à un
              problème temporaire du serveur ou à un problème avec le fichier lui-même.
            </p>
          </div>

          <div className="mt-4 text-sm text-muted-foreground">
            <p>Vous pouvez :</p>
            <ul className="list-disc pl-5 mt-2 space-y-1">
              <li>Réessayer le traitement de cette partie</li>
              <li>Ignorer cette partie et continuer avec les suivantes</li>
              <li>Annuler le traitement et recommencer avec un fichier différent</li>
            </ul>
          </div>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={onClose} disabled={isRetrying}>
            Ignorer cette partie
          </Button>
          <Button onClick={onRetry} disabled={isRetrying} className="gap-2">
            {isRetrying ? (
              <>
                <Loader2 className="h-4 w-4 animate-spin" />
                Réessai en cours...
              </>
            ) : (
              <>
                <RefreshCw className="h-4 w-4" />
                Réessayer
              </>
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}