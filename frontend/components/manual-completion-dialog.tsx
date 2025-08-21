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

import { CheckCircle, Loader2 } from 'lucide-react'

interface ManualCompletionDialogProps {
  isOpen: boolean
  onClose: () => void
  onConfirm: () => void
  processedCount: number
  totalCount: number
  isConfirming: boolean
}

export function ManualCompletionDialog({
  isOpen,
  onClose,
  onConfirm,
  processedCount,
  totalCount,
  isConfirming,
}: ManualCompletionDialogProps) {
  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="sm:max-w-[425px]">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <CheckCircle className="h-5 w-5 text-green-500" />
            Terminer le traitement
          </DialogTitle>
          <DialogDescription>
            {processedCount} sur {totalCount} parties ont été traitées avec succès.
          </DialogDescription>
        </DialogHeader>

        <div className="py-4">
          <div className="rounded-lg border border-amber-100 bg-amber-50 p-3 text-amber-800">
            <p className="text-sm font-medium mb-1">Attention :</p>
            <p className="text-sm">
              Certaines parties n'ont pas pu être traitées. Vous pouvez tout de même terminer le traitement avec les
              parties qui ont réussi.
            </p>
          </div>

          <div className="mt-4 text-sm text-muted-foreground">
            <p>En confirmant :</p>
            <ul className="list-disc pl-5 mt-2 space-y-1">
              <li>Les données des parties traitées avec succès seront conservées</li>
              <li>Les parties non traitées seront ignorées</li>
              <li>Vous pourrez réessayer d'importer les parties manquantes ultérieurement</li>
            </ul>
          </div>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={onClose} disabled={isConfirming}>
            Annuler
          </Button>
          <Button onClick={onConfirm} disabled={isConfirming} className="gap-2">
            {isConfirming ? (
              <>
                <Loader2 className="h-4 w-4 animate-spin" />
                Finalisation...
              </>
            ) : (
              "Terminer avec les parties réussies"
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}