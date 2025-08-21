"use client"

import type React from "react"

import { useState, useEffect } from "react"
import { Bell, Check, Trash2, X, FileUp, Database, RefreshCw } from "lucide-react"
import { Button } from "@/components/ui/button"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuGroup,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog"
import { ScrollArea } from "@/components/ui/scroll-area"
import {
  getNotifications,
  markNotificationAsRead,
  markAllNotificationsAsRead,
  purgeNotifications,
  formatNotificationDate,
  getUnreadCount,
  type UploadNotification,
} from "@/lib/notifications"

export function NotificationsDropdown() {
  const [notifications, setNotifications] = useState<UploadNotification[]>([])
  const [unreadCount, setUnreadCount] = useState(0)
  const [isPurgeDialogOpen, setIsPurgeDialogOpen] = useState(false)
  const [isOpen, setIsOpen] = useState(false)

  // Charger les notifications au montage et quand le dropdown s'ouvre
  useEffect(() => {
    if (isOpen) {
      const notifs = getNotifications()
      setNotifications(notifs)
      setUnreadCount(getUnreadCount())
    }
  }, [isOpen])

  // Rafraîchir les notifications toutes les 5 secondes si le dropdown est ouvert
  useEffect(() => {
    if (!isOpen) return

    const interval = setInterval(() => {
      const notifs = getNotifications()
      setNotifications(notifs)
      setUnreadCount(getUnreadCount())
    }, 5000)

    return () => clearInterval(interval)
  }, [isOpen])

  // Rafraîchir le compteur de notifications non lues toutes les 5 secondes
  useEffect(() => {
    const interval = setInterval(() => {
      setUnreadCount(getUnreadCount())
    }, 5000)

    return () => clearInterval(interval)
  }, [])

  const handleMarkAsRead = (id: string, event: React.MouseEvent) => {
    event.stopPropagation()
    markNotificationAsRead(id)
    setNotifications((prev) =>
      prev.map((notification) => (notification.id === id ? { ...notification, isRead: true } : notification)),
    )
    setUnreadCount((prev) => Math.max(0, prev - 1))
  }

  const handleMarkAllAsRead = () => {
    markAllNotificationsAsRead()
    setNotifications((prev) => prev.map((notification) => ({ ...notification, isRead: true })))
    setUnreadCount(0)
  }

  const handlePurgeNotifications = () => {
    purgeNotifications()
    setNotifications([])
    setUnreadCount(0)
    setIsPurgeDialogOpen(false)
  }

  const getNotificationIcon = (type: string) => {
    switch (type) {
      case "upload":
        return <FileUp className="h-4 w-4 text-blue-500" />
      case "append":
        return <RefreshCw className="h-4 w-4 text-green-500" />
      case "purge":
        return <Trash2 className="h-4 w-4 text-red-500" />
      default:
        return <Database className="h-4 w-4 text-gray-500" />
    }
  }

  const getNotificationTitle = (notification: UploadNotification) => {
    switch (notification.type) {
      case "upload":
        return "Nouveau fichier chargé"
      case "append":
        return "Données ajoutées"
      case "purge":
        return "Données purgées"
      default:
        return "Notification"
    }
  }

  return (
    <>
      <DropdownMenu open={isOpen} onOpenChange={setIsOpen}>
        <DropdownMenuTrigger asChild>
          <Button variant="outline" size="icon" className="relative">
            <Bell className="h-[1.2rem] w-[1.2rem]" />
            {unreadCount > 0 && (
              <span className="absolute -top-1 -right-1 flex h-5 w-5 items-center justify-center rounded-full bg-red-500 text-[10px] font-medium text-white">
                {unreadCount > 9 ? "9+" : unreadCount}
              </span>
            )}
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent className="w-80" align="end">
          <DropdownMenuLabel className="flex items-center justify-between">
            <span>Historique des chargements</span>
            {notifications.length > 0 && (
              <div className="flex gap-1">
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-7 px-2 text-xs"
                  onClick={handleMarkAllAsRead}
                  disabled={unreadCount === 0}
                >
                  <Check className="h-3.5 w-3.5 mr-1" />
                  Tout marquer comme lu
                </Button>
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-7 px-2 text-xs text-red-500 hover:text-red-600 hover:bg-red-50"
                  onClick={() => setIsPurgeDialogOpen(true)}
                >
                  <Trash2 className="h-3.5 w-3.5 mr-1" />
                  Purger
                </Button>
              </div>
            )}
          </DropdownMenuLabel>
          <DropdownMenuSeparator />

          {notifications.length === 0 ? (
            <div className="py-6 text-center text-sm text-muted-foreground">
              <Database className="h-10 w-10 mx-auto mb-2 text-muted-foreground/50" />
              <p>Aucun historique de chargement</p>
              <p className="text-xs mt-1">Les chargements de fichiers apparaîtront ici</p>
            </div>
          ) : (
            <ScrollArea className="h-[300px]">
              <DropdownMenuGroup>
                {notifications.map((notification) => (
                  <DropdownMenuItem key={notification.id} className="flex flex-col items-start p-3 cursor-default">
                    <div className="flex w-full justify-between items-start">
                      <div className="flex items-start gap-2">
                        <div className="mt-0.5">{getNotificationIcon(notification.type)}</div>
                        <div>
                          <div className="font-medium text-sm flex items-center gap-2">
                            {getNotificationTitle(notification)}
                            {!notification.isRead && <span className="h-2 w-2 rounded-full bg-blue-500"></span>}
                          </div>
                          <div className="text-xs text-muted-foreground mt-0.5">
                            {formatNotificationDate(notification.timestamp)}
                          </div>
                        </div>
                      </div>

                      {!notification.isRead && (
                        <Button
                          variant="ghost"
                          size="sm"
                          className="h-6 w-6 p-0 rounded-full"
                          onClick={(e) => handleMarkAsRead(notification.id, e)}
                        >
                          <X className="h-3.5 w-3.5" />
                          <span className="sr-only">Marquer comme lu</span>
                        </Button>
                      )}
                    </div>

                    <div className="text-xs mt-1 text-muted-foreground">
                      <p>
                        Fichier: <span className="font-medium text-foreground">{notification.fileName}</span>
                      </p>
                      {notification.type !== "purge" && (
                        <>
                          <p className="mt-0.5">
                            Lignes traitées:{" "}
                            <span className="font-medium text-foreground">{notification.rowsProcessed}</span>
                          </p>
                        </>
                      )}
                    </div>
                  </DropdownMenuItem>
                ))}
              </DropdownMenuGroup>
            </ScrollArea>
          )}
        </DropdownMenuContent>
      </DropdownMenu>

      <AlertDialog open={isPurgeDialogOpen} onOpenChange={setIsPurgeDialogOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Purger l&apos;historique</AlertDialogTitle>
            <AlertDialogDescription>
              Êtes-vous sûr de vouloir effacer tout l&apos;historique des chargements ? Cette action est irréversible.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Annuler</AlertDialogCancel>
            <AlertDialogAction onClick={handlePurgeNotifications} className="bg-red-600 hover:bg-red-700">
              Purger l&apos;historique
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  )
}
