import { toast } from "@/hooks/use-toast"

// Types pour les notifications
export interface UploadNotification {
  id: string
  timestamp: number
  fileName: string
  mappingFileName: string
  rowsProcessed: number
  totalRows: number
  duplicatesFound?: number
  isRead: boolean
  type: "upload" | "append" | "purge"
}

// Clé de stockage local
const NOTIFICATIONS_STORAGE_KEY = "operator_dashboard_notifications"

// Fonction pour générer un ID unique
export function generateId(): string {
  return Date.now().toString(36) + Math.random().toString(36).substring(2, 9)
}

// Fonction pour récupérer toutes les notifications
export function getNotifications(): UploadNotification[] {
  if (typeof window === "undefined") return []

  const storedNotifications = localStorage.getItem(NOTIFICATIONS_STORAGE_KEY)
  if (!storedNotifications) return []

  try {
    return JSON.parse(storedNotifications)
  } catch (error) {
    console.error("Erreur lors de la récupération des notifications:", error)
    return []
  }
}

// Fonction pour ajouter une notification
export function addNotification(
  notification: Omit<UploadNotification, "id" | "timestamp" | "isRead">,
): UploadNotification {
  const newNotification: UploadNotification = {
    ...notification,
    id: generateId(),
    timestamp: Date.now(),
    isRead: false,
  }

  const notifications = getNotifications()
  const updatedNotifications = [newNotification, ...notifications]

  localStorage.setItem(NOTIFICATIONS_STORAGE_KEY, JSON.stringify(updatedNotifications))

  return newNotification
}

// Fonction pour marquer une notification comme lue
export function markNotificationAsRead(id: string): void {
  const notifications = getNotifications()
  const updatedNotifications = notifications.map((notification) =>
    notification.id === id ? { ...notification, isRead: true } : notification,
  )

  localStorage.setItem(NOTIFICATIONS_STORAGE_KEY, JSON.stringify(updatedNotifications))
}

// Fonction pour marquer toutes les notifications comme lues
export function markAllNotificationsAsRead(): void {
  const notifications = getNotifications()
  const updatedNotifications = notifications.map((notification) => ({ ...notification, isRead: true }))

  localStorage.setItem(NOTIFICATIONS_STORAGE_KEY, JSON.stringify(updatedNotifications))
}

// Fonction pour supprimer une notification
export function deleteNotification(id: string): void {
  const notifications = getNotifications()
  const updatedNotifications = notifications.filter((notification) => notification.id !== id)

  localStorage.setItem(NOTIFICATIONS_STORAGE_KEY, JSON.stringify(updatedNotifications))
}

// Fonction pour purger toutes les notifications
export function purgeNotifications(): void {
  localStorage.removeItem(NOTIFICATIONS_STORAGE_KEY)

  toast({
    title: "Historique effacé",
    description: "L'historique des chargements a été effacé avec succès.",
  })
}

// Fonction pour obtenir le nombre de notifications non lues
export function getUnreadCount(): number {
  const notifications = getNotifications()
  return notifications.filter((notification) => !notification.isRead).length
}

// Fonction pour formater la date d'une notification
export function formatNotificationDate(timestamp: number): string {
  const date = new Date(timestamp)
  return date.toLocaleDateString("fr-FR", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  })
}
