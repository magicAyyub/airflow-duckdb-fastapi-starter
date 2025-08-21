"use client"

import { useState, useEffect } from "react"
import { useSearchParams } from "next/navigation"
import { getData } from "@/lib/data"

export function useData(page = 1, pageSize = 10) {
  const [data, setData] = useState([])
  const [isLoading, setIsLoading] = useState(true)
  const [totalPages, setTotalPages] = useState(0)
  const [message, setMessage] = useState(null)
  const [isFiltered, setIsFiltered] = useState(false)
  const searchParams = useSearchParams()

  useEffect(() => {
    const fetchData = async () => {
      setIsLoading(true)
      try {
        const result = await getData(page, pageSize)
        setData(result.data || [])
        setTotalPages(result.total_pages || 0)
        setMessage(result.message || null)
        setIsFiltered(result.is_filtered || false)
      } catch (error) {
        console.error("Erreur lors de la récupération des données:", error)
        setData([])
        setTotalPages(0)
        setMessage("error")
        setIsFiltered(false)
      } finally {
        setIsLoading(false)
      }
    }

    fetchData()
  }, [page, pageSize, searchParams])

  return { data, isLoading, totalPages, message, isFiltered }
}
