import { useState, useEffect, useCallback } from 'react'

/**
 * Хук для работы с localStorage с синхронизацией состояния
 */
export function useLocalStorage<T>(key: string, initialValue: T): [T, (value: T | ((val: T) => T)) => void] {
  // Инициализация из localStorage или начального значения
  const [storedValue, setStoredValue] = useState<T>(() => {
    try {
      const item = window.localStorage.getItem(key)
      return item ? JSON.parse(item) : initialValue
    } catch (error) {
      console.error(`[useLocalStorage] Error reading ${key}:`, error)
      return initialValue
    }
  })

  // Функция для обновления значения
  const setValue = useCallback(
    (value: T | ((val: T) => T)) => {
      try {
        // Поддерживаем функцию обновления как в useState
        const valueToStore = value instanceof Function ? value(storedValue) : value
        setStoredValue(valueToStore)
        window.localStorage.setItem(key, JSON.stringify(valueToStore))
      } catch (error) {
        console.error(`[useLocalStorage] Error setting ${key}:`, error)
      }
    },
    [key, storedValue]
  )

  // Синхронизация при изменении в других вкладках
  useEffect(() => {
    const handleStorageChange = (e: StorageEvent) => {
      if (e.key === key && e.newValue) {
        try {
          setStoredValue(JSON.parse(e.newValue))
        } catch (error) {
          console.error(`[useLocalStorage] Error parsing storage event for ${key}:`, error)
        }
      }
    }

    window.addEventListener('storage', handleStorageChange)
    return () => window.removeEventListener('storage', handleStorageChange)
  }, [key])

  return [storedValue, setValue]
}

