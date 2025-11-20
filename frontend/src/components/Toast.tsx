import React, { useEffect } from 'react'
import '../App.css'

export type ToastType = 'success' | 'error' | 'info' | 'warning'

interface ToastProps {
  message: string
  type?: ToastType
  duration?: number
  onClose: () => void
}

export const Toast: React.FC<ToastProps> = ({ message, type = 'info', duration = 3000, onClose }) => {
  useEffect(() => {
    const timer = setTimeout(() => {
      onClose()
    }, duration)

    return () => clearTimeout(timer)
  }, [duration, onClose])

  const getToastClass = () => {
    switch (type) {
      case 'success':
        return 'toast toast-success'
      case 'error':
        return 'toast toast-error'
      case 'warning':
        return 'toast toast-warning'
      default:
        return 'toast toast-info'
    }
  }

  return (
    <div className={getToastClass()}>
      <span>{message}</span>
      <button
        onClick={onClose}
        className="toast-close"
        aria-label="Закрыть уведомление"
      >
        ×
      </button>
    </div>
  )
}

interface ToastContainerProps {
  toasts: Array<{ id: string; message: string; type?: ToastType }>
  onRemove: (id: string) => void
}

export const ToastContainer: React.FC<ToastContainerProps> = ({ toasts, onRemove }) => {
  if (toasts.length === 0) return null

  return (
    <div className="toast-container">
      {toasts.map((toast) => (
        <Toast
          key={toast.id}
          message={toast.message}
          type={toast.type}
          onClose={() => onRemove(toast.id)}
        />
      ))}
    </div>
  )
}

