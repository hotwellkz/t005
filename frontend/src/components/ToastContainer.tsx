import React from 'react'
import { Toast } from '../hooks/useToast'
import '../App.css'

interface ToastContainerProps {
  toasts: Toast[]
  onRemove: (id: string) => void
}

const ToastContainer: React.FC<ToastContainerProps> = ({ toasts, onRemove }) => {
  if (toasts.length === 0) return null

  return (
    <div className="toast-container">
      {toasts.map((toast) => (
        <div key={toast.id} className={`toast toast-${toast.type}`}>
          <div style={{ flex: 1 }}>
            <strong>
              {toast.type === 'success' && '✓ '}
              {toast.type === 'error' && '✕ '}
              {toast.type === 'warning' && '⚠ '}
              {toast.type === 'info' && 'ℹ '}
            </strong>
            {toast.message}
          </div>
          <button
            className="toast-close"
            onClick={() => onRemove(toast.id)}
            aria-label="Закрыть уведомление"
          >
            ×
          </button>
        </div>
      ))}
    </div>
  )
}

export default ToastContainer

