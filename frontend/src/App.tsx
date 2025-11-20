import { useState } from 'react'
import './App.css'
import VideoGeneration from './components/VideoGeneration'
import ChannelSettings from './components/ChannelSettings'
import ToastContainer from './components/ToastContainer'
import { useToast } from './hooks/useToast'

type Tab = 'generation' | 'settings'

function App() {
  const [activeTab, setActiveTab] = useState<Tab>('generation')
  const toast = useToast()

  return (
    <div className="app">
      <header className="app-header">
        <h1>WhiteCoding Studio</h1>
        <nav className="tabs">
          <button
            className={activeTab === 'generation' ? 'active' : ''}
            onClick={() => setActiveTab('generation')}
            aria-label="Переключить на вкладку генерации видео"
          >
            Генерация видео
          </button>
          <button
            className={activeTab === 'settings' ? 'active' : ''}
            onClick={() => setActiveTab('settings')}
            aria-label="Переключить на вкладку настроек каналов"
          >
            Настройки каналов
          </button>
        </nav>
      </header>
      <main className="app-main">
        {activeTab === 'generation' && <VideoGeneration />}
        {activeTab === 'settings' && <ChannelSettings />}
      </main>
      <ToastContainer toasts={toast.toasts} onRemove={toast.removeToast} />
    </div>
  )
}

export default App

