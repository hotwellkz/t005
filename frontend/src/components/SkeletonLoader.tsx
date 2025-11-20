import React from 'react'
import '../App.css'

interface SkeletonLoaderProps {
  type?: 'card' | 'text' | 'title' | 'list'
  count?: number
}

const SkeletonLoader: React.FC<SkeletonLoaderProps> = ({ type = 'card', count = 1 }) => {
  if (type === 'list') {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
        {Array.from({ length: count }).map((_, i) => (
          <div key={i} className="skeleton skeleton-card">
            <div className="skeleton skeleton-title" style={{ marginBottom: '1rem' }}></div>
            <div className="skeleton skeleton-text"></div>
            <div className="skeleton skeleton-text" style={{ width: '80%' }}></div>
            <div className="skeleton skeleton-text" style={{ width: '60%' }}></div>
          </div>
        ))}
      </div>
    )
  }

  if (type === 'card') {
    return (
      <div className="skeleton skeleton-card">
        <div className="skeleton skeleton-title" style={{ marginBottom: '1rem' }}></div>
        <div className="skeleton skeleton-text"></div>
        <div className="skeleton skeleton-text" style={{ width: '80%' }}></div>
        <div className="skeleton skeleton-text" style={{ width: '60%' }}></div>
      </div>
    )
  }

  if (type === 'title') {
    return <div className="skeleton skeleton-title"></div>
  }

  return (
    <>
      {Array.from({ length: count }).map((_, i) => (
        <div key={i} className="skeleton skeleton-text" style={{ width: i === count - 1 ? '60%' : '100%' }}></div>
      ))}
    </>
  )
}

export default SkeletonLoader
