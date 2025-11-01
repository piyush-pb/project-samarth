import { useState, useEffect, useRef } from 'react'
import axios from 'axios'
import { Send, Loader, AlertCircle, Database, Sparkles } from 'lucide-react'

const sampleQuestions = [
  "Compare the average annual rainfall in Maharashtra and Gujarat for the last 5 years. In parallel, list the top 5 most produced cereals by volume in each state during the same period.",
  "Identify the district in Punjab with the highest wheat production in 2023 and compare that with the district with the lowest wheat production in Haryana.",
  "Analyze the rice production trend in West Bengal over the last decade. Correlate this trend with the corresponding rainfall data for the same period.",
  "A policy advisor is proposing a scheme to promote millets over rice in Karnataka. Based on historical data from the last 10 years, what are the three most compelling data-backed arguments to support this policy?"
]

export default function App() {
  const [messages, setMessages] = useState([])
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const messagesEndRef = useRef(null)

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }

  useEffect(() => {
    scrollToBottom()
  }, [messages])

  const timestampNow = () => new Date().toISOString()

  const sendQuery = async (queryText) => {
    const text = (typeof queryText === 'string' ? queryText : input).trim()
    setError('')
    if (!text) {
      setError('Please enter a question.')
      return
    }

    const userMsg = {
      role: 'user',
      content: text,
      sources: [],
      data: {},
      timestamp: timestampNow(),
    }
    setMessages((prev) => [...prev, userMsg])
    setLoading(true)
    setInput('')

    try {
      const res = await axios.post('/api/query', { query: text })
      const data = res?.data
      if (!data) {
        const errMsg = 'No response received'
        setMessages((prev) => [...prev, {
          role: 'error',
          content: errMsg,
          sources: [],
          data: {},
          timestamp: timestampNow(),
        }])
        setError(errMsg)
      } else {
        const assistantMsg = {
          role: 'assistant',
          content: data.answer || 'No answer generated.',
          sources: Array.isArray(data.sources) ? data.sources : [],
          data: data.data || {},
          timestamp: timestampNow(),
        }
        setMessages((prev) => [...prev, assistantMsg])
      }
    } catch (e) {
      const backendMsg = e?.response?.data?.detail?.message || e?.response?.data?.message
      const errMsg = backendMsg || (e?.message?.includes('Network') ? 'Failed to connect to server' : 'An error occurred')
      setMessages((prev) => [...prev, {
        role: 'error',
        content: errMsg,
        sources: [],
        data: {},
        timestamp: timestampNow(),
      }])
      setError(errMsg)
    } finally {
      setLoading(false)
    }
  }

  const onSubmit = (e) => {
    e.preventDefault()
    sendQuery()
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 to-green-50 p-4 md:p-8">
      <div className="max-w-5xl mx-auto space-y-6">
        {/* Header */}
        <header className="bg-white rounded-lg shadow-md p-6">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-3xl font-bold">🌾 Project Samarth</h1>
              <p className="text-gray-600 mt-1">Intelligent Q&A System for Indian Agricultural & Climate Data</p>
            </div>
            <div className="inline-flex items-center gap-2 text-sm px-3 py-1 rounded-full bg-blue-50 text-blue-700 border border-blue-200">
              <Sparkles className="w-4 h-4" aria-hidden="true" />
              <span>Powered by Google Gemini • data.gov.in API</span>
            </div>
          </div>
        </header>

        {/* Sample Questions */}
        {messages.length === 0 && (
          <section aria-label="Sample questions" className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {sampleQuestions.map((q, idx) => (
              <button
                key={idx}
                onClick={() => sendQuery(q)}
                className="text-left bg-white rounded-lg shadow-md p-4 border border-transparent hover:border-blue-300 hover:shadow-lg transform hover:scale-[1.01] transition-all focus:outline-none focus:ring-2 focus:ring-blue-400"
                aria-label={`Sample question ${idx + 1}`}
              >
                <div className="flex items-start gap-3">
                  <Database className="w-5 h-5 text-blue-600 mt-1" aria-hidden="true" />
                  <p className="text-gray-800">{q}</p>
                </div>
              </button>
            ))}
          </section>
        )}

        {/* Messages Area */}
        <section className="bg-white rounded-lg shadow-lg p-4 md:p-6 min-h-[400px] max-h-[60vh] overflow-y-auto" aria-live="polite">
          {messages.map((m, i) => (
            <MessageBubble key={i} message={m} />
          ))}

          {loading && (
            <div className="flex flex-col items-center justify-center py-10 animate-pulse">
              <Loader className="w-6 h-6 text-blue-600 animate-spin" aria-hidden="true" />
              <p className="mt-3 text-sm text-gray-600">Analyzing agricultural data...</p>
            </div>
          )}

          <div ref={messagesEndRef} />
        </section>

        {/* Error banner */}
        {error && (
          <div className="flex items-center gap-2 bg-red-50 text-red-700 border border-red-200 rounded-lg p-3">
            <AlertCircle className="w-4 h-4" aria-hidden="true" />
            <span className="text-sm">{error}</span>
            <button
              className="ml-auto text-sm underline decoration-red-400 hover:opacity-80"
              onClick={() => sendQuery(messages[messages.length - 1]?.content || '')}
            >
              Retry
            </button>
          </div>
        )}

        {/* Input */}
        <form onSubmit={onSubmit} className="bg-white rounded-lg shadow-md p-4">
          <div className="flex items-center gap-3">
            <input
              type="text"
              className="flex-1 border border-gray-300 rounded-lg px-4 py-3 focus:outline-none focus:ring-2 focus:ring-blue-400"
              placeholder="Ask a question about Indian agriculture and climate data..."
              aria-label="Your question"
              value={input}
              onChange={(e) => setInput(e.target.value)}
              disabled={loading}
              onKeyDown={(e) => { if (e.key === 'Enter' && !e.shiftKey) { onSubmit(e) } }}
            />
            <button
              type="submit"
              className="inline-flex items-center justify-center px-4 py-3 rounded-lg bg-blue-500 text-white disabled:opacity-60 hover:bg-blue-600 transition-colors focus:outline-none focus:ring-2 focus:ring-blue-400"
              aria-label="Send question"
              disabled={loading || input.trim().length === 0}
            >
              {loading ? <Loader className="w-5 h-5 animate-spin" aria-hidden="true" /> : <Send className="w-5 h-5" aria-hidden="true" />}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}

function MessageBubble({ message }) {
  const isUser = message.role === 'user'
  const isAssistant = message.role === 'assistant'
  const isError = message.role === 'error'
  const [showSources, setShowSources] = useState(true)

  if (isError) {
    return (
      <div className="flex justify-center my-2">
        <div className="max-w-3xl w-full bg-red-50 text-red-800 border border-red-200 rounded-lg p-3 text-center text-sm">
          {message.content}
        </div>
      </div>
    )
  }

  return (
    <div className={`my-3 flex ${isUser ? 'justify-end' : 'justify-start'}`}>
      <div className={`max-w-3xl rounded-lg px-4 py-3 shadow ${isUser ? 'bg-blue-500 text-white' : 'bg-gray-100 text-gray-900'}`}>
        <div className="whitespace-pre-wrap leading-relaxed">{message.content}</div>

        {/* Sources */}
        {Array.isArray(message.sources) && message.sources.length > 0 && (
          <div className="mt-3">
            <button
              onClick={() => setShowSources(v => !v)}
              className="text-sm underline decoration-gray-400 hover:opacity-80"
              aria-expanded={showSources}
              aria-controls={`sources-${message.timestamp}`}
            >
              {showSources ? 'Hide' : 'Show'} Sources
            </button>
            {showSources && (
              <div id={`sources-${message.timestamp}`} className={`mt-2 rounded-lg ${isUser ? 'bg-blue-400/20' : 'bg-white'} p-3`}>
                <h4 className="font-semibold mb-2">📚 Data Sources</h4>
                <ul className="space-y-2">
                  {message.sources.map((s, idx) => (
                    <li key={idx} className="text-sm">
                      <div className="font-semibold">{s.dataset || 'Dataset'}</div>
                      <div className="text-gray-600">Filters: {formatFilters(s.filters_applied)}</div>
                      <div className="text-gray-600">Records: {s.records_retrieved ?? 'N/A'}</div>
                      {s.url && (
                        <a
                          href={s.url}
                          target="_blank"
                          rel="noreferrer"
                          className="inline-flex items-center gap-1 text-blue-600 hover:underline mt-1"
                        >
                          <Database className="w-4 h-4" aria-hidden="true" />
                          <span>Open dataset</span>
                        </a>
                      )}
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </div>
        )}

        {/* Optional timestamp */}
        {message.timestamp && (
          <div className={`text-xs mt-2 ${isUser ? 'text-white/80' : 'text-gray-500'}`}>{new Date(message.timestamp).toLocaleString()}</div>
        )}
      </div>
    </div>
  )
}

function formatFilters(filters) {
  if (!filters || typeof filters !== 'object') return '—'
  try {
    return Object.entries(filters).map(([k, v]) => `${k}=${v}`).join(', ')
  } catch {
    return '—'
  }
}


