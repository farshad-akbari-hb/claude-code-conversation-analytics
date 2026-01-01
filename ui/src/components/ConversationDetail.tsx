'use client';

import { format } from 'date-fns';
import { Conversation } from '@/lib/types';

interface ConversationDetailProps {
  conversation: Conversation;
  onClose: () => void;
}

export function ConversationDetail({ conversation, onClose }: ConversationDetailProps) {
  return (
    <div className="bg-white rounded-lg shadow p-4 sticky top-4">
      <div className="flex justify-between items-start mb-4">
        <h3 className="text-lg font-semibold">Conversation Details</h3>
        <button
          onClick={onClose}
          className="text-gray-400 hover:text-gray-600"
        >
          <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>
      </div>

      {/* Metadata */}
      <div className="space-y-2 mb-4 text-sm">
        <div className="flex">
          <span className="font-medium w-24 text-gray-600">Type:</span>
          <span className="px-2 py-0.5 bg-blue-100 text-blue-700 rounded">
            {conversation.type}
          </span>
        </div>
        <div className="flex">
          <span className="font-medium w-24 text-gray-600">Project:</span>
          <span>{conversation.projectId}</span>
        </div>
        {conversation.sessionId && (
          <div className="flex">
            <span className="font-medium w-24 text-gray-600">Session:</span>
            <span className="font-mono text-xs">{conversation.sessionId}</span>
          </div>
        )}
        <div className="flex">
          <span className="font-medium w-24 text-gray-600">Ingested:</span>
          <span>{format(new Date(conversation.ingestedAt), 'MMM d, yyyy HH:mm:ss')}</span>
        </div>
        {conversation.timestamp && (
          <div className="flex">
            <span className="font-medium w-24 text-gray-600">Timestamp:</span>
            <span>{conversation.timestamp}</span>
          </div>
        )}
        <div className="flex">
          <span className="font-medium w-24 text-gray-600">Source:</span>
          <span className="text-xs truncate max-w-xs" title={conversation.sourceFile}>
            {conversation.sourceFile.split('/').pop()}
          </span>
        </div>
      </div>

      {/* Message Content */}
      <div>
        <h4 className="font-medium text-gray-600 mb-2">Message Content</h4>
        <div className="bg-gray-50 rounded p-3 max-h-96 overflow-auto">
          <pre className="text-xs whitespace-pre-wrap break-words">
            {typeof conversation.message === 'string'
              ? conversation.message
              : JSON.stringify(conversation.message, null, 2)}
          </pre>
        </div>
      </div>

      {/* Raw JSON */}
      <details className="mt-4">
        <summary className="cursor-pointer text-sm text-gray-500 hover:text-gray-700">
          View Raw JSON
        </summary>
        <div className="mt-2 bg-gray-900 text-gray-100 rounded p-3 max-h-64 overflow-auto">
          <pre className="text-xs">
            {JSON.stringify(conversation, null, 2)}
          </pre>
        </div>
      </details>
    </div>
  );
}
