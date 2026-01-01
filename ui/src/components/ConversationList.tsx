'use client';

import { useState } from 'react';
import { format } from 'date-fns';
import { Conversation } from '@/lib/types';
import { ConversationDetail } from './ConversationDetail';

interface ConversationListProps {
  conversations: Conversation[];
  hasMore: boolean;
  total: number;
  onLoadMore: () => void;
  isLoading: boolean;
}

export function ConversationList({
  conversations,
  hasMore,
  total,
  onLoadMore,
  isLoading,
}: ConversationListProps) {
  const [selectedId, setSelectedId] = useState<string | null>(null);

  if (conversations.length === 0 && !isLoading) {
    return (
      <div className="text-center py-12 text-gray-500">
        No conversations found. Select a project to get started.
      </div>
    );
  }

  const selectedConversation = conversations.find(
    (c) => c._id.toString() === selectedId
  );

  return (
    <div className="flex gap-4">
      {/* List */}
      <div className="flex-1">
        <div className="mb-2 text-sm text-gray-600">
          Showing {conversations.length} of {total} conversations
        </div>

        <div className="bg-white rounded-lg shadow overflow-hidden">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Type
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Session
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Ingested At
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Preview
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200">
              {conversations.map((conv) => (
                <tr
                  key={conv._id.toString()}
                  onClick={() => setSelectedId(conv._id.toString())}
                  className={`cursor-pointer hover:bg-gray-50 ${
                    selectedId === conv._id.toString() ? 'bg-blue-50' : ''
                  }`}
                >
                  <td className="px-4 py-3 whitespace-nowrap text-sm">
                    <span className="px-2 py-1 bg-gray-100 rounded text-gray-700">
                      {conv.type}
                    </span>
                  </td>
                  <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-500">
                    {conv.sessionId ? conv.sessionId.slice(0, 8) + '...' : '-'}
                  </td>
                  <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-500">
                    {format(new Date(conv.ingestedAt), 'MMM d, yyyy HH:mm')}
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-500 max-w-xs truncate">
                    {typeof conv.message === 'string'
                      ? conv.message.slice(0, 50)
                      : JSON.stringify(conv.message)?.slice(0, 50)}
                    ...
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {hasMore && (
          <div className="mt-4 text-center">
            <button
              onClick={onLoadMore}
              disabled={isLoading}
              className="px-4 py-2 bg-gray-100 text-gray-700 rounded-md hover:bg-gray-200 disabled:opacity-50"
            >
              {isLoading ? 'Loading...' : 'Load More'}
            </button>
          </div>
        )}
      </div>

      {/* Detail Panel */}
      {selectedConversation && (
        <div className="w-1/2 max-w-2xl">
          <ConversationDetail
            conversation={selectedConversation}
            onClose={() => setSelectedId(null)}
          />
        </div>
      )}
    </div>
  );
}
