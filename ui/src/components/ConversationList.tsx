'use client';

import { useState } from 'react';
import { format } from 'date-fns';
import { Loader2, MessageSquare } from 'lucide-react';

import { Conversation } from '@/lib/types';
import { ConversationDetail } from './ConversationDetail';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { ScrollArea } from '@/components/ui/scroll-area';
import { cn } from '@/lib/utils';

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
      <Card>
        <CardContent className="flex flex-col items-center justify-center py-16">
          <MessageSquare className="h-12 w-12 text-muted-foreground mb-4" />
          <p className="text-muted-foreground text-lg">
            No conversations found
          </p>
          <p className="text-muted-foreground text-sm mt-1">
            Select a project to get started
          </p>
        </CardContent>
      </Card>
    );
  }

  const selectedConversation = conversations.find(
    (c) => c._id.toString() === selectedId
  );

  return (
    <div className="flex gap-6">
      {/* List */}
      <div className={cn('flex-1', selectedConversation && 'max-w-[60%]')}>
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-base font-medium">
              Showing {conversations.length} of {total} conversations
            </CardTitle>
          </CardHeader>
          <CardContent className="p-0">
            <ScrollArea className="h-[600px]">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="w-[120px]">Type</TableHead>
                    <TableHead className="w-[100px]">Session</TableHead>
                    <TableHead className="w-[160px]">Ingested At</TableHead>
                    <TableHead>Preview</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {conversations.map((conv) => (
                    <TableRow
                      key={conv._id.toString()}
                      onClick={() => setSelectedId(conv._id.toString())}
                      className={cn(
                        'cursor-pointer',
                        selectedId === conv._id.toString() && 'bg-muted'
                      )}
                    >
                      <TableCell>
                        <Badge variant="secondary">{conv.type}</Badge>
                      </TableCell>
                      <TableCell className="font-mono text-xs text-muted-foreground">
                        {conv.sessionId ? conv.sessionId.slice(0, 8) + '...' : '-'}
                      </TableCell>
                      <TableCell className="text-muted-foreground">
                        {format(new Date(conv.ingestedAt), 'MMM d, yyyy HH:mm')}
                      </TableCell>
                      <TableCell className="max-w-xs truncate text-muted-foreground">
                        {typeof conv.message === 'string'
                          ? conv.message.slice(0, 60)
                          : JSON.stringify(conv.message)?.slice(0, 60)}
                        ...
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </ScrollArea>

            {hasMore && (
              <div className="p-4 border-t text-center">
                <Button
                  variant="outline"
                  onClick={onLoadMore}
                  disabled={isLoading}
                >
                  {isLoading ? (
                    <>
                      <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                      Loading...
                    </>
                  ) : (
                    'Load More'
                  )}
                </Button>
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Detail Panel */}
      {selectedConversation && (
        <div className="w-[40%] max-w-2xl">
          <ConversationDetail
            conversation={selectedConversation}
            onClose={() => setSelectedId(null)}
          />
        </div>
      )}
    </div>
  );
}
