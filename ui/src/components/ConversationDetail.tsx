'use client';

import { format } from 'date-fns';
import { X, FileText, Code, Clock, Folder, Hash } from 'lucide-react';

import { Conversation } from '@/lib/types';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from '@/components/ui/card';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Separator } from '@/components/ui/separator';

interface ConversationDetailProps {
  conversation: Conversation;
  onClose: () => void;
}

export function ConversationDetail({ conversation, onClose }: ConversationDetailProps) {
  return (
    <Card className="sticky top-4">
      <CardHeader className="pb-3">
        <div className="flex justify-between items-start">
          <CardTitle className="text-lg">Conversation Details</CardTitle>
          <Button variant="ghost" size="icon" onClick={onClose}>
            <X className="h-4 w-4" />
          </Button>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Metadata */}
        <div className="space-y-3">
          <div className="flex items-center gap-2">
            <Hash className="h-4 w-4 text-muted-foreground" />
            <span className="text-sm text-muted-foreground w-20">Type</span>
            <Badge>{conversation.type}</Badge>
          </div>

          <div className="flex items-center gap-2">
            <Folder className="h-4 w-4 text-muted-foreground" />
            <span className="text-sm text-muted-foreground w-20">Project</span>
            <span className="text-sm font-medium">{conversation.projectId}</span>
          </div>

          {conversation.sessionId && (
            <div className="flex items-center gap-2">
              <Code className="h-4 w-4 text-muted-foreground" />
              <span className="text-sm text-muted-foreground w-20">Session</span>
              <code className="text-xs bg-muted px-2 py-0.5 rounded">
                {conversation.sessionId}
              </code>
            </div>
          )}

          <div className="flex items-center gap-2">
            <Clock className="h-4 w-4 text-muted-foreground" />
            <span className="text-sm text-muted-foreground w-20">Ingested</span>
            <span className="text-sm">
              {format(new Date(conversation.ingestedAt), 'MMM d, yyyy HH:mm:ss')}
            </span>
          </div>

          {conversation.timestamp && (
            <div className="flex items-center gap-2">
              <Clock className="h-4 w-4 text-muted-foreground" />
              <span className="text-sm text-muted-foreground w-20">Timestamp</span>
              <span className="text-sm">{conversation.timestamp}</span>
            </div>
          )}

          <div className="flex items-center gap-2">
            <FileText className="h-4 w-4 text-muted-foreground" />
            <span className="text-sm text-muted-foreground w-20">Source</span>
            <span
              className="text-xs truncate max-w-[200px]"
              title={conversation.sourceFile}
            >
              {conversation.sourceFile.split('/').pop()}
            </span>
          </div>
        </div>

        <Separator />

        {/* Message Content */}
        <div>
          <h4 className="text-sm font-medium mb-2">Message Content</h4>
          <ScrollArea className="h-[300px]">
            <div className="bg-muted rounded-md p-3">
              <pre className="text-xs whitespace-pre-wrap break-words font-mono">
                {typeof conversation.message === 'string'
                  ? conversation.message
                  : JSON.stringify(conversation.message, null, 2)}
              </pre>
            </div>
          </ScrollArea>
        </div>

        <Separator />

        {/* Raw JSON */}
        <details className="group">
          <summary className="cursor-pointer text-sm text-muted-foreground hover:text-foreground flex items-center gap-2">
            <Code className="h-4 w-4" />
            View Raw JSON
          </summary>
          <ScrollArea className="h-[200px] mt-2">
            <div className="bg-zinc-900 text-zinc-100 rounded-md p-3">
              <pre className="text-xs font-mono">
                {JSON.stringify(conversation, null, 2)}
              </pre>
            </div>
          </ScrollArea>
        </details>
      </CardContent>
    </Card>
  );
}
