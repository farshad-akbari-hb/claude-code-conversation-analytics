'use client';

import { useState } from 'react';
import { format } from 'date-fns';
import { X, FileText, Code, Clock, Folder, Hash, Copy, Check } from 'lucide-react';

import { Conversation } from '@/lib/types';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from '@/components/ui/card';
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';
import { Separator } from '@/components/ui/separator';

interface ConversationDetailProps {
  conversation: Conversation;
  onClose: () => void;
}

export function ConversationDetail({ conversation, onClose }: ConversationDetailProps) {
  const [copiedMessage, setCopiedMessage] = useState(false);
  const [copiedJson, setCopiedJson] = useState(false);

  const copyToClipboard = async (text: string, type: 'message' | 'json') => {
    await navigator.clipboard.writeText(text);
    if (type === 'message') {
      setCopiedMessage(true);
      setTimeout(() => setCopiedMessage(false), 2000);
    } else {
      setCopiedJson(true);
      setTimeout(() => setCopiedJson(false), 2000);
    }
  };

  const getMessageContent = () => {
    return typeof conversation.message === 'string'
      ? conversation.message
      : JSON.stringify(conversation.message, null, 2);
  };

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
          <div className="flex items-center justify-between mb-2">
            <h4 className="text-sm font-medium">Message Content</h4>
            <Button
              variant="ghost"
              size="icon"
              className="h-6 w-6"
              onClick={() => copyToClipboard(getMessageContent(), 'message')}
            >
              {copiedMessage ? (
                <Check className="h-3.5 w-3.5 text-green-500" />
              ) : (
                <Copy className="h-3.5 w-3.5" />
              )}
            </Button>
          </div>
          <ScrollArea className="h-[300px]">
            <div className="bg-muted rounded-md p-3 min-w-0">
              <pre className="text-xs whitespace-pre-wrap break-words font-mono overflow-x-auto">
                {getMessageContent()}
              </pre>
            </div>
            <ScrollBar orientation="horizontal" />
          </ScrollArea>
        </div>

        <Separator />

        {/* Raw JSON */}
        <details className="group">
          <summary className="cursor-pointer text-sm text-muted-foreground hover:text-foreground flex items-center justify-between">
            <span className="flex items-center gap-2">
              <Code className="h-4 w-4" />
              View Raw JSON
            </span>
            <Button
              variant="ghost"
              size="icon"
              className="h-6 w-6"
              onClick={(e) => {
                e.preventDefault();
                copyToClipboard(JSON.stringify(conversation, null, 2), 'json');
              }}
            >
              {copiedJson ? (
                <Check className="h-3.5 w-3.5 text-green-500" />
              ) : (
                <Copy className="h-3.5 w-3.5" />
              )}
            </Button>
          </summary>
          <ScrollArea className="h-[200px] mt-2">
            <div className="bg-zinc-900 text-zinc-100 rounded-md p-3 min-w-0">
              <pre className="text-xs font-mono whitespace-pre-wrap break-words overflow-x-auto">
                {JSON.stringify(conversation, null, 2)}
              </pre>
            </div>
            <ScrollBar orientation="horizontal" />
          </ScrollArea>
        </details>
      </CardContent>
    </Card>
  );
}
