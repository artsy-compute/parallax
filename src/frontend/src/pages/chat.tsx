import { useEffect, useRef } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { DrawerLayout } from '../components/common';
import { ChatInput, ChatMessages } from '../components/inputs';
import { useChat } from '../services';

export default function PageChat() {
  const navigate = useNavigate();
  const { conversationId: routeConversationId = '' } = useParams();
  const [{ conversationId, status }, { loadConversation }] = useChat();
  const lastRouteLoadRef = useRef('');
  const pendingRouteConversationRef = useRef('');

  useEffect(() => {
    const normalizedRouteConversationId = String(routeConversationId || '').trim();
    if (!normalizedRouteConversationId || lastRouteLoadRef.current === normalizedRouteConversationId) {
      return;
    }
    lastRouteLoadRef.current = normalizedRouteConversationId;
    pendingRouteConversationRef.current = normalizedRouteConversationId;
    if (normalizedRouteConversationId) {
      void loadConversation(normalizedRouteConversationId);
    }
  }, [loadConversation, routeConversationId]);

  useEffect(() => {
    const normalizedRouteConversationId = String(routeConversationId || '').trim();
    const normalizedConversationId = String(conversationId || '').trim();
    if (!normalizedConversationId) {
      return;
    }
    if (
      pendingRouteConversationRef.current
      && pendingRouteConversationRef.current !== normalizedConversationId
    ) {
      return;
    }
    if (pendingRouteConversationRef.current === normalizedConversationId) {
      pendingRouteConversationRef.current = '';
    }
    const targetPath = `/chat/${normalizedConversationId}`;
    if (status === 'opened' || status === 'generating') {
      return;
    }
    if (targetPath !== `/chat/${normalizedRouteConversationId}`) {
      navigate(targetPath, { replace: true });
    }
  }, [conversationId, navigate, routeConversationId, status]);

  return (
    <DrawerLayout>
      <ChatMessages />
      <ChatInput />
    </DrawerLayout>
  );
}
