/**
 * server.js
 * Servidor Web e Sinalizador WebSocket para WebRTC com suporte a PIN
 */

import express from 'express';
import http from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import path from 'path';
import { fileURLToPath } from 'url';
import crypto from 'crypto';
import fs from 'fs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const server = http.createServer(app);
const PORT = process.env.PORT || 3000;

// Encontra o index.html na pasta "public" ou na raiz
const hasPublicDir = fs.existsSync(path.join(__dirname, 'public', 'index.html'));
const publicPath = hasPublicDir ? path.join(__dirname, 'public') : __dirname;
const indexPath = hasPublicDir 
  ? path.join(__dirname, 'public', 'index.html') 
  : path.join(__dirname, 'index.html');

app.use(express.static(publicPath));

app.get('*', (req, res) => {
  res.sendFile(indexPath);
});

const wss = new WebSocketServer({ server });

/**
 * Estrutura de Salas:
 * Map<roomName, { pin: string | null, peers: Map<peerId, { ws, peerId, userName, status }> }>
 */
const rooms = new Map();

function generateId() {
  return crypto.randomBytes(6).toString('hex');
}

function sendJSON(ws, data) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function broadcastToRoom(roomName, data, excludePeerId = null) {
  const roomData = rooms.get(roomName);
  if (!roomData || !roomData.peers) return;

  roomData.peers.forEach((client, peerId) => {
    if (peerId !== excludePeerId) {
      sendJSON(client.ws, data);
    }
  });
}

function removePeerFromRoom(ws) {
  const { roomName, peerId, userName } = ws;
  if (!roomName || !peerId) return;

  const roomData = rooms.get(roomName);
  if (roomData && roomData.peers && roomData.peers.has(peerId)) {
    roomData.peers.delete(peerId);
    console.log(`[Sala: ${roomName}] Usuário desconectado: ${userName || peerId}`);

    broadcastToRoom(roomName, {
      type: 'peer-left',
      peerId,
      userName
    });

    if (roomData.peers.size === 0) {
      rooms.delete(roomName);
      console.log(`[Sala: ${roomName}] Sala encerrada (sem membros)`);
    }
  }

  ws.roomName = null;
  ws.peerId = null;
}

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.peerId = null;
  ws.roomName = null;

  ws.on('pong', () => {
    ws.isAlive = true;
  });

  console.log('[WebSocket] Novo cliente conectado');

  ws.on('message', (messageRaw) => {
    let data;
    try {
      data = JSON.parse(messageRaw.toString());
    } catch (err) {
      console.error('[WebSocket] Erro ao analisar JSON:', err);
      return;
    }

    const { type } = data;

    switch (type) {
      case 'join': {
        const roomName = (data.room || 'geral').trim().toLowerCase();
        const userName = (data.userName || 'Anônimo').trim();
        const inputPin = (data.pin || '').trim();
        const peerId = data.peerId || generateId();

        if (ws.roomName) {
          removePeerFromRoom(ws);
        }

        let roomData = rooms.get(roomName);

        // Se a sala já existe e tem senha configurada, valida o PIN
        if (roomData) {
          if (roomData.pin && roomData.pin !== inputPin) {
            console.log(`[Sala: ${roomName}] PIN incorreto digitado por: ${userName}`);
            sendJSON(ws, {
              type: 'join-error',
              message: 'Senha/PIN incorreto para esta sala!'
            });
            return;
          }
        } else {
          // Cria nova sala com ou sem PIN
          roomData = {
            pin: inputPin.length > 0 ? inputPin : null,
            peers: new Map()
          };
          rooms.set(roomName, roomData);
          if (roomData.pin) {
            console.log(`[Sala: ${roomName}] Sala criada com senha/PIN`);
          }
        }

        ws.peerId = peerId;
        ws.roomName = roomName;
        ws.userName = userName;

        const existingPeers = [];
        roomData.peers.forEach((client, existingId) => {
          existingPeers.push({
            peerId: existingId,
            userName: client.userName,
            status: client.status || { isAudioMuted: false, isVideoMuted: false, isScreenSharing: false }
          });
        });

        roomData.peers.set(peerId, {
          ws,
          peerId,
          userName,
          status: data.status || { isAudioMuted: false, isVideoMuted: false, isScreenSharing: false }
        });

        console.log(`[Sala: ${roomName}] "${userName}" (${peerId}) entrou. Total: ${roomData.peers.size}`);

        sendJSON(ws, {
          type: 'room-joined',
          room: roomName,
          peerId,
          userName,
          hasPin: !!roomData.pin,
          peers: existingPeers
        });

        broadcastToRoom(roomName, {
          type: 'peer-joined',
          peerId,
          userName,
          status: data.status || { isAudioMuted: false, isVideoMuted: false, isScreenSharing: false }
        }, peerId);

        break;
      }

      case 'signal': {
        const { targetPeerId, payload } = data;
        const roomName = ws.roomName;
        const senderPeerId = ws.peerId;

        if (!roomName || !senderPeerId || !targetPeerId) return;

        const roomData = rooms.get(roomName);
        if (roomData && roomData.peers && roomData.peers.has(targetPeerId)) {
          const targetClient = roomData.peers.get(targetPeerId);
          sendJSON(targetClient.ws, {
            type: 'signal',
            fromPeerId: senderPeerId,
            userName: ws.userName,
            payload
          });
        }
        break;
      }

      case 'user-status': {
        const roomName = ws.roomName;
        const peerId = ws.peerId;
        if (!roomName || !peerId) return;

        const roomData = rooms.get(roomName);
        if (roomData && roomData.peers && roomData.peers.has(peerId)) {
          const client = roomData.peers.get(peerId);
          client.status = { ...client.status, ...data.status };

          broadcastToRoom(roomName, {
            type: 'peer-status',
            peerId,
            status: client.status
          }, peerId);
        }
        break;
      }

      case 'chat': {
        const roomName = ws.roomName;
        if (!roomName || !data.message) return;

        broadcastToRoom(roomName, {
          type: 'chat',
          peerId: ws.peerId,
          userName: ws.userName || 'Anônimo',
          message: data.message,
          timestamp: new Date().toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' })
        });
        break;
      }

      case 'leave': {
        removePeerFromRoom(ws);
        sendJSON(ws, { type: 'left-room' });
        break;
      }
    }
  });

  ws.on('close', () => {
    removePeerFromRoom(ws);
  });

  ws.on('error', (err) => {
    console.error('[WebSocket] Erro na conexão:', err);
    removePeerFromRoom(ws);
  });
});

const heartbeatInterval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

wss.on('close', () => {
  clearInterval(heartbeatInterval);
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`====================================================`);
  console.log(`🚀 Servidor WebRTC Rodando na porta ${PORT}!`);
  console.log(`🌐 Acesse no navegador: http://localhost:${PORT}`);
  console.log(`📡 WebSocket de sinalização ativo com suporte a PIN`);
  console.log(`====================================================`);
});
