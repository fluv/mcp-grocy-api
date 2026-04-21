import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import express from 'express';
import { randomUUID } from 'crypto';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { SSEServerTransport } from '@modelcontextprotocol/sdk/server/sse.js';
import { VERSION, PACKAGE_NAME as SERVER_NAME } from '../version.js';
import cors from 'cors';
import http from 'http';

// HTTP Transport for MCP (Context7 style)
export function startHttpServer(mcpServer: Server, port: number = 8080) {
  const app = express();

  // Enable JSON body parsing with increased limit
  app.use(express.json({
    limit: '10mb'
  }));

  // Enable CORS for all routes
  app.use(cors({
    origin: '*',
    methods: ['GET', 'POST', 'OPTIONS'],
    allowedHeaders: ['Origin', 'X-Requested-With', 'Content-Type', 'Accept', 'Mcp-Session-Id', 'Authorization'],
    exposedHeaders: ['Mcp-Session-Id', 'Content-Type'],
    optionsSuccessStatus: 200
  }));

  // Simple health check endpoint
  app.get('/', (req, res) => {
    res.json({
      status: 'ok',
      service: SERVER_NAME,
      version: VERSION,
      message: 'MCP server is running',
      endpoints: {
        streamable: '/mcp',
        sse: '/mcp/sse',
        sseMessages: '/mcp/messages'
      }
    });
  });

  // Session management for transports
  const streamableTransports: Record<string, StreamableHTTPServerTransport> = {};
  const sseTransports: Record<string, SSEServerTransport> = {};

  // Middleware to log all requests
  app.use((req, res, next) => {
    console.error(`[${new Date().toISOString()}] ${req.method} ${req.path} - Headers: ${JSON.stringify(req.headers)}`);
    next();
  });

  // GET /mcp — SSE keepalive stream (server-to-client notifications)
  //
  // We deliberately do NOT call transport.handleRequest here. With enableJsonResponse:true
  // all POST responses go inline in the HTTP response body. Calling handleRequest on the GET
  // would register the SSE stream as the response channel, causing a race condition when
  // ClaudeAI sends GET+POST simultaneously: the POST handler sees an active SSE channel and
  // tries to route its response through it before the stream is fully established.
  //
  // Grocy has no server-initiated notifications, so this stream is keepalive-only.
  app.get('/mcp', (req, res) => {
    const clientSessionId = req.headers['mcp-session-id'] as string | undefined;

    if (!clientSessionId) {
      res.status(400).json({ error: 'Missing Mcp-Session-Id header' });
      return;
    }

    if (!streamableTransports[clientSessionId]) {
      console.error(`[DEBUG] GET /mcp: session ${clientSessionId} not found — returning 404`);
      res.status(404).json({ error: `Session not found: ${clientSessionId}` });
      return;
    }

    console.error(`[DEBUG] GET /mcp: opening keepalive SSE stream for session ${clientSessionId}`);
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('Mcp-Session-Id', clientSessionId);
    res.flushHeaders();

    const keepalive = setInterval(() => {
      if (!res.writableEnded) res.write(': ping\n\n');
    }, 30000);
    res.on('close', () => clearInterval(keepalive));
  });

  // POST /mcp — main request channel (streamable HTTP transport)
  app.post('/mcp', async (req, res) => {
    try {
      const clientSessionId = req.headers['mcp-session-id'] as string | undefined;
      let transport: StreamableHTTPServerTransport | undefined = undefined;

      // Accept header check (can be done early)
      const accept = req.headers.accept || '';
      if (!accept.includes('application/json') && !accept.includes('text/event-stream')) {
        console.error('[ERROR] Client must accept application/json or text/event-stream');
        res.status(406).json({
          jsonrpc: '2.0',
          error: {
            code: -32000,
            message: 'Not Acceptable: Client must accept application/json or text/event-stream'
          },
          id: req.body?.id || null
        });
        return;
      }

      if (clientSessionId) {
        transport = streamableTransports[clientSessionId];
        if (transport) {
          console.error(`[DEBUG] Using existing transport for session ID: ${clientSessionId}`);
        } else {
          // Unknown session ID — only recover for initialize, otherwise 404 per MCP spec
          const isInitialize = req.body?.method === 'initialize';
          if (isInitialize) {
            console.error(`[DEBUG] Session ${clientSessionId} not found but initialize received — creating new session`);
            // Fall through to create new transport below
          } else {
            console.error(`[DEBUG] Session ${clientSessionId} not found and request is not initialize — returning 404`);
            res.status(404).json({
              jsonrpc: '2.0',
              error: { code: -32001, message: `Session not found: ${clientSessionId}. Please re-initialize.` },
              id: req.body?.id || null
            });
            return;
          }
        }
      }

      if (!transport) {
        // No session ID provided, or stale session on initialize — create new transport
        console.error('[DEBUG] No active transport found. Creating new transport.');
        const newGeneratedSessionId = randomUUID();

        const newTransportInstance = new StreamableHTTPServerTransport({
          sessionIdGenerator: () => {
            console.error(`[DEBUG] StreamableHTTPServerTransport sessionIdGenerator using ID: ${newGeneratedSessionId}`);
            return newGeneratedSessionId;
          },
          onsessioninitialized: (initializedSid: string) => {
            console.error(`[DEBUG] Session initialized with ID: ${initializedSid}`);
          },
          enableJsonResponse: true
        });

        transport = newTransportInstance;
        streamableTransports[newGeneratedSessionId] = transport;

        transport.onclose = () => {
          const closedSessionId = transport?.sessionId || newGeneratedSessionId;
          console.error(`[DEBUG] Transport for session ${closedSessionId} closed. Removing.`);
          delete streamableTransports[closedSessionId];
        };

        console.error(`[DEBUG] Connecting new transport (ID: ${newGeneratedSessionId}) to MCP server`);
        await mcpServer.connect(transport);
        console.error(`[DEBUG] New transport connected`);
      }

      if (transport.sessionId) {
        res.setHeader('Mcp-Session-Id', transport.sessionId);
      }

      await transport.handleRequest(req, res, req.body);

    } catch (error) {
      console.error('[ERROR] Failed to handle streamable HTTP request:', error);

      if (!res.headersSent) {
        res.status(500).json({
          jsonrpc: '2.0',
          error: {
            code: -32000,
            message: `Internal server error: ${error instanceof Error ? error.message : String(error)}`
          },
          id: req.body?.id || null
        });
      }
    }
  });

  // Legacy SSE endpoint
  app.get('/mcp/sse', async (req, res) => {
    console.error('[DEBUG] Incoming SSE connection request');

    try {
      res.setHeader('Content-Type', 'text/event-stream');
      res.setHeader('Cache-Control', 'no-cache');
      res.setHeader('Connection', 'keep-alive');

      const transport = new SSEServerTransport('/mcp/messages', res);
      const sessionId = transport.sessionId;

      console.error(`[DEBUG] Created SSE transport with session ID: ${sessionId}`);
      sseTransports[sessionId] = transport;

      const keepalive = setInterval(() => {
        if (!res.writableEnded) res.write(': ping\n\n');
      }, 30000);

      res.on('close', () => {
        console.error(`[DEBUG] SSE connection closed for session ID: ${sessionId}`);
        clearInterval(keepalive);
        delete sseTransports[sessionId];
      });

      res.on('error', (err) => {
        console.error(`[ERROR] SSE connection error for session ID: ${sessionId}:`, err);
        clearInterval(keepalive);
        delete sseTransports[sessionId];
      });

      await mcpServer.connect(transport);
      console.error(`[DEBUG] SSE transport connected for session ${sessionId}`);
      res.write(': connected\n\n');
    } catch (error) {
      console.error('[ERROR] Failed to handle SSE connection:', error);
      if (!res.headersSent) {
        res.status(500).send('Internal Server Error');
      } else {
        res.end();
      }
    }
  });

  // Message endpoint for legacy SSE
  app.post('/mcp/messages', async (req, res) => {
    const sessionId = req.query.sessionId as string;
    console.error(`[DEBUG] Incoming message for SSE session ID: ${sessionId}`);

    if (!sessionId) {
      console.error('[ERROR] No sessionId provided in request');
      res.status(400).json({
        error: 'Missing sessionId parameter',
        status: 400
      });
      return;
    }

    const transport = sseTransports[sessionId];
    if (transport) {
      try {
        console.error(`[DEBUG] Found transport for session ID: ${sessionId}, handling message`);
        await transport.handlePostMessage(req, res, req.body);
      } catch (error) {
        console.error(`[ERROR] Failed to handle SSE message for session ID: ${sessionId}:`, error);
        res.status(500).json({
          error: `Internal server error: ${error instanceof Error ? error.message : String(error)}`,
          status: 500
        });
      }
    } else {
      console.error(`[ERROR] No transport found for session ID: ${sessionId}`);
      res.status(404).json({
        error: `No active SSE connection found for session ID: ${sessionId}`,
        status: 404
      });
    }
  });

  // Create HTTP server with explicit error handling
  const server = http.createServer(app);

  server.on('error', (error) => {
    console.error(`[ERROR] HTTP server error: ${error.message}`);
  });

  server.listen(port, () => {
    console.error(`[MCP] HTTP server listening on port ${port}`);
    console.error(`[MCP] Available endpoints:`);
    console.error(`[MCP]   - Health check: http://localhost:${port}/`);
    console.error(`[MCP]   - Streamable HTTP: http://localhost:${port}/mcp`);
    console.error(`[MCP]   - SSE: http://localhost:${port}/mcp/sse`);
    console.error(`[MCP]   - SSE Messages: http://localhost:${port}/mcp/messages`);
  });

  return server;
}
