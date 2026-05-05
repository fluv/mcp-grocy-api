import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import express from 'express';
import { randomUUID } from 'crypto';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { SSEServerTransport } from '@modelcontextprotocol/sdk/server/sse.js';
import { VERSION, PACKAGE_NAME as SERVER_NAME } from '../version.js';
import cors from 'cors';
import http from 'http';

function log(level: string, event: string, fields: Record<string, unknown> = {}) {
  console.error(JSON.stringify({ ts: new Date().toISOString(), level, event, ...fields }));
}

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
    log('INFO', 'request', { method: req.method, path: req.path, headers: req.headers });
    next();
  });

  // GET /mcp — SSE keepalive channel.
  //
  // We do NOT call transport.handleRequest here to avoid the SDK's
  // single-stream-per-session limit (which returns 409 when the client
  // reopens GET between tool calls).
  //
  // With enableJsonResponse: true, all responses are delivered on the POST
  // connection; this GET stream is kept open for keepalive pings only.
  app.get('/mcp', (req, res) => {
    const clientSessionId = req.headers['mcp-session-id'] as string | undefined;

    if (!clientSessionId) {
      res.status(400).json({ error: 'Missing Mcp-Session-Id header' });
      return;
    }

    const transport = streamableTransports[clientSessionId];
    if (!transport) {
      log('DEBUG', 'session_not_found', { endpoint: 'GET /mcp', session: clientSessionId });
      res.status(404).json({ error: `Session not found: ${clientSessionId}` });
      return;
    }

    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('Mcp-Session-Id', clientSessionId);
    res.flushHeaders();

    const keepalive = setInterval(() => {
      if (!res.writableEnded) res.write(': ping\n\n');
    }, 30000);

    res.on('close', () => {
      clearInterval(keepalive);
    });
  });

  // POST /mcp — main request channel (streamable HTTP transport)
  app.post('/mcp', async (req, res) => {
    try {
      const clientSessionId = req.headers['mcp-session-id'] as string | undefined;
      let transport: StreamableHTTPServerTransport | undefined = undefined;

      // Accept header check (can be done early)
      const accept = req.headers.accept || '';
      if (!accept.includes('application/json') && !accept.includes('text/event-stream') && !accept.includes('*/*')) {
        log('ERROR', 'invalid_accept_header', { accept });
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
          log('DEBUG', 'using_existing_transport', { session: clientSessionId });
        } else {
          // Unknown session ID — only recover for initialize, otherwise 404 per MCP spec
          const isInitialize = req.body?.method === 'initialize';
          if (isInitialize) {
            log('DEBUG', 'session_not_found_initializing', { session: clientSessionId });
            // Fall through to create new transport below
          } else {
            log('DEBUG', 'session_not_found_404', { session: clientSessionId });
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
        log('DEBUG', 'creating_new_transport');
        const newGeneratedSessionId = randomUUID();

        const newTransportInstance = new StreamableHTTPServerTransport({
          sessionIdGenerator: () => {
            log('DEBUG', 'session_id_generated', { session: newGeneratedSessionId });
            return newGeneratedSessionId;
          },
          onsessioninitialized: (initializedSid: string) => {
            log('DEBUG', 'session_initialized', { session: initializedSid });
          },
          enableJsonResponse: true
        });

        transport = newTransportInstance;
        streamableTransports[newGeneratedSessionId] = transport;

        transport.onclose = () => {
          const closedSessionId = transport?.sessionId || newGeneratedSessionId;
          log('DEBUG', 'transport_closed', { session: closedSessionId });
          delete streamableTransports[closedSessionId];
        };

        log('DEBUG', 'connecting_transport', { session: newGeneratedSessionId });
        await mcpServer.connect(transport);
        log('DEBUG', 'transport_connected');
      }

      if (transport.sessionId) {
        res.setHeader('Mcp-Session-Id', transport.sessionId);
      }

      const { method: rpcMethod, id: rpcId, params: rpcParams } = req.body || {};
      const paramsPreview = rpcParams ? JSON.stringify(rpcParams).slice(0, 200) : 'none';
      log('REQ', 'rpc_request', {
        session: transport.sessionId ?? 'none',
        method: rpcMethod,
        id: rpcId,
        params: paramsPreview
      });

      await transport.handleRequest(req, res, req.body);

    } catch (error) {
      log('ERROR', 'streamable_http_error', {
        message: error instanceof Error ? error.message : String(error),
        stack: error instanceof Error ? error.stack : undefined
      });

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
    log('DEBUG', 'incoming_sse_connection');

    try {
      res.setHeader('Content-Type', 'text/event-stream');
      res.setHeader('Cache-Control', 'no-cache');
      res.setHeader('Connection', 'keep-alive');

      const transport = new SSEServerTransport('/mcp/messages', res);
      const sessionId = transport.sessionId;

      log('DEBUG', 'sse_transport_created', { session: sessionId });
      sseTransports[sessionId] = transport;

      const keepalive = setInterval(() => {
        if (!res.writableEnded) res.write(': ping\n\n');
      }, 30000);

      res.on('close', () => {
        log('DEBUG', 'sse_connection_closed', { session: sessionId });
        clearInterval(keepalive);
        delete sseTransports[sessionId];
      });

      res.on('error', (err) => {
        log('ERROR', 'sse_connection_error', {
          session: sessionId,
          message: err instanceof Error ? err.message : String(err)
        });
        clearInterval(keepalive);
        delete sseTransports[sessionId];
      });

      await mcpServer.connect(transport);
      log('DEBUG', 'sse_transport_connected', { session: sessionId });
      res.write(': connected\n\n');
    } catch (error) {
      log('ERROR', 'sse_connection_error', {
        message: error instanceof Error ? error.message : String(error)
      });
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
    log('DEBUG', 'incoming_sse_message', { session: sessionId });

    if (!sessionId) {
      log('ERROR', 'missing_session_id');
      res.status(400).json({
        error: 'Missing sessionId parameter',
        status: 400
      });
      return;
    }

    const transport = sseTransports[sessionId];
    if (transport) {
      try {
        log('DEBUG', 'handling_sse_message', { session: sessionId });
        await transport.handlePostMessage(req, res, req.body);
      } catch (error) {
        log('ERROR', 'sse_message_error', {
          session: sessionId,
          message: error instanceof Error ? error.message : String(error)
        });
        res.status(500).json({
          error: `Internal server error: ${error instanceof Error ? error.message : String(error)}`,
          status: 500
        });
      }
    } else {
      log('ERROR', 'transport_not_found', { session: sessionId });
      res.status(404).json({
        error: `No active SSE connection found for session ID: ${sessionId}`,
        status: 404
      });
    }
  });

  // Create HTTP server with explicit error handling
  const server = http.createServer(app);

  server.on('error', (error) => {
    log('ERROR', 'http_server_error', { message: error.message });
  });

  server.listen(port, () => {
    log('INFO', 'startup', { port });
    log('INFO', 'endpoints', {
      health: `http://localhost:${port}/`,
      streamable: `http://localhost:${port}/mcp`,
      sse: `http://localhost:${port}/mcp/sse`,
      sse_messages: `http://localhost:${port}/mcp/messages`
    });
  });

  return server;
}
