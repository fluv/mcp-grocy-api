import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import express from 'express';
import { randomUUID } from 'crypto';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { SSEServerTransport } from '@modelcontextprotocol/sdk/server/sse.js';
import { VERSION, PACKAGE_NAME as SERVER_NAME } from '../version.js';
import cors from 'cors';
import http from 'http';

// Fail-fast watchdog: if "No connection established for request ID" errors
// recur rapidly, the transport is wedged in a way the fallback paths below
// can't recover from.  Exit(1) so Kubernetes restarts the pod rather than
// letting the client hang indefinitely waiting for responses that will never
// arrive.  Sliding window: N errors within T ms.
const STUCK_ERROR_THRESHOLD = 3;
const STUCK_ERROR_WINDOW_MS = 60_000;
const stuckErrorTimestamps: number[] = [];

function log(level: string, event: string, fields: Record<string, unknown> = {}) {
  console.error(JSON.stringify({ ts: new Date().toISOString(), level, event, ...fields }));
}

function recordStuckError(context: string) {
  const now = Date.now();
  stuckErrorTimestamps.push(now);
  while (stuckErrorTimestamps.length > 0 && stuckErrorTimestamps[0]! < now - STUCK_ERROR_WINDOW_MS) {
    stuckErrorTimestamps.shift();
  }
  if (stuckErrorTimestamps.length >= STUCK_ERROR_THRESHOLD) {
    log('FATAL', 'transport_wedged', {
      count: stuckErrorTimestamps.length,
      window_ms: STUCK_ERROR_WINDOW_MS,
      session: context
    });
    process.exit(1);
  }
}

// Patch a transport's send() to fall back to the standalone GET SSE when the
// POST connection closes before a response is ready.
//
// ClaudeAI sends GET before every POST and closes the POST connection
// immediately after the request body is sent, expecting responses on the GET
// SSE stream.  Without this patch those responses are lost with:
//   "No connection established for request ID: N"
//
// When the GET SSE is available the response is written there directly.
// When it is not (the client closed the old GET and hasn't opened the new one
// yet — common for fast Grocy errors like 400s), the response is buffered in
// pendingResponses and flushed as soon as the next GET arrives for the same
// session.
function patchTransportSend(
  transport: StreamableHTTPServerTransport,
  pendingResponses: Map<string, any[]>
) {
  const original = (transport as any).send.bind(transport);
  (transport as any).send = async (message: any, options?: any) => {
    try {
      await original(message, options);
    } catch (error: any) {
      if (error?.message?.includes('No connection established for request ID')) {
        const standaloneSseId: string = (transport as any)._standaloneSseStreamId;
        const sseStream = (transport as any)._streamMapping?.get(standaloneSseId);

        if (sseStream && !sseStream.writableEnded) {
          log('FALLBACK', 'post_closed_routed_to_sse');
          sseStream.write(`event: message\ndata: ${JSON.stringify(message)}\n\n`);
        } else {
          // GET SSE not available yet (client is between GETs); buffer until next GET.
          const sessionId: string | undefined = transport.sessionId;
          if (sessionId) {
            log('FALLBACK', 'no_sse_buffering', { session: sessionId });
            if (!pendingResponses.has(sessionId)) pendingResponses.set(sessionId, []);
            pendingResponses.get(sessionId)!.push(message);
          } else {
            log('FALLBACK', 'response_dropped', { session: 'none', error: error.message });
          }
        }

        recordStuckError(transport.sessionId ?? 'no-session');
      } else {
        throw error;
      }
    }
  };
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

  // Buffered responses for sessions where the GET SSE wasn't available when
  // the POST response was ready (flushed on the next GET for the same session).
  const pendingResponses: Map<string, any[]> = new Map();

  // Middleware to log all requests
  app.use((req, res, next) => {
    log('INFO', 'request', { method: req.method, path: req.path, headers: req.headers });
    next();
  });

  // GET /mcp — SSE channel: keepalive + fallback response delivery
  //
  // We do NOT call transport.handleRequest here to avoid the SDK's
  // single-stream-per-session limit (which returns 409 when the client
  // reopens GET between tool calls).
  //
  // Instead:
  //   1. We manually inject res into the transport's _streamMapping so
  //      patchTransportSend can route responses here when POST closes early.
  //   2. We flush any responses buffered while the GET was unavailable.
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

    // Register as the standalone SSE fallback channel.
    const standaloneSseId: string = (transport as any)._standaloneSseStreamId;
    if (standaloneSseId) {
      (transport as any)._streamMapping?.set(standaloneSseId, res);
      log('DEBUG', 'sse_registered', { session: clientSessionId });
    }

    // Flush any responses buffered while GET was unavailable.
    const pending = pendingResponses.get(clientSessionId);
    if (pending?.length) {
      log('FALLBACK', 'flushing_buffered_responses', { session: clientSessionId, count: pending.length });
      for (const message of pending) {
        if (!res.writableEnded) {
          res.write(`event: message\ndata: ${JSON.stringify(message)}\n\n`);
        }
      }
      pendingResponses.delete(clientSessionId);
    }

    const keepalive = setInterval(() => {
      if (!res.writableEnded) res.write(': ping\n\n');
    }, 30000);

    res.on('close', () => {
      clearInterval(keepalive);
      // Deregister only if this is still the current GET SSE.
      if (standaloneSseId) {
        const current = (transport as any)._streamMapping?.get(standaloneSseId);
        if (current === res) (transport as any)._streamMapping?.delete(standaloneSseId);
      }
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

        // Patch send() to fall back to GET SSE when POST closes early.
        patchTransportSend(newTransportInstance, pendingResponses);

        transport = newTransportInstance;
        streamableTransports[newGeneratedSessionId] = transport;

        transport.onclose = () => {
          const closedSessionId = transport?.sessionId || newGeneratedSessionId;
          log('DEBUG', 'transport_closed', { session: closedSessionId });
          delete streamableTransports[closedSessionId];
          pendingResponses.delete(closedSessionId);
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
