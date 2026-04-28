import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import express from 'express';
import { randomUUID } from 'crypto';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { SSEServerTransport } from '@modelcontextprotocol/sdk/server/sse.js';
import { VERSION, PACKAGE_NAME as SERVER_NAME } from '../version.js';
import cors from 'cors';
import http from 'http';
import axios from 'axios';
import https from 'https';

// Fail-fast watchdog: if "No connection established for request ID" errors
// recur rapidly, the transport is wedged in a way the fallback paths below
// can't recover from.  Exit(1) so Kubernetes restarts the pod rather than
// letting the client hang indefinitely waiting for responses that will never
// arrive.  Sliding window: N errors within T ms.
const STUCK_ERROR_THRESHOLD = 3;
const STUCK_ERROR_WINDOW_MS = 60_000;
const stuckErrorTimestamps: number[] = [];
let transportErrorTotal = 0;

function recordStuckError(context: string) {
  transportErrorTotal++;
  const now = Date.now();
  stuckErrorTimestamps.push(now);
  while (stuckErrorTimestamps.length > 0 && stuckErrorTimestamps[0]! < now - STUCK_ERROR_WINDOW_MS) {
    stuckErrorTimestamps.shift();
  }
  if (stuckErrorTimestamps.length >= STUCK_ERROR_THRESHOLD) {
    console.error(
      `[FATAL] ${stuckErrorTimestamps.length} "No connection established" errors ` +
        `within ${STUCK_ERROR_WINDOW_MS}ms (${context}); transport is wedged, exiting ` +
        `to let Kubernetes restart the pod.`
    );
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
          console.error(`[FALLBACK] POST closed — routing response to open GET SSE`);
          sseStream.write(`event: message\ndata: ${JSON.stringify(message)}\n\n`);
        } else {
          // GET SSE not available yet (client is between GETs); buffer until next GET.
          const sessionId: string | undefined = transport.sessionId;
          if (sessionId) {
            console.error(`[FALLBACK] No GET SSE — buffering response for session ${sessionId}`);
            if (!pendingResponses.has(sessionId)) pendingResponses.set(sessionId, []);
            pendingResponses.get(sessionId)!.push(message);
          } else {
            console.error(`[FALLBACK] No session ID — response dropped (${error.message})`);
          }
        }

        recordStuckError(transport.sessionId ?? 'no-session');
      } else {
        throw error;
      }
    }
  };
}

function buildGrocyClient() {
  const baseURL = process.env.GROCY_BASE_URL || '';
  const apiKey = process.env.GROCY_APIKEY_VALUE || '';
  const sslVerify = process.env.GROCY_ENABLE_SSL_VERIFY !== 'false';
  return axios.create({
    baseURL,
    headers: { 'GROCY-API-KEY': apiKey },
    timeout: 10_000,
    ...(sslVerify ? {} : { httpsAgent: new https.Agent({ rejectUnauthorized: false }) }),
  });
}

// HTTP Transport for MCP (Context7 style)
//
// Each session gets its own Server instance via the factory. The MCP SDK's
// Server class stores a single _transport reference; sharing one Server across
// multiple sessions causes responses for session A to be sent via session B's
// transport after B connects (overwriting _transport), producing cross-session
// "No connection established" errors and lost responses.
export function startHttpServer(createMcpServer: () => Server, port: number = 8080) {
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

  // Session management: one transport + one Server per session.
  const streamableTransports: Record<string, StreamableHTTPServerTransport> = {};
  const streamableServers: Record<string, Server> = {};
  const sseTransports: Record<string, SSEServerTransport> = {};
  const sseServers: Record<string, Server> = {};

  // Buffered responses for sessions where the GET SSE wasn't available when
  // the POST response was ready (flushed on the next GET for the same session).
  const pendingResponses: Map<string, any[]> = new Map();

  // Tech metrics state — request counts by path + status class, accumulated via res.on('finish').
  const requestCounts: Map<string, Map<string, number>> = new Map();

  // Prometheus metrics — registered before the request logger to keep scrapes out of logs.
  app.get('/metrics', async (_req, res) => {
    const start = Date.now();
    try {
      const client = buildGrocyClient();
      const [volatile, shoppingList, tasks] = await Promise.all([
        client.get('/api/stock/volatile').then((r: any) => r.data),
        client.get('/api/objects/shopping_list').then((r: any) => r.data),
        client.get('/api/objects/tasks').then((r: any) => r.data),
      ]);
      const duration = (Date.now() - start) / 1000;

      const expired  = Array.isArray(volatile?.expired_products)  ? volatile.expired_products.length  : 0;
      const overdue  = Array.isArray(volatile?.overdue_products)  ? volatile.overdue_products.length  : 0;
      const dueSoon  = Array.isArray(volatile?.due_soon_products) ? volatile.due_soon_products.length : 0;
      const missing  = Array.isArray(volatile?.missing_products)  ? volatile.missing_products.length  : 0;
      const shopping = Array.isArray(shoppingList) ? shoppingList.length : 0;
      const openTasks = Array.isArray(tasks)
        ? tasks.filter((t: any) => Number(t.done) === 0).length
        : 0;

      const activeSessions = Object.keys(streamableTransports).length + Object.keys(sseTransports).length;
      const requestLines: string[] = [];
      for (const [path, byStatus] of requestCounts) {
        for (const [statusClass, count] of byStatus) {
          requestLines.push(`grocy_mcp_http_requests_total{path="${path}",status_class="${statusClass}"} ${count}`);
        }
      }

      const body = [
        '# HELP grocy_mcp_http_requests_total HTTP requests served, by path and response status class',
        '# TYPE grocy_mcp_http_requests_total counter',
        ...requestLines,
        '# HELP grocy_mcp_active_sessions Currently active MCP sessions (streamable HTTP + legacy SSE)',
        '# TYPE grocy_mcp_active_sessions gauge',
        `grocy_mcp_active_sessions ${activeSessions}`,
        '# HELP grocy_mcp_transport_errors_total Cumulative transport stuck-error watchdog firings',
        '# TYPE grocy_mcp_transport_errors_total counter',
        `grocy_mcp_transport_errors_total ${transportErrorTotal}`,
        '# HELP grocy_up 1 if the Grocy API is reachable, 0 otherwise',
        '# TYPE grocy_up gauge',
        'grocy_up 1',
        '# HELP grocy_scrape_duration_seconds Duration of the last Grocy API scrape in seconds',
        '# TYPE grocy_scrape_duration_seconds gauge',
        `grocy_scrape_duration_seconds ${duration.toFixed(3)}`,
        '# HELP grocy_stock_expired_products_total Number of expired products in stock',
        '# TYPE grocy_stock_expired_products_total gauge',
        `grocy_stock_expired_products_total ${expired}`,
        '# HELP grocy_stock_overdue_products_total Number of overdue products past best-before date',
        '# TYPE grocy_stock_overdue_products_total gauge',
        `grocy_stock_overdue_products_total ${overdue}`,
        '# HELP grocy_stock_due_soon_products_total Number of products due soon in stock',
        '# TYPE grocy_stock_due_soon_products_total gauge',
        `grocy_stock_due_soon_products_total ${dueSoon}`,
        '# HELP grocy_stock_missing_products_total Number of products below minimum stock level',
        '# TYPE grocy_stock_missing_products_total gauge',
        `grocy_stock_missing_products_total ${missing}`,
        '# HELP grocy_shopping_list_items_total Number of items on the shopping list',
        '# TYPE grocy_shopping_list_items_total gauge',
        `grocy_shopping_list_items_total ${shopping}`,
        '# HELP grocy_tasks_open_total Number of open (incomplete) tasks',
        '# TYPE grocy_tasks_open_total gauge',
        `grocy_tasks_open_total ${openTasks}`,
      ].join('\n') + '\n';

      res.setHeader('Content-Type', 'text/plain; version=0.0.4; charset=utf-8');
      res.status(200).send(body);
    } catch (err: any) {
      const duration = (Date.now() - start) / 1000;
      console.error('[ERROR] /metrics Grocy scrape failed:', err?.message);
      const activeSessions = Object.keys(streamableTransports).length + Object.keys(sseTransports).length;
      const requestLines: string[] = [];
      for (const [path, byStatus] of requestCounts) {
        for (const [statusClass, count] of byStatus) {
          requestLines.push(`grocy_mcp_http_requests_total{path="${path}",status_class="${statusClass}"} ${count}`);
        }
      }
      const body = [
        '# HELP grocy_mcp_http_requests_total HTTP requests served, by path and response status class',
        '# TYPE grocy_mcp_http_requests_total counter',
        ...requestLines,
        '# HELP grocy_mcp_active_sessions Currently active MCP sessions (streamable HTTP + legacy SSE)',
        '# TYPE grocy_mcp_active_sessions gauge',
        `grocy_mcp_active_sessions ${activeSessions}`,
        '# HELP grocy_mcp_transport_errors_total Cumulative transport stuck-error watchdog firings',
        '# TYPE grocy_mcp_transport_errors_total counter',
        `grocy_mcp_transport_errors_total ${transportErrorTotal}`,
        '# HELP grocy_up 1 if the Grocy API is reachable, 0 otherwise',
        '# TYPE grocy_up gauge',
        'grocy_up 0',
        '# HELP grocy_scrape_duration_seconds Duration of the last Grocy API scrape in seconds',
        '# TYPE grocy_scrape_duration_seconds gauge',
        `grocy_scrape_duration_seconds ${duration.toFixed(3)}`,
      ].join('\n') + '\n';
      res.setHeader('Content-Type', 'text/plain; version=0.0.4; charset=utf-8');
      res.status(200).send(body);
    }
  });

  // Middleware to log all requests and accumulate request-count metrics.
  app.use((req, res, next) => {
    res.on('finish', () => {
      const statusClass = `${Math.floor(res.statusCode / 100)}xx`;
      if (!requestCounts.has(req.path)) requestCounts.set(req.path, new Map());
      const byStatus = requestCounts.get(req.path)!;
      byStatus.set(statusClass, (byStatus.get(statusClass) ?? 0) + 1);
    });
    console.error(`[${new Date().toISOString()}] ${req.method} ${req.path} - Headers: ${JSON.stringify(req.headers)}`);
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
      console.error(`[DEBUG] GET /mcp: session ${clientSessionId} not found — returning 404`);
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
      console.error(`[DEBUG] GET /mcp: SSE registered for session ${clientSessionId}`);
    }

    // Flush any responses buffered while GET was unavailable.
    const pending = pendingResponses.get(clientSessionId);
    if (pending?.length) {
      console.error(`[FALLBACK] Flushing ${pending.length} buffered response(s) to new GET SSE`);
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
        // No session ID provided, or stale session on initialize — create new transport.
        // A fresh Server instance is created for each session: the MCP SDK stores a
        // single _transport on the Server and overwrites it on every connect(), so a
        // shared Server causes responses for session A to be dispatched through session
        // B's transport after B connects.
        console.error('[DEBUG] No active transport found. Creating new transport.');
        const newGeneratedSessionId = randomUUID();
        const sessionServer = createMcpServer();

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

        // Patch send() to fall back to GET SSE when POST closes early.
        patchTransportSend(newTransportInstance, pendingResponses);

        transport = newTransportInstance;
        streamableTransports[newGeneratedSessionId] = transport;
        streamableServers[newGeneratedSessionId] = sessionServer;

        console.error(`[DEBUG] Connecting new transport (ID: ${newGeneratedSessionId}) to session server`);
        await sessionServer.connect(transport);
        console.error(`[DEBUG] New transport connected`);

        // Re-install cleanup AFTER connect(): the SDK's connect() overwrites
        // transport.onclose to clear its own _transport reference.  We chain our
        // cleanup so both the SDK and the HTTP layer tidy up correctly.
        const sdkOnClose = transport.onclose;
        transport.onclose = async () => {
          await sdkOnClose?.();
          const closedSessionId = transport?.sessionId || newGeneratedSessionId;
          console.error(`[DEBUG] Transport for session ${closedSessionId} closed. Removing.`);
          delete streamableTransports[closedSessionId];
          delete streamableServers[closedSessionId];
          pendingResponses.delete(closedSessionId);
          sessionServer.close().catch((err: Error) =>
            console.error(`[DEBUG] Error closing session server ${closedSessionId}:`, err)
          );
        };
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
      const sseSessionServer = createMcpServer();

      console.error(`[DEBUG] Created SSE transport with session ID: ${sessionId}`);
      sseTransports[sessionId] = transport;
      sseServers[sessionId] = sseSessionServer;

      const keepalive = setInterval(() => {
        if (!res.writableEnded) res.write(': ping\n\n');
      }, 30000);

      res.on('close', () => {
        console.error(`[DEBUG] SSE connection closed for session ID: ${sessionId}`);
        clearInterval(keepalive);
        delete sseTransports[sessionId];
        delete sseServers[sessionId];
        sseSessionServer.close().catch((err: Error) =>
          console.error(`[DEBUG] Error closing SSE session server ${sessionId}:`, err)
        );
      });

      res.on('error', (err) => {
        console.error(`[ERROR] SSE connection error for session ID: ${sessionId}:`, err);
        clearInterval(keepalive);
        delete sseTransports[sessionId];
        delete sseServers[sessionId];
      });

      await sseSessionServer.connect(transport);
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
